# tests/test_producer.py
"""
Unit tests for the Kafka producer.

These tests run WITHOUT a live Kafka connection — we test
the logic (event generation, skew, serialization) in isolation.
Testing against a live Kafka broker belongs in integration tests
(added in Phase 3).

Key principle: unit tests should be FAST and have NO external
dependencies (no Kafka, no Spark, no MinIO, no network calls).
"""

import json
import pytest
import random

from lakehouse.producer.event_schema import (
    ProductEvent,
    VALID_EVENT_TYPES,
    PRICE_RANGES,
)
from lakehouse.producer.producer import (
    build_product_catalog,
    pick_product_with_skew,
    generate_event,
)


# ─────────────────────────────────────────────
# Fixtures — reusable test setup
# A fixture is a function that returns something
# your tests need. pytest injects it automatically
# when you put it as a parameter in a test function.
# ─────────────────────────────────────────────

@pytest.fixture
def sample_config():
    """
    Returns a minimal config dict for testing.
    We don't load from local.yml here — tests must
    never depend on files outside the tests/ folder.
    """
    return {
        "producer": {
            "total_events":        100,
            "batch_size":          10,
            "skew_ratio":          0.80,
            "viral_product_count": 10,
            "total_products":      100,
            "total_users":         500,
        }
    }


@pytest.fixture
def sample_catalog(sample_config):
    """
    Returns a product catalog built from sample_config.
    Depends on sample_config fixture — pytest resolves
    the dependency chain automatically.
    """
    return build_product_catalog(sample_config)


# ─────────────────────────────────────────────
# Tests: ProductEvent serialization
# ─────────────────────────────────────────────

class TestProductEventSerialization:
    """Tests that events serialize and deserialize correctly."""

    def test_to_json_returns_string(self):
        """to_json() must return a string, not a dict or bytes."""
        event = ProductEvent(
            event_id   = "test-id-001",
            product_id = "p0001",
            user_id    = "u0001",
            event_type = "purchase",
            quantity   = 2,
            price      = 29.99,
            ts         = "2026-05-12T10:00:00+00:00",
        )
        result = event.to_json()
        assert isinstance(result, str), \
            f"Expected str, got {type(result)}"

    def test_to_json_contains_all_fields(self):
        """Every field must appear in the JSON output."""
        event = ProductEvent(
            event_id   = "test-id-001",
            product_id = "p0001",
            user_id    = "u0001",
            event_type = "view",
            quantity   = 1,
            price      = 9.99,
            ts         = "2026-05-12T10:00:00+00:00",
        )
        parsed = json.loads(event.to_json())

        assert parsed["event_id"]   == "test-id-001"
        assert parsed["product_id"] == "p0001"
        assert parsed["user_id"]    == "u0001"
        assert parsed["event_type"] == "view"
        assert parsed["quantity"]   == 1
        assert parsed["price"]      == 9.99

    def test_from_json_roundtrip(self):
        """
        Roundtrip test: event → JSON string → event
        The result must equal the original.
        This is the most important serialization test.
        """
        original = ProductEvent(
            event_id   = "test-id-002",
            product_id = "p0005",
            user_id    = "u0042",
            event_type = "add_to_cart",
            quantity   = 3,
            price      = 49.99,
            ts         = "2026-05-12T10:00:00+00:00",
        )
        json_str     = original.to_json()
        reconstructed = ProductEvent.from_json(json_str)

        assert reconstructed == original, \
            f"Roundtrip failed.\nOriginal:      {original}\nReconstructed: {reconstructed}"

    def test_to_json_is_valid_json(self):
        """The output of to_json() must be parseable by json.loads()."""
        event = ProductEvent(
            event_id="x", product_id="p0001", user_id="u0001",
            event_type="view", quantity=1, price=9.99,
            ts="2026-05-12T10:00:00+00:00",
        )
        try:
            json.loads(event.to_json())
        except json.JSONDecodeError as e:
            pytest.fail(f"to_json() produced invalid JSON: {e}")


# ─────────────────────────────────────────────
# Tests: Product catalog building
# ─────────────────────────────────────────────

class TestBuildProductCatalog:
    """Tests that the product catalog splits correctly."""

    def test_total_product_count(self, sample_config):
        """Total products must match config."""
        catalog = build_product_catalog(sample_config)
        total = len(catalog["all"])
        assert total == 100, f"Expected 100 products, got {total}"

    def test_viral_product_count(self, sample_config):
        """Viral product count must match config."""
        catalog = build_product_catalog(sample_config)
        viral_count = len(catalog["viral"])
        assert viral_count == 10, \
            f"Expected 10 viral products, got {viral_count}"

    def test_normal_product_count(self, sample_config):
        """Normal products = total - viral."""
        catalog = build_product_catalog(sample_config)
        normal_count = len(catalog["normal"])
        assert normal_count == 90, \
            f"Expected 90 normal products, got {normal_count}"

    def test_viral_and_normal_no_overlap(self, sample_config):
        """A product cannot be both viral and normal."""
        catalog = build_product_catalog(sample_config)
        viral_set  = set(catalog["viral"])
        normal_set = set(catalog["normal"])
        overlap    = viral_set & normal_set

        assert len(overlap) == 0, \
            f"Products in both viral and normal: {overlap}"

    def test_product_id_format(self, sample_config):
        """Product IDs must follow the p0001 format."""
        catalog = build_product_catalog(sample_config)
        for pid in catalog["all"]:
            assert pid.startswith("p"), \
                f"Product ID should start with 'p': {pid}"
            assert len(pid) == 5, \
                f"Product ID should be 5 chars (p0001): {pid}"


# ─────────────────────────────────────────────
# Tests: Skew injection
# ─────────────────────────────────────────────

class TestSkewInjection:
    """
    Tests that the skew ratio produces the right distribution.

    We use a large sample (10,000 picks) and allow a 5%
    tolerance band around the target. This is called
    statistical testing — we can't test randomness exactly,
    but we can test it's within acceptable bounds.
    """

    def test_skew_ratio_within_tolerance(self, sample_catalog):
        """80% of picks should come from viral products (±5%)."""
        random.seed(42)   # fix seed for reproducibility
        skew_ratio  = 0.80
        sample_size = 10_000
        viral_ids   = set(sample_catalog["viral"])

        picks = [
            pick_product_with_skew(sample_catalog, skew_ratio)
            for _ in range(sample_size)
        ]

        viral_count    = sum(1 for p in picks if p in viral_ids)
        actual_ratio   = viral_count / sample_size
        tolerance      = 0.05   # allow ±5%

        assert abs(actual_ratio - skew_ratio) < tolerance, \
            f"Skew ratio {actual_ratio:.2f} is outside " \
            f"expected range [{skew_ratio-tolerance:.2f}, " \
            f"{skew_ratio+tolerance:.2f}]"

    def test_only_valid_products_returned(self, sample_catalog):
        """pick_product_with_skew must only return known product IDs."""
        random.seed(42)
        all_ids = set(sample_catalog["all"])

        for _ in range(1000):
            pid = pick_product_with_skew(sample_catalog, 0.80)
            assert pid in all_ids, \
                f"Unknown product ID returned: {pid}"

    def test_zero_skew_returns_only_normal(self, sample_catalog):
        """With skew_ratio=0, all picks should be normal products."""
        random.seed(42)
        normal_ids = set(sample_catalog["normal"])

        for _ in range(100):
            pid = pick_product_with_skew(sample_catalog, skew_ratio=0.0)
            assert pid in normal_ids, \
                f"Expected normal product, got: {pid}"

    def test_full_skew_returns_only_viral(self, sample_catalog):
        """With skew_ratio=1, all picks should be viral products."""
        random.seed(42)
        viral_ids = set(sample_catalog["viral"])

        for _ in range(100):
            pid = pick_product_with_skew(sample_catalog, skew_ratio=1.0)
            assert pid in viral_ids, \
                f"Expected viral product, got: {pid}"


# ─────────────────────────────────────────────
# Tests: Event generation
# ─────────────────────────────────────────────

class TestGenerateEvent:
    """Tests that generated events have valid field values."""

    def test_event_type_is_valid(self, sample_catalog, sample_config):
        """event_type must be one of: view, add_to_cart, purchase."""
        random.seed(42)
        for _ in range(100):
            event = generate_event(sample_catalog, sample_config)
            assert event.event_type in VALID_EVENT_TYPES, \
                f"Invalid event_type: {event.event_type}"

    def test_quantity_is_positive(self, sample_catalog, sample_config):
        """Quantity must be between 1 and 10."""
        random.seed(42)
        for _ in range(100):
            event = generate_event(sample_catalog, sample_config)
            assert 1 <= event.quantity <= 10, \
                f"Invalid quantity: {event.quantity}"

    def test_price_is_positive(self, sample_catalog, sample_config):
        """Price must be greater than 0."""
        random.seed(42)
        for _ in range(100):
            event = generate_event(sample_catalog, sample_config)
            assert event.price > 0, \
                f"Price must be positive, got: {event.price}"

    def test_event_id_is_unique(self, sample_catalog, sample_config):
        """Every event must have a unique event_id (UUID)."""
        random.seed(42)
        ids = [
            generate_event(sample_catalog, sample_config).event_id
            for _ in range(100)
        ]
        assert len(ids) == len(set(ids)), \
            "Duplicate event_ids detected — UUIDs must be unique"

    def test_product_id_is_from_catalog(self, sample_catalog, sample_config):
        """product_id must come from the known catalog."""
        random.seed(42)
        all_ids = set(sample_catalog["all"])
        for _ in range(100):
            event = generate_event(sample_catalog, sample_config)
            assert event.product_id in all_ids, \
                f"Unknown product_id: {event.product_id}"

    def test_ts_is_iso_format(self, sample_catalog, sample_config):
        """Timestamp must be a non-empty ISO 8601 string."""
        event = generate_event(sample_catalog, sample_config)
        assert isinstance(event.ts, str), "ts must be a string"
        assert len(event.ts) > 0, "ts must not be empty"
        assert "T" in event.ts, \
            f"ts doesn't look like ISO 8601: {event.ts}"