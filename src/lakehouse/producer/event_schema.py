# src/lakehouse/producer/event_schema.py
"""
Single source of truth for the product event schema.
Every layer (Bronze, Silver, Gold) imports from here.
If the schema changes, change it in ONE place only.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json
import uuid


@dataclass
class ProductEvent:
    """
    Represents one product interaction event.

    A dataclass is like a regular class but Python auto-generates
    __init__, __repr__, and __eq__ methods for you.
    You just define the fields and their types.
    """
    event_id:   str    # unique ID for this event (UUID)
    product_id: str    # which product (p001 to p100)
    user_id:    str    # which user (u0001 to u0500)
    event_type: str    # "view", "add_to_cart", or "purchase"
    quantity:   int    # how many units
    price:      float  # price per unit in USD
    ts:         str    # ISO 8601 timestamp

    def to_json(self) -> str:
        """
        Converts this event to a JSON string for sending to Kafka.
        Kafka messages are bytes — we serialize to JSON string first.
        """
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str: str) -> "ProductEvent":
        """
        Rebuilds a ProductEvent from a JSON string.
        Used by the consumer (Bronze layer) when reading from Kafka.
        """
        data = json.loads(json_str)
        return ProductEvent(**data)


# Valid event types — used for validation and generation
VALID_EVENT_TYPES = ["view", "add_to_cart", "purchase"]

# Price range per event type (realistic e-commerce pricing)
PRICE_RANGES = {
    "view":        (9.99,  199.99),
    "add_to_cart": (9.99,  199.99),
    "purchase":    (9.99,  499.99),
}