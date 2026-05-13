# src/lakehouse/producer/producer.py
"""
Kafka Producer for the 100M+ Event Lakehouse.

Generates synthetic product events and publishes them to Kafka.
Deliberately injects data skew (80/20 rule) to simulate viral products.

Local:  produces to Docker Kafka at localhost:9092
Cloud:  produces to Amazon MSK (Phase 7)
"""

import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

from lakehouse.producer.event_schema import (
    ProductEvent,
    VALID_EVENT_TYPES,
    PRICE_RANGES,
)
from lakehouse.utils.config_loader import load_config

# Seed random for reproducibility during testing
# Remove seed in production for true randomness
random.seed(42)
fake = Faker()
Faker.seed(42)


def build_product_catalog(config: dict) -> dict:
    """
    Builds two lists of product IDs:
    - viral_products: the 1% that get 80% of traffic
    - normal_products: the remaining 99%

    Returns a dict with both lists for easy access.
    """
    total    = config["producer"]["total_products"]
    viral_n  = config["producer"]["viral_product_count"]

    # Generate ALL product IDs: p0001, p0002, ... p0100
    all_products = [f"p{str(i).zfill(4)}" for i in range(1, total + 1)]

    # First N are "viral", rest are normal
    viral_products  = all_products[:viral_n]
    normal_products = all_products[viral_n:]

    print(f"[Catalog] Total products : {len(all_products)}")
    print(f"[Catalog] Viral products : {viral_products}")
    print(f"[Catalog] Normal products: {len(normal_products)}")

    return {
        "viral":  viral_products,
        "normal": normal_products,
        "all":    all_products,
    }


def pick_product_with_skew(catalog: dict, skew_ratio: float) -> str:
    """
    Picks a product ID with deliberate skew.

    skew_ratio = 0.80 means:
    - 80% chance  → pick from viral_products  (small list)
    - 20% chance  → pick from normal_products (large list)

    This creates massive partition imbalance in Spark later,
    which is exactly what we want to test our salting logic.

    Args:
        catalog:    the product catalog dict from build_product_catalog()
        skew_ratio: float between 0 and 1 (0.80 = 80% skew)

    Returns:
        A product ID string like "p0001"
    """
    if random.random() < skew_ratio:
        return random.choice(catalog["viral"])   # 80% of the time
    else:
        return random.choice(catalog["normal"])  # 20% of the time


def generate_event(catalog: dict, config: dict) -> ProductEvent:
    """
    Generates one synthetic ProductEvent.

    Args:
        catalog: product catalog with viral/normal lists
        config:  loaded config dict

    Returns:
        A ProductEvent dataclass instance
    """
    skew_ratio  = config["producer"]["skew_ratio"]
    total_users = config["producer"]["total_users"]

    product_id  = pick_product_with_skew(catalog, skew_ratio)
    event_type  = random.choice(VALID_EVENT_TYPES)
    price_min, price_max = PRICE_RANGES[event_type]

    return ProductEvent(
        event_id   = str(uuid.uuid4()),
        product_id = product_id,
        user_id    = f"u{str(random.randint(1, total_users)).zfill(4)}",
        event_type = event_type,
        quantity   = random.randint(1, 10),
        price      = round(random.uniform(price_min, price_max), 2),
        ts         = datetime.now(timezone.utc).isoformat(),
    )


def create_kafka_producer(config: dict) -> KafkaProducer:
    """
    Creates and returns a KafkaProducer connected to the
    broker specified in config (localhost for local, MSK for cloud).

    KafkaProducer config explained:
    - bootstrap_servers: the Kafka broker address
    - value_serializer:  converts our Python string to bytes
                         (Kafka only accepts bytes, not strings)
    - acks='all':        wait for ALL replicas to confirm
                         before considering message delivered
                         (strongest durability guarantee)
    - retries=3:         retry up to 3 times on transient errors
    - linger_ms=10:      wait 10ms to batch messages together
                         (better throughput than sending one by one)
    """
    servers = config["kafka"]["bootstrap_servers"]
    print(f"[Kafka] Connecting to: {servers}")

    producer = KafkaProducer(
        bootstrap_servers = servers,
        value_serializer  = lambda v: v.encode("utf-8"),
        acks              = "all",
        retries           = 3,
        linger_ms         = 10,
        batch_size        = 16384,  # 16KB batch buffer
    )

    print("[Kafka] Connected successfully")
    return producer


def run_producer():
    """
    Main entry point. Loads config, builds catalog,
    generates events, sends to Kafka.

    Prints progress every 1000 events so you can watch it run.
    """
    config  = load_config()
    topic   = config["kafka"]["topic"]
    total   = config["producer"]["total_events"]
    batch   = config["producer"]["batch_size"]

    catalog  = build_product_catalog(config)
    producer = create_kafka_producer(config)

    print(f"\n[Producer] Starting — {total:,} events → topic '{topic}'")
    print(f"[Producer] Skew ratio: {config['producer']['skew_ratio']*100:.0f}% "
          f"viral / {(1-config['producer']['skew_ratio'])*100:.0f}% normal\n")

    start_time  = time.time()
    sent_count  = 0
    error_count = 0

    # Track skew distribution for verification
    viral_count  = 0
    normal_count = 0
    viral_ids    = set(catalog["viral"])

    for i in range(total):
        event = generate_event(catalog, config)

        # Track distribution
        if event.product_id in viral_ids:
            viral_count += 1
        else:
            normal_count += 1

        # Send to Kafka
        # key=product_id ensures all events for same product
        # go to the SAME Kafka partition (important for ordering)
        producer.send(
            topic,
            key   = event.product_id.encode("utf-8"),
            value = event.to_json(),
        )

        sent_count += 1

        # Progress update every 1000 events
        if sent_count % 1000 == 0:
            elapsed  = time.time() - start_time
            rate     = sent_count / elapsed
            pct_done = (sent_count / total) * 100
            print(f"  [{pct_done:5.1f}%] {sent_count:,} sent | "
                  f"{rate:,.0f} events/sec | "
                  f"{error_count} errors")

    # Flush ensures all buffered messages are sent before exit
    print("\n[Producer] Flushing remaining messages...")
    producer.flush()
    producer.close()

    # Final summary
    elapsed    = time.time() - start_time
    total_rate = total / elapsed
    actual_skew = viral_count / total * 100

    print(f"\n{'='*55}")
    print(f"  PRODUCER COMPLETE")
    print(f"{'='*55}")
    print(f"  Total sent    : {sent_count:,}")
    print(f"  Total errors  : {error_count}")
    print(f"  Time elapsed  : {elapsed:.1f}s")
    print(f"  Avg rate      : {total_rate:,.0f} events/sec")
    print(f"  Viral %       : {actual_skew:.1f}% "
          f"(target: {config['producer']['skew_ratio']*100:.0f}%)")
    print(f"  Normal %      : {100-actual_skew:.1f}%")
    print(f"{'='*55}\n")


# This block only runs when you execute this file directly:
# python producer.py
# It does NOT run when another file imports from this file.
if __name__ == "__main__":
    run_producer()