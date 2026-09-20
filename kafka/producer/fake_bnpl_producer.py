"""Continuously emit realistic label-free BNPL application events to Kafka."""

from __future__ import annotations

import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "bnpl.transactions.raw")
EVENTS_PER_SECOND = max(float(os.getenv("FAKE_EVENTS_PER_SECOND", "2")), 0.01)
INVALID_RATE = min(max(float(os.getenv("FAKE_INVALID_RATE", "0.02")), 0.0), 1.0)
RANDOM_SEED = int(os.getenv("FAKE_RANDOM_SEED", "42"))

MERCHANTS = {
    "electronics": "Electronics Store",
    "fashion": "Fashion Store",
    "furniture": "Furniture Store",
    "groceries": "Groceries Store",
    "other": "General Store",
}
STATES = ["Lagos", "Abuja (FCT)", "Kano", "Kaduna", "Ogun", "Rivers", "Oyo", "Enugu"]
PROVIDERS = ["FairMoney", "Branch", "Carbon", "Renmoney", "PayLater"]


def generate_event(rng: random.Random, sequence: int) -> dict:
    category = rng.choice(list(MERCHANTS))
    tenor_days = rng.choice([14, 30, 60, 90])
    event = {
        "transaction_id": f"TX_STREAM_{sequence:012d}_{uuid.uuid4().hex[:8]}",
        "purchase_date": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"CUS-{rng.randint(1, 9_999_999):08d}",
        "merchant_name": f"{MERCHANTS[category]} {rng.randint(1, 999)}",
        "merchant_category": category,
        "customer_state": rng.choice(STATES),
        "principal_ngn": round(rng.uniform(5_000, 500_000), 2),
        "interest_rate_monthly": round(rng.uniform(0, 0.05), 6),
        "tenor_days": tenor_days,
        "num_installments": max(1, tenor_days // 30),
        "provider": rng.choice(PROVIDERS),
        "credit_score": rng.randint(300, 850),
        "first_time_customer": rng.choice([True, False]),
    }
    if rng.random() < INVALID_RATE:
        rng.choice(
            [
                lambda: event.update(credit_score=999),
                lambda: event.update(principal_ngn=-1),
                lambda: event.update(purchase_date="not-a-date"),
                lambda: event.pop("transaction_id"),
            ]
        )()
    return event


def main() -> None:
    running = True

    def stop(*_args):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    rng = random.Random(RANDOM_SEED)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        linger_ms=50,
    )
    sequence = 1
    interval = 1.0 / EVENTS_PER_SECOND
    try:
        while running:
            event = generate_event(rng, sequence)
            producer.send(TOPIC, key=event.get("transaction_id", f"invalid-{sequence}"), value=event)
            if sequence % 100 == 0:
                producer.flush()
                print(f"topic={TOPIC} published={sequence}", flush=True)
            sequence += 1
            time.sleep(interval)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
