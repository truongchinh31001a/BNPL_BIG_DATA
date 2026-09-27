"""Continuously emit realistic label-free BNPL application events to Kafka."""

from __future__ import annotations

import json
import math
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
STATES = [
    "Abia", "Adamawa", "Akwa Ibom", "Anambra", "Bauchi", "Bayelsa", "Benue",
    "Borno", "Cross River", "Delta", "Ebonyi", "Edo", "Ekiti", "Enugu",
    "Abuja (FCT)", "Gombe", "Imo", "Jigawa", "Kaduna", "Kano", "Katsina",
    "Kebbi", "Kogi", "Kwara", "Lagos", "Nasarawa", "Niger", "Ogun", "Ondo",
    "Osun", "Oyo", "Plateau", "Rivers", "Sokoto", "Taraba", "Yobe", "Zamfara",
]
STATE_WEIGHTS = [1] * len(STATES)
STATE_WEIGHTS[STATES.index("Lagos")] = 7
STATE_WEIGHTS[STATES.index("Abuja (FCT)")] = 4
STATE_WEIGHTS[STATES.index("Kano")] = 3
STATE_WEIGHTS[STATES.index("Rivers")] = 3
PROVIDERS = ["FairMoney", "Branch", "Carbon", "Renmoney", "PayLater"]
PROVIDER_WEIGHTS = [0.299, 0.249, 0.201, 0.151, 0.100]
CATEGORY_WEIGHTS = [0.349, 0.299, 0.201, 0.101, 0.050]
TENORS = [14, 30, 60, 90]
TENOR_WEIGHTS = [0.20, 0.45, 0.25, 0.10]


def _bounded_lognormal(rng: random.Random, median: float, p95: float) -> float:
    sigma = (math.log(p95) - math.log(median)) / 1.645
    return min(max(rng.lognormvariate(math.log(median), sigma), 5_000), 500_000)


def _bounded_credit_score(rng: random.Random) -> int:
    return min(max(round(rng.gauss(620, 115)), 300), 850)


def generate_event(rng: random.Random, sequence: int) -> dict:
    category = rng.choices(list(MERCHANTS), weights=CATEGORY_WEIGHTS, k=1)[0]
    tenor_days = rng.choices(TENORS, weights=TENOR_WEIGHTS, k=1)[0]
    event = {
        "transaction_id": f"TX_STREAM_{sequence:012d}_{uuid.uuid4().hex[:8]}",
        "purchase_date": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"CUS-{rng.randint(1, 9_999_999):08d}",
        "merchant_name": f"{MERCHANTS[category]} {rng.randint(1, 999)}",
        "merchant_category": category,
        "customer_state": rng.choices(STATES, weights=STATE_WEIGHTS, k=1)[0],
        "principal_ngn": round(_bounded_lognormal(rng, median=36_214, p95=136_106), 2),
        "interest_rate_monthly": round(rng.triangular(0, 0.05, 0.03), 6),
        "tenor_days": tenor_days,
        "num_installments": max(1, tenor_days // 30),
        "provider": rng.choices(PROVIDERS, weights=PROVIDER_WEIGHTS, k=1)[0],
        "credit_score": _bounded_credit_score(rng),
        "first_time_customer": rng.random() < 0.399,
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
