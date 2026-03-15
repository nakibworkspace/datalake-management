"""
Step 1 — Ingest: Generate e-commerce order events and land raw JSON in S3.

Simulates a real-world ingestion pipeline where events arrive from an
upstream system (API gateway, Kafka, etc.) and are dumped as raw JSON
into the data lake's landing zone.

Usage:
    python scripts/ingest.py              # default 500 events
    python scripts/ingest.py --count 2000 # custom count
"""

from __future__ import annotations

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta

import boto3


# ── Realistic seed data ─────────────────────────────────────────────
PRODUCTS = [
    {"id": "P001", "name": "Wireless Mouse",       "category": "Electronics",  "price": 29.99},
    {"id": "P002", "name": "USB-C Hub",             "category": "Electronics",  "price": 49.99},
    {"id": "P003", "name": "Standing Desk Mat",     "category": "Office",       "price": 39.99},
    {"id": "P004", "name": "Noise Cancelling Buds", "category": "Electronics",  "price": 79.99},
    {"id": "P005", "name": "Mechanical Keyboard",   "category": "Electronics",  "price": 119.99},
    {"id": "P006", "name": "Monitor Arm",           "category": "Office",       "price": 89.99},
    {"id": "P007", "name": "Webcam HD",             "category": "Electronics",  "price": 59.99},
    {"id": "P008", "name": "Desk Lamp LED",         "category": "Office",       "price": 34.99},
    {"id": "P009", "name": "Laptop Stand",          "category": "Office",       "price": 44.99},
    {"id": "P010", "name": "Cable Management Kit",  "category": "Accessories",  "price": 14.99},
    {"id": "P011", "name": "Portable Charger",      "category": "Accessories",  "price": 24.99},
    {"id": "P012", "name": "Screen Protector",      "category": "Accessories",  "price": 9.99},
]

CITIES = [
    ("New York",   "US"), ("Los Angeles",  "US"), ("Chicago",    "US"),
    ("London",     "UK"), ("Manchester",   "UK"), ("Berlin",     "DE"),
    ("Munich",     "DE"), ("Toronto",      "CA"), ("Vancouver",  "CA"),
    ("Sydney",     "AU"),
]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "bank_transfer"]
ORDER_STATUSES  = ["completed", "completed", "completed", "pending", "cancelled", "refunded"]


def generate_event(base_time: datetime) -> dict:
    """Generate a single realistic order event."""
    product  = random.choice(PRODUCTS)
    city, country = random.choice(CITIES)
    quantity = random.randint(1, 5)

    # Offset event time randomly within the past 30 days
    offset_seconds = random.randint(0, 30 * 24 * 3600)
    event_time = base_time - timedelta(seconds=offset_seconds)

    return {
        "order_id":       str(uuid.uuid4()),
        "customer_id":    f"C{random.randint(1000, 9999)}",
        "product_id":     product["id"],
        "product_name":   product["name"],
        "category":       product["category"],
        "quantity":       quantity,
        "unit_price":     product["price"],
        "total_amount":   round(product["price"] * quantity, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status":   random.choice(ORDER_STATUSES),
        "shipping_city":  city,
        "shipping_country": country,
        "event_timestamp": event_time.isoformat(),
    }


def upload_to_s3(s3_client, bucket: str, events: list[dict], batch_id: str):
    """Upload a batch of events as newline-delimited JSON to S3."""
    # Partition by date for the first event in the batch (standard practice)
    first_date = events[0]["event_timestamp"][:10]  # YYYY-MM-DD
    key = f"orders/date={first_date}/batch-{batch_id}.json"

    body = "\n".join(json.dumps(e) for e in events)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    return key


def main():
    parser = argparse.ArgumentParser(description="Ingest e-commerce order events into S3")
    parser.add_argument("--count", type=int, default=int(os.getenv("BATCH_SIZE", "500")),
                        help="Number of events to generate")
    args = parser.parse_args()

    bucket = os.environ["S3_BUCKET"]

    s3 = boto3.client("s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    print(f"Generating {args.count} order events ...")
    base_time = datetime.utcnow()
    events = [generate_event(base_time) for _ in range(args.count)]

    # Group events by date for partitioned uploads
    by_date: dict[str, list[dict]] = {}
    for evt in events:
        dt = evt["event_timestamp"][:10]
        by_date.setdefault(dt, []).append(evt)

    total_keys = 0
    for date_str, date_events in sorted(by_date.items()):
        batch_id = uuid.uuid4().hex[:8]
        key = upload_to_s3(s3, bucket, date_events, batch_id)
        total_keys += 1
        print(f"  -> s3://{bucket}/{key}  ({len(date_events)} events)")

    print(f"\nDone. Uploaded {args.count} events across {total_keys} files.")
    print(f"Landing zone: s3://{bucket}/orders/")


if __name__ == "__main__":
    main()
