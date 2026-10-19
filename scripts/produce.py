#!/usr/bin/env python3
"""Publish CSV rows in the same envelope used by the fraud detection service."""

import argparse
import csv
import json
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import uuid4

from kafka import KafkaProducer
from kafka.errors import KafkaError


MODEL_FIELDS = {
    "transaction_time": ("transaction_time", "trans_date_trans_time"),
    "gender": ("gender",),
    "merch": ("merch", "merchant"),
    "cat_id": ("cat_id", "category"),
    "one_city": ("one_city", "city"),
    "us_state": ("us_state", "state"),
    "jobs": ("jobs", "job"),
    "amount": ("amount", "amt"),
    "population_city": ("population_city", "city_pop"),
    "lat": ("lat",),
    "lon": ("lon", "long"),
    "merchant_lat": ("merchant_lat", "merch_lat"),
    "merchant_lon": ("merchant_lon", "merch_long"),
}


def find_input_column(headers: set[str], candidates: tuple[str, ...]) -> str | None:
    return next((candidate for candidate in candidates if candidate in headers), None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, type=Path, help="Input CSV file")
    parser.add_argument("--topic", default="transactions", help="Kafka topic")
    parser.add_argument("--brokers", default="localhost:9095", help="Kafka bootstrap servers")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to publish")
    parser.add_argument("--retries", type=int, default=20, help="Connection attempts before failing")
    parser.add_argument("--retry-delay", type=float, default=3, help="Delay between connection attempts")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.file.is_file():
        print(f"CSV not found: {args.file}", file=sys.stderr)
        return 2
    if args.limit is not None and args.limit < 1:
        print("--limit must be positive", file=sys.stderr)
        return 2

    producer = None
    for attempt in range(1, args.retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.brokers.split(","),
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=5,
                max_block_ms=30000,
            )
            break
        except KafkaError as exc:
            if attempt == args.retries:
                print(f"Could not connect to Kafka at {args.brokers}: {exc}", file=sys.stderr)
                return 1
            print(f"Kafka is not ready (attempt {attempt}/{args.retries}); retrying...", file=sys.stderr)
            time.sleep(args.retry_delay)

    sent = 0
    try:
        with args.file.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if not reader.fieldnames:
                raise ValueError("CSV is missing a header row")
            columns = {name.strip(): name for name in reader.fieldnames if name}
            mapping = {
                model_name: find_input_column(set(columns), alternatives)
                for model_name, alternatives in MODEL_FIELDS.items()
            }
            missing = [name for name, source_column in mapping.items() if source_column is None]
            if missing:
                details = ", ".join(f"{name} (or {MODEL_FIELDS[name][-1]})" for name in missing)
                raise ValueError(f"CSV is missing columns required by the model: {details}")

            for line_number, row in enumerate(reader, start=2):
                if args.limit is not None and sent >= args.limit:
                    break
                try:
                    amount = Decimal(row[mapping["amount"]].strip())
                    if not amount.is_finite() or amount < 0:
                        raise InvalidOperation
                    transaction = dict(row)
                    for model_name, source_column in mapping.items():
                        transaction[model_name] = row[source_column].strip()
                    transaction["amount"] = format(amount.quantize(Decimal("0.01")), "f")
                    transaction["us_state"] = transaction["us_state"].upper()
                    if not transaction["cat_id"] or not transaction["us_state"]:
                        raise ValueError("category and state cannot be empty")
                    producer.send(
                        args.topic,
                        {"transaction_id": str(uuid4()), "data": transaction},
                    )
                    sent += 1
                except (ValueError, InvalidOperation, TypeError) as exc:
                    raise ValueError(f"invalid CSV row at line {line_number}: {exc}") from exc
        producer.flush()
    except Exception as exc:
        print(f"Failed to publish CSV: {exc}", file=sys.stderr)
        return 1
    finally:
        producer.close()

    print(f"Published {sent} rows to topic {args.topic!r} at {args.brokers}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

