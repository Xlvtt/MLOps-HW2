#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CSV_CONTAINER_PATH="/app/data/sample_transactions.csv"
EXPECTED_ROWS="$(python3 -c 'import csv; from pathlib import Path; p=Path("data/sample_transactions.csv"); print(sum(1 for _ in csv.DictReader(p.open(newline="", encoding="utf-8-sig"))))')"

BEFORE="$(docker compose exec -T clickhouse clickhouse-client --query 'SELECT count() FROM hw2.transactions' | tr -d '\r')"
echo "Rows before test: $BEFORE"

docker compose --profile tools run --build --rm csv-producer \
  --file "$CSV_CONTAINER_PATH" --topic transactions --brokers kafka:9092

TARGET=$((BEFORE + EXPECTED_ROWS))
for attempt in $(seq 1 60); do
  CURRENT="$(docker compose exec -T clickhouse clickhouse-client --query 'SELECT count() FROM hw2.transactions' | tr -d '\r')"
  if [[ "$CURRENT" -ge "$TARGET" ]]; then
    break
  fi
  sleep 1
done

if [[ "$CURRENT" -lt "$TARGET" ]]; then
  echo "Timed out waiting for ClickHouse: expected at least $TARGET rows, got $CURRENT" >&2
  exit 1
fi

mkdir -p results
docker compose exec -T clickhouse clickhouse-client \
  --query "$(cat sql/queries/category_of_largest_transaction.sql)" \
  --format CSVWithNames > results/category_of_largest_transaction.csv

echo "Rows after test: $CURRENT"
echo "Query output: results/category_of_largest_transaction.csv"
cat results/category_of_largest_transaction.csv
