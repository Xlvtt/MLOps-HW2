# HW2: Fraud Detection + ClickHouse

Проект отправляет транзакции из CSV в Kafka. ClickHouse читает топик `transactions`, сохраняет записи и поддерживает агрегат максимальной суммы по категории и штату. Исходный микросервис fraud detection также использует этот топик.

## Запуск

Нужны Docker Engine и Docker Compose v2. Тестовые данные находятся в `data/sample_transactions.csv`. Для полного прогона скачайте датасет задания и сохраните его как `data/transactions.csv`.

```bash
docker compose up --build -d
bash scripts/run_sample_test.sh
```

Скрипт отправит тестовый CSV, дождётся загрузки и сохранит результат в `results/category_of_largest_transaction.csv`.

Для полного датасета загрузите CSV в Kafka:

```bash
docker compose --profile tools run --build --rm csv-producer \
  --file /app/data/transactions.csv --topic transactions --brokers kafka:9092
```

Выгрузить результат запроса:

```bash
docker compose exec -T clickhouse clickhouse-client \
  --query "$(cat sql/queries/category_of_largest_transaction.sql)" \
  --format CSVWithNames > results/category_of_largest_transaction.csv
```

## Структура

- `scripts/produce.py` — CSV → Kafka; `scripts/run_sample_test.sh` — полный тест.
- `sql/00_baseline.sql` — базовая схема; `sql/01_optimized.sql` — оптимизированная схема, используемая Compose.
- `sql/queries/category_of_largest_transaction.sql` — запрос результата.
- `results/sample_result.csv` — пример результата для тестового CSV.

Оптимизированная схема использует `LowCardinality`, `Decimal`, codecs, месячное партиционирование и `AggregatingMergeTree` с материализованным агрегатом.

Интерфейсы: Streamlit — http://localhost:8501, Kafka UI — http://localhost:8080, ClickHouse HTTP — http://localhost:8123.
