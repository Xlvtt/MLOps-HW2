-- Baseline DDL for comparison. Apply instead of 01_optimized.sql on a fresh volume.
CREATE DATABASE IF NOT EXISTS hw2;

CREATE TABLE IF NOT EXISTS hw2.transactions_kafka
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions_baseline',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS hw2.transactions
(
    trans_date DateTime,
    category String,
    amount Float64,
    state String
)
ENGINE = MergeTree
ORDER BY trans_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.transactions_from_kafka
TO hw2.transactions
AS SELECT
    parseDateTimeBestEffortOrNull(coalesce(
        nullIf(JSONExtractString(message, 'data', 'transaction_time'), ''),
        JSONExtractString(message, 'data', 'trans_date_trans_time')
    )) AS trans_date,
    coalesce(
        nullIf(JSONExtractString(message, 'data', 'cat_id'), ''),
        JSONExtractString(message, 'data', 'category')
    ) AS category,
    coalesce(
        toFloat64OrNull(coalesce(
            nullIf(JSONExtractString(message, 'data', 'amount'), ''),
            nullIf(nullIf(JSONExtractRaw(message, 'data', 'amount'), 'null'), ''),
            nullIf(JSONExtractString(message, 'data', 'amt'), ''),
            JSONExtractRaw(message, 'data', 'amt')
        )),
        0.
    ) AS amount,
    upper(coalesce(
        nullIf(JSONExtractString(message, 'data', 'us_state'), ''),
        JSONExtractString(message, 'data', 'state')
    )) AS state
FROM hw2.transactions_kafka
WHERE _error = ''
  AND trans_date IS NOT NULL
  AND category != ''
  AND state != '';

