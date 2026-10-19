CREATE DATABASE IF NOT EXISTS hw2;

-- Kafka messages are envelopes: {"transaction_id": "...", "data": {...}}.
-- JSONAsString lets ClickHouse extract raw and model-normalized column names.
CREATE TABLE IF NOT EXISTS hw2.transactions_kafka
(
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 3,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS hw2.transactions
(
    trans_date DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    category LowCardinality(String) CODEC(ZSTD(1)),
    amount Decimal(18, 2) CODEC(Delta, ZSTD(1)),
    state LowCardinality(String) CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trans_date)
ORDER BY (state, trans_date, category)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.transactions_from_kafka
TO hw2.transactions
AS SELECT
    parseDateTime64BestEffortOrNull(
        coalesce(
            nullIf(JSONExtractString(message, 'data', 'transaction_time'), ''),
            JSONExtractString(message, 'data', 'trans_date_trans_time')
        ),
        3,
        'UTC'
    ) AS trans_date,
    coalesce(
        nullIf(JSONExtractString(message, 'data', 'cat_id'), ''),
        JSONExtractString(message, 'data', 'category')
    ) AS category,
    toDecimal64OrNull(
        coalesce(
            nullIf(JSONExtractString(message, 'data', 'amount'), ''),
            nullIf(nullIf(JSONExtractRaw(message, 'data', 'amount'), 'null'), ''),
            nullIf(JSONExtractString(message, 'data', 'amt'), ''),
            JSONExtractRaw(message, 'data', 'amt')
        ),
        2
    ) AS amount,
    upper(coalesce(
        nullIf(JSONExtractString(message, 'data', 'us_state'), ''),
        JSONExtractString(message, 'data', 'state')
    )) AS state
FROM hw2.transactions_kafka
WHERE _error = ''
  AND trans_date IS NOT NULL
  AND amount IS NOT NULL
  AND category != ''
  AND state != '';

CREATE TABLE IF NOT EXISTS hw2.state_category_max
(
    state LowCardinality(String),
    category_state AggregateFunction(argMax, String, Decimal(18, 2)),
    amount_state AggregateFunction(max, Decimal(18, 2))
)
ENGINE = AggregatingMergeTree
ORDER BY state;

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.state_category_max_mv
TO hw2.state_category_max
AS SELECT
    state,
    argMaxState(CAST(category AS String), amount) AS category_state,
    maxState(amount) AS amount_state
FROM hw2.transactions
GROUP BY state;

