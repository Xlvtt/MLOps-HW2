import json
import logging
import os
import time
import uuid

import psycopg2
from confluent_kafka import Consumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud_detection"),
    "user": os.getenv("POSTGRES_USER", "fraud_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud_password"),
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS transaction_scores (
    transaction_id UUID PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

UPSERT_SQL = """
INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s)
ON CONFLICT (transaction_id) DO UPDATE SET
    score = EXCLUDED.score,
    fraud_flag = EXCLUDED.fraud_flag,
    created_at = NOW()
"""


def connect_to_postgres():
    while True:
        try:
            connection = psycopg2.connect(**POSTGRES_CONFIG)
            connection.autocommit = False
            with connection.cursor() as cursor:
                cursor.execute(CREATE_TABLE_SQL)
            connection.commit()
            return connection
        except psycopg2.OperationalError as error:
            logger.warning("PostgreSQL is not ready: %s", error)
            time.sleep(3)


def validate_score(raw_message):
    data = json.loads(raw_message.decode("utf-8"))
    expected_fields = {"transaction_id", "score", "fraud_flag"}
    if set(data) != expected_fields:
        raise ValueError(f"Expected exactly {sorted(expected_fields)}, got {sorted(data)}")

    transaction_id = str(uuid.UUID(str(data["transaction_id"])))
    score = float(data["score"])
    fraud_flag = int(data["fraud_flag"])
    if not 0 <= score <= 1:
        raise ValueError("score must be between 0 and 1")
    if fraud_flag not in (0, 1):
        raise ValueError("fraud_flag must be 0 or 1")
    return transaction_id, score, fraud_flag


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "score-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_SCORES_TOPIC])
    connection = connect_to_postgres()
    logger.info("Listening to Kafka topic %s", KAFKA_SCORES_TOPIC)

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error("Kafka error: %s", message.error())
                continue

            try:
                record = validate_score(message.value())
                with connection.cursor() as cursor:
                    cursor.execute(UPSERT_SQL, record)
                connection.commit()
                consumer.commit(message=message, asynchronous=False)
                logger.info("Saved score for transaction %s", record[0])
            except (ValueError, TypeError, json.JSONDecodeError) as error:
                connection.rollback()
                logger.error("Skipping invalid score message: %s", error)
                consumer.commit(message=message, asynchronous=False)
            except psycopg2.Error as error:
                connection.rollback()
                logger.error("PostgreSQL error: %s; reconnecting", error)
                connection.close()
                connection = connect_to_postgres()
    finally:
        connection.close()
        consumer.close()


if __name__ == "__main__":
    main()
