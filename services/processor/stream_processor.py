from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


import json
import logging
import signal
import sys
import time
from typing import Dict, Iterable, List

import psycopg2
from psycopg2.extras import Json, execute_values
from confluent_kafka import Consumer, KafkaException

from common.config import KafkaConfig, PostgresConfig, ProcessorConfig
from common.transform import normalize_wikimedia_recentchange


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | processor | %(message)s",
)
logger = logging.getLogger(__name__)

_running = True


def handle_shutdown(signum, frame) -> None:
    global _running
    logger.info("Received shutdown signal: %s", signum)
    _running = False


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


INSERT_SQL = """
INSERT INTO fact_recent_changes (
    event_id,
    event_time,
    ingest_time,
    wiki,
    domain,
    server_name,
    user_name,
    title,
    change_type,
    namespace_id,
    is_bot,
    is_minor,
    length_old,
    length_new,
    bytes_delta,
    rev_old,
    rev_new,
    page_url,
    comment,
    raw_event
)
VALUES %s
ON CONFLICT (event_id) DO NOTHING
"""


def build_consumer(kafka_config: KafkaConfig) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "group.id": kafka_config.consumer_group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
        }
    )
    consumer.subscribe([kafka_config.topic_raw])
    return consumer


def connect_postgres(config: PostgresConfig):
    while _running:
        try:
            conn = psycopg2.connect(config.dsn())
            conn.autocommit = False
            logger.info("Connected to PostgreSQL %s:%s/%s", config.host, config.port, config.database)
            return conn
        except psycopg2.Error as exc:
            logger.error("PostgreSQL connection failed: %s. Retrying...", exc)
            time.sleep(3)
    raise RuntimeError("Stopped before PostgreSQL connection was established")


def row_tuple(row: Dict):
    return (
        row["event_id"],
        row["event_time"],
        row["ingest_time"],
        row["wiki"],
        row["domain"],
        row["server_name"],
        row["user_name"],
        row["title"],
        row["change_type"],
        row["namespace_id"],
        row["is_bot"],
        row["is_minor"],
        row["length_old"],
        row["length_new"],
        row["bytes_delta"],
        row["rev_old"],
        row["rev_new"],
        row["page_url"],
        row["comment"],
        Json(row["raw_event"]),
    )


def write_batch(conn, rows: Iterable[Dict]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    values = [row_tuple(row) for row in rows]

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, values, page_size=500)

    conn.commit()
    return len(rows)


def run() -> None:
    kafka_config = KafkaConfig()
    pg_config = PostgresConfig()
    processor_config = ProcessorConfig()

    consumer = build_consumer(kafka_config)
    conn = connect_postgres(pg_config)

    buffer: List[Dict] = []
    last_flush = time.monotonic()
    total_written_attempts = 0
    total_invalid = 0

    logger.info(
        "Consuming topic=%s as group=%s",
        kafka_config.topic_raw,
        kafka_config.consumer_group,
    )

    try:
        while _running:
            msg = consumer.poll(1.0)

            if msg is None:
                should_flush = buffer and (time.monotonic() - last_flush >= processor_config.flush_seconds)
                if should_flush:
                    written = write_batch(conn, buffer)
                    consumer.commit(asynchronous=False)
                    total_written_attempts += written
                    logger.info("Flushed %s rows. Total attempted writes=%s", written, total_written_attempts)
                    buffer.clear()
                    last_flush = time.monotonic()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                event = json.loads(msg.value().decode("utf-8"))
                normalized = normalize_wikimedia_recentchange(event)
                buffer.append(normalized)
            except Exception as exc:
                total_invalid += 1
                logger.warning("Skipping invalid message. invalid_count=%s error=%s", total_invalid, exc)
                continue

            if len(buffer) >= processor_config.batch_size:
                written = write_batch(conn, buffer)
                consumer.commit(asynchronous=False)
                total_written_attempts += written
                logger.info("Flushed %s rows. Total attempted writes=%s", written, total_written_attempts)
                buffer.clear()
                last_flush = time.monotonic()

    except psycopg2.Error:
        logger.exception("PostgreSQL error. Rolling back current transaction.")
        conn.rollback()
        raise
    finally:
        if buffer:
            try:
                written = write_batch(conn, buffer)
                consumer.commit(asynchronous=False)
                logger.info("Final flush %s rows", written)
            except Exception:
                logger.exception("Final flush failed")
        consumer.close()
        conn.close()
        logger.info("Processor stopped")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(0)
