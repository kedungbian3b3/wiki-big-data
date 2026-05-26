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
from typing import Dict, Iterator, Optional

import requests
from confluent_kafka import Producer

from common.config import KafkaConfig, ProducerConfig


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | producer | %(message)s",
)
logger = logging.getLogger(__name__)

_running = True


def handle_shutdown(signum, frame) -> None:
    global _running
    logger.info("Received shutdown signal: %s", signum)
    _running = False


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


def iter_sse_events(response: requests.Response) -> Iterator[Dict[str, Optional[str]]]:
    """Minimal Server-Sent Events parser.

    Wikimedia sends lines like:
      event: message
      id: ...
      data: {...json...}

    Blank line = end of one SSE event.
    """
    event_id: Optional[str] = None
    event_name: Optional[str] = None
    data_lines = []

    for raw_line in response.iter_lines(decode_unicode=True):
        if not _running:
            break

        if raw_line is None:
            continue

        line = raw_line.rstrip("\r")

        if line == "":
            if data_lines:
                yield {
                    "id": event_id,
                    "event": event_name,
                    "data": "\n".join(data_lines),
                }
            event_id = None
            event_name = None
            data_lines = []
            continue

        if line.startswith(":"):
            # SSE comment / heartbeat
            continue

        if ":" not in line:
            continue

        field, value = line.split(":", 1)
        value = value.lstrip(" ")

        if field == "id":
            event_id = value
        elif field == "event":
            event_name = value
        elif field == "data":
            data_lines.append(value)


def delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Delivery failed: %s", err)


def build_producer(config: KafkaConfig, producer_config: ProducerConfig) -> Producer:
    return Producer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": producer_config.client_id,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "linger.ms": 50,
            "compression.type": "gzip",
        }
    )


def run() -> None:
    kafka_config = KafkaConfig()
    producer_config = ProducerConfig()
    producer = build_producer(kafka_config, producer_config)

    total_sent = 0
    reconnect_sleep = 1

    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "wiki-bigdata-streaming-lab/1.0 (learning project)",
    }

    while _running:
        try:
            logger.info("Connecting to Wikimedia stream: %s", producer_config.stream_url)
            with requests.get(
                producer_config.stream_url,
                headers=headers,
                stream=True,
                timeout=(10, 90),
            ) as response:
                response.raise_for_status()
                logger.info("Connected. Producing to topic=%s", kafka_config.topic_raw)
                reconnect_sleep = 1

                for sse in iter_sse_events(response):
                    if not _running:
                        break

                    data = sse.get("data")
                    if not data:
                        continue

                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        logger.warning("Skipping invalid JSON SSE payload")
                        continue

                    key = str(event.get("wiki") or event.get("server_name") or "unknown")
                    value = json.dumps(event, ensure_ascii=False, separators=(",", ":"))

                    producer.produce(
                        kafka_config.topic_raw,
                        key=key.encode("utf-8"),
                        value=value.encode("utf-8"),
                        callback=delivery_report,
                    )
                    producer.poll(0)
                    total_sent += 1

                    if total_sent % producer_config.log_every == 0:
                        logger.info("Produced %s events. Last key=%s", total_sent, key)

        except requests.RequestException as exc:
            logger.error("Wikimedia stream connection error: %s", exc)
        except BufferError as exc:
            logger.error("Kafka local producer queue is full: %s", exc)
            producer.poll(1)
        except Exception:
            logger.exception("Unexpected producer error")

        if _running:
            logger.info("Reconnecting in %s seconds...", reconnect_sleep)
            time.sleep(reconnect_sleep)
            reconnect_sleep = min(reconnect_sleep * 2, 30)

    logger.info("Flushing producer before exit...")
    producer.flush(15)
    logger.info("Producer stopped. Total sent=%s", total_sent)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(0)
