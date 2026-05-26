import os
from dataclasses import dataclass


def env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = env_str("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    topic_raw: str = env_str("KAFKA_TOPIC_RAW", "wiki.recentchange.raw")
    consumer_group: str = env_str("KAFKA_CONSUMER_GROUP", "wiki-postgres-writer")


@dataclass(frozen=True)
class PostgresConfig:
    host: str = env_str("POSTGRES_HOST", "postgres")
    port: int = env_int("POSTGRES_PORT", 5432)
    database: str = env_str("POSTGRES_DB", "streaming")
    user: str = env_str("POSTGRES_USER", "stream")
    password: str = env_str("POSTGRES_PASSWORD", "stream")

    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class ProducerConfig:
    stream_url: str = env_str(
        "WIKIMEDIA_STREAM_URL",
        "https://stream.wikimedia.org/v2/stream/recentchange",
    )
    client_id: str = env_str("PRODUCER_CLIENT_ID", "wiki-recentchange-producer")
    log_every: int = env_int("PRODUCER_LOG_EVERY", 100)


@dataclass(frozen=True)
class ProcessorConfig:
    batch_size: int = env_int("PROCESSOR_BATCH_SIZE", 100)
    flush_seconds: int = env_int("PROCESSOR_FLUSH_SECONDS", 3)


@dataclass(frozen=True)
class DashboardConfig:
    refresh_ms: int = env_int("DASHBOARD_REFRESH_MS", 3000)
