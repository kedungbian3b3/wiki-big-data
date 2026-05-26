# Bước tiếp theo với Flink

Bản lab mặc định dùng Python processor để bạn chạy được nhanh trên laptop.

Sau khi hiểu luồng, bạn nên thay processor bằng Flink vì Flink mạnh hơn ở:

- Stateful processing.
- Window aggregation.
- Watermark và late event.
- Exactly-once checkpoint.
- Join stream với dimension/changelog.
- Scale distributed.

## Mapping từ project hiện tại sang Flink

| Hiện tại | Khi dùng Flink |
|---|---|
| `services/processor/stream_processor.py` | Flink SQL / PyFlink / Java Flink job |
| Batch insert PostgreSQL | Flink JDBC sink |
| SQL dashboard aggregate | Flink window aggregate trước rồi sink |
| Python manual offset commit | Flink checkpoint quản lý offset |
| Deduplicate bằng PK DB | Flink state + upsert sink |

## Flink SQL source concept

Ví dụ source table đọc Kafka JSON:

```sql
CREATE TABLE wiki_recentchange_raw (
    id BIGINT,
    wiki STRING,
    type STRING,
    title STRING,
    user_name STRING,
    bot BOOLEAN,
    minor BOOLEAN,
    `timestamp` BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(`timestamp` * 1000, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'wiki.recentchange.raw',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);
```

## Flink tumbling window concept

```sql
SELECT
    window_start,
    window_end,
    wiki,
    COUNT(*) AS total_events
FROM TABLE(
    TUMBLE(TABLE wiki_recentchange_raw, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, wiki;
```

## Học Flink theo thứ tự

1. Chạy project hiện tại cho hiểu Kafka topic và event.
2. Viết Flink SQL đọc topic và `SELECT *`.
3. Viết tumbling window count theo wiki.
4. Sink kết quả window sang PostgreSQL.
5. Bật checkpoint.
6. Test restart Flink job và kiểm tra có duplicate không.
