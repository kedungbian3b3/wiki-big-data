# Phương pháp xử lý dữ liệu

## 1. Ingestion

Nguồn dữ liệu là Server-Sent Events.

Producer dùng HTTP streaming:

```text
GET https://stream.wikimedia.org/v2/stream/recentchange
Accept: text/event-stream
```

Mỗi SSE message có field `data`, bên trong là JSON.

## 2. Serialization

Producer ghi JSON UTF-8 vào Kafka topic.

Key hiện tại dùng `wiki` hoặc `server_name`.

Ý nghĩa:

- Event cùng wiki có xu hướng đi cùng partition.
- Dễ mở rộng nếu muốn xử lý theo từng wiki.

## 3. Transform

Processor flatten JSON:

| Raw field | Output column |
|---|---|
| `id` + `wiki` | `event_id` |
| `timestamp` | `event_time` |
| `meta.domain` | `domain` |
| `length.old` | `length_old` |
| `length.new` | `length_new` |
| `length.new - length.old` | `bytes_delta` |
| `revision.old` | `rev_old` |
| `revision.new` | `rev_new` |

## 4. Load

Ghi vào PostgreSQL bằng batch insert.

Câu SQL chính:

```sql
INSERT INTO fact_recent_changes (...)
VALUES (...)
ON CONFLICT (event_id) DO NOTHING;
```

Lý do:

- Tránh duplicate nếu processor restart.
- Đảm bảo idempotent sink.
- Phù hợp với at-least-once processing.

## 5. Dashboard query

Dashboard không lưu state riêng. Mỗi 3 giây query lại PostgreSQL.

Ví dụ query timeline:

```sql
SELECT
    date_bin('10 seconds', event_time, TIMESTAMPTZ '2000-01-01') AS bucket,
    COUNT(*) AS events
FROM fact_recent_changes
WHERE event_time >= now() - interval '10 minutes'
GROUP BY 1
ORDER BY 1;
```

## 6. Tối ưu hiệu năng

Đã có index:

```sql
CREATE INDEX idx_fact_recent_changes_event_time
ON fact_recent_changes (event_time DESC);
```

Khi dữ liệu lớn hơn, cần thêm:

- Partition table theo ngày.
- Retention policy.
- Sink sang ClickHouse/Pinot/Druid.
- Aggregate table/materialized view.
