# Giải thích concept bằng chính project này

## 1. Event

Một event là một thay đổi trên Wikimedia.

Ví dụ:

```json
{
  "wiki": "enwiki",
  "type": "edit",
  "title": "Data engineering",
  "user": "ExampleUser",
  "bot": false,
  "timestamp": 1710000000
}
```

## 2. Producer

Producer đọc stream HTTP từ Wikimedia và đẩy vào Kafka topic.

Lý do cần producer:

- Không muốn processor phụ thuộc trực tiếp vào source.
- Có thể replay dữ liệu từ Kafka.
- Có thể thêm nhiều consumer khác sau này.

## 3. Broker

Broker giống buffer trung tâm.

Trong project này topic là:

```text
wiki.recentchange.raw
```

Topic có 6 partitions để mô phỏng scale song song.

## 4. Consumer group

Processor thuộc group:

```text
wiki-postgres-writer
```

Nếu bạn scale processor lên nhiều replicas, Kafka/Redpanda sẽ chia partition cho các processor.

Ví dụ:

```powershell
docker compose up --scale processor=3 -d
```

Lưu ý: nếu scale, cần bỏ `container_name` của service processor trong compose. Bản lab mặc định để `container_name` cho dễ debug.

## 5. Offset

Offset là vị trí đã đọc trong partition.

Processor chỉ commit offset sau khi PostgreSQL commit thành công.

## 6. Schema

Raw event JSON rất nhiều field và nested field. Data Engineer thường cần normalize:

- `length.old`, `length.new` → `length_old`, `length_new`
- `revision.old`, `revision.new` → `rev_old`, `rev_new`
- `timestamp` Unix → `event_time` timestamptz
- Tính thêm `bytes_delta`

## 7. Serving layer

PostgreSQL trong lab đóng vai serving DB.

Dashboard không đọc trực tiếp Kafka vì:

- Dashboard cần query linh hoạt.
- Dashboard cần aggregate nhanh.
- Kafka không phải database query analytical trực tiếp.
