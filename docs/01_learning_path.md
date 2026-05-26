# Lộ trình học Data Streaming từ A đến Z

## Giai đoạn 1: Nền tảng bắt buộc

Bạn cần chắc:

- SQL: `SELECT`, `GROUP BY`, window theo thời gian, index.
- Python: đọc API, JSON, logging, retry, batch insert.
- Docker: container, compose, volume, port mapping.
- Database: PostgreSQL, primary key, index, upsert.

## Giai đoạn 2: Pub/Sub model

Nắm 3 vai trò:

- Producer: đẩy event vào broker.
- Broker: lưu event trong topic/partition.
- Consumer: đọc event theo offset.

Trong project này:

- Producer: `services/producer/wiki_producer.py`
- Broker: `redpanda`
- Consumer/processor: `services/processor/stream_processor.py`

## Giai đoạn 3: Kafka basics

Cần hiểu:

- Topic: nơi chứa event cùng loại.
- Partition: chia topic để scale song song.
- Offset: vị trí đọc của consumer trong partition.
- Consumer group: nhóm consumer chia nhau partition.
- Retention: broker giữ event trong bao lâu.
- Key: quyết định event đi vào partition nào.

## Giai đoạn 4: Stream processing

Có 2 loại xử lý:

### Stateless

Không cần nhớ quá khứ:

- Parse JSON.
- Rename field.
- Cast data type.
- Filter event lỗi.

### Stateful

Cần nhớ trạng thái:

- Count số event theo phút.
- Window aggregation.
- Deduplicate.
- Join stream với dimension table.
- Alert khi vượt ngưỡng.

Project hiện tại xử lý transform + lưu fact table. Dashboard dùng SQL để aggregate realtime.

## Giai đoạn 5: Window, watermark, late event

Trong production với Flink/Spark:

- Tumbling window: cửa sổ cố định, ví dụ mỗi 1 phút.
- Sliding window: cửa sổ trượt, ví dụ 5 phút, trượt mỗi 10 giây.
- Session window: gom event theo phiên hoạt động.
- Watermark: mốc cho biết hệ thống chấp nhận event trễ tới đâu.
- Late event: event đến muộn hơn thời điểm nó xảy ra.

## Giai đoạn 6: Delivery semantics

Các mức đảm bảo:

- At-most-once: có thể mất event.
- At-least-once: không mất event nhưng có thể duplicate.
- Exactly-once: không mất, không duplicate ở kết quả cuối.

Project này dùng chiến lược thực tế:

- Processor ghi DB trước.
- Sau khi DB commit mới commit Kafka offset.
- DB có primary key `event_id`.
- Nếu chạy lại bị đọc trùng thì `ON CONFLICT DO NOTHING` chống duplicate.

Đây là kiểu **at-least-once + idempotent sink**.

## Giai đoạn 7: Observability

Cần theo dõi:

- Producer có còn nhận event không?
- Kafka topic có tăng message không?
- Consumer lag có tăng không?
- DB insert có lỗi không?
- Dashboard latest event time có gần hiện tại không?

## Giai đoạn 8: Scale lên Big Data thật

Khi dữ liệu lớn hơn:

- Tăng partition topic.
- Chạy nhiều consumer cùng group.
- Dùng Flink/Spark thay Python processor.
- Sink sang ClickHouse/Pinot/Druid/OpenSearch thay PostgreSQL.
- Thêm Schema Registry để quản lý schema.
- Thêm monitoring: Prometheus + Grafana.
