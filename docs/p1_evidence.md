# P1 execution evidence

Ngày kiểm chứng: 2026-09-21. Các kết quả dưới đây được chạy trên Docker Desktop với 12 CPU và 15.54 GiB RAM khả dụng.

## Incremental processing

Batch 1 dùng run `p0_20260921_001`, sau đó replay bằng `p0_retry_20260921_001` với cùng `batch_id=20260921`:

| Mốc | Fact rows | Distinct transaction_id | Kết quả |
|---|---:|---:|---|
| Batch 1 | 100,000 | 100,000 | Thành công |
| Replay cùng batch | 100,000 | 100,000 | Không duplicate |

Batch 2 được trigger bằng run `p1_incremental_20260921_001`:

```json
{
  "batch_id": "p1_incremental_20260921",
  "model_version": "v1",
  "ingest_max_rows": 110000
}
```

DAG chạy end-to-end thành công trong `00:11:06.526898`. Registry ghi 110,000 Bronze rows cho mỗi source; source BNPL có trạng thái Bronze, Silver và Gold đều `SUCCESS`.

| Kiểm tra sau Batch 2 | Giá trị |
|---|---:|
| Fact rows | 110,000 |
| Distinct transaction_id | 110,000 |
| Số transaction tăng thêm | 10,000 |
| DQ duplicate_count | 0 |
| DQ rejected_record_count | 0 |
| DQ valid_record_rate | 1.0 |

Kết quả chứng minh Delta `MERGE` cập nhật 100,000 key cũ và thêm 10,000 key mới, thay vì append thành 210,000 rows.

## Kafka và Structured Streaming

Hai profile đã được chạy cùng Apache Kafka, fake producer, Kafka-to-Bronze consumer và inference consumer:

```powershell
docker compose --profile streaming up -d --build
docker compose --profile streaming --profile inference up -d
```

| Điểm kiểm tra | Kết quả |
|---|---:|
| Topic `bnpl.transactions.raw` high-watermark | 9,594 |
| Bronze streaming Parquet files | 448 |
| Rejected streaming Parquet files | 196 |
| Tổng prediction trong PostgreSQL | 18,764 |
| Transaction đã dự đoán | 9,382 |
| Prediction trên mỗi transaction | 2 (30D và 90D) |

`ml.vw_prediction_monitoring` trả về đủ nhóm risk cho cả 30D và 90D. Một event mẫu:

| Trường | Giá trị |
|---|---|
| transaction_id | `TX_STREAM_000000008915_335ae917` |
| Kafka event timestamp | `2026-09-21T12:58:28.719555+00:00` |
| 30D prediction | MEDIUM, default=true, probability=0.675965 |
| 30D latency | 5.110 giây |
| 90D prediction | HIGH, default=true, probability=0.835424 |
| 90D latency | 6.517 giây |

Sau khi chuyển storage, raw event, invalid event và checkpoint được lưu dưới HDFS `/bnpl-data` thay vì writable container layer. Cần chạy lại bài kiểm tra persistence trên hai named volume HDFS trước khi dùng làm bằng chứng nộp bài.

## Legacy benchmark trước HDFS

Bảng dưới đây được giữ làm lịch sử từ lần đo ngày 21/09/2026. Implementation cũ dùng nhãn `incremental` cho một sample theo tỷ lệ, chưa phải Delta MERGE thật, và storage khi đó chưa phải HDFS. Vì vậy số liệu này không được dùng làm benchmark chính thức cho kiến trúc hiện tại.

| Size | Mode | Processed | 1 worker median (s) | 1 worker rows/s | 2 workers median (s) | 2 workers rows/s |
|---:|---|---:|---:|---:|---:|---:|
| 100K | full | 100,000 | 20.536 | 4,870 | 30.118 | 3,320 |
| 500K | full | 500,000 | 20.951 | 23,865 | 34.148 | 14,642 |
| 1M | full | 1,000,000 | 20.928 | 47,783 | 40.603 | 24,629 |
| 2M | full | 2,000,000 | 22.572 | 88,606 | 34.256 | 58,383 |
| 100K | incremental | 25,044 | 18.797 | 1,332 | 28.384 | 882 |
| 500K | incremental | 125,203 | 20.585 | 6,082 | 28.527 | 4,389 |
| 1M | incremental | 250,214 | 19.684 | 12,712 | 28.984 | 8,633 |
| 2M | incremental | 500,222 | 21.564 | 23,198 | 44.522 | 11,235 |

Legacy artifacts:

- Raw summary: `docs/evidence/p1/benchmark_summary_legacy_pre_hdfs.csv`
- Runtime chart: `docs/evidence/p1/benchmark_runtime_legacy_pre_hdfs.svg`
- Throughput chart: `docs/evidence/p1/benchmark_throughput_legacy_pre_hdfs.svg`

Hai workers không nhanh hơn trong môi trường đã đo. Cả hai cùng chia sẻ một Docker Desktop host, network bridge và disk; startup, shuffle và storage write là phần lớn runtime. Kết luận này cũng chỉ mang tính lịch sử cho lần chạy cũ.

## Benchmark HDFS hiện tại

Runner hiện tách hai suite độc lập: `scalability` đo read/filter/group/join/write, còn `incremental` so sánh full reload với Delta MERGE thật. Cả hai suite mở rộng deterministic Silver source tới đúng requested size, kể cả 500K, 1M và 2M; incremental batch mặc định là 25%. Exporter thêm `benchmark_suite` trước khi tính median để không trộn hai workload.

Ma trận HDFS đầy đủ vẫn cần chạy lại trước khi nộp số liệu benchmark mới. Output chính thức sẽ dùng `benchmark_summary.csv`, hai biểu đồ scalability và hai biểu đồ full reload/MERGE không có hậu tố `legacy`.

Trong lần chạy đầu, `DISK_ONLY` cache của benchmark làm writable layers của hai worker tăng khoảng 24.8 GiB và Docker hết dung lượng. Hai worker được tạo lại mà không xóa named volume; container usage giảm từ 26.52 GiB xuống 1.69 GiB. `spark.worker.cleanup.enabled` hiện được bật với interval 60 giây và app-data TTL 300 giây để tự dọn thư mục ứng dụng cũ. Không dùng `docker compose down -v` vì sẽ xóa HDFS và PostgreSQL.

## Tests

Lệnh Docker trong README đã được chạy trên image `bnpl-spark:3.5.1`:

```bash
docker run --rm -e PYTHONPATH=/workspace/spark:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip -v "${PWD}:/workspace" -w /workspace bnpl-spark:3.5.1 python -m pytest -q tests
```

Kết quả mới nhất: `15 passed in 26.63s`. Bộ test gồm HDFS path config, schema contract, batch/streaming quality gate, shared feature engineering, Bronze -> Silver -> Gold integration và replay idempotency.
