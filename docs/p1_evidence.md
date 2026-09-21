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

Hai profile đã được chạy cùng Redpanda, fake producer, Kafka-to-Bronze consumer và inference consumer:

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

Raw event và invalid event vẫn tồn tại trong MinIO sau khi container được tạo lại, xác nhận dữ liệu nằm trong named volume thay vì writable container layer.

## Spark scalability benchmark

Ma trận gồm 4 sizes, 2 worker counts, 2 modes và 3 runs chính thức cho mỗi cấu hình, tổng cộng 48 lần đo. Mỗi cặp worker/mode có một warm-up không đưa vào median.

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

Artifacts:

- Raw summary: `docs/evidence/p1/benchmark_summary.csv`
- Runtime chart: `docs/evidence/p1/benchmark_runtime.svg`
- Throughput chart: `docs/evidence/p1/benchmark_throughput.svg`
- Delta detail: `s3a://bnpl-data/benchmarks/results`

Hai workers không nhanh hơn trong môi trường này. Cả hai cùng chia sẻ một Docker Desktop host, một MinIO node, network bridge và disk; startup, shuffle và object-store write là phần lớn runtime. Mỗi worker được cấu hình 2 cores/2 GiB, trong khi executor của lượt đo dùng 1 GiB. Đây là kết quả scale-up cục bộ, không đại diện cho hai máy vật lý độc lập.

Benchmark dùng 100,000 transaction thật từ Silver rồi nhân bản có deterministic unique ID để tạo size lớn hơn. Vì vậy số liệu đo Spark transform/shuffle/write, không đo tốc độ tải source ngoài. Incremental mode chọn deterministic khoảng 25% transaction theo hash; dataset và streaming events đều là synthetic nên chưa phản ánh drift, seasonality hoặc hành vi tín dụng thực tế.

Trong lần chạy đầu, `DISK_ONLY` cache của benchmark làm writable layers của hai worker tăng khoảng 24.8 GiB và Docker hết dung lượng. Hai worker được tạo lại mà không xóa named volume; container usage giảm từ 26.52 GiB xuống 1.69 GiB. `spark.worker.cleanup.enabled` hiện được bật với interval 60 giây và app-data TTL 300 giây để tự dọn thư mục ứng dụng cũ. Kết quả Delta, MinIO và PostgreSQL không mất.

## Tests

Lệnh Docker trong README đã được chạy trên image `bnpl-spark:3.5.1`:

```powershell
docker run --rm `
  -e PYTHONPATH=/workspace/spark:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip `
  -v "${PWD}:/workspace" -w /workspace `
  bnpl-spark:3.5.1 python -m pytest -q tests
```

Kết quả cuối: `10 passed in 15.16s`. Bộ test gồm schema contract, batch/streaming quality gate, shared feature engineering, Bronze -> Silver -> Gold integration và replay idempotency.
