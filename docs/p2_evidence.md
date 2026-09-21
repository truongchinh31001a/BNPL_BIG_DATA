# P2 execution evidence

Ngày kiểm chứng: 2026-09-21.

## Kafka UI

Redpanda Console `v3.12.0` chạy tại http://localhost:8089. Health endpoint trả HTTP 200 và API `/api/topics` nhìn thấy:

| Topic | Partitions | Replication factor | Stored size |
|---|---:|---:|---:|
| `bnpl.transactions.raw` | 1 | 1 | 4,703,047 bytes |

Console kết nối broker nội bộ `redpanda:9092` và Admin API `redpanda:9644`; không cần publish thêm cổng nội bộ.

## Prometheus và Grafana

Prometheus `v2.55.1` và Grafana `11.2.2` chạy trong profile `monitoring`. Tất cả targets đã trả trạng thái `up`:

| Prometheus job | Targets up |
|---|---:|
| `prometheus` | 1 |
| `redpanda` | 1 |
| `spark-master` | 1 |
| `spark-applications` | 1 |
| `spark-workers` | 2 |

Spark endpoint `/metrics/master/prometheus/` trả `text/plain` thay vì Spark UI HTML sau khi nạp `spark/conf/metrics.properties`. Prometheus đã nạp rule group `bnpl-platform` với 2 alert rules.

Grafana health báo database `ok`; provisioning tạo folder `BNPL Platform`, Prometheus datasource và dashboard `BNPL Platform Overview` (`uid=bnpl-platform-overview`).

## Continuous integration

Workflow `.github/workflows/tests.yml` chạy khi push hoặc mở pull request. Job thực hiện:

1. Validate `docker compose config` bằng `.env.example`.
2. Build Spark image từ `spark/Dockerfile`.
3. Chạy toàn bộ tests với đúng `PYTHONPATH` và Docker command dùng ở local.

Mô phỏng workflow local đã thành công: Spark image build hoàn tất và `10 passed in 13.99s`.

Không thêm external API, Kubernetes, Flink, Databricks, Elasticsearch hoặc LLM. P2 chỉ bổ sung observability và automation quanh kiến trúc đã kiểm chứng ở P0/P1.
