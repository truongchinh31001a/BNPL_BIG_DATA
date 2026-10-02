# P2 execution evidence

Ngày kiểm chứng: 2026-09-21.

## Apache Kafka

Broker Apache Kafka KRaft được kiểm tra bằng Kafka CLI và topic có trạng thái:

| Topic | Partitions | Replication factor | Stored size |
|---|---:|---:|---:|
| `bnpl.transactions.raw` | 1 | 1 | 4,703,047 bytes |

Broker nội bộ dùng `kafka:9092`; client trên host dùng `localhost:19092`. Không có ZooKeeper hoặc console phụ thuộc một Kafka-compatible implementation khác.

## Prometheus và Grafana

Prometheus `v2.55.1` và Grafana `11.2.2` chạy trong profile `monitoring`. Tất cả targets đã trả trạng thái `up`:

| Prometheus job | Targets up |
|---|---:|
| `prometheus` | 1 |
| `spark-master` | 1 |
| `spark-applications` | 1 |
| `spark-workers` | 2 |

Spark endpoint `/metrics/master/prometheus/` trả `text/plain` thay vì Spark UI HTML sau khi nạp `spark/conf/metrics.properties`. Prometheus theo dõi Spark master, applications và workers.

Grafana health báo database `ok`; provisioning tạo folder `BNPL Platform`, Prometheus datasource và dashboard `BNPL Platform Overview` (`uid=bnpl-platform-overview`).

## Continuous integration

Workflow `.github/workflows/tests.yml` chạy khi push hoặc mở pull request. Job thực hiện:

1. Validate `docker compose config` bằng `.env.example`.
2. Build Spark image từ `spark/Dockerfile`.
3. Chạy toàn bộ tests với đúng `PYTHONPATH` và Docker command dùng ở local.

Mô phỏng workflow local mới nhất đã thành công: Spark image build hoàn tất, project validator pass và `18 passed in 22.95s`.

Không thêm external API, Kubernetes, Flink, Databricks, Elasticsearch hoặc LLM. P2 chỉ bổ sung observability và automation quanh kiến trúc đã kiểm chứng ở P0/P1.
