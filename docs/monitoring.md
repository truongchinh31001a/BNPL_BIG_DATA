# Monitoring

Các thành phần quan sát nằm trong Compose profiles để batch stack mặc định không tốn thêm RAM.

## Khởi động

```powershell
docker compose --profile monitoring up -d
docker compose --profile monitoring ps
```

Profile `monitoring` bật Prometheus và Grafana. Spark master/workers vẫn dùng cùng batch stack hiện tại.

| Thành phần | URL | Mục đích |
|---|---|---|
| Prometheus | http://localhost:9090 | Targets, PromQL và alert rules |
| Grafana | http://localhost:3000 | Dashboard `BNPL Platform Overview` |
| Spark master metrics | http://localhost:8080/metrics/master/prometheus/ | Standalone master metrics |

Grafana dùng `GRAFANA_ADMIN_USER` và `GRAFANA_ADMIN_PASSWORD`; `.env.example` đặt giá trị demo `admin` / `admin`.

## Scrape targets

Prometheus đọc cấu hình `monitoring/prometheus.yml`:

- `spark-master`: Spark standalone master.
- `spark-applications`: danh sách/trạng thái application.
- `spark-workers`: DNS discovery để thu thập mọi replica của service worker.
- `prometheus`: self-monitoring.

Spark Prometheus servlet được bật bởi `spark/conf/metrics.properties`. Không cần mở port worker ra host vì Prometheus truy cập qua `bnpl-network`.

## Alerts và dashboard

`monitoring/alerts.yml` có alert nền tảng ban đầu:

- `BnplServiceDown`: target Spark không scrape được trong một phút.

Grafana tự provision Prometheus datasource và dashboard từ `monitoring/grafana/`. Dashboard hiển thị availability, scrape duration và số metric samples.

## Kiểm tra nhanh

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:9090/-/ready
Invoke-RestMethod http://localhost:3000/api/health
Invoke-RestMethod http://localhost:9090/api/v1/targets
```

Dừng riêng monitoring UI/storage mà không xóa dữ liệu nghiệp vụ:

```powershell
docker compose --profile monitoring stop prometheus grafana
```

Không dùng `docker compose down -v` vì tùy chọn `-v` sẽ xóa cả named volumes PostgreSQL, HDFS và Kafka.
