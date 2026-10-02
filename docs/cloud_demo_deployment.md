# Phương án cloud và server Docker cho demo

## 1. Mục tiêu

Tài liệu này tổng hợp phương án đưa BNPL Big Data Platform ra khỏi máy cá nhân để trình diễn. Mục tiêu gần nhất là có một môi trường ổn định trong vài giờ, đủ chạy batch, streaming, prediction và dashboard. Mục tiêu này khác với việc xây dựng một hệ thống production có nhiều node và khả năng chịu lỗi.

**Khuyến nghị:** thuê một server Linux mạnh trong ngày demo, triển khai nguyên Docker Compose, chuẩn bị dữ liệu và model trước giờ trình bày, sau đó chỉ chạy smoke batch và luồng streaming trực tiếp. Google Cloud managed services phù hợp làm kiến trúc đích trong báo cáo, nhưng không kinh tế bằng một server ngắn hạn cho buổi demo.

## 2. Ba phương án triển khai

| Phương án | Phạm vi thay đổi | Chi phí | Giá trị chứng minh | Phù hợp hiện tại |
|---|---|---:|---|---|
| Local Docker | Không thay đổi | Thấp nhất | Pipeline end to end và xử lý phân tán logic | Dự phòng |
| Một server Docker | Chủ yếu cấu hình, bảo mật và volume | Thấp khi thuê theo giờ | Demo online ổn định, tài nguyên lớn hơn máy cá nhân | **Khuyến nghị** |
| Google Cloud managed | Thay MinIO, Kafka, Spark, Airflow và serving | Cao hơn | Tách compute và storage, multi-zone, managed operations | Kiến trúc đích |

Một server Docker vẫn là single-node deployment. Hai Spark workers chạy trong hai container nhưng dùng chung CPU, RAM, disk và network của một máy. Vì vậy kết quả chỉ chứng minh logical distributed processing, không chứng minh physical multi-node scale-out.

## 3. Kiến trúc Google Cloud tham chiếu

| Thành phần hiện tại | Dịch vụ Google Cloud phù hợp |
|---|---|
| MinIO | Cloud Storage |
| Redpanda/Kafka | Managed Service for Apache Kafka hoặc Pub/Sub |
| Spark batch | Serverless for Apache Spark |
| Spark Structured Streaming | Dataproc cluster chạy dài hạn |
| Airflow | Managed Service for Apache Airflow |
| PostgreSQL analytics | BigQuery |
| PostgreSQL prediction serving | Cloud SQL for PostgreSQL |
| Model files | Cloud Storage và tùy chọn Vertex AI Model Registry |
| Prometheus/Grafana | Cloud Monitoring và Cloud Logging |
| Secrets trong `.env` | Secret Manager và Service Account |

Luồng dữ liệu mục tiêu:

```text
Historical source -> Cloud Storage Staging -> Spark batch
                  -> Bronze -> Quality Gate -> Silver Delta
                  -> Gold Analytics / Gold ML -> BigQuery

Producer -> Managed Kafka -> Spark Structured Streaming
         -> Bronze raw -> validation/features -> 30D/90D prediction
         -> Cloud SQL + BigQuery monitoring
```

Giữ Managed Kafka sẽ giảm thay đổi code vì project đang phụ thuộc topic, partition, offset và Spark Kafka connector. Pub/Sub vận hành đơn giản và có mô hình pay per use, nhưng yêu cầu thay producer, consumer và semantics replay.

## 4. Chi phí cloud và credit giáo dục

Google Cloud có chương trình dùng thử 300 USD trong 90 ngày cho tài khoản mới đủ điều kiện. Email trường không tự động mở khóa miễn phí. Với Education Credits, giảng viên hoặc nhà trường phải đăng ký chương trình, sau đó sinh viên xác minh email trường và nhận coupon. Tài liệu Google hiện nêu mức tối đa 50 USD cho mỗi sinh viên và credit có hiệu lực một năm.

Các mức miễn phí hữu ích cho demo nhỏ:

- BigQuery: 10 GiB lưu trữ và 1 TiB dữ liệu query mỗi tháng.
- Pub/Sub: 10 GiB throughput mỗi tháng.
- Cloud Storage: 5 GB mỗi tháng tại một số region của Mỹ.
- Serverless Spark: trả theo tài nguyên trong thời gian job chạy, không có máy thường trực.

Managed Kafka, Managed Airflow và Cloud SQL có tài nguyên chạy nền. Chúng có thể tiêu credit nhanh nếu giữ hoạt động liên tục. Vì vậy không nên triển khai full managed stack chỉ để phục vụ một buổi bảo vệ.

Nguồn giá và chính sách, kiểm tra ngày 27/09/2026:

- [Google Cloud Free Program](https://docs.cloud.google.com/free/docs/free-cloud-features)
- [Google Cloud Education Credits](https://docs.cloud.google.com/billing/docs/how-to/edu-grants)
- [BigQuery pricing](https://cloud.google.com/bigquery/pricing)
- [Pub/Sub pricing](https://cloud.google.com/pubsub/pricing)
- [Managed Kafka pricing](https://cloud.google.com/managed-service-for-apache-kafka/pricing)
- [Managed Service for Apache Spark pricing](https://cloud.google.com/products/managed-service-for-apache-spark/pricing)

## 5. Phương án server V100 đã chọn

Thông số nhà cung cấp:

| Tài nguyên | Giá trị | Đánh giá cho project |
|---|---:|---|
| GPU | Tesla V100 SXM2 32 GB | Không được code hiện tại sử dụng |
| CPU | Xeon Gold 6248, 10 cores | Đủ chia hai Spark workers |
| RAM | 94 GB | Dư cho toàn bộ Docker stack |
| Disk | 890 GB | Đủ cho images, MinIO, Delta và benchmark |
| Disk read/write | 2,377 / 1,081 MB/s | Phù hợp Parquet, Delta và shuffle local |
| Network up/down | 9,438 / 17,792 Mbps | Dư cho demo và tải image |
| Khu vực | `na-01` | Có thể tăng latency giao diện từ Việt Nam |
| SLA | 99.9% | Phù hợp một buổi demo, vẫn cần phương án dự phòng |
| Giá | 9,000 VNĐ/giờ | Hợp lý khi thuê ngắn hạn |

Chi phí ước tính nếu đơn giá là 9,000 VNĐ mỗi giờ:

| Thời gian | Chi phí |
|---:|---:|
| 3 giờ | 27,000 VNĐ |
| 8 giờ | 72,000 VNĐ |
| 24 giờ | 216,000 VNĐ |
| 3 ngày | 648,000 VNĐ |
| 7 ngày | 1,512,000 VNĐ |
| 30 ngày | 6,480,000 VNĐ |

GPU V100 không làm Spark MLlib Logistic Regression, Random Forest hoặc GBT hiện tại nhanh hơn một cách tự động. Muốn dùng GPU phải bổ sung RAPIDS Accelerator, XGBoost GPU hoặc một framework như PyTorch. Với khoảng 110,000 dòng training, thay đổi này không cần thiết cho mục tiêu demo và có thể làm tăng rủi ro tương thích.

## 6. Phân bổ tài nguyên đề xuất

Thiết lập bình thường:

```env
SPARK_WORKER_REPLICAS=2
SPARK_WORKER_CORES=3
SPARK_WORKER_MEMORY=20G
```

Ngân sách tài nguyên gần đúng:

| Workload | CPU | RAM dự kiến |
|---|---:|---:|
| Hai Spark workers | 6 cores | 40 GB |
| Hai streaming drivers | 1–2 cores | 8–12 GB |
| Redpanda | Chia sẻ | 2–4 GB |
| PostgreSQL và MinIO | Chia sẻ | 4–6 GB |
| Airflow | Chia sẻ | 3–4 GB |
| Prometheus và Grafana | Chia sẻ | 2–3 GB |
| Linux, Docker và file cache | Phần còn lại | 12–20 GB |

Khi chạy benchmark, dừng producer, streaming và monitoring rồi có thể nâng mỗi worker lên 4 cores và 28 GB. Không chạy ma trận 2M rows trong lúc thuyết trình.

## 7. Mạng và bảo mật

Chỉ mở public các port sau:

- `22` cho SSH bằng key; giới hạn source IP nếu nhà cung cấp hỗ trợ.
- `80` và `443` cho reverse proxy HTTPS.

PostgreSQL, MinIO API, Kafka, Spark RPC và Prometheus phải nằm trong Docker network. Airflow, Grafana, Redpanda Console và MinIO Console chỉ đi qua Nginx hoặc Caddy có xác thực. Nếu không cần URL công khai, dùng SSH tunnel hoặc VPN sẽ an toàn hơn.

Trước khi deploy:

- Tạo `.env.production` với password mới.
- Không commit secret hoặc private key.
- Bật firewall và Docker log rotation.
- Gắn named volumes vào data disk bền vững.
- Cấu hình backup PostgreSQL và MinIO.
- Kiểm tra nhà cung cấp có tiếp tục tính tiền khi stop máy hoặc giữ disk/IP hay không.

## 8. Quy trình chuẩn bị demo

### Trước ngày demo

1. Tạo server Ubuntu LTS và cập nhật hệ thống.
2. Cài Docker Engine, Compose plugin và Git.
3. Clone repository, tạo `.env.production`, build toàn bộ image.
4. Chạy batch pipeline với 100,000 hoặc 500,000 rows.
5. Xác nhận Silver, Gold, model registry và PostgreSQL serving đã sẵn sàng.
6. Chạy streaming và inference, xác nhận sáu Kafka partitions có prediction.
7. Chạy smoke benchmark và lưu evidence.
8. Kiểm tra Airflow, Spark UI, Redpanda Console, Grafana và Power BI.
9. Tạo snapshot server nếu nhà cung cấp hỗ trợ.

### Trong buổi demo

1. Mở kiến trúc và Airflow DAG để giới thiệu pipeline.
2. Cho producer phát event mới vào sáu Kafka partitions.
3. Quan sát Spark Structured Streaming xử lý micro-batch.
4. Mở PostgreSQL hoặc Power BI để chứng minh prediction 30D/90D xuất hiện.
5. Mở Grafana và các view latency, consistency, Data Quality.
6. Trình bày benchmark đã chạy trước; không chạy full matrix trực tiếp.

### Sau buổi demo

1. Tải log, CSV, ảnh và evidence cần giữ.
2. Backup database hoặc MinIO nếu cần tái sử dụng.
3. Xóa server, disk, snapshot và public IP không còn dùng.
4. Kiểm tra trang billing để xác nhận không còn tài nguyên tính phí.

## 9. Kịch bản dự phòng

- Giữ Docker environment trên laptop để fallback.
- Lưu video ngắn của luồng producer, Kafka, Spark và prediction.
- Chuẩn bị sẵn ảnh dashboard và kết quả benchmark trong repository.
- Giữ một batch/model hoàn chỉnh để không phụ thuộc Hugging Face hoặc mạng trong giờ trình bày.
- Nếu streaming gặp lỗi, trình bày raw event trong Redpanda Console và prediction đã lưu trong PostgreSQL.

## 10. Cách diễn giải khi thuyết trình

Cách mô tả chính xác:

> Project triển khai một data platform BNPL kết hợp batch và streaming. Spark xử lý dữ liệu phân tán qua nhiều executor container, Kafka phân phối event qua sáu partitions, MinIO lưu data lake và PostgreSQL phục vụ analytics cùng prediction. Bản demo cloud chạy trên một server mạnh để bảo đảm ổn định, nên chứng minh logical distributed processing chứ chưa đại diện cho production multi-node scale-out.

Không nên tuyên bố hệ thống đã chứng minh high availability, broker replication, multi-node fault tolerance hoặc khả năng scale tuyến tính theo số worker. Những nội dung này thuộc kiến trúc đích Google Cloud hoặc một deployment có nhiều máy độc lập.

## 11. Quyết định

Thuê server V100 theo giờ trong một ngày là phương án phù hợp nhất cho demo hiện tại. Server có đủ CPU, RAM, disk và network để giảm rủi ro vận hành; GPU không phải lý do chính để chọn máy. Nhóm nên dùng máy để chuẩn bị trước, chạy smoke workload trong lúc trình bày và xóa toàn bộ tài nguyên ngay sau buổi demo.
