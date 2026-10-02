# Repository working rules

- Không tạo script PowerShell (`.ps1`). Automation và utility script chỉ dùng Python (`.py`) hoặc notebook (`.ipynb`).
- Khi chạy command, Docker job, Spark job hoặc pipeline dài, không đọc log trong lúc tiến trình đang chạy. Chờ tiến trình kết thúc rồi mới đọc output hoặc log kết quả.
- Kiến trúc HDFS chuẩn của project là 1 NameNode + 3 DataNodes với `dfs.replication=3`; không hạ replication hoặc gom các DataNode vào cùng một volume.
- Mọi xử lý dữ liệu lớn dùng Spark 3.5.1 (PySpark SQL, Structured Streaming, MLlib), không thêm Hadoop MapReduce.
- Bronze raw nằm trên HDFS dưới dạng Parquet; streaming Bronze phải partition theo `event_date/event_hour`. Silver/Gold dùng Delta Lake 3.2 và MERGE theo `transaction_id`.
- PostgreSQL chỉ là serving layer cho Star Schema, metrics, registry và predictions; không ghi raw Big Data vào PostgreSQL.
