# Repository working rules

- Không tạo script PowerShell (`.ps1`). Automation và utility script chỉ dùng Python (`.py`) hoặc notebook (`.ipynb`).
- Khi chạy command, Docker job, Spark job hoặc pipeline dài, không đọc log trong lúc tiến trình đang chạy. Chờ tiến trình kết thúc rồi mới đọc output hoặc log kết quả.
