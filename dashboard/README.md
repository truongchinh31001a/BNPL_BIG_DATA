# Power BI dashboard

Project đã tạo: `BNPL Big Data Report.pbip`.

Mở project bằng Power BI Desktop, chọn **Refresh now**, sau đó nhập PostgreSQL credential từ `.env` ở lần kết nối đầu tiên. Semantic model dùng Import mode và không lưu password trong source control.

Kiểm tra cấu trúc report trước khi mở:

```powershell
node scripts/generate_powerbi_project.mjs
powerbi-report-author validate "dashboard/BNPL Big Data Report.Report"
```

Ảnh kiểm chứng 5 trang nằm tại `docs/evidence/powerbi/`.

## Kết nối

Kết nối Power BI tới PostgreSQL:

- Host `localhost`, port `5432`, database `bnpl_dw`
- Credential lấy từ `.env`
- Import mode phù hợp demo; DirectQuery có thể dùng cho trang streaming monitoring

Các views/tables khuyến nghị:

- Overview: `analytics.vw_bnpl_overview`
- Risk, customer và geography: `analytics.vw_bnpl_transactions`
- Streaming risk: `ml.vw_prediction_monitoring`
- Model monitoring: `ml.model_metrics`
- Data quality: `data_quality.data_quality_metrics`

## Dashboard pages

1. Overview: total transactions, total amount, average loan, 30D/90D default rates.
2. Risk Analysis: default theo credit band, loan size, provider, merchant category.
3. Customer/Geography: state, first-time/returning và volume.
4. Streaming Predictions: transactions received, 30D/90D risk và risk-level distribution.
5. Platform Monitoring: model metrics, batch registry và data-quality trends.
