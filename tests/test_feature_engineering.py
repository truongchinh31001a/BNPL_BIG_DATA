from datetime import date

import pytest

from bnpl_common.features import add_bnpl_features


def test_financial_and_date_features(spark):
    frame = spark.createDataFrame(
        [("TX-1", date(2026, 9, 17), 120000.0, 0.04, 60, 3, 612)],
        ["transaction_id", "purchase_date", "principal_ngn", "interest_rate_monthly", "tenor_days", "num_installments", "credit_score"],
    )
    row = add_bnpl_features(frame).first()
    assert row.credit_score_band == "Fair"
    assert row.loan_size_category == "Medium"
    assert row.estimated_interest == pytest.approx(9600.0)
    assert row.estimated_total_payment == pytest.approx(129600.0)
    assert row.installment_amount == pytest.approx(43200.0)
    assert row.purchase_month == 9
    assert row.purchase_quarter == 3
    assert row.purchase_day_of_week == 5
