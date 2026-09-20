"""Reusable feature transformations shared by training and inference."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_bnpl_features(df: DataFrame) -> DataFrame:
    """Add deterministic BNPL risk features without labels or model state."""

    months = F.col("tenor_days") / F.lit(30.0)
    return (
        df.withColumn(
            "credit_score_band",
            F.when(F.col("credit_score") < 580, "Poor")
            .when(F.col("credit_score") < 670, "Fair")
            .when(F.col("credit_score") < 740, "Good")
            .when(F.col("credit_score") < 800, "Very Good")
            .otherwise("Excellent"),
        )
        .withColumn(
            "loan_size_category",
            F.when(F.col("principal_ngn") < 50_000, "Small")
            .when(F.col("principal_ngn") < 200_000, "Medium")
            .otherwise("Large"),
        )
        .withColumn(
            "estimated_interest",
            F.col("principal_ngn") * F.col("interest_rate_monthly") * months,
        )
        .withColumn("estimated_total_payment", F.col("principal_ngn") + F.col("estimated_interest"))
        .withColumn("installment_amount", F.col("estimated_total_payment") / F.col("num_installments"))
        .withColumn("purchase_month", F.month("purchase_date"))
        .withColumn("purchase_quarter", F.quarter("purchase_date"))
        .withColumn("purchase_day_of_week", F.dayofweek("purchase_date"))
    )
