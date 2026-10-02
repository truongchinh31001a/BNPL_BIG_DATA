"""Population drift helpers shared by monitoring jobs and tests."""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


DRIFT_FEATURES = (
    "credit_score_band",
    "loan_size_category",
    "merchant_category",
    "provider",
    "customer_state",
    "first_time_customer_bucket",
    "tenor_bucket",
    "interest_rate_bucket",
)


def add_drift_buckets(df: DataFrame) -> DataFrame:
    """Create stable business buckets so batch and streaming distributions are comparable."""

    return (
        df.withColumn(
            "first_time_customer_bucket",
            F.when(F.col("first_time_customer"), "first_time").otherwise("returning"),
        )
        .withColumn(
            "tenor_bucket",
            F.when(F.col("tenor_days") <= 14, "01_0_14")
            .when(F.col("tenor_days") <= 30, "02_15_30")
            .when(F.col("tenor_days") <= 60, "03_31_60")
            .otherwise("04_61_plus"),
        )
        .withColumn(
            "interest_rate_bucket",
            F.when(F.col("interest_rate_monthly") < 0.01, "01_lt_1pct")
            .when(F.col("interest_rate_monthly") < 0.03, "02_1_3pct")
            .when(F.col("interest_rate_monthly") <= 0.05, "03_3_5pct")
            .otherwise("04_gt_5pct"),
        )
    )


def _feature_distribution(df: DataFrame, feature: str, prefix: str) -> DataFrame:
    count_name = f"{prefix}_count"
    share_name = f"{prefix}_share"
    counts = (
        df.select(F.coalesce(F.col(feature).cast("string"), F.lit("__NULL__")).alias("bucket"))
        .groupBy("bucket")
        .agg(F.count(F.lit(1)).alias(count_name))
    )
    total = counts.agg(F.sum(count_name).alias("_total"))
    return counts.crossJoin(F.broadcast(total)).withColumn(
        share_name, F.col(count_name) / F.col("_total")
    ).drop("_total")


def calculate_psi(
    baseline: DataFrame,
    current: DataFrame,
    features: tuple[str, ...] = DRIFT_FEATURES,
    epsilon: float = 1e-6,
) -> DataFrame:
    """Return bucket-level Population Stability Index details for each feature."""

    frames = []
    for feature in features:
        baseline_distribution = _feature_distribution(baseline, feature, "baseline")
        current_distribution = _feature_distribution(current, feature, "current")
        joined = baseline_distribution.join(current_distribution, "bucket", "full")
        baseline_share = F.greatest(
            F.coalesce(F.col("baseline_share"), F.lit(0.0)), F.lit(epsilon)
        )
        current_share = F.greatest(
            F.coalesce(F.col("current_share"), F.lit(0.0)), F.lit(epsilon)
        )
        frames.append(
            joined.select(
                F.lit(feature).alias("feature"),
                "bucket",
                F.coalesce(F.col("baseline_count"), F.lit(0)).alias("baseline_count"),
                F.coalesce(F.col("current_count"), F.lit(0)).alias("current_count"),
                baseline_share.alias("baseline_share"),
                current_share.alias("current_share"),
                (
                    (current_share - baseline_share)
                    * F.log(current_share / baseline_share)
                ).alias("psi_component"),
            )
        )

    combined = reduce(lambda left, right: left.unionByName(right), frames)
    feature_window = Window.partitionBy("feature")
    return (
        combined.withColumn("psi", F.sum("psi_component").over(feature_window))
        .withColumn(
            "drift_level",
            F.when(F.col("psi") < 0.10, "STABLE")
            .when(F.col("psi") < 0.25, "MODERATE")
            .otherwise("SIGNIFICANT"),
        )
    )
