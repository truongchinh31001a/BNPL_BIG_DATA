"""Spark ML pipeline definitions shared by both prediction horizons."""

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler


CATEGORICAL_COLUMNS = [
    "merchant_category",
    "provider",
    "customer_state",
    "credit_score_band",
    "loan_size_category",
]

NUMERICAL_COLUMNS = [
    "principal_ngn",
    "interest_rate_monthly",
    "tenor_days",
    "num_installments",
    "credit_score",
    "first_time_customer_num",
    "estimated_interest",
    "estimated_total_payment",
    "installment_amount",
    "purchase_month",
    "purchase_quarter",
    "purchase_day_of_week",
]


def classifier_candidates(seed: int = 42):
    return {
        "logistic_regression": LogisticRegression(featuresCol="features", labelCol="label"),
        "random_forest": RandomForestClassifier(featuresCol="features", labelCol="label", seed=seed),
        "gbt": GBTClassifier(featuresCol="features", labelCol="label", seed=seed),
    }


def build_pipeline(classifier) -> Pipeline:
    indexers = [
        StringIndexer(inputCol=column, outputCol=f"{column}_idx", handleInvalid="keep")
        for column in CATEGORICAL_COLUMNS
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{column}_idx" for column in CATEGORICAL_COLUMNS],
        outputCols=[f"{column}_vec" for column in CATEGORICAL_COLUMNS],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=NUMERICAL_COLUMNS + [f"{column}_vec" for column in CATEGORICAL_COLUMNS],
        outputCol="features",
        handleInvalid="keep",
    )
    return Pipeline(stages=[*indexers, encoder, assembler, classifier])
