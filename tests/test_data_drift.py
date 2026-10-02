from bnpl_common.drift import calculate_psi


def test_psi_is_near_zero_for_identical_distributions(spark):
    baseline = spark.createDataFrame([("A",), ("A",), ("B",)], ["segment"])
    current = spark.createDataFrame([("A",), ("A",), ("B",)], ["segment"])

    psi = calculate_psi(baseline, current, features=("segment",))

    assert psi.select("psi").first().psi == 0.0
    assert psi.select("drift_level").first().drift_level == "STABLE"


def test_psi_flags_a_shifted_distribution(spark):
    baseline = spark.createDataFrame([("A",)] * 9 + [("B",)], ["segment"])
    current = spark.createDataFrame([("A",)] + [("B",)] * 9, ["segment"])

    psi = calculate_psi(baseline, current, features=("segment",))

    assert psi.select("psi").first().psi > 0.25
    assert psi.select("drift_level").first().drift_level == "SIGNIFICANT"
