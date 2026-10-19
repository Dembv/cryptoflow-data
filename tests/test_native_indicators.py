"""Les expressions Spark natives doivent reproduire exactement les fonctions Python pures.

Nécessite pyspark et une JVM ; le test est ignoré si Spark ne peut pas démarrer.
"""

import pytest

pytest.importorskip("pyspark")

from spark.indicators import (  # noqa: E402
    bollinger_bands, bollinger_cols, rsi_approx_col, rsi_from_window_stats, vwap, vwap_col,
)


@pytest.fixture(scope="module")
def spark():
    import os
    import sys

    from pyspark.sql import SparkSession

    # Les workers Python de Spark doivent utiliser l'interpréteur courant (sous Windows, `python`
    # peut pointer vers le raccourci du Microsoft Store).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    try:
        session = (
            SparkSession.builder.master("local[1]").appName("cryptoflow-tests")
            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate()
        )
    except Exception as exc:                                  # JVM absente, config Hadoop Windows...
        pytest.skip(f"Spark indisponible : {exc}")
    yield session
    session.stop()


def same(actual, expected):
    if expected is None:
        return actual is None
    return actual == pytest.approx(expected, rel=1e-6)


def test_vwap_matches_pure_function(spark):
    from pyspark.sql import functions as F

    data = [(10.0, 2.0), (7.5, 0.0), (None, 2.0), (3.0, None), (123456.78, 0.5)]
    df = spark.createDataFrame(data, "sum_pq double, sum_q double")
    rows = df.select(vwap_col(F.col("sum_pq"), F.col("sum_q")).alias("v")).collect()
    for row, (pq, q) in zip(rows, data):
        assert same(row.v, vwap(pq, q)), (pq, q)


def test_bollinger_matches_pure_function(spark):
    from pyspark.sql import functions as F

    data = [(100.0, 2.0), (100.0, 0.0), (None, 1.0), (100.0, None), (0.0, 1.0), (65000.123, 12.5)]
    df = spark.createDataFrame(data, "avg double, std double")
    bands = bollinger_cols(F.col("avg"), F.col("std"), 2.0)
    rows = df.select(*[col.alias(name) for name, col in bands.items()]).collect()
    for row, (avg, std) in zip(rows, data):
        expected = bollinger_bands(avg, std, 2.0)
        for name in ("upper", "middle", "lower", "bandwidth"):
            assert same(row[name], expected[name]), (name, avg, std)


def test_rsi_approx_matches_pure_function(spark):
    from pyspark.sql import functions as F

    data = [(100.0, 110.0, 105.0), (110.0, 100.0, 105.0), (100.0, 100.0, 100.0), (None, 1.0, 1.0), (1.0, None, 1.0)]
    df = spark.createDataFrame(data, "o double, c double, a double")
    rows = df.select(rsi_approx_col(F.col("o"), F.col("c"), F.col("a")).alias("r")).collect()
    for row, (o, c, a) in zip(rows, data):
        assert same(row.r, rsi_from_window_stats(o, c, a)), (o, c, a)
