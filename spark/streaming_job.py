# path spark/streaming_job.py

"""
CryptoFlow — Sprint 2
Spark Structured Streaming Job

Consomme le topic Kafka `crypto.prices` et calcule en temps réel :
  - VWAP   (Volume Weighted Average Price)
  - RSI    (Relative Strength Index, période 14)
  - Bollinger Bands (20 périodes, 2 écarts-types)

Fenêtres glissantes : 1min / 5min / 1h avec watermark de 10 minutes.
Résultats publiés dans le topic Kafka `crypto.indicators`.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType
)
from indicators import vwap_col, bollinger_cols, rsi_approx_col


# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN        = os.getenv("TOPIC_PRICES",     "crypto.prices")
TOPIC_OUT       = os.getenv("TOPIC_INDICATORS", "crypto.indicators")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR",   "/tmp/cryptoflow/checkpoints")
WATERMARK_DELAY = "10 minutes"
ENABLE_CONSOLE  = os.getenv("ENABLE_CONSOLE_SINK", "false").lower() == "true"   # debug uniquement

WINDOWS = ["1 minute", "5 minutes", "1 hour"]

# ── Schema du message Binance (Sprint 1) ─────────────────────────────────────

PRICE_SCHEMA = StructType([
    StructField("event_type",     StringType(),    True),
    StructField("symbol",         StringType(),    True),
    StructField("pair",           StringType(),    True),
    StructField("price",          DoubleType(),    True),
    StructField("quantity",       DoubleType(),    True),
    StructField("trade_id",       LongType(),      True),
    StructField("side",           StringType(),    True),
    StructField("trade_time",     LongType(),      True),
    StructField("trade_time_iso", StringType(),    True),
    StructField("ingested_at",    StringType(),    True),
    StructField("source",         StringType(),    True),
])

# ── SparkSession ──────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoFlow-Streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.kafka:kafka-clients:3.5.0"
        )
        .getOrCreate()
    )

# ── Lecture Kafka ─────────────────────────────────────────────────────────────

def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10_000)
        .load()
    )

# ── Parsing des messages ──────────────────────────────────────────────────────

def parse_messages(raw_df):
    return (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                PRICE_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        # Convertir trade_time (ms epoch) en TimestampType
        .withColumn(
            "event_time",
            (F.col("trade_time") / 1000).cast(TimestampType())
        )
        # Filtrer les messages malformés
        .filter(
            F.col("symbol").isNotNull() &
            F.col("price").isNotNull() &
            F.col("quantity").isNotNull() &
            (F.col("price") > 0) &
            (F.col("quantity") > 0)
        )
        # Watermark pour gérer les late events
        .withWatermark("event_time", WATERMARK_DELAY)
        # Déduplication par (symbol, trade_id) : les trade_id Binance ne sont uniques que par paire.
        # "WithinWatermark" borne l'état Spark (sans cela il grossit indéfiniment).
        .dropDuplicatesWithinWatermark(["symbol", "trade_id"])
    )

# ── Calcul des indicateurs par fenêtre ───────────────────────────────────────

def compute_window_indicators(parsed_df, window_duration: str):
    """
    Pour chaque fenêtre temporelle :
    1. Agrégation : sum(price*qty), sum(qty), count, min, max, stddev
    2. Calcul VWAP, RSI approximé, Bollinger Bands
    """
    window_spec = F.window("event_time", window_duration, window_duration)

    agg_df = (
        parsed_df
        .groupBy("symbol", window_spec)
        .agg(
            # VWAP components
            F.sum(F.col("price") * F.col("quantity")).alias("sum_pq"),
            F.sum("quantity").alias("sum_q"),

            # Price stats (pour Bollinger)
            F.avg("price").alias("price_avg"),
            F.stddev("price").alias("price_stddev"),
            F.min("price").alias("price_min"),
            F.max("price").alias("price_max"),
            F.last("price").alias("price_close"),
            F.first("price").alias("price_open"),

            # Volume stats
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),

            # Side distribution
            F.sum(
                F.when(F.col("side") == "buy", F.col("quantity")).otherwise(0)
            ).alias("buy_volume"),
            F.sum(
                F.when(F.col("side") == "sell", F.col("quantity")).otherwise(0)
            ).alias("sell_volume"),
        )
    )

    # Indicateurs via expressions Spark natives (pas d'UDF Python : pas de sérialisation JVM ↔ Python)
    bands = bollinger_cols(F.col("price_avg"), F.col("price_stddev"), 2.0)
    result_df = (
        agg_df
        .withColumn("vwap",          vwap_col(F.col("sum_pq"), F.col("sum_q")))
        .withColumn("rsi_approx",    rsi_approx_col(F.col("price_open"), F.col("price_close"), F.col("price_avg")))
        .withColumn("bb_upper",      bands["upper"])
        .withColumn("bb_lower",      bands["lower"])
        .withColumn("bb_middle",     F.col("price_avg"))
        .withColumn("bb_bandwidth",  bands["bandwidth"])
        .withColumn("buy_ratio",     F.col("buy_volume") / F.col("sum_q"))
        .withColumn("window_label",  F.lit(window_duration))
        .withColumn("window_start",  F.col("window.start"))
        .withColumn("window_end",    F.col("window.end"))
        .withColumn("computed_at",   F.current_timestamp())
        .drop("window", "sum_pq", "sum_q")
    )

    return result_df

# ── Sérialisation vers Kafka ──────────────────────────────────────────────────

def to_kafka_format(df):
    """
    Transforme le DataFrame en format clé/valeur JSON pour Kafka.
    Clé = symbol, Valeur = JSON du message d'indicateurs.
    """

    value_cols = F.to_json(F.struct(*[F.col(c) for c in df.columns]))

    return (
        df
        .withColumn("key",   F.col("symbol").cast("string"))
        .withColumn("value", value_cols)
        .select("key", "value")
    )

# ── Sink Kafka ────────────────────────────────────────────────────────────────

def write_to_kafka(df, window_label: str):
    safe_label = window_label.replace(" ", "_")
    checkpoint = f"{CHECKPOINT_DIR}/indicators_{safe_label}"

    return (
        to_kafka_format(df)
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("topic", TOPIC_OUT)
        .option("checkpointLocation", checkpoint)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

# ── Sink console (debug) ──────────────────────────────────────────────────────

def write_to_console(df, window_label: str):
    safe_label = window_label.replace(" ", "_")
    checkpoint = f"{CHECKPOINT_DIR}/console_{safe_label}"

    return (
        df
        .select(
            "symbol", "window_label", "window_start",
            F.round("vwap", 4).alias("vwap"),
            F.round("rsi_approx", 2).alias("rsi_approx"),
            F.round("bb_upper", 4).alias("bb_upper"),
            F.round("bb_middle", 4).alias("bb_middle"),
            F.round("bb_lower", 4).alias("bb_lower"),
            F.round("buy_ratio", 4).alias("buy_ratio"),
            "trade_count", "volume"
        )
        .writeStream
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", checkpoint)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("CryptoFlow — Spark Streaming Job démarré")
    print(f"  Source  : {TOPIC_IN}")
    print(f"  Sink    : {TOPIC_OUT}")
    print(f"  Windows : {WINDOWS}")
    print("=" * 60)

    # Lecture et parsing
    raw_df    = read_kafka(spark)
    parsed_df = parse_messages(raw_df)

    # Lancer un job par fenêtre temporelle
    queries = []
    for window in WINDOWS:
        indicators_df = compute_window_indicators(parsed_df, window)

        # Production → Kafka
        q_kafka = write_to_kafka(indicators_df, window)
        queries.append(q_kafka)

        # Debug → console (désactivé par défaut : ENABLE_CONSOLE_SINK=true pour l'activer)
        if ENABLE_CONSOLE:
            queries.append(write_to_console(indicators_df, window))

    # Attendre la fin de tous les streams
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()