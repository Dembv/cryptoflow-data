# path spark/delta_sink_job.py

"""
CryptoFlow — Sprint 3
Spark Structured Streaming → Delta Lake (Bronze)

Lit les 4 topics Kafka et écrit en Bronze sur MinIO (S3a).
Architecture Medallion :
  Bronze : raw, append-only, pas de transformation
  Silver : à implémenter en batch dbt (Sprint 4)
  Gold   : à implémenter en batch dbt (Sprint 4)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY     = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY     = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DELTA_PATH     = os.getenv("DELTA_LAKE_PATH", "s3a://cryptoflow")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/cryptoflow/checkpoints")

TOPICS = {
    "crypto.prices":     "bronze_prices",
    "crypto.history":    "bronze_history",
    "crypto.onchain":    "bronze_onchain",
    "crypto.indicators": "bronze_indicators",
}

# ── SparkSession ──────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoFlow-DeltaSink")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO / S3a
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.kafka:kafka-clients:3.5.0,"
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .getOrCreate()
    )

# ── Sink Bronze ───────────────────────────────────────────────────────────────

def write_bronze(spark: SparkSession, topic: str, table_name: str):
    """
    Lit un topic Kafka et écrit le JSON brut en Delta Lake Bronze.
    Pas de parsing — on stocke value as-is + métadonnées Kafka.
    """
    checkpoint = f"{CHECKPOINT_DIR}/delta_{table_name}"
    output_path = f"{DELTA_PATH}/bronze/{table_name}"

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")   # Bronze = tout capturer
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
    )

    bronze_df = (
        raw_df
        .select(
            F.col("key").cast(StringType()).alias("kafka_key"),
            F.col("value").cast(StringType()).alias("raw_json"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        # Partition par date pour optimiser les lectures
        .withColumn("date_partition", F.to_date("kafka_timestamp"))
    )

    return (
        bronze_df
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .option("path", output_path)
        .partitionBy("date_partition", "topic")
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .start()
    )

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("CryptoFlow — Delta Lake Bronze Sink démarré")
    print(f"  MinIO   : {MINIO_ENDPOINT}")
    print(f"  Path    : {DELTA_PATH}/bronze/")
    print(f"  Topics  : {list(TOPICS.keys())}")
    print("=" * 60)

    queries = []
    for topic, table_name in TOPICS.items():
        q = write_bronze(spark, topic, table_name)
        queries.append(q)
        print(f"  ✓ Stream démarré : {topic} → {DELTA_PATH}/bronze/{table_name}")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()