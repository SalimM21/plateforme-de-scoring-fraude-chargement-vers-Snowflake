# Job Spark Structured Streaming avec Kafka
# streaming_job.py
# ---------------------------------------------------
# Objectif : Traiter les transactions en temps réel via Spark Structured Streaming
# - Ingestion Kafka
# - Feature engineering (moyenne glissante, anomalies)
# - Écriture vers Snowflake ou BigQuery
# ---------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, window, count, stddev, when
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os


def create_spark_session(app_name="StreamingTransactions"):
    """
    Initialise une session Spark avec support Kafka, Snowflake et BigQuery.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .config("spark.jars", 
                "/opt/spark/jars/snowflake-jdbc.jar,/opt/spark/jars/spark-snowflake_2.12.jar")
        .getOrCreate()
    )
    return spark


def read_kafka_stream(spark):
    """
    Lecture du flux Kafka en temps réel.
    """
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "transactions")

    print(f"📡 Lecture du topic Kafka '{kafka_topic}' à partir de {kafka_bootstrap}")

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )


def parse_transactions(df):
    """
    Parse le JSON Kafka et applique le schéma.
    """
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    parsed_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    return parsed_df


def feature_engineering(df):
    """
    Ajoute des features : moyenne glissante, détection d'anomalies.
    """
    windowed_df = (
        df.groupBy(
            window(col("timestamp"), "10 minutes", "5 minutes"),
            col("customer_id")
        )
        .agg(
            avg("amount").alias("rolling_avg_amount"),
            stddev("amount").alias("stddev_amount"),
            count("*").alias("transaction_count")
        )
    )

    # Détection d’anomalies : transaction_count trop élevé ou montant déviant fortement
    anomalies = windowed_df.withColumn(
        "anomaly_flag",
        when((col("rolling_avg_amount") > 10000) | (col("transaction_count") > 50), 1).otherwise(0)
    )

    return anomalies


# --------------------------
# 🔹 Écriture vers Snowflake
# --------------------------
def write_to_snowflake(batch_df, batch_id):
    """
    Écrit chaque micro-batch dans Snowflake.
    """
    SNOWFLAKE_OPTIONS = {
        "sfURL": os.getenv("SNOWFLAKE_URL"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    }

    (
        batch_df.write
        .format("snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", "STREAMING_TRANSACTIONS_MONITORING")
        .mode("append")
        .save()
    )

    print(f"✅ Batch {batch_id} chargé dans Snowflake ({batch_df.count()} lignes)")


# --------------------------
# 🔹 Écriture vers BigQuery
# --------------------------
def write_to_bigquery(batch_df, batch_id):
    """
    Écrit chaque micro-batch dans BigQuery.
    """
    project_id = os.getenv("BQ_PROJECT_ID", "mlops-platform")
    dataset = os.getenv("BQ_DATASET", "streaming_data")
    table = f"{project_id}:{dataset}.streaming_transactions_monitoring"

    (
        batch_df.write
        .format("bigquery")
        .option("table", table)
        .option("temporaryGcsBucket", os.getenv("GCS_TEMP_BUCKET", "gcs-temp-bucket"))
        .mode("append")
        .save()
    )

    print(f"✅ Batch {batch_id} chargé dans BigQuery ({batch_df.count()} lignes)")


def main(target="snowflake"):
    """
    Pipeline principal de streaming.
    """
    spark = create_spark_session()

    kafka_df = read_kafka_stream(spark)
    parsed_df = parse_transactions(kafka_df)
    engineered_df = feature_engineering(parsed_df)

    query = (
        engineered_df.writeStream
        .outputMode("update")
        .foreachBatch(write_to_snowflake if target == "snowflake" else write_to_bigquery)
        .option("checkpointLocation", "checkpoints/streaming_transactions/")
        .start()
    )

    print("🚀 Job Spark Structured Streaming démarré...")
    query.awaitTermination()


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "snowflake"
    main(target)
