# Nettoyage et transformation PySpark
# transform_pyspark.py
# -------------------------------------
# Module de transformation PySpark
# Objectif : nettoyer, transformer et enrichir les données CRM et transactions
# avant le chargement dans Snowflake ou BigQuery.

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, when, lit,
    to_date, concat_ws, current_timestamp, regexp_replace
)
from pyspark.sql.types import DoubleType

def create_spark_session(app_name="PySparkTransformation"):
    """
    Crée une session Spark configurée pour Snowflake/BigQuery.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    return spark


def clean_crm_data(df_crm):
    """
    Nettoie et transforme les données CRM.
    """
    df_crm_cleaned = (
        df_crm
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("is_active", when(col("status") == "active", lit(1)).otherwise(lit(0)))
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
        .dropDuplicates(["customer_id"])
    )
    return df_crm_cleaned


def clean_transaction_data(df_tx):
    """
    Nettoie et transforme les données de transactions.
    """
    df_tx_cleaned = (
        df_tx
        .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("is_valid", when(col("amount") > 0, lit(True)).otherwise(lit(False)))
        .dropDuplicates(["transaction_id"])
    )
    return df_tx_cleaned


def join_crm_transactions(df_crm, df_tx):
    """
    Jointure CRM ↔ Transactions.
    """
    df_joined = (
        df_tx.join(df_crm, on="customer_id", how="left")
        .withColumn("etl_timestamp", current_timestamp())
    )
    return df_joined


def transform_data(spark, input_paths, output_path):
    """
    Exécute le pipeline de transformation complet.
    """
    print("📥 Lecture des données CRM et transactions...")
    df_crm = spark.read.parquet(input_paths["crm"])
    df_tx = spark.read.parquet(input_paths["transactions"])

    print("🧹 Nettoyage des données CRM...")
    df_crm_cleaned = clean_crm_data(df_crm)

    print("🧾 Nettoyage des transactions...")
    df_tx_cleaned = clean_transaction_data(df_tx)

    print("🔗 Jointure CRM ↔ Transactions...")
    df_final = join_crm_transactions(df_crm_cleaned, df_tx_cleaned)

    print("💾 Sauvegarde des données transformées...")
    df_final.write.mode("overwrite").parquet(output_path)

    print("✅ Transformation terminée avec succès !")


if __name__ == "__main__":
    spark = create_spark_session("CRM_Transactions_Transformation")

    input_paths = {
        "crm": "data/raw/crm/",
        "transactions": "data/raw/transactions/"
    }

    output_path = "data/processed/transactions_joined/"

    transform_data(spark, input_paths, output_path)
    spark.stop()
