# Écriture vers Snowflake / BigQuery
# sink_snowflake.py
# ---------------------------------------------------
# Objectif : Écrire les données finales vers Snowflake ou BigQuery
# Compatible avec les jobs batch et streaming Spark
# ---------------------------------------------------

from pyspark.sql import DataFrame
import os


# ----------------------------
# 🔹 Écriture vers Snowflake
# ----------------------------
def write_to_snowflake(df: DataFrame, table_name: str):
    """
    Écrit un DataFrame Spark vers Snowflake.
    
    Args:
        df (DataFrame): Données à insérer
        table_name (str): Nom de la table cible
    """
    SNOWFLAKE_OPTIONS = {
        "sfURL": os.getenv("SNOWFLAKE_URL"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    }

    print(f"🚀 Chargement vers Snowflake -> table {table_name}")

    (
        df.write
        .format("snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", table_name)
        .mode("append")
        .save()
    )

    print(f"✅ Données écrites dans Snowflake : {table_name}")


# ----------------------------
# 🔹 Écriture vers BigQuery
# ----------------------------
def write_to_bigquery(df: DataFrame, table_name: str):
    """
    Écrit un DataFrame Spark vers BigQuery.
    
    Args:
        df (DataFrame): Données à insérer
        table_name (str): Nom de la table cible au format dataset.table
    """
    project_id = os.getenv("BQ_PROJECT_ID", "mlops-platform")
    dataset = os.getenv("BQ_DATASET", "data_warehouse")
    full_table = f"{project_id}:{dataset}.{table_name}"

    print(f"🚀 Chargement vers BigQuery -> table {full_table}")

    (
        df.write
        .format("bigquery")
        .option("table", full_table)
        .option("temporaryGcsBucket", os.getenv("GCS_TEMP_BUCKET", "gcs-temp-bucket"))
        .mode("append")
        .save()
    )

    print(f"✅ Données écrites dans BigQuery : {full_table}")


# ----------------------------
# 🔹 Wrapper générique
# ----------------------------
def sink_data(df: DataFrame, table_name: str, target: str = "snowflake"):
    """
    Route les données vers le bon entrepôt (Snowflake ou BigQuery).
    
    Args:
        df (DataFrame): Données transformées
        table_name (str): Nom de la table cible
        target (str): 'snowflake' ou 'bigquery'
    """
    if target == "snowflake":
        write_to_snowflake(df, table_name)
    elif target == "bigquery":
        write_to_bigquery(df, table_name)
    else:
        raise ValueError("❌ Cible inconnue. Utilisez 'snowflake' ou 'bigquery'.")


# ----------------------------
# 🔹 Exemple d’exécution
# ----------------------------
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("SinkSnowflakeBigQuery")
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .config("spark.jars",
                "/opt/spark/jars/snowflake-jdbc.jar,/opt/spark/jars/spark-snowflake_2.12.jar")
        .getOrCreate()
    )

    # Exemple : DataFrame simulé
    data = [
        ("C001", 1200.50, "2025-10-14 10:30:00"),
        ("C002", 850.75, "2025-10-14 10:31:00"),
    ]
    columns = ["customer_id", "amount", "timestamp"]

    df = spark.createDataFrame(data, columns)

    # Appel de la fonction
    sink_data(df, "TRANSACTIONS_ANALYSED", target="snowflake")

    spark.stop()
