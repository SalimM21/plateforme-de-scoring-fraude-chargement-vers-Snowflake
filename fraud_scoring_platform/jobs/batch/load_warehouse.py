# Chargement dans Snowflake / BigQuery
# load_warehouse.py
# ---------------------------------------------------
# Objectif : Charger les données transformées dans le Data Warehouse (Snowflake / BigQuery)
# Compatible avec Airflow et PySpark
# ---------------------------------------------------

from pyspark.sql import SparkSession
import os


def create_spark_session(app_name="PySparkLoadWarehouse"):
    """
    Crée une session Spark configurée pour le chargement vers Snowflake et BigQuery.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Support BigQuery
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        # Support Snowflake
        .config("spark.jars", "/opt/spark/jars/snowflake-jdbc.jar,/opt/spark/jars/spark-snowflake_2.12.jar")
        .getOrCreate()
    )
    return spark


# --------------------------
# 🔹 Snowflake Loader
# --------------------------
def load_to_snowflake(df, table_name, mode="overwrite"):
    """
    Charge les données dans Snowflake.
    """
    SNOWFLAKE_OPTIONS = {
        "sfURL": os.getenv("SNOWFLAKE_URL"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "sfRole": os.getenv("SNOWFLAKE_ROLE"),
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    }

    print(f"🚀 Chargement des données vers Snowflake → {table_name}")
    (
        df.write
        .format("snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", table_name)
        .mode(mode)
        .save()
    )
    print("✅ Chargement Snowflake terminé avec succès.")


# --------------------------
# 🔹 BigQuery Loader
# --------------------------
def load_to_bigquery(df, table_name, project_id, dataset, mode="overwrite"):
    """
    Charge les données dans BigQuery.
    """
    full_table = f"{project_id}:{dataset}.{table_name}"
    print(f"🚀 Chargement des données vers BigQuery → {full_table}")

    (
        df.write
        .format("bigquery")
        .option("table", full_table)
        .option("temporaryGcsBucket", os.getenv("GCS_TEMP_BUCKET", "gcs-temp-bucket"))
        .mode(mode)
        .save()
    )
    print("✅ Chargement BigQuery terminé avec succès.")


# --------------------------
# 🔹 Main Pipeline
# --------------------------
def main(target="snowflake"):
    """
    Pipeline principal de chargement.
    """
    spark = create_spark_session("LoadWarehouse")

    # Lecture des données transformées
    processed_path = "data/processed/transactions_joined/"
    df = spark.read.parquet(processed_path)

    # Vérification des données
    print(f"📊 Nombre d’enregistrements à charger : {df.count()}")

    if target.lower() == "snowflake":
        load_to_snowflake(df, table_name="CUSTOMERS_TRANSACTIONS_FACT")
    elif target.lower() == "bigquery":
        load_to_bigquery(
            df,
            table_name="customers_transactions_fact",
            project_id=os.getenv("BQ_PROJECT_ID", "mlops-platform"),
            dataset=os.getenv("BQ_DATASET", "crm_analytics"),
        )
    else:
        raise ValueError("❌ Cible de chargement invalide : choisir 'snowflake' ou 'bigquery'")

    spark.stop()


if __name__ == "__main__":
    # Exécution manuelle possible (utile pour tests Airflow locaux)
    # Exemple :
    #   python load_warehouse.py snowflake
    #   python load_warehouse.py bigquery
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "snowflake"
    main(target)
