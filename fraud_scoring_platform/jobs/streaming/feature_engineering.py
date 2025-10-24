# Calcul des moyennes glissantes et détection d’anomalies

# ---------------------------------------------------
# Objectif : calculer des moyennes glissantes et détecter des anomalies
# à partir des données de transactions nettoyées.
# Compatible avec Spark Batch ou Streaming.
# ---------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, count, when, window
)
from pyspark.sql import DataFrame


def create_spark_session(app_name="FeatureEngineering"):
    """
    Crée une session Spark.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    return spark


def compute_features(df: DataFrame, time_window="10 minutes", slide="5 minutes") -> DataFrame:
    """
    Calcule les moyennes glissantes, écart-types et anomalies.
    
    Args:
        df (DataFrame): données contenant 'customer_id', 'amount', 'timestamp'
        time_window (str): taille de la fenêtre glissante
        slide (str): intervalle de glissement
    
    Returns:
        DataFrame enrichi avec :
          - rolling_avg_amount
          - stddev_amount
          - transaction_count
          - anomaly_flag
    """
    windowed_df = (
        df.groupBy(
            window(col("timestamp"), time_window, slide),
            col("customer_id")
        )
        .agg(
            avg("amount").alias("rolling_avg_amount"),
            stddev("amount").alias("stddev_amount"),
            count("*").alias("transaction_count")
        )
    )

    # Détection simple d’anomalies :
    # - Montant moyen trop élevé
    # - Volume de transactions anormalement élevé
    # - Ecart-type très élevé
    enriched_df = windowed_df.withColumn(
        "anomaly_flag",
        when(
            (col("rolling_avg_amount") > 10000) |
            (col("transaction_count") > 50) |
            (col("stddev_amount") > 5000),
            1
        ).otherwise(0)
    )

    return enriched_df


def main(input_path="data/processed/transactions_cleaned/", output_path="data/processed/transactions_features/"):
    """
    Script exécutable pour calculer les features sur les données batch.
    """
    spark = create_spark_session()

    # Lecture des données nettoyées
    df = spark.read.parquet(input_path)

    print(f"📊 Données chargées depuis {input_path}")
    print(f"Nombre d'enregistrements : {df.count()}")

    # Calcul des features
    features_df = compute_features(df)

    # Écriture du résultat
    (
        features_df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"✅ Features calculées et enregistrées dans : {output_path}")

    spark.stop()


if __name__ == "__main__":
    import sys
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/processed/transactions_cleaned/"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "data/processed/transactions_features/"
    main(input_path, output_path)
