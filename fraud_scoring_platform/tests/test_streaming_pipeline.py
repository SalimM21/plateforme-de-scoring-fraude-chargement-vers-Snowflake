# Tests du pipeline temps réel
# -----------------------------------
# Tests unitaires du pipeline streaming temps réel :
# - Lecture Kafka (transactions)
# - Transformation PySpark Streaming
# - Feature engineering (moyennes glissantes, anomalies)
# - Écriture vers Snowflake / BigQuery
# -----------------------------------

import pytest
from unittest.mock import patch, MagicMock
from streaming.streaming_job import start_streaming_job
from streaming.feature_engineering import compute_features
from streaming.sink_snowflake import write_to_warehouse


@pytest.fixture
def mock_streaming_data():
    """Jeu de données simulé pour le flux Kafka."""
    return [
        {"transaction_id": "tx001", "customer_id": 1, "amount": 150.0, "timestamp": "2025-10-14T10:00:00Z"},
        {"transaction_id": "tx002", "customer_id": 2, "amount": 50.0, "timestamp": "2025-10-14T10:00:10Z"},
    ]


@patch("streaming.streaming_job.SparkSession")
@patch("streaming.streaming_job.spark_read_kafka")
def test_streaming_job_initialization(mock_read_kafka, mock_spark, mock_streaming_data):
    """Teste que le job Spark Structured Streaming démarre correctement et lit depuis Kafka."""
    spark_mock = MagicMock()
    mock_spark.builder.getOrCreate.return_value = spark_mock

    df_mock = MagicMock()
    mock_read_kafka.return_value = df_mock

    start_streaming_job(spark_mock)
    mock_read_kafka.assert_called_once()
    assert df_mock is not None


def test_feature_engineering(mock_streaming_data):
    """Vérifie le calcul correct des features (moyennes glissantes, détection d’anomalies)."""
    result_df = compute_features(mock_streaming_data)
    assert isinstance(result_df, list)
    assert "avg_amount_rolling" in result_df[0]
    assert "anomaly_flag" in result_df[0]


@patch("streaming.sink_snowflake.write_to_warehouse")
def test_sink_write(mock_write):
    """Vérifie l’appel de la fonction d’écriture vers Snowflake/BigQuery."""
    df_mock = MagicMock()
    write_to_warehouse(df_mock, destination="BigQuery")
    mock_write.assert_called_once_with(df_mock, destination="BigQuery")


@patch("streaming.feature_engineering.compute_features")
@patch("streaming.sink_snowflake.write_to_warehouse")
def test_end_to_end_streaming(mock_sink, mock_features, mock_streaming_data):
    """Test bout-à-bout du pipeline streaming (transformation + écriture)."""
    mock_features.return_value = [{"customer_id": 1, "avg_amount_rolling": 120.0, "anomaly_flag": False}]
    df_mock = MagicMock()

    # Simulation du flux : transformation puis envoi
    transformed = compute_features(mock_streaming_data)
    write_to_warehouse(df_mock, destination="Snowflake")

    assert len(transformed) > 0
    assert "avg_amount_rolling" in transformed[0]
    mock_sink.assert_called_once()
