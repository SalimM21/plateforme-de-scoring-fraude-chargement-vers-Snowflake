# Tests unitaires du pipeline batch
# -----------------------------------
# Tests unitaires pour le pipeline batch :
# - Extraction (CRM + Transactions)
# - Transformation PySpark
# - Chargement vers l'entrepôt (Snowflake / BigQuery)
# - Vérification des règles de qualité des données
# -----------------------------------

import pytest
from unittest.mock import patch, MagicMock
from batch.extraction import extract_crm_data, extract_transaction_data
from batch.transform_pyspark import transform_data
from batch.load_warehouse import load_to_warehouse
from quality.great_expectations_checks import validate_data_quality

@pytest.fixture
def mock_data():
    """Jeu de données simulé pour tests unitaires."""
    return [
        {"customer_id": 1, "amount": 120.0, "date": "2025-10-13"},
        {"customer_id": 2, "amount": 85.0, "date": "2025-10-13"},
    ]

@patch("batch.extraction.extract_crm_data")
@patch("batch.extraction.extract_transaction_data")
def test_extraction(mock_txn, mock_crm, mock_data):
    """Vérifie que les fonctions d'extraction renvoient les bons formats."""
    mock_crm.return_value = mock_data
    mock_txn.return_value = mock_data

    crm = extract_crm_data()
    txn = extract_transaction_data()

    assert isinstance(crm, list)
    assert isinstance(txn, list)
    assert "customer_id" in crm[0]
    assert "amount" in txn[0]

def test_transformation_pyspark(mock_data):
    """Teste la transformation PySpark (nettoyage et enrichissement)."""
    with patch("batch.transform_pyspark.SparkSession.builder.getOrCreate") as mock_spark:
        mock_df = MagicMock()
        mock_spark.return_value.createDataFrame.return_value = mock_df
        mock_df.count.return_value = len(mock_data)

        result_df = transform_data(mock_data)
        assert result_df.count() == len(mock_data)

@patch("batch.load_warehouse.load_to_warehouse")
def test_load_to_warehouse(mock_load):
    """Teste que le chargement est bien appelé avec les bons arguments."""
    df_mock = MagicMock()
    load_to_warehouse(df_mock, destination="Snowflake")
    mock_load.assert_called_once_with(df_mock, destination="Snowflake")

@patch("quality.great_expectations_checks.validate_data_quality")
def test_data_quality(mock_validate):
    """Vérifie que les contrôles de qualité sont bien effectués."""
    mock_validate.return_value = {"status": "success", "passed_expectations": 10}
    result = validate_data_quality()
    assert result["status"] == "success"
    assert result["passed_expectations"] == 10
