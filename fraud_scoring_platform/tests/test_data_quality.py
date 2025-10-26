# Tests des règles de validation
# ------------------------------------------------------
# Tests unitaires des règles de validation de la qualité :
# - Vérifie les règles Great Expectations (valeurs nulles, doublons, schéma)
# - Valide la génération du rapport de qualité
# - Simule la notification Slack/Email
# ------------------------------------------------------

import pytest
from unittest.mock import patch, MagicMock
from quality.great_expectations_checks import run_data_quality_checks
from quality.generate_quality_report import generate_quality_report
from quality.notify_quality_status import send_notification


@pytest.fixture
def sample_data():
    """Jeu de données simulé pour les tests de qualité."""
    return [
        {"customer_id": 1, "amount": 100.0, "region": "North"},
        {"customer_id": 2, "amount": 50.0, "region": "East"},
        {"customer_id": 2, "amount": 50.0, "region": "East"},  # doublon volontaire
        {"customer_id": 3, "amount": None, "region": "West"},  # valeur nulle
    ]


@patch("quality.great_expectations_checks.ge")
def test_run_data_quality_checks(mock_ge, sample_data):
    """Vérifie que les règles Great Expectations sont exécutées correctement."""
    mock_context = MagicMock()
    mock_batch = MagicMock()
    mock_result = MagicMock()
    mock_result.success = False

    mock_ge.data_context.DataContext.return_value = mock_context
    mock_context.create_batch.return_value = mock_batch
    mock_context.run_validation_operator.return_value = {"success": False}

    result = run_data_quality_checks(sample_data)
    assert result["success"] is False


@patch("quality.generate_quality_report.generate_html_report")
def test_generate_quality_report(mock_generate):
    """Teste la génération du rapport de qualité HTML."""
    mock_generate.return_value = "/tmp/quality_report.html"
    path = generate_quality_report()
    mock_generate.assert_called_once()
    assert path.endswith(".html")


@patch("quality.notify_quality_status.SlackClient")
def test_send_notification_slack(mock_slack):
    """Vérifie que la notification Slack est envoyée avec succès."""
    mock_client = MagicMock()
    mock_slack.return_value = mock_client

    send_notification("Data Quality Report available", channel="#data-quality")
    mock_client.chat_postMessage.assert_called_once()


@patch("quality.notify_quality_status.smtplib.SMTP")
def test_send_notification_email(mock_smtp):
    """Vérifie que la notification email est envoyée correctement."""
    mock_server = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_server

    send_notification("Data Quality failed", method="email", recipient="team@company.com")
    mock_server.sendmail.assert_called_once()
