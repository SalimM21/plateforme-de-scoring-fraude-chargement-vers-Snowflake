# ===============================================
# Script PowerShell : création d’arborescence fraud_scoring_platform/
# Auteur : Salim Majide
# Description : Crée la structure complète du projet Data Engineering
# ===============================================

$root = "fraud_scoring_platform"

function New-File {
    param(
        [string]$path,
        [string]$content = ""
    )
    $dir = Split-Path $path
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    Set-Content -Path $path -Value $content
    Write-Host "✅ Créé : $path"
}

# ===============================================
# DAGS (Airflow)
# ===============================================
New-File "$root/dags/dag_batch_customers_transactions.py" "# DAG Airflow pour le pipeline batch (extraction CRM + transactions J-1)"
New-File "$root/dags/dag_streaming_transactions.py" "# DAG Airflow pour le pipeline streaming (transactions en temps réel)"
New-File "$root/dags/dag_data_quality.py" "# DAG Airflow pour la validation et le reporting de la qualité"

# ===============================================
# JOBS
# ===============================================
# Batch
New-File "$root/jobs/batch/extraction.py" "# Extraction des données CRM et transactions"
New-File "$root/jobs/batch/transform_pyspark.py" "# Nettoyage et transformation PySpark"
New-File "$root/jobs/batch/load_warehouse.py" "# Chargement dans Snowflake / BigQuery"

# Streaming
New-File "$root/jobs/streaming/streaming_job.py" "# Job Spark Structured Streaming avec Kafka"
New-File "$root/jobs/streaming/feature_engineering.py" "# Calcul des moyennes glissantes et détection d’anomalies"
New-File "$root/jobs/streaming/sink_snowflake.py" "# Écriture vers Snowflake / BigQuery"

# Quality
New-File "$root/jobs/quality/great_expectations_checks.py" "# Définition des règles Great Expectations"
New-File "$root/jobs/quality/generate_quality_report.py" "# Génération du rapport de qualité hebdomadaire"
New-File "$root/jobs/quality/notify_quality_status.py" "# Notification (Slack/Email)"

# ===============================================
# CONFIGS
# ===============================================
New-File "$root/configs/airflow_config.yaml" "# Configuration Airflow (connexions, scheduling)"
New-File "$root/configs/spark_config.yaml" "# Configuration Spark (batch et streaming)"
New-File "$root/configs/warehouse_config.yaml" "# Connexions Snowflake/BigQuery"
New-File "$root/configs/quality_rules.yaml" "# Règles de validation de la qualité des données"

# ===============================================
# UTILS
# ===============================================
New-File "$root/utils/logger.py" "# Gestion des logs centralisés"
New-File "$root/utils/monitoring.py" "# Suivi de la latence et métriques"
New-File "$root/utils/schema_validation.py" "# Validation de schéma Avro/Parquet"
New-File "$root/utils/helpers.py" "# Fonctions utilitaires génériques"

# ===============================================
# REPORTS
# ===============================================
New-File "$root/reports/data_quality_report.html" "<!-- Rapport généré automatiquement par Great Expectations -->"
New-File "$root/reports/metrics_dashboard.csv" "metric,value"

# ===============================================
# TESTS
# ===============================================
New-File "$root/tests/test_batch_pipeline.py" "# Tests unitaires du pipeline batch"
New-File "$root/tests/test_streaming_pipeline.py" "# Tests du pipeline temps réel"
New-File "$root/tests/test_data_quality.py" "# Tests des règles de validation"

# ===============================================
# RACINE DU PROJET
# ===============================================
New-File "$root/Dockerfile" "# Image Airflow + Spark + Great Expectations"
New-File "$root/requirements.txt" "# Dépendances Python"
New-File "$root/README.md" "# Documentation du projet Fraud Scoring Platform"

Write-Host ""
Write-Host "🎯 Arborescence complète créée sous : $root/"
