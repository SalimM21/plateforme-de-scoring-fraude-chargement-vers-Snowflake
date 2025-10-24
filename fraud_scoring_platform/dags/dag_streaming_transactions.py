# DAG Airflow pour le pipeline streaming (transactions en temps réel)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import subprocess
import logging
import os

# ============================
# ⚙️ Configuration du DAG
# ============================
default_args = {
    "owner": "streaming_team",
    "depends_on_past": False,
    "email": ["alerts@dataplatform.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    dag_id="dag_streaming_transactions",
    description="Pipeline streaming : Ingestion Kafka -> Feature Engineering (moyennes glissantes, anomalies) -> Sink Snowflake/BigQuery",
    schedule_interval="@once",  # déclenchement manuel (streaming continu)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["streaming", "spark", "kafka", "snowflake", "bigquery"]
)

# ============================
# 📂 Chemins et scripts
# ============================
BASE_PATH = "/opt/airflow/jobs/streaming"
STREAMING_JOB = os.path.join(BASE_PATH, "streaming_job.py")
FEATURE_ENGINEERING = os.path.join(BASE_PATH, "feature_engineering.py")
SINK_SCRIPT = os.path.join(BASE_PATH, "sink_snowflake.py")

# ============================
# 🧠 Fonctions Python
# ============================
def start_streaming_job(**context):
    """
    Démarre le job Spark Structured Streaming
    pour ingérer les transactions en temps réel depuis Kafka.
    """
    logging.info("🚀 Lancement du job Spark Structured Streaming (Kafka -> DataFrame)...")
    result = subprocess.run(["spark-submit", STREAMING_JOB], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur Spark Streaming : {result.stderr}")
    logging.info(result.stdout)

def run_feature_engineering(**context):
    """
    Exécute le traitement de feature engineering
    (moyennes glissantes + détection d'anomalies).
    """
    logging.info("🧠 Application du Feature Engineering...")
    result = subprocess.run(["python3", FEATURE_ENGINEERING], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur Feature Engineering : {result.stderr}")
    logging.info(result.stdout)

def sink_to_warehouse(**context):
    """
    Écrit le flux traité vers Snowflake ou BigQuery.
    """
    logging.info("🏦 Écriture du flux traité dans Snowflake/BigQuery...")
    result = subprocess.run(["python3", SINK_SCRIPT], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur d'écriture vers le Data Warehouse : {result.stderr}")
    logging.info(result.stdout)

# ============================
# 🧱 Tâches Airflow
# ============================
start_stream = PythonOperator(
    task_id="start_streaming_job",
    python_callable=start_streaming_job,
    dag=dag,
)

feature_engineering = PythonOperator(
    task_id="run_feature_engineering",
    python_callable=run_feature_engineering,
    dag=dag,
)

sink_warehouse = PythonOperator(
    task_id="sink_to_warehouse",
    python_callable=sink_to_warehouse,
    dag=dag,
)

# ============================
# 🔗 Dépendances du pipeline
# ============================
start_stream >> feature_engineering >> sink_warehouse
