# DAG Airflow pour le pipeline batch (extraction CRM + transactions J-1)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
import subprocess
import logging

# ======================
# 🧩 Configuration DAG
# ======================
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="dag_batch_customers_transactions",
    description="Pipeline batch : Extraction CRM + Transactions J-1 -> Transformation PySpark -> Load Snowflake/BigQuery",
    schedule_interval="0 3 * * *",  # tous les jours à 03h du matin
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["batch", "pyspark", "snowflake", "bigquery"]
)

# ======================
# 🧾 Définition des chemins
# ======================
BASE_PATH = "/opt/airflow/jobs/batch"
EXTRACT_SCRIPT = os.path.join(BASE_PATH, "extraction.py")
TRANSFORM_SCRIPT = os.path.join(BASE_PATH, "transform_pyspark.py")
LOAD_SCRIPT = os.path.join(BASE_PATH, "load_warehouse.py")

# ======================
# 🧠 Fonctions Python
# ======================
def extract_data(**context):
    logging.info("🚀 Extraction des données CRM et Transactions J-1...")
    result = subprocess.run(["python3", EXTRACT_SCRIPT], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur d'extraction : {result.stderr}")
    logging.info(result.stdout)

def transform_data(**context):
    logging.info("🧹 Transformation et nettoyage des données via PySpark...")
    result = subprocess.run(["python3", TRANSFORM_SCRIPT], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur de transformation : {result.stderr}")
    logging.info(result.stdout)

def load_data(**context):
    logging.info("🏦 Chargement des données transformées dans Snowflake/BigQuery...")
    result = subprocess.run(["python3", LOAD_SCRIPT], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur de chargement : {result.stderr}")
    logging.info(result.stdout)

# ======================
# 🧱 Tâches Airflow
# ======================
extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

# ======================
# 🔗 Dépendances du pipeline
# ======================
extract_task >> transform_task >> load_task
