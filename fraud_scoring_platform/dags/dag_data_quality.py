# DAG Airflow pour la validation et le reporting de la qualité
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import os
import subprocess
import logging

# ==========================
# ⚙️ CONFIGURATION DU DAG
# ==========================
default_args = {
    "owner": "data_quality_team",
    "depends_on_past": False,
    "email": ["data-quality@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="dag_data_quality",
    description="Pipeline de validation et reporting de la qualité des données (Great Expectations)",
    schedule_interval="0 8 * * 1",  # Tous les lundis à 08h
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["quality", "great_expectations", "reporting"]
)

# ==========================
# 📂 CHEMINS
# ==========================
BASE_PATH = "/opt/airflow/jobs/quality"
VALIDATION_SCRIPT = os.path.join(BASE_PATH, "run_great_expectations.py")
REPORT_SCRIPT = os.path.join(BASE_PATH, "generate_quality_report.py")
ARCHIVE_PATH = "/opt/airflow/reports/quality"
os.makedirs(ARCHIVE_PATH, exist_ok=True)

REPORT_HTML = os.path.join(ARCHIVE_PATH, "weekly_quality_report.html")

# ==========================
# 🧠 FONCTIONS PYTHON
# ==========================
def run_great_expectations(**context):
    """
    Lance les validations Great Expectations sur les datasets (clients, transactions…)
    """
    logging.info("🚀 Exécution des validations Great Expectations...")
    result = subprocess.run(["python3", VALIDATION_SCRIPT], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur GE : {result.stderr}")
    logging.info(result.stdout)

def generate_report(**context):
    """
    Génère un rapport de synthèse (HTML/PNG) à partir des résultats GE
    """
    logging.info("📊 Génération du rapport hebdomadaire de qualité...")
    result = subprocess.run(["python3", REPORT_SCRIPT], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur génération rapport : {result.stderr}")
    logging.info(result.stdout)

# ==========================
# 🧱 TÂCHES AIRFLOW
# ==========================
validate_data = PythonOperator(
    task_id="validate_data_with_ge",
    python_callable=run_great_expectations,
    dag=dag
)

generate_html_report = PythonOperator(
    task_id="generate_html_report",
    python_callable=generate_report,
    dag=dag
)

send_email_report = EmailOperator(
    task_id="send_email_quality_report",
    to=["data-team@company.com"],
    subject="📈 Rapport hebdomadaire - Qualité des données",
    html_content=open(REPORT_HTML).read() if os.path.exists(REPORT_HTML) else "<p>Rapport non disponible</p>",
    files=[REPORT_HTML] if os.path.exists(REPORT_HTML) else [],
    dag=dag
)

# ==========================
# 🔗 DÉPENDANCES
# ==========================
validate_data >> generate_html_report >> send_email_report
