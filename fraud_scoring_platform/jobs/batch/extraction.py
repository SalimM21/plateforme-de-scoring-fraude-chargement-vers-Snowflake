# Extraction des données CRM et transactions
"""
extraction.py
-------------
Extraction des données CRM et transactions J-1.
Les données sont ensuite stockées localement (ou sur un bucket S3/GCS) 
pour être consommées par le job PySpark de transformation.
"""

import os
import pandas as pd
import datetime
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# ==========================
# ⚙️ Configuration
# ==========================
DATA_DIR = "/opt/airflow/data/raw"
os.makedirs(DATA_DIR, exist_ok=True)

CRM_API_URL = "https://api.company.com/crm/customers"
TRANSACTIONS_DB = {
    "host": "transactions-db",
    "port": 5432,
    "database": "transactions",
    "user": "airflow",
    "password": "secure_password"
}

DATE_J1 = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==========================
# 📦 Extraction CRM (via API REST)
# ==========================
def extract_crm():
    logging.info("📡 Extraction des données CRM depuis l’API...")
    try:
        response = requests.get(CRM_API_URL, timeout=30)
        response.raise_for_status()
        crm_data = response.json()
        df_crm = pd.DataFrame(crm_data)
        output_path = os.path.join(DATA_DIR, f"crm_{DATE_J1}.parquet")
        df_crm.to_parquet(output_path, index=False)
        logging.info(f"✅ Données CRM extraites et sauvegardées dans {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"❌ Erreur d’extraction CRM : {e}")
        raise

# ==========================
# 💳 Extraction Transactions J-1 (via PostgreSQL)
# ==========================
def extract_transactions():
    logging.info("💾 Extraction des transactions J-1 depuis PostgreSQL...")
    query = f"""
        SELECT * 
        FROM transactions
        WHERE DATE(transaction_date) = '{DATE_J1}';
    """
    try:
        conn = psycopg2.connect(**TRANSACTIONS_DB)
        df_tx = pd.read_sql_query(query, conn)
        conn.close()
        output_path = os.path.join(DATA_DIR, f"transactions_{DATE_J1}.parquet")
        df_tx.to_parquet(output_path, index=False)
        logging.info(f"✅ Transactions J-1 sauvegardées dans {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"❌ Erreur d’extraction Transactions : {e}")
        raise

# ==========================
# 🚀 Main
# ==========================
if __name__ == "__main__":
    logging.info("🚀 Début de l’extraction CRM + Transactions J-1")
    crm_file = extract_crm()
    tx_file = extract_transactions()
    logging.info(f"🎯 Extraction terminée : CRM → {crm_file}, Transactions → {tx_file}")
