# Définition des règles Great Expectations
"""
great_expectations_checks.py
----------------------------
Définition des règles de validation des données à l’aide de Great Expectations :
- Validation des données CRM (client)
- Validation des transactions
- Génération automatique des rapports Data Docs.
"""

import great_expectations as ge
from great_expectations.dataset import PandasDataset
import pandas as pd
import os

# 📁 Dossier de configuration Great Expectations
GE_CONFIG_PATH = "great_expectations"

# =========================
# 1️⃣ Validation CRM Dataset
# =========================
def validate_crm_data(df: pd.DataFrame):
    """
    Valide les données CRM :
    - Vérifie la présence des colonnes clés
    - Vérifie la complétude et unicité
    """
    dataset = ge.from_pandas(df)

    dataset.expect_column_to_exist("customer_id")
    dataset.expect_column_to_exist("email")
    dataset.expect_column_values_to_not_be_null("customer_id")
    dataset.expect_column_values_to_be_unique("customer_id")
    dataset.expect_column_values_to_match_regex("email", r"[^@]+@[^@]+\.[^@]+")

    dataset.expect_column_values_to_be_of_type("age", "int64")
    dataset.expect_column_values_to_be_between("age", min_value=18, max_value=100)

    results = dataset.validate()
    return results


# =============================
# 2️⃣ Validation Transactions
# =============================
def validate_transactions_data(df: pd.DataFrame):
    """
    Valide les données de transactions :
    - Vérifie la cohérence des montants
    - Vérifie les types de données
    - Vérifie la présence de clés de jointure (customer_id)
    """
    dataset = ge.from_pandas(df)

    dataset.expect_column_to_exist("transaction_id")
    dataset.expect_column_to_exist("customer_id")
    dataset.expect_column_to_exist("amount")
    dataset.expect_column_values_to_not_be_null("transaction_id")
    dataset.expect_column_values_to_be_unique("transaction_id")
    dataset.expect_column_values_to_be_of_type("amount", "float64")
    dataset.expect_column_values_to_be_between("amount", min_value=0.0, max_value=100000.0)

    dataset.expect_column_values_to_match_regex("currency", r"^[A-Z]{3}$")
    dataset.expect_column_values_to_be_in_set("status", ["SUCCESS", "FAILED", "PENDING"])

    results = dataset.validate()
    return results


# ==============================
# 3️⃣ Génération de rapports GE
# ==============================
def generate_data_docs(context=None):
    """
    Génère les rapports HTML Great Expectations (Data Docs).
    """
    if context is None:
        context = ge.get_context(GE_CONFIG_PATH)

    context.build_data_docs()
    print("✅ Rapports de validation générés dans great_expectations/uncommitted/data_docs/")


# ==============================
# 4️⃣ Exemple d’utilisation
# ==============================
if __name__ == "__main__":
    # Exemple CRM
    crm_data = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "email": ["a@test.com", "b@test.com", "invalid-email"],
        "age": [25, 30, 150]
    })
    crm_results = validate_crm_data(crm_data)
    print("Résultats validation CRM :", crm_results["statistics"])

    # Exemple Transactions
    transactions_data = pd.DataFrame({
        "transaction_id": ["t1", "t2", "t3"],
        "customer_id": [1, 2, 3],
        "amount": [100.0, -5.0, 200.0],
        "currency": ["USD", "EUR", "BAD"],
        "status": ["SUCCESS", "FAILED", "PENDING"]
    })
    trx_results = validate_transactions_data(transactions_data)
    print("Résultats validation Transactions :", trx_results["statistics"])
