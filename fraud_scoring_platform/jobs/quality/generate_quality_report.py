# Génération du rapport de qualité hebdomadaire
"""
generate_quality_report.py
--------------------------
Génération du rapport de qualité hebdomadaire à partir des résultats
des validations Great Expectations (CRM + transactions).

Fonctionnalités :
- Lecture des résultats GE (JSON)
- Calcul des indicateurs de qualité (succès, échecs, taux)
- Génération d’un rapport consolidé (PDF ou HTML)
- Envoi optionnel du rapport par e-mail / Slack
"""

import os
import json
import pandas as pd
from datetime import datetime
from fpdf import FPDF

# 📁 Répertoires des résultats Great Expectations
GE_RESULTS_DIR = "great_expectations/uncommitted/validations"
REPORTS_DIR = "reports/data_quality"


# ==========================
# 1️⃣ Collecte des résultats
# ==========================
def collect_validation_results():
    """
    Parcourt les dossiers Great Expectations et collecte les résultats JSON.
    """
    all_results = []
    for root, _, files in os.walk(GE_RESULTS_DIR):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    data = json.load(f)
                    expectation_suite_name = data.get("meta", {}).get("expectation_suite_name", "unknown_suite")
                    stats = data.get("statistics", {})
                    all_results.append({
                        "suite": expectation_suite_name,
                        "success_percent": stats.get("success_percent", 0),
                        "evaluated_expectations": stats.get("evaluated_expectations", 0),
                        "successful_expectations": stats.get("successful_expectations", 0),
                        "unsuccessful_expectations": stats.get("unsuccessful_expectations", 0)
                    })
    return pd.DataFrame(all_results)


# ==============================
# 2️⃣ Calcul des indicateurs
# ==============================
def compute_quality_metrics(df: pd.DataFrame):
    """
    Calcule les indicateurs de qualité globaux.
    """
    if df.empty:
        return {
            "global_success_rate": 0,
            "total_checks": 0,
            "total_failures": 0
        }

    global_success_rate = round(df["success_percent"].mean(), 2)
    total_checks = df["evaluated_expectations"].sum()
    total_failures = df["unsuccessful_expectations"].sum()

    return {
        "global_success_rate": global_success_rate,
        "total_checks": total_checks,
        "total_failures": total_failures
    }


# ==============================
# 3️⃣ Génération du rapport PDF
# ==============================
class PDFReport(FPDF):
    def header(self):
        self.set_font("Arial", "B", 14)
        self.cell(0, 10, "📊 Rapport Hebdomadaire de Qualité des Données", 0, 1, "C")

    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"Page {self.page_no()} / {datetime.today().strftime('%Y-%m-%d')}", 0, 0, "C")


def generate_pdf_report(df: pd.DataFrame, metrics: dict):
    """
    Génère un rapport PDF récapitulatif des validations GE.
    """
    os.makedirs(REPORTS_DIR, exist_ok=True)
    filename = os.path.join(REPORTS_DIR, f"data_quality_report_{datetime.today().strftime('%Y_%m_%d')}.pdf")

    pdf = PDFReport()
    pdf.add_page()
    pdf.set_font("Arial", "", 12)

    pdf.ln(10)
    pdf.cell(0, 10, f"Taux global de succès : {metrics['global_success_rate']}%", ln=True)
    pdf.cell(0, 10, f"Nombre total de contrôles : {metrics['total_checks']}", ln=True)
    pdf.cell(0, 10, f"Nombre total d’échecs : {metrics['total_failures']}", ln=True)
    pdf.ln(10)

    # Tableau détaillé
    pdf.set_font("Arial", "B", 12)
    pdf.cell(60, 10, "Suite", 1)
    pdf.cell(40, 10, "Taux succès (%)", 1)
    pdf.cell(40, 10, "Vérifs totales", 1)
    pdf.cell(40, 10, "Échecs", 1)
    pdf.ln()

    pdf.set_font("Arial", "", 11)
    for _, row in df.iterrows():
        pdf.cell(60, 10, row["suite"], 1)
        pdf.cell(40, 10, str(row["success_percent"]), 1)
        pdf.cell(40, 10, str(row["evaluated_expectations"]), 1)
        pdf.cell(40, 10, str(row["unsuccessful_expectations"]), 1)
        pdf.ln()

    pdf.output(filename)
    print(f"✅ Rapport de qualité généré : {filename}")


# ==============================
# 4️⃣ Pipeline principal
# ==============================
def main():
    print("📥 Collecte des résultats Great Expectations...")
    df = collect_validation_results()

    print("📊 Calcul des métriques de qualité...")
    metrics = compute_quality_metrics(df)

    print("📝 Génération du rapport PDF...")
    generate_pdf_report(df, metrics)


if __name__ == "__main__":
    main()
