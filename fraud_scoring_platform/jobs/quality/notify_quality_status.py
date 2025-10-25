# Notification (Slack/Email)
"""
notify_quality_status.py
-------------------------
Notification automatique (Slack / Email) sur le statut de la qualité des données.

Fonctionnalités :
- Lecture du dernier rapport de qualité généré
- Envoi d’un message résumé vers Slack et/ou Email
- Niveau d’alerte selon le taux global de succès
"""

import os
import json
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests

# 📁 Répertoires et configuration
REPORTS_DIR = "reports/data_quality"
CONFIG_FILE = "config/notification_config.json"


# ==========================================
# 1️⃣ Lecture du rapport et configuration
# ==========================================
def load_config():
    """
    Charge la configuration Slack/Email depuis un fichier JSON.
    """
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"⚠️ Fichier de configuration manquant : {CONFIG_FILE}")
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def get_latest_report():
    """
    Récupère le dernier rapport PDF ou JSON généré.
    """
    files = sorted(
        [f for f in os.listdir(REPORTS_DIR) if f.endswith(".pdf") or f.endswith(".json")],
        reverse=True
    )
    if not files:
        raise FileNotFoundError("⚠️ Aucun rapport trouvé dans le répertoire des rapports.")
    return os.path.join(REPORTS_DIR, files[0])


# ==========================================
# 2️⃣ Génération du message résumé
# ==========================================
def create_notification_message(metrics: dict):
    """
    Crée un message résumé avec un emoji selon le taux de qualité.
    """
    success_rate = metrics.get("global_success_rate", 0)
    if success_rate >= 95:
        status_emoji = "✅"
        level = "Excellent"
    elif 80 <= success_rate < 95:
        status_emoji = "⚠️"
        level = "Moyen"
    else:
        status_emoji = "❌"
        level = "Critique"

    message = (
        f"{status_emoji} *Rapport de Qualité - {datetime.today().strftime('%Y-%m-%d')}*\n"
        f"**Taux global de succès :** {success_rate}%\n"
        f"**Statut :** {level}\n"
        f"**Total de contrôles :** {metrics.get('total_checks', 0)}\n"
        f"**Échecs :** {metrics.get('total_failures', 0)}\n\n"
        f"🗂 Rapport : {get_latest_report()}"
    )
    return message


# ==========================================
# 3️⃣ Notification Slack
# ==========================================
def send_slack_notification(message, webhook_url):
    """
    Envoie une notification vers Slack via Webhook.
    """
    response = requests.post(webhook_url, json={"text": message})
    if response.status_code == 200:
        print("📢 Notification Slack envoyée avec succès.")
    else:
        print(f"⚠️ Erreur Slack ({response.status_code}) : {response.text}")


# ==========================================
# 4️⃣ Notification Email
# ==========================================
def send_email_notification(message, config):
    """
    Envoie une notification Email.
    """
    sender = config["email"]["sender"]
    recipients = config["email"]["recipients"]
    password = config["email"]["password"]
    smtp_server = config["email"]["smtp_server"]
    smtp_port = config["email"]["smtp_port"]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Rapport Qualité Données - {datetime.today().strftime('%Y-%m-%d')}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(message, "plain"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.send_message(msg)
        print("📧 Email envoyé avec succès.")
    except Exception as e:
        print(f"⚠️ Erreur lors de l’envoi de l’email : {e}")


# ==========================================
# 5️⃣ Pipeline principal
# ==========================================
def main():
    config = load_config()

    # Charger les métriques depuis un fichier JSON s’il existe
    metrics_file = os.path.join(REPORTS_DIR, "last_quality_metrics.json")
    if os.path.exists(metrics_file):
        with open(metrics_file, "r") as f:
            metrics = json.load(f)
    else:
        metrics = {"global_success_rate": 0, "total_checks": 0, "total_failures": 0}

    message = create_notification_message(metrics)

    # Slack
    if "slack" in config and config["slack"].get("webhook_url"):
        send_slack_notification(message, config["slack"]["webhook_url"])

    # Email
    if "email" in config:
        send_email_notification(message, config)


if __name__ == "__main__":
    main()
