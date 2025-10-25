# Gestion des logs centralisés
"""
--------------------------------------------------------------------
Gestion centralisée des logs pour tous les composants du pipeline :
Airflow DAGs, Spark jobs, et scripts de qualité de données.
Permet un suivi unifié des événements et erreurs.
--------------------------------------------------------------------
"""

import logging
import os
from datetime import datetime

# Optionnel : intégration Elasticsearch pour la centralisation
try:
    from elasticsearch import Elasticsearch
except ImportError:
    Elasticsearch = None


class CentralizedLogger:
    def __init__(self, name: str, log_dir: str = "logs", elastic_config: dict = None):
        """
        Initialise un logger centralisé.
        :param name: Nom du module (ex: 'dag_data_quality', 'spark_streaming_job').
        :param log_dir: Dossier local où enregistrer les logs.
        :param elastic_config: Configuration Elasticsearch (optionnelle).
        """
        self.name = name
        self.elastic_config = elastic_config
        self.es_client = None

        # Configuration du répertoire de logs
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d")
        log_file = os.path.join(log_dir, f"{name}_{timestamp}.log")

        # Configuration du logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Formatter standard
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Handler fichier
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Handler console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # Connexion Elasticsearch si configurée
        if self.elastic_config and Elasticsearch:
            self.es_client = Elasticsearch(
                hosts=[self.elastic_config.get("host", "localhost")],
                http_auth=(
                    self.elastic_config.get("user"),
                    self.elastic_config.get("password")
                ),
                verify_certs=self.elastic_config.get("verify_certs", True)
            )

    def log(self, level: str, message: str, extra: dict = None):
        """
        Log un message avec un certain niveau (INFO, WARNING, ERROR, etc.)
        et envoie vers Elasticsearch si configuré.
        """
        log_func = getattr(self.logger, level.lower(), self.logger.info)
        log_func(message)

        # Envoi optionnel vers Elasticsearch
        if self.es_client:
            doc = {
                "timestamp": datetime.utcnow().isoformat(),
                "level": level.upper(),
                "component": self.name,
                "message": message,
                "extra": extra or {}
            }
            try:
                self.es_client.index(index="data_platform_logs", document=doc)
            except Exception as e:
                self.logger.warning(f"Erreur d’envoi vers Elasticsearch : {e}")

    # Méthodes simplifiées
    def info(self, msg, extra=None): self.log("INFO", msg, extra)
    def warning(self, msg, extra=None): self.log("WARNING", msg, extra)
    def error(self, msg, extra=None): self.log("ERROR", msg, extra)
    def debug(self, msg, extra=None): self.log("DEBUG", msg, extra)


# Exemple d’utilisation
if __name__ == "__main__":
    # Exemple avec log local uniquement
    logger = CentralizedLogger(name="spark_streaming_job")

    logger.info("Démarrage du job Spark Structured Streaming")
    logger.warning("Retard de réception de batch détecté")
    logger.error("Erreur critique : connexion à Kafka échouée", extra={"topic": "transactions"})

    # Exemple avec Elasticsearch
    elastic_conf = {
        "host": "http://localhost:9200",
        "user": "elastic",
        "password": "changeme"
    }
    es_logger = CentralizedLogger("airflow_dag_quality", elastic_config=elastic_conf)
    es_logger.info("Exécution du DAG qualité terminée avec succès")
