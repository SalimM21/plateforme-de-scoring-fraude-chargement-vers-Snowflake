# Suivi de la latence et métriques
"""
--------------------------------------------------------------------
Module de monitoring pour les pipelines de données :
- Mesure de la latence et du throughput (Spark, Kafka, Airflow)
- Collecte et export des métriques Prometheus
- Alerte en cas de dépassement des seuils
--------------------------------------------------------------------
"""

import time
import random
from prometheus_client import Gauge, Counter, start_http_server
from logger import CentralizedLogger


class PipelineMonitor:
    def __init__(self, name: str, prometheus_port: int = 8000):
        """
        Initialise le monitoring du pipeline.
        :param name: Nom du pipeline (ex: 'batch_customers', 'streaming_transactions')
        :param prometheus_port: Port d’exposition Prometheus (par défaut : 8000)
        """
        self.name = name
        self.logger = CentralizedLogger(name=f"monitor_{name}")

        # Démarrer le serveur Prometheus pour exposer les métriques
        start_http_server(prometheus_port)
        self.logger.info(f"Serveur Prometheus lancé sur le port {prometheus_port}")

        # Définition des métriques
        self.latency_gauge = Gauge(f"{name}_latency_seconds", "Temps de latence moyen des traitements")
        self.throughput_gauge = Gauge(f"{name}_throughput_records_per_sec", "Nombre d’enregistrements traités par seconde")
        self.error_counter = Counter(f"{name}_errors_total", "Nombre total d’erreurs détectées")

    def record_latency(self, start_time: float, end_time: float):
        """Enregistre la latence entre deux timestamps."""
        latency = round(end_time - start_time, 3)
        self.latency_gauge.set(latency)
        self.logger.info(f"Latence enregistrée : {latency} secondes")
        return latency

    def record_throughput(self, records_processed: int, duration: float):
        """Calcule et enregistre le throughput."""
        throughput = round(records_processed / duration, 2) if duration > 0 else 0
        self.throughput_gauge.set(throughput)
        self.logger.info(f"Throughput : {throughput} enregistrements/s")
        return throughput

    def record_error(self, message: str):
        """Incrémente le compteur d’erreurs."""
        self.error_counter.inc()
        self.logger.error(f"Erreur détectée : {message}")

    def alert_if_latency_high(self, threshold: float):
        """Vérifie si la latence dépasse un seuil."""
        latency = self.latency_gauge._value.get()
        if latency and latency > threshold:
            alert_msg = f"⚠️ Latence élevée ({latency}s > seuil {threshold}s)"
            self.logger.warning(alert_msg)
            return True
        return False

    def alert_if_errors(self, threshold: int = 5):
        """Déclenche une alerte si trop d’erreurs ont été enregistrées."""
        errors = self.error_counter._value.get()
        if errors >= threshold:
            self.logger.warning(f"⚠️ Trop d’erreurs détectées ({errors} erreurs >= {threshold})")
            return True
        return False


# Exemple d’utilisation autonome
if __name__ == "__main__":
    monitor = PipelineMonitor(name="streaming_transactions", prometheus_port=9090)

    for i in range(5):
        start = time.time()
        # Simulation du traitement
        time.sleep(random.uniform(0.2, 1.0))
        end = time.time()

        monitor.record_latency(start, end)
        monitor.record_throughput(records_processed=random.randint(500, 2000), duration=(end - start))

        if random.random() < 0.2:  # 20% de chance d’erreur
            monitor.record_error("Erreur simulée lors du traitement Kafka")

        monitor.alert_if_latency_high(threshold=0.8)
        monitor.alert_if_errors(threshold=3)
        time.sleep(2)
