# Fonctions utilitaires génériques
"""
--------------------------------------------------------------------
Module utilitaire regroupant les fonctions génériques utilisées dans
les pipelines batch, streaming et qualité :
- Gestion des dates (timestamps, intervalles)
- Lecture de fichiers de configuration YAML
- Manipulation de chemins et nettoyage de données
- Helpers pour connexions Cloud (GCP / Snowflake)
--------------------------------------------------------------------
"""

import os
import yaml
import json
import datetime
import pytz
from typing import Any, Dict, Optional
from logger import CentralizedLogger


logger = CentralizedLogger("helpers")


# ==========================================================
# 🕓 Fonctions de gestion de temps
# ==========================================================

def get_current_timestamp(tz: str = "UTC") -> str:
    """Retourne un timestamp ISO formaté selon un fuseau horaire."""
    timezone = pytz.timezone(tz)
    return datetime.datetime.now(timezone).strftime("%Y-%m-%dT%H:%M:%S")


def get_yesterday_date() -> str:
    """Retourne la date d’hier au format YYYY-MM-DD."""
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def get_date_n_days_ago(n: int) -> str:
    """Retourne la date N jours en arrière au format YYYY-MM-DD."""
    date = datetime.date.today() - datetime.timedelta(days=n)
    return date.strftime("%Y-%m-%d")


# ==========================================================
# ⚙️ Fonctions de configuration
# ==========================================================

def load_yaml_config(path: str) -> Dict[str, Any]:
    """Charge un fichier YAML de configuration."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logger.info(f"Configuration chargée : {path}")
        return config
    except Exception as e:
        logger.error(f"Erreur lors du chargement du fichier YAML {path}: {e}")
        return {}


def load_json_config(path: str) -> Dict[str, Any]:
    """Charge un fichier JSON de configuration."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.info(f"Configuration JSON chargée : {path}")
        return config
    except Exception as e:
        logger.error(f"Erreur lors du chargement du fichier JSON {path}: {e}")
        return {}


# ==========================================================
# 🧩 Helpers pour chemins et fichiers
# ==========================================================

def ensure_dir_exists(directory: str):
    """Crée un répertoire s’il n’existe pas."""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Répertoire créé : {directory}")
    else:
        logger.debug(f"Répertoire déjà existant : {directory}")


def list_files_with_extension(directory: str, extension: str) -> list:
    """Liste tous les fichiers avec une extension donnée dans un dossier."""
    files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(extension)
    ]
    logger.info(f"{len(files)} fichiers trouvés avec extension '{extension}' dans {directory}")
    return files


def clean_column_names(df):
    """Nettoie les noms de colonnes dans un DataFrame Spark ou Pandas."""
    new_cols = [col.lower().replace(" ", "_").replace("-", "_") for col in df.columns]
    logger.info("Nettoyage des noms de colonnes effectué.")
    df = df.toDF(*new_cols) if hasattr(df, "toDF") else df.rename(columns=dict(zip(df.columns, new_cols)))
    return df


# ==========================================================
# ☁️ Helpers Cloud
# ==========================================================

def build_gcs_path(bucket_name: str, file_name: str, prefix: Optional[str] = None) -> str:
    """Construit un chemin complet vers un objet GCS."""
    path = f"gs://{bucket_name}/{prefix}/{file_name}" if prefix else f"gs://{bucket_name}/{file_name}"
    logger.debug(f"Chemin GCS généré : {path}")
    return path


def build_snowflake_stage_path(database: str, schema: str, stage: str, file_name: str) -> str:
    """Construit le chemin d’un fichier dans un stage Snowflake."""
    path = f"@{database}.{schema}.{stage}/{file_name}"
    logger.debug(f"Chemin Snowflake stage généré : {path}")
    return path


# ==========================================================
# 🧠 Divers
# ==========================================================

def safe_cast(value: Any, to_type: type, default: Any = None) -> Any:
    """Convertit une valeur en un type donné sans lever d’exception."""
    try:
        return to_type(value)
    except (ValueError, TypeError):
        logger.warning(f"Impossible de caster {value} en {to_type.__name__}, valeur par défaut utilisée.")
        return default


def print_json_pretty(data: Dict[str, Any]):
    """Affiche un JSON de manière lisible."""
    print(json.dumps(data, indent=4, ensure_ascii=False))


# Exemple d'utilisation autonome
if __name__ == "__main__":
    print("Timestamp actuel :", get_current_timestamp("Africa/Casablanca"))
    print("Date J-1 :", get_yesterday_date())

    config = load_yaml_config("configs/airflow_config.yaml")
    print_json_pretty(config)
