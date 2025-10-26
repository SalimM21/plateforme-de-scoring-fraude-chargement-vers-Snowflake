# Validation de schéma Avro/Parquet
"""
--------------------------------------------------------------------
Validation des schémas de données pour garantir la cohérence
entre les fichiers sources (Avro, Parquet) et les schémas attendus.

Ce module est utilisé dans :
 - les DAGs batch (Airflow)
 - les jobs Spark streaming
--------------------------------------------------------------------
"""

import fastavro
import pyarrow.parquet as pq
import json
from logger import CentralizedLogger


class SchemaValidator:
    def __init__(self, expected_schema_path: str, format: str = "avro"):
        """
        Initialise le validateur de schéma.
        :param expected_schema_path: Chemin vers le schéma de référence (Avro .avsc ou JSON)
        :param format: Type de fichier ("avro" ou "parquet")
        """
        self.format = format.lower()
        self.logger = CentralizedLogger(f"schema_validator_{self.format}")

        # Charger le schéma attendu
        with open(expected_schema_path, "r", encoding="utf-8") as f:
            self.expected_schema = json.load(f)

        self.logger.info(f"Schéma attendu chargé depuis {expected_schema_path}")

    def validate_avro(self, file_path: str) -> bool:
        """Valide un fichier Avro selon le schéma attendu."""
        try:
            with open(file_path, "rb") as fo:
                reader = fastavro.reader(fo)
                actual_schema = reader.writer_schema

            # Vérifier que les champs correspondent
            expected_fields = {f["name"]: f["type"] for f in self.expected_schema["fields"]}
            actual_fields = {f["name"]: f["type"] for f in actual_schema["fields"]}

            missing = set(expected_fields.keys()) - set(actual_fields.keys())
            extra = set(actual_fields.keys()) - set(expected_fields.keys())

            if missing:
                self.logger.error(f"Champs manquants dans {file_path}: {missing}")
            if extra:
                self.logger.warning(f"Champs supplémentaires détectés dans {file_path}: {extra}")

            type_mismatches = {
                field: (expected_fields[field], actual_fields[field])
                for field in expected_fields
                if field in actual_fields and expected_fields[field] != actual_fields[field]
            }

            if type_mismatches:
                for field, (exp, act) in type_mismatches.items():
                    self.logger.error(f"Type mismatch pour '{field}': attendu {exp}, obtenu {act}")

            valid = not (missing or type_mismatches)
            self.logger.info(f"Validation Avro pour {file_path} : {'✅ OK' if valid else '❌ NON VALIDE'}")
            return valid

        except Exception as e:
            self.logger.error(f"Erreur lors de la validation Avro : {e}")
            return False

    def validate_parquet(self, file_path: str) -> bool:
        """Valide un fichier Parquet selon le schéma attendu."""
        try:
            table = pq.read_table(file_path)
            actual_schema = {field.name: str(field.type) for field in table.schema}
            expected_schema = {f["name"]: f["type"] for f in self.expected_schema["fields"]}

            missing = set(expected_schema.keys()) - set(actual_schema.keys())
            extra = set(actual_schema.keys()) - set(expected_schema.keys())

            if missing:
                self.logger.error(f"Champs manquants dans {file_path}: {missing}")
            if extra:
                self.logger.warning(f"Champs supplémentaires détectés dans {file_path}: {extra}")

            mismatches = {
                field: (expected_schema[field], actual_schema[field])
                for field in expected_schema
                if field in actual_schema and expected_schema[field].lower() not in actual_schema[field].lower()
            }

            if mismatches:
                for field, (exp, act) in mismatches.items():
                    self.logger.error(f"Type mismatch pour '{field}': attendu {exp}, obtenu {act}")

            valid = not (missing or mismatches)
            self.logger.info(f"Validation Parquet pour {file_path} : {'✅ OK' if valid else '❌ NON VALIDE'}")
            return valid

        except Exception as e:
            self.logger.error(f"Erreur lors de la validation Parquet : {e}")
            return False

    def validate(self, file_path: str) -> bool:
        """Interface unifiée de validation."""
        if self.format == "avro":
            return self.validate_avro(file_path)
        elif self.format == "parquet":
            return self.validate_parquet(file_path)
        else:
            self.logger.error(f"Format non supporté : {self.format}")
            return False


# Exemple d’utilisation autonome
if __name__ == "__main__":
    # Exemple : valider un fichier Avro
    validator = SchemaValidator(expected_schema_path="schemas/customer_schema.avsc", format="avro")
    validator.validate("data/customers_2025-10-14.avro")

    # Exemple : valider un fichier Parquet
    validator_parquet = SchemaValidator(expected_schema_path="schemas/transactions_schema.json", format="parquet")
    validator_parquet.validate("data/transactions_2025-10-14.parquet")
