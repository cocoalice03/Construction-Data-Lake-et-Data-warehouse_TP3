"""
Synchroniser les tables ksqlDB dans un petit Data Warehouse MySQL.

Ce script reste volontairement simple pour un rendu de niveau Master 1 :
- on récupère chaque table ksqlDB via l'API HTTP,
- on insère les lignes dans les tables MySQL correspondantes,
- on garde un schéma avec des clés primaires et étrangères faciles à expliquer.

Diagramme texte (PK = clé primaire, FK = clé étrangère) :
    dim_users (PK user_id)
      ├── fact_user_transaction_summary (FK user_id)
      └── fact_user_transaction_summary_eur (FK user_id)
    dim_payment_methods (PK payment_method_id)
      └── fact_payment_method_totals (FK payment_method_id)
    fact_product_purchase_counts (PK product_id) est isolée pour rester lisible.
"""
import argparse
import json
from typing import Any, Dict, Iterable, List

import pandas as pd
import requests
import mysql.connector
from mysql.connector import Error

from data_lake_config import KSQLDB_CONFIG, TABLES_CONFIG


# Configuration MySQL par défaut. Le mot de passe est passé via la CLI.
MYSQL_CONFIG: Dict[str, str] = {
    "host": "localhost",
    "port": 3306,
    "database": "data_warehouse",
    "user": "root",
    "password": "",
    "charset": "utf8mb4",
    "use_unicode": True,
}


EXPECTED_COLUMNS: Dict[str, List[str]] = {
    "user_transaction_summary": [
        "user_id",
        "user_name",
        "user_email",
        "user_country",
        "user_city",
        "transaction_type",
        "total_amount",
        "transaction_count",
        "avg_amount",
        "min_amount",
        "max_amount",
        "last_transaction_date",
    ],
    "user_transaction_summary_eur": [
        "user_id",
        "user_name",
        "user_email",
        "user_country",
        "user_city",
        "transaction_type",
        "total_amount_eur",
        "transaction_count",
        "avg_amount_eur",
        "exchange_rate",
    ],
    "payment_method_totals": [
        "payment_method",
        "total_amount",
        "transaction_count",
        "avg_amount",
    ],
    "product_purchase_counts": [
        "product_id",
        "product_name",
        "product_category",
        "purchase_count",
        "total_revenue",
        "avg_price",
        "unique_buyers",
    ],
}


def fetch_table_from_ksql(table_name: str) -> pd.DataFrame:
    """Interroge ksqlDB et retourne le contenu d'une table sous forme de DataFrame."""
    url = f"http://{KSQLDB_CONFIG['host']}:{KSQLDB_CONFIG['port']}/query"
    payload = {"ksql": f"SELECT * FROM {table_name};", "streamsProperties": {}}

    response = requests.post(
        url,
        json=payload,
        timeout=KSQLDB_CONFIG["timeout"],
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()

    rows = []
    for line in response.text.strip().split("\n"):
        if not line:
            continue
        record = json.loads(line)
        if "row" in record:
            rows.append(record["row"]["columns"])

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    expected_cols = EXPECTED_COLUMNS.get(table_name)
    if expected_cols and len(expected_cols) == df.shape[1]:
        df.columns = expected_cols
    return df


class SimpleWarehouse:
    """Connexion MySQL + helpers d'insertion; volontairement sans ORM compliqué."""

    def __init__(self, mysql_config: Dict[str, str]) -> None:
        self.config = mysql_config
        self.connection = mysql.connector.connect(**mysql_config)
        self.cursor = self.connection.cursor(dictionary=True)

    def close(self) -> None:
        self.cursor.close()
        self.connection.close()

    def commit(self) -> None:
        self.connection.commit()

    # -- Helpers d'insertion -------------------------------------------------
    def upsert_user(self, row: Dict[str, Any]) -> None:
        query = """
            INSERT INTO dim_users (user_id, user_name, user_email, user_country, user_city)
            VALUES (%(user_id)s, %(user_name)s, %(user_email)s, %(user_country)s, %(user_city)s)
            ON DUPLICATE KEY UPDATE
              user_name = VALUES(user_name),
              user_email = VALUES(user_email),
              user_country = VALUES(user_country),
              user_city = VALUES(user_city),
              updated_at = CURRENT_TIMESTAMP;
        """
        self.cursor.execute(query, row)

    def get_payment_method_id(self, payment_method_name: str) -> int:
        query = "SELECT payment_method_id FROM dim_payment_methods WHERE payment_method_name = %s"
        self.cursor.execute(query, (payment_method_name,))
        result = self.cursor.fetchone()
        if not result:
            raise ValueError(f"Méthode de paiement inconnue côté MySQL: {payment_method_name}")
        return result["payment_method_id"]

    def insert_user_summary(self, row: Dict[str, Any]) -> None:
        query = """
            INSERT INTO fact_user_transaction_summary (
                user_id, transaction_type, total_amount, transaction_count,
                avg_amount, min_amount, max_amount, last_transaction_date,
                snapshot_date, snapshot_version
            )
            VALUES (
                %(user_id)s, %(transaction_type)s, %(total_amount)s, %(transaction_count)s,
                %(avg_amount)s, %(min_amount)s, %(max_amount)s, %(last_transaction_date)s,
                %(snapshot_date)s, %(snapshot_version)s
            )
            ON DUPLICATE KEY UPDATE
              total_amount = VALUES(total_amount),
              transaction_count = VALUES(transaction_count),
              avg_amount = VALUES(avg_amount),
              min_amount = VALUES(min_amount),
              max_amount = VALUES(max_amount),
              last_transaction_date = VALUES(last_transaction_date),
              updated_at = CURRENT_TIMESTAMP;
        """
        self.cursor.execute(query, row)

    def insert_user_summary_eur(self, row: Dict[str, Any]) -> None:
        query = """
            INSERT INTO fact_user_transaction_summary_eur (
                user_id, transaction_type, total_amount_eur, transaction_count,
                avg_amount_eur, exchange_rate, snapshot_date, snapshot_version
            )
            VALUES (
                %(user_id)s, %(transaction_type)s, %(total_amount_eur)s, %(transaction_count)s,
                %(avg_amount_eur)s, %(exchange_rate)s, %(snapshot_date)s, %(snapshot_version)s
            )
            ON DUPLICATE KEY UPDATE
              total_amount_eur = VALUES(total_amount_eur),
              transaction_count = VALUES(transaction_count),
              avg_amount_eur = VALUES(avg_amount_eur),
              exchange_rate = VALUES(exchange_rate),
              updated_at = CURRENT_TIMESTAMP;
        """
        self.cursor.execute(query, row)

    def insert_payment_totals(self, row: Dict[str, Any]) -> None:
        query = """
            INSERT INTO fact_payment_method_totals (
                payment_method_id, payment_method_name, total_amount,
                transaction_count, avg_amount, snapshot_date, snapshot_version
            )
            VALUES (
                %(payment_method_id)s, %(payment_method_name)s, %(total_amount)s,
                %(transaction_count)s, %(avg_amount)s, %(snapshot_date)s, %(snapshot_version)s
            )
            ON DUPLICATE KEY UPDATE
              total_amount = VALUES(total_amount),
              transaction_count = VALUES(transaction_count),
              avg_amount = VALUES(avg_amount),
              updated_at = CURRENT_TIMESTAMP;
        """
        self.cursor.execute(query, row)

    def insert_product_counts(self, row: Dict[str, Any]) -> None:
        query = """
            INSERT INTO fact_product_purchase_counts (
                product_id, product_name, product_category, purchase_count,
                total_revenue, avg_price, unique_buyers, snapshot_date, snapshot_version
            )
            VALUES (
                %(product_id)s, %(product_name)s, %(product_category)s, %(purchase_count)s,
                %(total_revenue)s, %(avg_price)s, %(unique_buyers)s,
                %(snapshot_date)s, %(snapshot_version)s
            )
            ON DUPLICATE KEY UPDATE
              purchase_count = VALUES(purchase_count),
              total_revenue = VALUES(total_revenue),
              avg_price = VALUES(avg_price),
              unique_buyers = VALUES(unique_buyers),
              updated_at = CURRENT_TIMESTAMP;
        """
        self.cursor.execute(query, row)


def sync_user_transaction_summary(df: pd.DataFrame, warehouse: SimpleWarehouse, snapshot_tags: Dict[str, Any]) -> None:
    """Charge la table user_transaction_summary + la dimension utilisateur."""
    for _, raw_row in df.iterrows():
        user_payload = {
            "user_id": raw_row.get("user_id"),
            "user_name": raw_row.get("user_name"),
            "user_email": raw_row.get("user_email"),
            "user_country": raw_row.get("user_country"),
            "user_city": raw_row.get("user_city"),
        }
        warehouse.upsert_user(user_payload)

        fact_payload = {
            "user_id": raw_row.get("user_id"),
            "transaction_type": raw_row.get("transaction_type"),
            "total_amount": raw_row.get("total_amount"),
            "transaction_count": raw_row.get("transaction_count"),
            "avg_amount": raw_row.get("avg_amount"),
            "min_amount": raw_row.get("min_amount"),
            "max_amount": raw_row.get("max_amount"),
            "last_transaction_date": raw_row.get("last_transaction_date"),
            **snapshot_tags,
        }
        warehouse.insert_user_summary(fact_payload)


def sync_user_transaction_summary_eur(df: pd.DataFrame, warehouse: SimpleWarehouse, snapshot_tags: Dict[str, Any]) -> None:
    """Même logique mais avec les montants convertis en EUR."""
    for _, raw_row in df.iterrows():
        user_payload = {
            "user_id": raw_row.get("user_id"),
            "user_name": raw_row.get("user_name"),
            "user_email": raw_row.get("user_email"),
            "user_country": raw_row.get("user_country"),
            "user_city": raw_row.get("user_city"),
        }
        warehouse.upsert_user(user_payload)

        fact_payload = {
            "user_id": raw_row.get("user_id"),
            "transaction_type": raw_row.get("transaction_type"),
            "total_amount_eur": raw_row.get("total_amount_eur"),
            "transaction_count": raw_row.get("transaction_count"),
            "avg_amount_eur": raw_row.get("avg_amount_eur"),
            "exchange_rate": raw_row.get("exchange_rate", 1.0),
            **snapshot_tags,
        }
        warehouse.insert_user_summary_eur(fact_payload)


def sync_payment_method_totals(df: pd.DataFrame, warehouse: SimpleWarehouse, snapshot_tags: Dict[str, Any]) -> None:
    """Relie chaque total à sa méthode de paiement (FK vers dim_payment_methods)."""
    for _, raw_row in df.iterrows():
        payment_method_name = raw_row.get("payment_method")
        payment_method_id = warehouse.get_payment_method_id(payment_method_name)

        fact_payload = {
            "payment_method_id": payment_method_id,
            "payment_method_name": payment_method_name,
            "total_amount": raw_row.get("total_amount"),
            "transaction_count": raw_row.get("transaction_count"),
            "avg_amount": raw_row.get("avg_amount"),
            **snapshot_tags,
        }
        warehouse.insert_payment_totals(fact_payload)


def sync_product_purchase_counts(df: pd.DataFrame, warehouse: SimpleWarehouse, snapshot_tags: Dict[str, Any]) -> None:
    """Stocke les compteurs produits. Pas de FK ici pour ne pas multiplier les dimensions."""
    for _, raw_row in df.iterrows():
        fact_payload = {
            "product_id": raw_row.get("product_id"),
            "product_name": raw_row.get("product_name"),
            "product_category": raw_row.get("product_category"),
            "purchase_count": raw_row.get("purchase_count"),
            "total_revenue": raw_row.get("total_revenue"),
            "avg_price": raw_row.get("avg_price"),
            "unique_buyers": raw_row.get("unique_buyers"),
            **snapshot_tags,
        }
        warehouse.insert_product_counts(fact_payload)


SYNC_HANDLERS = {
    "user_transaction_summary": sync_user_transaction_summary,
    "user_transaction_summary_eur": sync_user_transaction_summary_eur,
    "payment_method_totals": sync_payment_method_totals,
    "product_purchase_counts": sync_product_purchase_counts,
}


def sync_tables(table_names: Iterable[str], snapshot_version: int, mysql_password: str) -> None:
    """Boucle principale : fetch ksqlDB -> charge MySQL."""
    mysql_cfg = MYSQL_CONFIG.copy()
    mysql_cfg["password"] = mysql_password

    try:
        warehouse = SimpleWarehouse(mysql_cfg)
    except Error as err:
        raise SystemExit(f"Connexion MySQL impossible : {err}") from err

    snapshot_tags = {"snapshot_date": pd.Timestamp.utcnow().date(), "snapshot_version": snapshot_version}

    try:
        for table_name in table_names:
            print(f"--> Synchronisation de {table_name}")
            df = fetch_table_from_ksql(table_name)
            if df.empty:
                print("    Rien à synchroniser pour cette table.")
                continue

            handler = SYNC_HANDLERS[table_name]
            handler(df, warehouse, snapshot_tags)
            warehouse.commit()
            print(f"    ✓ {len(df)} lignes insérées/actualisées.")

    finally:
        warehouse.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronise les tables ksqlDB vers MySQL.")
    parser.add_argument("--mysql-password", required=True, help="Mot de passe MySQL (root par défaut).")
    parser.add_argument(
        "--table",
        choices=list(SYNC_HANDLERS.keys()),
        help="Nom d'une table précise à synchroniser. Sinon tout est fait.",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=1,
        help="Numéro de snapshot stocké dans les tables de faits (facile pour suivre l'historique).",
    )
    args = parser.parse_args()

    if args.table:
        tables_to_sync = [args.table]
    else:
        # On prend seulement les tables activées dans la configuration.
        tables_to_sync = [
            table_name
            for table_name, conf in TABLES_CONFIG.items()
            if conf.get("enabled", True) and table_name in SYNC_HANDLERS
        ]

    if not tables_to_sync:
        raise SystemExit("Aucune table à synchroniser (vérifie TABLES_CONFIG).")

    sync_tables(tables_to_sync, args.version, args.mysql_password)


if __name__ == "__main__":
    main()
