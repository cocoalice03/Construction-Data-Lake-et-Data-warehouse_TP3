"""
Kafka consumer simple pour alimenter le data warehouse MySQL.

Chaque message reçu est transformé en DataFrame puis passé aux fonctions
simples du module sync_to_mysql.
"""
import argparse
import json
from datetime import date
from typing import Dict, List

from kafka import KafkaConsumer
import pandas as pd

from sync_to_mysql import MYSQL_CONFIG, SimpleWarehouse, SYNC_HANDLERS


def create_consumer(topic: str, bootstrap_servers: str) -> KafkaConsumer:
    """Instancie un consumer Kafka pour un topic unique."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def flush_buffer(
    topic: str,
    rows: List[Dict],
    warehouse: SimpleWarehouse,
    snapshot_info: Dict[str, int],
) -> None:
    """Envoie le contenu du buffer dans MySQL via la fonction adaptée."""
    if not rows:
        return

    handler = SYNC_HANDLERS[topic]
    df = pd.DataFrame(rows)
    handler(df, warehouse, snapshot_info)
    warehouse.commit()
    print(f"{len(rows)} messages insérés pour {topic}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka consumer simple pour MySQL.")
    parser.add_argument(
        "--topic",
        required=True,
        choices=SYNC_HANDLERS.keys(),
        help="Topic/table à synchroniser (ex: user_transaction_summary).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Adresse du cluster Kafka. Par défaut: localhost:9092.",
    )
    parser.add_argument(
        "--mysql-password",
        required=True,
        help="Mot de passe MySQL pour l'utilisateur défini dans MYSQL_CONFIG.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Nombre de messages accumulés avant insertion MySQL.",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=1,
        help="Numéro de snapshot stocké dans les tables de faits.",
    )

    args = parser.parse_args()

    consumer = create_consumer(args.topic, args.bootstrap_servers)

    mysql_cfg = MYSQL_CONFIG.copy()
    mysql_cfg["password"] = args.mysql_password
    warehouse = SimpleWarehouse(mysql_cfg)

    buffer: List[Dict] = []
    snapshot_info = {"snapshot_date": date.today(), "snapshot_version": args.version}

    print(f"Consommation démarrée sur le topic {args.topic}")

    try:
        for message in consumer:
            buffer.append(message.value)
            if len(buffer) >= args.batch_size:
                flush_buffer(args.topic, buffer, warehouse, snapshot_info)
                buffer = []
    except KeyboardInterrupt:
        print("Arrêt demandé par l'utilisateur.")
    finally:
        flush_buffer(args.topic, buffer, warehouse, snapshot_info)
        warehouse.close()
        consumer.close()


if __name__ == "__main__":
    main()
