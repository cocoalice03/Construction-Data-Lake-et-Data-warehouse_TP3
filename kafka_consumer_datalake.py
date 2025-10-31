"""
Kafka consumer simple pour alimenter le data lake local.

On consomme les messages JSON d’un ou plusieurs topics et on les écrit
dans des fichiers Parquet partitionnés par date, sous data_lake/streams/<topic>.
"""
import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List

from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_lake_config import STREAMS_DIR, get_date_partition_path, ensure_directories


def create_consumer(topics: List[str], bootstrap_servers: str) -> KafkaConsumer:
    """Instancie un consumer Kafka qui lit des messages JSON."""
    return KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def write_batch(topic: str, rows: List[Dict], compression: str = "snappy") -> Path:
    """Sauvegarde un lot dans un parquet partitionné year/month/day."""
    today = date.today()
    target_dir = get_date_partition_path(
        STREAMS_DIR / topic, today.year, today.month, today.day
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)
    file_name = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    file_path = target_dir / file_name
    pq.write_table(table, file_path, compression=compression)
    return file_path


def consume_and_store(
    topics: List[str], bootstrap_servers: str, batch_size: int, compression: str
) -> None:
    """Boucle de consommation : buffer puis écriture parquet."""
    ensure_directories()
    consumer = create_consumer(topics, bootstrap_servers)
    buffers: Dict[str, List[Dict]] = {topic: [] for topic in topics}

    print(f"Consommation démarrée sur {topics}")

    try:
        for message in consumer:
            buffers[message.topic].append(message.value)

            if len(buffers[message.topic]) >= batch_size:
                file_path = write_batch(message.topic, buffers[message.topic], compression)
                print(f"{len(buffers[message.topic])} messages écrits dans {file_path}")
                buffers[message.topic] = []

    except KeyboardInterrupt:
        print("Arrêt demandé par l'utilisateur.")

    finally:
        for topic, rows in buffers.items():
            if rows:
                file_path = write_batch(topic, rows, compression)
                print(f"(flush final) {len(rows)} messages écrits dans {file_path}")
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka consumer simple pour le data lake.")
    parser.add_argument(
        "--topics",
        nargs="+",
        required=True,
        help="Liste des topics à consommer (ex: transaction_stream transaction_flattened).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Adresse du cluster Kafka. Par défaut: localhost:9092.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Nombre de messages lus avant d'écrire un fichier Parquet.",
    )
    parser.add_argument(
        "--compression",
        default="snappy",
        help="Compression Parquet (snappy, gzip, etc.).",
    )

    args = parser.parse_args()
    consume_and_store(args.topics, args.bootstrap_servers, args.batch_size, args.compression)


if __name__ == "__main__":
    main()
