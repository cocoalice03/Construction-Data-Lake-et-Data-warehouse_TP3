"""
Configuration centralisée pour le Data Lake
"""
import os
from pathlib import Path
from enum import Enum
from typing import Dict, List


class StorageMode(Enum):
    """Modes de stockage disponibles"""
    APPEND = "append"      # Ajouter sans modifier l'existant (streams)
    OVERWRITE = "overwrite"  # Remplacer complètement (tables)
    IGNORE = "ignore"      # Ne rien faire si existe déjà


class FeedType(Enum):
    """Types de feeds"""
    STREAM = "stream"
    TABLE = "table"


class PartitioningType(Enum):
    """Types de partitionnement"""
    DATE = "date"          # year/month/day
    VERSION = "version"    # version=v1, v2, etc.


# Chemin racine du data lake
DATA_LAKE_ROOT = Path(__file__).parent / "data_lake"

# Structure des dossiers
STREAMS_DIR = DATA_LAKE_ROOT / "streams"
TABLES_DIR = DATA_LAKE_ROOT / "tables"
FEEDS_DIR = DATA_LAKE_ROOT / "feeds"
LOGS_DIR = DATA_LAKE_ROOT / "logs"

# Configuration ksqlDB
KSQLDB_CONFIG = {
    "host": "localhost",
    "port": 8088,
    "timeout": 30
}

# Configuration des streams ksqlDB
STREAMS_CONFIG: Dict[str, dict] = {
    "transaction_stream": {
        "type": FeedType.STREAM,
        "description": "Stream principal avec données brutes",
        "partitioning": PartitioningType.DATE,
        "storage_mode": StorageMode.APPEND,
        "retention_days": 365,
        "enabled": True
    },
    "transaction_flattened": {
        "type": FeedType.STREAM,
        "description": "Stream avec schéma aplati",
        "partitioning": PartitioningType.DATE,
        "storage_mode": StorageMode.APPEND,
        "retention_days": 365,
        "enabled": True
    },
    "transaction_stream_anonymized": {
        "type": FeedType.STREAM,
        "description": "Stream anonymisé + conversion EUR",
        "partitioning": PartitioningType.DATE,
        "storage_mode": StorageMode.APPEND,
        "retention_days": 730,  # 2 ans pour conformité
        "enabled": True
    },
    "transaction_stream_blacklisted": {
        "type": FeedType.STREAM,
        "description": "Transactions des villes blacklistées",
        "partitioning": PartitioningType.DATE,
        "storage_mode": StorageMode.APPEND,
        "retention_days": 365,
        "enabled": True
    }
}

# Configuration des tables ksqlDB
TABLES_CONFIG: Dict[str, dict] = {
    "user_transaction_summary": {
        "type": FeedType.TABLE,
        "description": "Montants par utilisateur et type",
        "partitioning": PartitioningType.VERSION,
        "storage_mode": StorageMode.OVERWRITE,
        "retention_versions": 7,  # Garder 7 dernières versions
        "enabled": True
    },
    "user_transaction_summary_eur": {
        "type": FeedType.TABLE,
        "description": "Montants en EUR par utilisateur",
        "partitioning": PartitioningType.VERSION,
        "storage_mode": StorageMode.OVERWRITE,
        "retention_versions": 7,
        "enabled": True
    },
    "payment_method_totals": {
        "type": FeedType.TABLE,
        "description": "Totaux par méthode de paiement",
        "partitioning": PartitioningType.VERSION,
        "storage_mode": StorageMode.OVERWRITE,
        "retention_versions": 7,
        "enabled": True
    },
    "product_purchase_counts": {
        "type": FeedType.TABLE,
        "description": "Compteurs par produit",
        "partitioning": PartitioningType.VERSION,
        "storage_mode": StorageMode.OVERWRITE,
        "retention_versions": 7,
        "enabled": True
    }
}

# Format de stockage
STORAGE_FORMAT = "parquet"

# Compression Parquet (gzip, snappy, zstd, lz4)
PARQUET_COMPRESSION = "snappy"

# Taille des batches pour l'export
BATCH_SIZE = 10000

# Configuration des logs
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = "INFO"


def get_stream_path(stream_name: str) -> Path:
    """Retourne le chemin d'un stream"""
    return STREAMS_DIR / stream_name


def get_table_path(table_name: str) -> Path:
    """Retourne le chemin d'une table"""
    return TABLES_DIR / table_name


def get_date_partition_path(base_path: Path, year: int, month: int, day: int) -> Path:
    """Retourne le chemin d'une partition par date"""
    return base_path / f"year={year}" / f"month={month:02d}" / f"day={day:02d}"


def get_version_partition_path(base_path: Path, version: int) -> Path:
    """Retourne le chemin d'une partition par version"""
    return base_path / f"version=v{version}"


def ensure_directories():
    """Crée la structure de dossiers du data lake"""
    directories = [
        DATA_LAKE_ROOT,
        STREAMS_DIR,
        TABLES_DIR,
        FEEDS_DIR,
        FEEDS_DIR / "active",
        FEEDS_DIR / "archived",
        LOGS_DIR
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    
    # Créer les dossiers pour chaque stream
    for stream_name in STREAMS_CONFIG.keys():
        get_stream_path(stream_name).mkdir(parents=True, exist_ok=True)
    
    # Créer les dossiers pour chaque table
    for table_name in TABLES_CONFIG.keys():
        get_table_path(table_name).mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    # Test de la configuration
    print("Configuration du Data Lake")
    print(f"Racine: {DATA_LAKE_ROOT}")
    print(f"\nStreams configurés: {len(STREAMS_CONFIG)}")
    for name, config in STREAMS_CONFIG.items():
        print(f"  - {name}: {config['description']}")
    
    print(f"\nTables configurées: {len(TABLES_CONFIG)}")
    for name, config in TABLES_CONFIG.items():
        print(f"  - {name}: {config['description']}")
    
    print("\nCréation de la structure de dossiers...")
    ensure_directories()
    print("✓ Structure créée avec succès")
