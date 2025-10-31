"""
Script d'export des données ksqlDB vers le Data Lake
"""
import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_lake_config import (
    KSQLDB_CONFIG, STREAMS_CONFIG, TABLES_CONFIG,
    StorageMode, FeedType, PartitioningType,
    get_stream_path, get_table_path,
    get_date_partition_path, get_version_partition_path,
    STORAGE_FORMAT, PARQUET_COMPRESSION, BATCH_SIZE,
    LOG_FORMAT, LOG_LEVEL, LOGS_DIR, ensure_directories
)


# Configuration du logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            LOGS_DIR / f"export_{datetime.now().strftime('%Y%m')}.log"
        )
    ]
)
logger = logging.getLogger(__name__)


class KsqlDBClient:
    """Client pour interagir avec ksqlDB"""
    
    def __init__(self, host: str, port: int, timeout: int = 30):
        self.base_url = f"http://{host}:{port}"
        self.timeout = timeout
        logger.info(f"Initialisation du client ksqlDB: {self.base_url}")
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Exécute une requête ksqlDB et retourne les résultats"""
        url = f"{self.base_url}/query"
        payload = {
            "ksql": query,
            "streamsProperties": {}
        }
        
        try:
            logger.debug(f"Exécution de la requête: {query}")
            response = requests.post(
                url,
                json=payload,
                timeout=self.timeout,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            # Parser la réponse (format JSON délimité par des lignes)
            results = []
            for line in response.text.strip().split('\n'):
                if line:
                    data = json.loads(line)
                    if 'row' in data:
                        results.append(data['row']['columns'])
            
            logger.info(f"Requête réussie: {len(results)} lignes récupérées")
            return results
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de l'exécution de la requête: {e}")
            raise
    
    def get_stream_data(self, stream_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Récupère les données d'un stream"""
        query = f"SELECT * FROM {stream_name} EMIT CHANGES"
        if limit:
            query += f" LIMIT {limit}"
        
        results = self.execute_query(query)
        return pd.DataFrame(results) if results else pd.DataFrame()
    
    def get_table_data(self, table_name: str) -> pd.DataFrame:
        """Récupère les données d'une table (snapshot)"""
        query = f"SELECT * FROM {table_name}"
        results = self.execute_query(query)
        return pd.DataFrame(results) if results else pd.DataFrame()


class DataLakeExporter:
    """Gestionnaire d'export vers le Data Lake"""
    
    def __init__(self, ksqldb_client: KsqlDBClient):
        self.client = ksqldb_client
        ensure_directories()
        logger.info("DataLakeExporter initialisé")
    
    def export_stream(self, stream_name: str, config: dict, date: Optional[datetime] = None):
        """Exporte un stream vers le data lake"""
        if not config.get('enabled', True):
            logger.info(f"Stream {stream_name} désactivé, skip")
            return
        
        logger.info(f"Export du stream: {stream_name}")
        
        # Date par défaut = aujourd'hui
        if date is None:
            date = datetime.now()
        
        try:
            # Récupérer les données du stream
            df = self.client.get_stream_data(stream_name)
            
            if df.empty:
                logger.warning(f"Aucune donnée pour {stream_name}")
                return
            
            # Chemin de base du stream
            base_path = get_stream_path(stream_name)
            
            # Créer le chemin de partition par date
            partition_path = get_date_partition_path(
                base_path,
                date.year,
                date.month,
                date.day
            )
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Nom du fichier avec timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = partition_path / f"data_{timestamp}.parquet"
            
            # Mode APPEND: toujours ajouter un nouveau fichier
            self._write_parquet(df, file_path, StorageMode.APPEND)
            
            # Mettre à jour les métadonnées
            self._update_metadata(
                base_path,
                stream_name,
                FeedType.STREAM,
                config,
                df,
                partition_path
            )
            
            logger.info(f"✓ Stream {stream_name} exporté: {len(df)} lignes -> {file_path}")
        
        except Exception as e:
            logger.error(f"Erreur lors de l'export du stream {stream_name}: {e}")
            raise
    
    def export_table(self, table_name: str, config: dict, version: Optional[int] = None):
        """Exporte une table vers le data lake"""
        if not config.get('enabled', True):
            logger.info(f"Table {table_name} désactivée, skip")
            return
        
        logger.info(f"Export de la table: {table_name}")
        
        try:
            # Récupérer les données de la table
            df = self.client.get_table_data(table_name)
            
            if df.empty:
                logger.warning(f"Aucune donnée pour {table_name}")
                return
            
            # Chemin de base de la table
            base_path = get_table_path(table_name)
            
            # Déterminer la version
            if version is None:
                version = self._get_next_version(base_path)
            
            # Créer le chemin de partition par version
            partition_path = get_version_partition_path(base_path, version)
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Nom du fichier avec timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = partition_path / f"snapshot_{timestamp}.parquet"
            
            # Mode OVERWRITE: remplacer la version existante
            self._write_parquet(df, file_path, StorageMode.OVERWRITE)
            
            # Mettre à jour les métadonnées
            self._update_metadata(
                base_path,
                table_name,
                FeedType.TABLE,
                config,
                df,
                partition_path
            )
            
            # Nettoyer les anciennes versions
            retention = config.get('retention_versions', 7)
            self._cleanup_old_versions(base_path, retention)
            
            logger.info(f"✓ Table {table_name} exportée: {len(df)} lignes -> {file_path}")
        
        except Exception as e:
            logger.error(f"Erreur lors de l'export de la table {table_name}: {e}")
            raise
    
    def _write_parquet(self, df: pd.DataFrame, file_path: Path, mode: StorageMode):
        """Écrit un DataFrame en format Parquet"""
        try:
            # Convertir en Arrow Table pour plus de contrôle
            table = pa.Table.from_pandas(df)
            
            # Écrire le fichier Parquet avec compression
            pq.write_table(
                table,
                file_path,
                compression=PARQUET_COMPRESSION,
                use_dictionary=True,
                write_statistics=True
            )
            
            logger.debug(f"Fichier Parquet écrit: {file_path} ({file_path.stat().st_size / 1024:.2f} KB)")
        
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture Parquet: {e}")
            raise
    
    def _get_next_version(self, base_path: Path) -> int:
        """Détermine le prochain numéro de version"""
        versions = []
        for version_dir in base_path.glob("version=v*"):
            try:
                version_num = int(version_dir.name.replace("version=v", ""))
                versions.append(version_num)
            except ValueError:
                continue
        
        return max(versions) + 1 if versions else 1
    
    def _cleanup_old_versions(self, base_path: Path, retention: int):
        """Supprime les anciennes versions au-delà de la rétention"""
        versions = []
        for version_dir in base_path.glob("version=v*"):
            try:
                version_num = int(version_dir.name.replace("version=v", ""))
                versions.append((version_num, version_dir))
            except ValueError:
                continue
        
        # Trier par version décroissante
        versions.sort(reverse=True)
        
        # Supprimer les versions au-delà de la rétention
        for version_num, version_dir in versions[retention:]:
            logger.info(f"Suppression de l'ancienne version: {version_dir}")
            for file in version_dir.glob("*"):
                file.unlink()
            version_dir.rmdir()
    
    def _update_metadata(
        self,
        base_path: Path,
        source_name: str,
        feed_type: FeedType,
        config: dict,
        df: pd.DataFrame,
        partition_path: Path
    ):
        """Met à jour le fichier de métadonnées"""
        metadata_file = base_path / "_metadata.json"
        
        # Charger les métadonnées existantes ou créer nouvelles
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
        else:
            metadata = {
                "source": source_name,
                "type": feed_type.value,
                "storage_mode": config['storage_mode'].value,
                "format": STORAGE_FORMAT,
                "partitioning": config['partitioning'].value,
                "created_at": datetime.now().isoformat(),
                "total_records": 0,
                "total_size_mb": 0,
                "partitions": []
            }
        
        # Mettre à jour les statistiques
        metadata["last_export"] = datetime.now().isoformat()
        metadata["total_records"] += len(df)
        
        # Calculer la taille de la partition
        partition_size = sum(f.stat().st_size for f in partition_path.glob("*.parquet"))
        partition_size_mb = partition_size / (1024 * 1024)
        metadata["total_size_mb"] += partition_size_mb
        
        # Ajouter les infos de la partition
        partition_info = {
            "path": str(partition_path.relative_to(base_path)),
            "records": len(df),
            "size_mb": round(partition_size_mb, 2),
            "exported_at": datetime.now().isoformat()
        }
        
        # Vérifier si la partition existe déjà
        existing = next(
            (p for p in metadata["partitions"] if p["path"] == partition_info["path"]),
            None
        )
        
        if existing:
            existing.update(partition_info)
        else:
            metadata["partitions"].append(partition_info)
        
        # Sauvegarder les métadonnées
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.debug(f"Métadonnées mises à jour: {metadata_file}")


def main():
    """Point d'entrée principal"""
    parser = argparse.ArgumentParser(
        description="Export des données ksqlDB vers le Data Lake"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Exporter tous les streams et tables"
    )
    parser.add_argument(
        "--stream",
        type=str,
        help="Nom du stream à exporter"
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Nom de la table à exporter"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date pour le partitionnement (format: YYYY-MM-DD)"
    )
    parser.add_argument(
        "--version",
        type=int,
        help="Version pour les tables (par défaut: auto-incrémenté)"
    )
    
    args = parser.parse_args()
    
    # Parser la date si fournie
    export_date = None
    if args.date:
        try:
            export_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            logger.error("Format de date invalide. Utilisez YYYY-MM-DD")
            sys.exit(1)
    
    # Initialiser le client ksqlDB
    try:
        client = KsqlDBClient(
            KSQLDB_CONFIG['host'],
            KSQLDB_CONFIG['port'],
            KSQLDB_CONFIG['timeout']
        )
    except Exception as e:
        logger.error(f"Impossible de se connecter à ksqlDB: {e}")
        sys.exit(1)
    
    # Initialiser l'exporteur
    exporter = DataLakeExporter(client)
    
    # Exporter selon les arguments
    try:
        if args.all:
            logger.info("Export de tous les streams et tables")
            
            # Exporter tous les streams
            for stream_name, config in STREAMS_CONFIG.items():
                exporter.export_stream(stream_name, config, export_date)
            
            # Exporter toutes les tables
            for table_name, config in TABLES_CONFIG.items():
                exporter.export_table(table_name, config, args.version)
        
        elif args.stream:
            if args.stream not in STREAMS_CONFIG:
                logger.error(f"Stream inconnu: {args.stream}")
                sys.exit(1)
            
            exporter.export_stream(
                args.stream,
                STREAMS_CONFIG[args.stream],
                export_date
            )
        
        elif args.table:
            if args.table not in TABLES_CONFIG:
                logger.error(f"Table inconnue: {args.table}")
                sys.exit(1)
            
            exporter.export_table(
                args.table,
                TABLES_CONFIG[args.table],
                args.version
            )
        
        else:
            parser.print_help()
            sys.exit(1)
        
        logger.info("✓ Export terminé avec succès")
    
    except Exception as e:
        logger.error(f"Erreur lors de l'export: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
