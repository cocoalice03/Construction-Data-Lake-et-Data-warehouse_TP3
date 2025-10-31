"""
Script de gestion des feeds du Data Lake
Permet d'ajouter, modifier, désactiver et archiver des feeds
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from data_lake_config import (
    FEEDS_DIR, FeedType, PartitioningType, StorageMode,
    STREAMS_CONFIG, TABLES_CONFIG, STORAGE_FORMAT,
    get_stream_path, get_table_path, ensure_directories
)


class FeedManager:
    """Gestionnaire de feeds pour le Data Lake"""
    
    def __init__(self):
        self.active_dir = FEEDS_DIR / "active"
        self.archived_dir = FEEDS_DIR / "archived"
        ensure_directories()
    
    def list_feeds(self, show_archived: bool = False):
        """Liste tous les feeds actifs (et archivés si demandé)"""
        print("\n📋 Feeds actifs:")
        print("-" * 80)
        
        active_feeds = list(self.active_dir.glob("*.json"))
        if not active_feeds:
            print("  Aucun feed actif")
        else:
            for feed_file in sorted(active_feeds):
                self._display_feed(feed_file)
        
        if show_archived:
            print("\n📦 Feeds archivés:")
            print("-" * 80)
            
            archived_feeds = list(self.archived_dir.glob("*.json"))
            if not archived_feeds:
                print("  Aucun feed archivé")
            else:
                for feed_file in sorted(archived_feeds):
                    self._display_feed(feed_file)
    
    def _display_feed(self, feed_file: Path):
        """Affiche les informations d'un feed"""
        with open(feed_file, 'r') as f:
            feed = json.load(f)
        
        status = "✓ Actif" if feed.get('enabled', True) else "✗ Désactivé"
        print(f"\n  {feed['feed_name']} ({status})")
        print(f"    Type: {feed['feed_type']}")
        print(f"    Source: {feed['ksqldb_source']}")
        print(f"    Description: {feed['description']}")
        print(f"    Partitionnement: {feed['partitioning']['type']}")
        print(f"    Mode de stockage: {feed['storage_mode']}")
        print(f"    Rétention: {feed.get('retention_days', 'N/A')} jours")
        print(f"    Créé le: {feed['created_at']}")
    
    def add_feed(
        self,
        name: str,
        feed_type: str,
        source: str,
        description: str,
        partitioning: str = "date",
        storage_mode: str = "append",
        retention_days: int = 365
    ):
        """Ajoute un nouveau feed"""
        # Valider le type de feed
        try:
            feed_type_enum = FeedType(feed_type)
        except ValueError:
            print(f"❌ Type de feed invalide: {feed_type}")
            print(f"   Types valides: {', '.join([t.value for t in FeedType])}")
            return False
        
        # Valider le partitionnement
        try:
            partitioning_enum = PartitioningType(partitioning)
        except ValueError:
            print(f"❌ Type de partitionnement invalide: {partitioning}")
            print(f"   Types valides: {', '.join([t.value for t in PartitioningType])}")
            return False
        
        # Valider le mode de stockage
        try:
            storage_mode_enum = StorageMode(storage_mode)
        except ValueError:
            print(f"❌ Mode de stockage invalide: {storage_mode}")
            print(f"   Modes valides: {', '.join([m.value for m in StorageMode])}")
            return False
        
        # Vérifier si le feed existe déjà
        feed_file = self.active_dir / f"{name}.json"
        if feed_file.exists():
            print(f"❌ Le feed '{name}' existe déjà")
            return False
        
        # Créer la configuration du feed
        feed_config = {
            "feed_name": name,
            "feed_type": feed_type,
            "ksqldb_source": source,
            "description": description,
            "partitioning": {
                "type": partitioning,
                "columns": self._get_partition_columns(partitioning_enum)
            },
            "storage_mode": storage_mode,
            "format": STORAGE_FORMAT,
            "retention_days": retention_days,
            "enabled": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        # Sauvegarder la configuration
        with open(feed_file, 'w') as f:
            json.dump(feed_config, f, indent=2)
        
        # Créer la structure de dossiers
        if feed_type_enum == FeedType.STREAM:
            target_path = get_stream_path(name)
        else:
            target_path = get_table_path(name)
        
        target_path.mkdir(parents=True, exist_ok=True)
        
        print(f"✓ Feed '{name}' créé avec succès")
        print(f"  Fichier de configuration: {feed_file}")
        print(f"  Dossier de stockage: {target_path}")
        return True
    
    def _get_partition_columns(self, partitioning: PartitioningType) -> list:
        """Retourne les colonnes de partitionnement selon le type"""
        if partitioning == PartitioningType.DATE:
            return ["year", "month", "day"]
        elif partitioning == PartitioningType.VERSION:
            return ["version"]
        return []
    
    def update_feed(self, name: str, **kwargs):
        """Met à jour la configuration d'un feed"""
        feed_file = self.active_dir / f"{name}.json"
        
        if not feed_file.exists():
            print(f"❌ Le feed '{name}' n'existe pas")
            return False
        
        # Charger la configuration existante
        with open(feed_file, 'r') as f:
            feed_config = json.load(f)
        
        # Mettre à jour les champs fournis
        updated = False
        for key, value in kwargs.items():
            if value is not None and key in feed_config:
                feed_config[key] = value
                updated = True
                print(f"  ✓ {key}: {value}")
        
        if updated:
            feed_config['updated_at'] = datetime.now().isoformat()
            
            # Sauvegarder
            with open(feed_file, 'w') as f:
                json.dump(feed_config, f, indent=2)
            
            print(f"✓ Feed '{name}' mis à jour")
            return True
        else:
            print(f"⚠ Aucune modification apportée au feed '{name}'")
            return False
    
    def enable_feed(self, name: str):
        """Active un feed"""
        return self.update_feed(name, enabled=True)
    
    def disable_feed(self, name: str):
        """Désactive un feed"""
        return self.update_feed(name, enabled=False)
    
    def archive_feed(self, name: str):
        """Archive un feed (le déplace vers le dossier archived)"""
        feed_file = self.active_dir / f"{name}.json"
        
        if not feed_file.exists():
            print(f"❌ Le feed '{name}' n'existe pas")
            return False
        
        # Déplacer vers archived
        archived_file = self.archived_dir / f"{name}.json"
        feed_file.rename(archived_file)
        
        # Mettre à jour la configuration
        with open(archived_file, 'r') as f:
            feed_config = json.load(f)
        
        feed_config['enabled'] = False
        feed_config['archived_at'] = datetime.now().isoformat()
        
        with open(archived_file, 'w') as f:
            json.dump(feed_config, f, indent=2)
        
        print(f"✓ Feed '{name}' archivé")
        return True
    
    def restore_feed(self, name: str):
        """Restaure un feed archivé"""
        archived_file = self.archived_dir / f"{name}.json"
        
        if not archived_file.exists():
            print(f"❌ Le feed archivé '{name}' n'existe pas")
            return False
        
        # Déplacer vers active
        feed_file = self.active_dir / f"{name}.json"
        archived_file.rename(feed_file)
        
        # Mettre à jour la configuration
        with open(feed_file, 'r') as f:
            feed_config = json.load(f)
        
        feed_config['enabled'] = True
        feed_config['restored_at'] = datetime.now().isoformat()
        if 'archived_at' in feed_config:
            del feed_config['archived_at']
        
        with open(feed_file, 'w') as f:
            json.dump(feed_config, f, indent=2)
        
        print(f"✓ Feed '{name}' restauré")
        return True
    
    def delete_feed(self, name: str, confirm: bool = False):
        """Supprime définitivement un feed (nécessite confirmation)"""
        # Chercher dans active et archived
        feed_file = self.active_dir / f"{name}.json"
        if not feed_file.exists():
            feed_file = self.archived_dir / f"{name}.json"
        
        if not feed_file.exists():
            print(f"❌ Le feed '{name}' n'existe pas")
            return False
        
        if not confirm:
            print(f"⚠ ATTENTION: Cette action est irréversible!")
            print(f"  Pour confirmer, utilisez --confirm")
            return False
        
        # Supprimer le fichier de configuration
        feed_file.unlink()
        
        print(f"✓ Feed '{name}' supprimé définitivement")
        print(f"  Note: Les données dans le data lake ne sont pas supprimées")
        return True
    
    def sync_from_config(self):
        """Synchronise les feeds depuis la configuration Python"""
        print("\n🔄 Synchronisation des feeds depuis la configuration...")
        
        synced = 0
        
        # Synchroniser les streams
        for stream_name, config in STREAMS_CONFIG.items():
            feed_file = self.active_dir / f"{stream_name}.json"
            
            if not feed_file.exists():
                print(f"  Création du feed: {stream_name}")
                self.add_feed(
                    name=stream_name,
                    feed_type="stream",
                    source=stream_name,
                    description=config['description'],
                    partitioning=config['partitioning'].value,
                    storage_mode=config['storage_mode'].value,
                    retention_days=config.get('retention_days', 365)
                )
                synced += 1
        
        # Synchroniser les tables
        for table_name, config in TABLES_CONFIG.items():
            feed_file = self.active_dir / f"{table_name}.json"
            
            if not feed_file.exists():
                print(f"  Création du feed: {table_name}")
                self.add_feed(
                    name=table_name,
                    feed_type="table",
                    source=table_name,
                    description=config['description'],
                    partitioning=config['partitioning'].value,
                    storage_mode=config['storage_mode'].value,
                    retention_days=config.get('retention_days', 365)
                )
                synced += 1
        
        print(f"\n✓ Synchronisation terminée: {synced} feed(s) créé(s)")


def main():
    """Point d'entrée principal"""
    parser = argparse.ArgumentParser(
        description="Gestion des feeds du Data Lake"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commandes disponibles')
    
    # Commande: list
    list_parser = subparsers.add_parser('list', help='Liste les feeds')
    list_parser.add_argument(
        '--archived',
        action='store_true',
        help='Inclure les feeds archivés'
    )
    
    # Commande: add
    add_parser = subparsers.add_parser('add', help='Ajoute un nouveau feed')
    add_parser.add_argument('--name', required=True, help='Nom du feed')
    add_parser.add_argument(
        '--type',
        required=True,
        choices=['stream', 'table'],
        help='Type de feed'
    )
    add_parser.add_argument('--source', required=True, help='Source ksqlDB')
    add_parser.add_argument('--description', required=True, help='Description')
    add_parser.add_argument(
        '--partitioning',
        default='date',
        choices=['date', 'version'],
        help='Type de partitionnement'
    )
    add_parser.add_argument(
        '--storage-mode',
        default='append',
        choices=['append', 'overwrite', 'ignore'],
        help='Mode de stockage'
    )
    add_parser.add_argument(
        '--retention-days',
        type=int,
        default=365,
        help='Nombre de jours de rétention'
    )
    
    # Commande: update
    update_parser = subparsers.add_parser('update', help='Met à jour un feed')
    update_parser.add_argument('--name', required=True, help='Nom du feed')
    update_parser.add_argument('--description', help='Nouvelle description')
    update_parser.add_argument('--retention-days', type=int, help='Nouvelle rétention')
    
    # Commande: enable
    enable_parser = subparsers.add_parser('enable', help='Active un feed')
    enable_parser.add_argument('name', help='Nom du feed')
    
    # Commande: disable
    disable_parser = subparsers.add_parser('disable', help='Désactive un feed')
    disable_parser.add_argument('name', help='Nom du feed')
    
    # Commande: archive
    archive_parser = subparsers.add_parser('archive', help='Archive un feed')
    archive_parser.add_argument('name', help='Nom du feed')
    
    # Commande: restore
    restore_parser = subparsers.add_parser('restore', help='Restaure un feed archivé')
    restore_parser.add_argument('name', help='Nom du feed')
    
    # Commande: delete
    delete_parser = subparsers.add_parser('delete', help='Supprime définitivement un feed')
    delete_parser.add_argument('name', help='Nom du feed')
    delete_parser.add_argument('--confirm', action='store_true', help='Confirmer la suppression')
    
    # Commande: sync
    sync_parser = subparsers.add_parser('sync', help='Synchronise depuis la configuration')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Initialiser le gestionnaire
    manager = FeedManager()
    
    # Exécuter la commande
    try:
        if args.command == 'list':
            manager.list_feeds(args.archived)
        
        elif args.command == 'add':
            manager.add_feed(
                name=args.name,
                feed_type=args.type,
                source=args.source,
                description=args.description,
                partitioning=args.partitioning,
                storage_mode=args.storage_mode,
                retention_days=args.retention_days
            )
        
        elif args.command == 'update':
            manager.update_feed(
                name=args.name,
                description=args.description,
                retention_days=args.retention_days
            )
        
        elif args.command == 'enable':
            manager.enable_feed(args.name)
        
        elif args.command == 'disable':
            manager.disable_feed(args.name)
        
        elif args.command == 'archive':
            manager.archive_feed(args.name)
        
        elif args.command == 'restore':
            manager.restore_feed(args.name)
        
        elif args.command == 'delete':
            manager.delete_feed(args.name, args.confirm)
        
        elif args.command == 'sync':
            manager.sync_from_config()
    
    except Exception as e:
        print(f"❌ Erreur: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
