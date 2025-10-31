# 🏞️ Data Lake - Architecture et Documentation

## 📋 Vue d'ensemble

Ce data lake stocke toutes les données provenant de ksqlDB sur le système de fichiers local avec un partitionnement par date pour optimiser les requêtes et la gestion des données.

## 🗂️ Structure du Data Lake

```
data_lake/
├── streams/                          # Données des streams ksqlDB
│   ├── transaction_stream/           # Stream principal (données brutes)
│   │   ├── year=2025/
│   │   │   ├── month=01/
│   │   │   │   ├── day=15/
│   │   │   │   │   └── data_20250115_*.parquet
│   │   │   │   └── day=16/
│   │   │   │       └── data_20250116_*.parquet
│   │   │   └── month=02/
│   │   └── _metadata.json
│   │
│   ├── transaction_flattened/        # Stream avec schéma aplati
│   │   ├── year=2025/
│   │   │   └── month=01/
│   │   └── _metadata.json
│   │
│   ├── transaction_stream_anonymized/ # Stream anonymisé + conversion EUR
│   │   ├── year=2025/
│   │   │   └── month=01/
│   │   └── _metadata.json
│   │
│   └── transaction_stream_blacklisted/ # Transactions blacklistées
│       ├── year=2025/
│       │   └── month=01/
│       └── _metadata.json
│
├── tables/                           # Données des tables ksqlDB (agrégations)
│   ├── user_transaction_summary/     # Agrégation par utilisateur
│   │   ├── version=v1/
│   │   │   └── snapshot_20250115_120000.parquet
│   │   ├── version=v2/
│   │   │   └── snapshot_20250116_120000.parquet
│   │   └── _metadata.json
│   │
│   ├── user_transaction_summary_eur/ # Agrégation en EUR
│   │   ├── version=v1/
│   │   └── _metadata.json
│   │
│   ├── payment_method_totals/        # Totaux par méthode de paiement
│   │   ├── version=v1/
│   │   └── _metadata.json
│   │
│   └── product_purchase_counts/      # Compteurs par produit
│       ├── version=v1/
│       └── _metadata.json
│
├── feeds/                            # Configuration des feeds
│   ├── active/                       # Feeds actifs
│   │   ├── transaction_feed.json
│   │   └── payment_feed.json
│   └── archived/                     # Feeds archivés
│
└── logs/                             # Logs d'export
    └── export_logs_2025_01.log
```

## 📊 Streams et Tables ksqlDB

### Streams (Données en flux continu)

| Stream | Description | Partitionnement | Mode de stockage |
|--------|-------------|-----------------|------------------|
| `transaction_stream` | Stream principal avec données brutes | Par date (year/month/day) | **APPEND** |
| `transaction_flattened` | Stream avec schéma aplati | Par date (year/month/day) | **APPEND** |
| `transaction_stream_anonymized` | Stream anonymisé + conversion EUR | Par date (year/month/day) | **APPEND** |
| `transaction_stream_blacklisted` | Transactions des villes blacklistées | Par date (year/month/day) | **APPEND** |

### Tables (Agrégations matérialisées)

| Table | Description | Partitionnement | Mode de stockage |
|-------|-------------|-----------------|------------------|
| `user_transaction_summary` | Montants par utilisateur et type | Par version | **OVERWRITE** |
| `user_transaction_summary_eur` | Montants en EUR par utilisateur | Par version | **OVERWRITE** |
| `payment_method_totals` | Totaux par méthode de paiement | Par version | **OVERWRITE** |
| `product_purchase_counts` | Compteurs par produit | Par version | **OVERWRITE** |

## 🎯 Justification des Modes de Stockage

### Mode APPEND (Streams)

**Utilisé pour**: Tous les streams de transactions

**Justification**:
- ✅ Les streams représentent des **événements immuables** dans le temps
- ✅ Chaque transaction est un **événement unique** qui ne doit jamais être modifié
- ✅ Permet de **conserver l'historique complet** pour audit et traçabilité
- ✅ Facilite le **replay** des données en cas de besoin
- ✅ Optimise les **performances d'écriture** (pas de recherche/mise à jour)
- ✅ Compatible avec les principes de **Event Sourcing**

**Exemple**: Une transaction du 15 janvier 2025 sera toujours stockée dans `year=2025/month=01/day=15/` et ne sera jamais modifiée.

### Mode OVERWRITE (Tables)

**Utilisé pour**: Toutes les tables d'agrégation

**Justification**:
- ✅ Les tables contiennent des **agrégations calculées** qui évoluent dans le temps
- ✅ Chaque export représente un **snapshot complet** de l'état actuel
- ✅ Évite la **duplication de données** agrégées
- ✅ Simplifie les **requêtes analytiques** (toujours la dernière version)
- ✅ Réduit l'**espace disque** utilisé
- ✅ Facilite la **maintenance** (pas d'accumulation de versions obsolètes)

**Exemple**: `user_transaction_summary` est recalculé chaque jour avec les totaux à jour. La version v2 remplace la version v1.

### Mode IGNORE (Non utilisé)

**Pourquoi pas utilisé**:
- ❌ Ne convient pas aux streams (besoin d'ajouter continuellement)
- ❌ Ne convient pas aux tables (besoin de mettre à jour les agrégations)
- ❌ Pourrait être utilisé uniquement pour des **données de référence statiques** (non présentes dans ce projet)

## 📁 Format de Stockage: Parquet

**Choix**: Apache Parquet

**Justifications**:
1. **Compression efficace**: Réduction de 70-90% de l'espace disque vs JSON
2. **Lecture optimisée**: Format columnaire pour requêtes analytiques rapides
3. **Schéma intégré**: Pas besoin de fichier de schéma séparé
4. **Compatible**: Spark, Pandas, DuckDB, Arrow, etc.
5. **Partitionnement**: Support natif du partitionnement par colonnes
6. **Types de données**: Support complet des types complexes (STRUCT, ARRAY, MAP)

## 🔄 Gestion des Nouveaux Feeds

### Processus d'ajout d'un nouveau feed

1. **Créer la configuration du feed** dans `feeds/active/`
2. **Le script détecte automatiquement** le nouveau feed
3. **Création automatique** de la structure de dossiers
4. **Export des données** selon la configuration

### Configuration d'un feed (JSON)

```json
{
  "feed_name": "payment_feed",
  "feed_type": "stream",
  "ksqldb_source": "payment_stream",
  "description": "Stream des paiements",
  "partitioning": {
    "type": "date",
    "columns": ["year", "month", "day"]
  },
  "storage_mode": "append",
  "format": "parquet",
  "retention_days": 365,
  "enabled": true,
  "created_at": "2025-01-15T10:00:00Z"
}
```

## 📅 Partitionnement par Date

### Avantages

1. **Performance**: Requêtes filtrées par date sont ultra-rapides
2. **Maintenance**: Suppression facile des anciennes partitions
3. **Scalabilité**: Ajout de nouvelles partitions sans impact
4. **Parallélisme**: Lecture/écriture parallèle par partition
5. **Coût**: Archivage sélectif des anciennes données

### Exemple de requête optimisée

```python
# Lire uniquement les données de janvier 2025
df = pd.read_parquet(
    'data_lake/streams/transaction_stream_anonymized',
    filters=[('year', '=', 2025), ('month', '=', 1)]
)
```

## 🔐 Sécurité et Conformité

### Données Anonymisées

- Les données sensibles sont stockées **uniquement** dans `transaction_stream_anonymized`
- Les données brutes dans `transaction_stream` doivent être **protégées** avec des permissions restrictives
- Les transactions blacklistées sont **isolées** dans un stream séparé

### Permissions recommandées

```bash
# Streams bruts: lecture restreinte
chmod 750 data_lake/streams/transaction_stream/

# Streams anonymisés: lecture étendue
chmod 755 data_lake/streams/transaction_stream_anonymized/

# Tables: lecture étendue
chmod 755 data_lake/tables/
```

## 📊 Métriques et Monitoring

### Fichiers de métadonnées (_metadata.json)

Chaque stream/table contient un fichier `_metadata.json` avec:

```json
{
  "source": "transaction_stream",
  "type": "stream",
  "storage_mode": "append",
  "format": "parquet",
  "partitioning": "date",
  "last_export": "2025-01-15T14:30:00Z",
  "total_records": 150000,
  "total_size_mb": 45.2,
  "partitions": [
    {
      "path": "year=2025/month=01/day=15",
      "records": 5000,
      "size_mb": 1.5
    }
  ],
  "schema_version": "1.0",
  "created_at": "2025-01-01T00:00:00Z"
}
```

## 🚀 Utilisation

### Export manuel

```bash
# Exporter tous les streams et tables
python export_to_data_lake.py --all

# Exporter un stream spécifique
python export_to_data_lake.py --stream transaction_stream_anonymized

# Exporter une table spécifique
python export_to_data_lake.py --table user_transaction_summary
```

### Export automatique (cron)

```bash
# Ajouter au crontab pour export quotidien à 2h du matin
0 2 * * * cd /path/to/project && python export_to_data_lake.py --all >> logs/export.log 2>&1
```

### Ajouter un nouveau feed

```bash
python manage_feeds.py add --name new_feed --type stream --source new_stream
```

## 📈 Évolution Future

### Possibilités d'extension

1. **Compression avancée**: Utiliser Snappy ou ZSTD pour Parquet
2. **Tiering**: Déplacer les anciennes données vers un stockage froid (S3, Azure Blob)
3. **Indexation**: Créer des index pour accélérer les recherches
4. **Catalogue de données**: Intégrer avec Apache Hive Metastore ou AWS Glue
5. **Qualité des données**: Ajouter des validations et des checks de qualité
6. **Versioning**: Implémenter Delta Lake pour le versioning des données

## 📚 Fichiers du Projet

- `export_to_data_lake.py` - Script principal d'export
- `manage_feeds.py` - Gestion des feeds
- `data_lake_config.py` - Configuration centralisée
- `README.md` - Cette documentation
