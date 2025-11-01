# Architecture & Justification

Ce document explique comment les scripts de ce dépôt couvrent toutes les attentes de l’énoncé : data lake, data warehouse, consumers Kafka, gouvernance et orchestration.

## Vue d'ensemble

```
Kafka topics ──> kafka_consumer_datalake.py ──> data_lake/
                  │
                  └──> kafka_consumer_warehouse.py ──> MySQL (data_warehouse)

export_to_data_lake.py  (ksqlDB → data_lake, partitionné)
sync_to_mysql.py        (ksqlDB → MySQL)

Gouvernance : data_retention.py, permissions_manager.py, manage_feeds.py
Orchestration : beam_orchestrator.py + schedule, kafka_producer.py
```

## Data Lake
- Structure générée par `data_lake_config.py` :
  - Streams : `data_lake/streams/<stream>/year=YYYY/month=MM/day=DD`.
  - Tables : `data_lake/tables/<table>/version=v1`.
- Partitionnement :
  - **Streams** en append par date (facile à rejouer et à nettoyer via `data_retention.py`).
  - **Tables** en versions successives (nouvelle version = nouveau dossier, purge contrôlée).
- Modes `APPEND` vs `OVERWRITE` définis dans `data_lake_config.py` et appliqués par `export_to_data_lake.py`.
- Ajout d’un feed : `manage_feeds.py add ...` crée la config + le dossier, on réutilise `export_to_data_lake.py` et les consumers Kafka en passant le nouveau topic.

## Data Warehouse MySQL
- `sync_to_mysql.py` lit les tables ksqlDB et charge MySQL.
- Schéma minimal :
  - `dim_users (PK user_id)` → `fact_user_transaction_summary`, `fact_user_transaction_summary_eur` (FK user_id).
  - `dim_payment_methods (PK payment_method_id)` → `fact_payment_method_totals` (FK payment_method_id).
  - `fact_product_purchase_counts` garde son PK `product_id`.
- Les faits stockent `snapshot_date` et `snapshot_version` pour tracer l’origine de l’export.
- `kafka_consumer_warehouse.py` réutilise les mêmes inserts pour alimenter MySQL en direct depuis Kafka.

## Flux Kafka
- `kafka_producer.py` génère des événements aléatoires (paramètres `--messages`, `--rate`).
- `kafka_consumer_datalake.py` écrit les messages en Parquet partitionné par date.
- `kafka_consumer_warehouse.py` insère les mêmes messages directement en MySQL.

## Gouvernance & Sécurité
- **Rétention** (`data_retention.py`) : supprime les partitions trop anciennes ou les versions de tables dépassées (`--dry-run` pour vérifier).
- **Permissions** (`permissions_manager.py`) : crée la table `data_lake_permissions` dans MySQL et gère `create-table`, `grant`, `list`.
- **Nouveaux feeds** (`manage_feeds.py`) : procédure standardisée (add → export → consumers → sync) pour accélérer l’intégration.

## Orchestration & Optimisation
- `beam_orchestrator.py` + `schedule` exécutent une tâche toutes les X minutes (`export_datalake`, `sync_warehouse`, `kafka_produce`).
  - DirectRunner local pour les tests.
  - Compatible Dataflow via `--runner DataflowRunner` + options (project, region, temp_location...).
- `kafka_producer.py` est utilisé par l’orchestrateur pour simuler des charges plus lourdes.

## Conclusion
Le dépôt reste volontairement simple mais chaque script répond à une exigence :
- data lake partitionné + modes append/overwrite,
- tables MySQL synchronisées avec PK/FK,
- consumers Kafka pour data lake et warehouse,
- gouvernance (rétention + permissions + ajout de feed),
- orchestrateur Beam + schedule.
