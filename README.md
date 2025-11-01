# Data Lake & Warehouse — Guide Étudiant

Ce dépôt rassemble les scripts minimaux pour répondre à l’énoncé : design du data lake, design du data warehouse, consumers Kafka, gouvernance/sécurité, orchestration. Chaque script est volontairement simple pour être utilisable en Master 1.

## Prérequis
- Python 3.10+
- Kafka en local (topics déjà créés)
- MySQL local (base `data_warehouse`)
- `pip install -r requirements.txt`

## Mise en place
```bash
pip install -r requirements.txt
python data_lake_config.py   # crée data_lake/streams et data_lake/tables
```

## Parcours principal
1. **Produire des messages**
   ```bash
   python kafka_producer.py --topic transaction_stream --messages 20000 --rate 1000
   ```
2. **Consommer vers le data lake**
   ```bash
   python kafka_consumer_datalake.py --topics transaction_stream
   ```
3. **Vérifier les partitions**
   ```bash
   ls -R data_lake/streams/transaction_stream
   ```
4. **Exporter toutes les tables ksqlDB vers MySQL**
   ```bash
   python sync_to_mysql.py --mysql-password mon_mot_de_passe
   ```
5. **Consommer directement vers MySQL** (facultatif)
   ```bash
   python kafka_consumer_warehouse.py --topic user_transaction_summary --mysql-password mon_mot_de_passe
   ```

### Schéma MySQL (résumé)
- `dim_users (PK user_id)` → `fact_user_transaction_summary`, `fact_user_transaction_summary_eur`
- `dim_payment_methods (PK payment_method_id)` → `fact_payment_method_totals`
- `fact_product_purchase_counts` conserve son propre `product_id`

## Gouvernance & sécurité
- **Suppression des historiques**
  ```bash
  python data_retention.py --stream transaction_stream --retention-days 30 --dry-run
  python data_retention.py --table user_transaction_summary --keep-versions 5
  ```
- **Permissions par dossier**
  ```bash
  python permissions_manager.py create-table --mysql-password mon_mot_de_passe
  python permissions_manager.py grant --email alice@example.com \
      --folder streams/transaction_stream --permission read \
      --granted-by admin@example.com --mysql-password mon_mot_de_passe
  python permissions_manager.py list --mysql-password mon_mot_de_passe
  ```
  → La table `data_lake_permissions` dans MySQL stocke `user_email`, `folder_path`, le type de droit (`read`, `write`, `admin`) et la date de fin éventuelle.
- **Nouveau feed**
  1. `python manage_feeds.py add --name new_feed --type stream --source new_feed --description "..."`
  2. `python export_to_data_lake.py --stream new_feed` (ou `--table`).
  3. Lancer les consumers Kafka avec le nouveau topic (`--topics` / `--topic`).
  4. Si besoin MySQL : ajouter la table dans `TABLES_CONFIG` et relancer `sync_to_mysql.py`.

## Orchestration & optimisation
- **Apache Beam + schedule**
  ```bash
  python beam_orchestrator.py --task export_datalake --every-minutes 10
  python beam_orchestrator.py --task kafka_produce --topic transaction_stream --messages 50000 --rate 2000
  # En mode Dataflow : ajouter --runner DataflowRunner et les options (project, region, temp_location...)
  ```
- **Producteur Kafka** : augmenter `--messages` / `--rate` pour tester la montée en charge.

## Résumé des scripts
- `data_lake_config.py` : structure du data lake + configuration streams/tables.
- `export_to_data_lake.py` : export ksqlDB → Parquet (partition date ou version).
- `sync_to_mysql.py` : synchronisation ksqlDB → MySQL avec PK/FK.
- `kafka_consumer_datalake.py` / `kafka_consumer_warehouse.py` : consumers minimalistes.
- `manage_feeds.py` : ajout/archivage/restauration de feeds.
- `data_retention.py` : suppression des partitions/versions anciennes.
- `permissions_manager.py` : gestion des droits dans MySQL.
- `beam_orchestrator.py` : planification toutes les X minutes (Beam + schedule).
- `kafka_producer.py` : génération de messages Kafka configurable.
- `ARCHITECTURE.md` : justification détaillée du design.

## Documentation
- `ARCHITECTURE.md` pour la justification courte.
- Pour les documents plus complets du projet initial (DESIGN_DOCUMENT.md, DATA_FLOW.md, etc.), se référer au dépôt d’origine si besoin.
