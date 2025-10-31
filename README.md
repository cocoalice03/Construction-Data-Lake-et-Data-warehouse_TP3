# Data Lake — Guide étudiant 


## Prérequis
- Python 3.10+
- Kafka en local (topic créé)

## Installation rapide
```bash
pip install -r requirements.txt
python data_lake_config.py   # crée la structure minimale
```

## Démarrage rapide
1) Produire des messages vers un topic (ex: transaction_stream)
```bash
python kafka_producer.py --topic transaction_stream --messages 100 --rate 50
```
2) Consommer et écrire en Parquet (Data Lake)
```bash
python kafka_consumer_datalake.py --topics transaction_stream
```
3) Vérifier les fichiers
```bash
ls -R data_lake/streams/transaction_stream
```


## Annexes (contenu avancé)
- Design/Architecture: ARCHITECTURE.md, DESIGN_DOCUMENT.md
- Gouvernance/Sécurité: GOVERNANCE_SECURITY.md, GOVERNANCE_SUMMARY.md
- Guides complets: QUICK_START.md, DATA_FLOW.md, KAFKA_CONSUMERS_GUIDE.md
- Data Warehouse: DATA_WAREHOUSE_DESIGN.md, DATA_WAREHOUSE_QUICKSTART.md

## Notes
Ce dépôt conserve toutes les documentations détaillées en fichiers séparés (voir Annexes).
