# Procurement Data Pipeline

## Vue d'ensemble

Pipeline de données batch simplifié pour un système d'approvisionnement retail.
Il collecte les commandes quotidiennes des points de vente, calcule la demande nette (MRP)
et génère automatiquement des bons de commande fournisseurs chaque soir à 22h.

## Architecture

```
Points de Vente (JSON)       Entrepôts (CSV)        Master Data (PostgreSQL)
       │                          │                    products / suppliers
       ▼                          ▼                    product_suppliers
  [HDFS /raw/orders]        [HDFS /raw/stock]               │
       │                          │                         │
       └──────────────┬───────────┘                         │
                      ▼                                     │
              [Trino / Hive]  ◄────────────────────────────┘
         Agrégation + MRP + ranked_suppliers
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
   /processed/   /output/    /logs/
   (Parquet)  (JSON+coûts)  (exceptions)
          │
          ▼
   [Dashboard Streamlit]
```

## Stack technique

| Composant       | Technologie                | Rôle                                        |
|-----------------|----------------------------|---------------------------------------------|
| Stockage        | HDFS (2 DataNodes, ×2)     | Data lake distribué, réplication factor=2   |
| Compute         | Trino 435 (Presto)         | Requêtes SQL fédérées HDFS + PostgreSQL     |
| Orchestration   | Apache Airflow 2.10        | Batch journalier à 22h00                    |
| Master Data     | PostgreSQL 15              | products, suppliers, product_suppliers      |
| Metastore       | Hive Metastore             | Catalogue des tables externes HDFS          |
| Visualisation   | Streamlit + Plotly         | Dashboard procurement temps réel            |
| Format données  | Parquet (HDFS), JSON       | Stockage colonnaire + exports fournisseurs  |

## Prérequis

- Docker et Docker Compose v2+
- 8 Go RAM minimum (16 Go recommandé)
- 20 Go d'espace disque libre

## Démarrage rapide

```bash
# 1. Lancer l'infrastructure
cd docker
docker-compose up -d

# Attendre ~3-4 minutes puis surveiller
watch -n 5 docker-compose ps

# 2. HDFS initialisé automatiquement via hdfs-init (entrypoint Docker)

# 3. Créer/aligner les tables Trino/Hive
#    ✅ Plus besoin d'un DAG séparé: le DAG `procurement_pipeline_v2`
#    commence par la tâche `setup_trino_hive` (idempotent).

# 4. Générer les données de test
docker exec airflow_scheduler python /opt/airflow/data-generators/generate_orders.py
docker exec airflow_scheduler python /opt/airflow/data-generators/generate_inventory.py
docker exec airflow_scheduler python /opt/airflow/data-generators/generate_products.py
docker exec airflow_scheduler python /opt/airflow/data-generators/generate_suppliers.py

# 5. Lancer le pipeline complet
#    Airflow UI -> DAG procurement_pipeline_v2 -> Trigger
#    Ou directement :
docker exec airflow_scheduler python /opt/airflow/pipeline/orchestrator.py
```

## Accès aux services

| Service           | URL                    | Identifiants            |
|-------------------|------------------------|-------------------------|
| Airflow UI        | http://localhost:8082  | admin / admin           |
| HDFS NameNode UI  | http://localhost:9870  | -                       |
| Trino UI          | http://localhost:8080  | -                       |
| pgAdmin           | http://localhost:5050  | admin@admin.com / admin |
| Dashboard         | http://localhost:8501  | -                       |
| PostgreSQL        | localhost:5432         | de_user / de_pass       |

## Structure du projet

```
procurement-pipeline/
├── config/
│   └── pipeline_config.yaml
├── dags/
│   └── procurement_dag.py            # DAG unique (setup + pipeline complet)
├── pipeline/
│   ├── __init__.py
│   ├── trino_client.py
│   ├── process_data.py
│   ├── convert_to_parquet.py
│   ├── ingest_to_hdfs.py
│   ├── export_orders.py              # JSON structuré avec total_cost
│   ├── copy_to_processed.py          # Archivage HDFS /processed/
│   ├── exceptions.py                 # 5 checks qualité
│   ├── sync_partitions.py
│   └── orchestrator.py
├── sql-queries/
│   ├── aggregate_orders.sql
│   └── calculate_net_demand.sql      # MRP + ranked_suppliers + estimated_cost
├── data-generators/
│   ├── generate_orders.py
│   ├── generate_inventory.py
│   ├── generate_products.py
│   └── generate_suppliers.py
├── dashboard/
│   ├── dashboard.py
│   └── Dockerfile
├── docker/
│   ├── docker-compose.yml
│   ├── hadoop/hadoop.env
│   ├── hive/Dockerfile
│   ├── pgadmin/servers.json
│   ├── postgres/
│   │   ├── init.sql                  # products + suppliers + product_suppliers
│   │   └── init_hive.sql
│   └── presto/catalog/
│       ├── hive.properties
│       └── postgresql.properties
├── data/                             # Données runtime (ne pas versionner)
├── README.md
└── requirements.txt
```

## Structure HDFS

```
/procurement/
├── raw/
│   ├── orders/exec_date=YYYY-MM-DD/
│   └── inventory/exec_date=YYYY-MM-DD/
├── processed/
│   ├── aggregated_orders/exec_date=YYYY-MM-DD/
│   └── net_demand/exec_date=YYYY-MM-DD/
├── output/
│   └── supplier_orders/order_date=YYYY-MM-DD/
└── logs/
    └── exceptions/exec_date=YYYY-MM-DD/
```

## DAG Airflow

```
setup_trino_hive
      |
generate_data
      |
convert_parquet
      |
ingest_to_hdfs
      |
sync_partitions
      |
aggregate_orders
      |
calculate_net_demand
      |---------------------|
  export_orders       quality_checks
      |---------------------|
      |
copy_to_processed
```

Schedule : 0 22 * * * (tous les jours a 22h00)

## Formule MRP

```
net_demand      = MAX(0, total_orders + safety_stock - (available_stock - reserved_stock))
final_order_qty = CEIL(net_demand / pack_size) x pack_size   [si >= MOQ]
estimated_cost  = final_order_qty x unit_cost
```

Le fournisseur est selectionne via ranked_suppliers (priority ASC, unit_cost ASC).

## Schema PostgreSQL

### products
| Colonne        | Type         | Description               |
|----------------|--------------|---------------------------|
| sku            | VARCHAR(20)  | PK                        |
| name           | VARCHAR(100) | Nom du produit            |
| supplier_id    | VARCHAR(20)  | Fournisseur principal     |
| pack_size      | INT          | Taille du conditionnement |
| moq            | INT          | Quantite minimale         |
| lead_time_days | INT          | Delai livraison           |
| safety_stock   | INT          | Stock de securite         |

### suppliers
| Colonne           | Type         | Description           |
|-------------------|--------------|-----------------------|
| supplier_id       | VARCHAR(20)  | PK                    |
| name              | VARCHAR(100) | Nom                   |
| contact_email     | VARCHAR(100) | Email de commande     |
| lead_time_days    | INT          | Delai par defaut      |
| reliability_score | DECIMAL(3,2) | Score fiabilite (0-1) |

### product_suppliers (N:N)
| Colonne      | Type          | Description                       |
|--------------|---------------|-----------------------------------|
| sku          | VARCHAR(20)   | FK products                       |
| supplier_id  | VARCHAR(20)   | FK suppliers                      |
| unit_cost    | DECIMAL(10,2) | Cout unitaire chez ce fournisseur |
| priority     | INT           | Priorite (1 = prefere)            |
| is_preferred | BOOLEAN       | Fournisseur prefere ?             |

### (Important) net_demand n'est PAS dans PostgreSQL
Le pipeline écrit `net_demand` dans **Hive/Trino** (`processed.net_demand`) au format Parquet (HDFS),
et utilise PostgreSQL uniquement pour les master data (`products`, `suppliers`, `product_suppliers`).

## Format JSON fournisseurs

```json
{
  "supplier_id": "SUP_001",
  "supplier_name": "FreshFruit Co",
  "contact_email": "orders@freshfruit.com",
  "order_date": "2026-01-15",
  "data_date": "2026-01-14",
  "generated_at": "2026-01-14T22:05:12.483",
  "items": [
    {
      "sku": "SKU_1001",
      "product_name": "Bananas",
      "quantity": 48,
      "unit_cost": 1.20,
      "total_cost": 57.60
    }
  ],
  "total_estimated_cost": 57.60
}
```

## Controles qualite (5 checks)

| Check | Description |
|-------|-------------|
| missing_pos_files       | Tous les stores ont envoye leurs fichiers ? |
| missing_inventory       | Snapshot CSV du jour present ?             |
| missing_supplier_mapping| Chaque SKU mappe a un fournisseur ?        |
| demand_spike            | Quantites anormalement elevees ?           |
| inventory_inconsistency | Stock reserve > disponible ? Negatif ?     |

Rapport : data/logs/exceptions/exceptions_YYYY-MM-DD.json

## Depannage

```bash
# Services qui ne demarrent pas
docker-compose logs <service>
docker-compose restart <service>

# HDFS
docker exec namenode hdfs dfsadmin -report
docker exec namenode hdfs dfs -ls /procurement

# Trino
docker exec presto_procurement trino --user de_user \
  --execute "SHOW SCHEMAS FROM hive"

# PostgreSQL
docker exec postgres_procurement psql -U de_user -d procurement_db \
  -c "SELECT * FROM product_suppliers;"

# Airflow
docker exec airflow_scheduler airflow dags list
```

## Equipe

- Hafsa El-Mahdi — Data Engineering, 2eme annee
- Module : Fondements Big Data — ENSAH Al-Hoceima

## Licence

Projet academique — Tous droits reserves