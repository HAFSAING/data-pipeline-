"""
DAG: trino_setup
Crée les tables externes dans Trino/Hive.
FIX: exec_date ajouté dans le schéma des tables partitionnées.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import trino

def create_hive_tables():
    """Crée les tables externes pointant vers HDFS."""
    conn = trino.dbapi.connect(
        host='presto_procurement',
        port=8080,
        user='de_user',
        catalog='hive'
    )
    cursor = conn.cursor()

    def run(sql):
        cursor.execute(sql)
        try:
            cursor.fetchall()
        except Exception:
            pass

    # Créer les schémas
    run("CREATE SCHEMA IF NOT EXISTS hive.raw")
    run("CREATE SCHEMA IF NOT EXISTS hive.processed")
    print("Schémas créés")

    # Table orders — exec_date DOIT être dans le schéma ET dans partitioned_by
    run("""
        CREATE TABLE IF NOT EXISTS hive.raw.orders (
            order_id  VARCHAR,
            store_id  VARCHAR,
            sku       VARCHAR,
            quantity  INTEGER,
            timestamp VARCHAR,
            exec_date VARCHAR
        )
        WITH (
            format            = 'PARQUET',
            partitioned_by    = ARRAY['exec_date'],
            external_location = 'hdfs://namenode:9000/procurement/raw/orders'
        )
    """)
    print("Table hive.raw.orders créée")

    # Table inventory — même règle
    run("""
        CREATE TABLE IF NOT EXISTS hive.raw.inventory (
            sku             VARCHAR,
            available_stock INTEGER,
            reserved_stock  INTEGER,
            warehouse       VARCHAR,
            exec_date       VARCHAR
        )
        WITH (
            format            = 'PARQUET',
            partitioned_by    = ARRAY['exec_date'],
            external_location = 'hdfs://namenode:9000/procurement/raw/inventory'
        )
    """)
    print("Table hive.raw.inventory créée")

    # Tables processed (pas de partition — simples)
    run("""
        CREATE TABLE IF NOT EXISTS hive.processed.aggregated_orders (
            sku          VARCHAR,
            total_demand BIGINT,
            exec_date    DATE
        )
        WITH (
            format            = 'PARQUET',
            external_location = 'hdfs://namenode:9000/procurement/processed/aggregated_orders'
        )
    """)
    print("Table hive.processed.aggregated_orders créée")

    run("""
        CREATE TABLE IF NOT EXISTS hive.processed.net_demand (
            sku              VARCHAR,
            supplier_id      VARCHAR,
            total_demand     BIGINT,
            available_stock  INTEGER,
            reserved_stock   INTEGER,
            safety_stock     INTEGER,
            pack_size        INTEGER,
            moq              INTEGER,
            unit_cost        DOUBLE,
            raw_demand       BIGINT,
            final_order_qty  BIGINT,
            estimated_cost   DOUBLE,
            exec_date        DATE
        )
        WITH (
            format            = 'PARQUET',
            external_location = 'hdfs://namenode:9000/procurement/processed/net_demand'
        )
    """)
    print("Table hive.processed.net_demand créée")

    conn.close()
    print("Tables Trino créées avec succès")


with DAG(
    'trino_setup',
    description='Initialisation des tables Trino',
    schedule_interval=None,
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['setup', 'trino']
) as dag:

    setup_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_hive_tables
    )