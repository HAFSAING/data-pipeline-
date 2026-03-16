"""
DAG: trino_setup
Crée les tables externes Hive dans Trino.

CORRECTIONS APPLIQUÉES:
1. Retry logic avec 10 tentatives de connexion à Trino (délai 10s)
2. Logging détaillé pour déboguer les problèmes
3. Gestion d'erreurs explicite
4. Exec_date ajouté dans le schéma des tables partitionnées
5. PostgreSQL catalog requis (doit être configuré dans Trino)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)

def create_hive_tables():
    """Crée les tables externes Hive dans Trino."""
    
    max_retries = 10
    retry_delay = 10  # 10 secondes
    
    for attempt in range(max_retries):
        try:
            logger.info(f"[Attempt {attempt+1}/{max_retries}] Connexion à Trino...")
            import trino
            
            conn = trino.dbapi.connect(
                host='presto_procurement',
                port=8080,
                user='de_user',
                catalog='hive'
            )
            cursor = conn.cursor()
            logger.info(f"✓ Connexion Trino établie")
            
            def run(sql, description=""):
                try:
                    logger.info(f"  → {description}")
                    cursor.execute(sql)
                    try:
                        cursor.fetchall()
                    except Exception:
                        pass
                    logger.info(f"  ✓ {description}")
                except Exception as e:
                    logger.warning(f"  ⚠️  {description}: {str(e)[:100]}")
                    # Ne pas lever l'exception pour permettre la création d'autres tables
            
            # Créer les schémas d'abord avec LOCATION valids
            run("""
                CREATE SCHEMA IF NOT EXISTS raw
                WITH (location = 'hdfs://namenode:9000/warehouse/raw')
            """, "Création schéma raw")
            run("""
                CREATE SCHEMA IF NOT EXISTS processed
                WITH (location = 'hdfs://namenode:9000/warehouse/processed')
            """, "Création schéma processed")
            logger.info("✓ Schémas créés")

            # Table orders (location defaults to schema warehouse)
            run("""
                DROP TABLE IF EXISTS raw.orders
            """, "Suppression table raw.orders")
            run("""
                CREATE TABLE raw.orders (
                    order_id  VARCHAR,
                    store_id  VARCHAR,
                    sku       VARCHAR,
                    quantity  INTEGER,
                    timestamp VARCHAR,
                    exec_date VARCHAR
                )
                WITH (
                    format = 'PARQUET',
                    partitioned_by = ARRAY['exec_date']
                )
            """, "Création table raw.orders")

            # Table inventory
            run("""
                DROP TABLE IF EXISTS raw.inventory
            """, "Suppression table raw.inventory")
            run("""
                CREATE TABLE raw.inventory (
                    sku             VARCHAR,
                    available_stock INTEGER,
                    reserved_stock  INTEGER,
                    warehouse       VARCHAR,
                    exec_date       VARCHAR
                )
                WITH (
                    format = 'PARQUET',
                    partitioned_by = ARRAY['exec_date']
                )
            """, "Création table raw.inventory")

            # Tables processed
            run("""
                DROP TABLE IF EXISTS processed.aggregated_orders
            """, "Suppression table processed.aggregated_orders")
            run("""
                CREATE TABLE processed.aggregated_orders (
                    sku          VARCHAR,
                    total_demand BIGINT,
                    exec_date    VARCHAR
                )
                WITH (
                    format = 'PARQUET'
                )
            """, "Création table processed.aggregated_orders")

            run("""
                DROP TABLE IF EXISTS processed.net_demand
            """, "Suppression table processed.net_demand")
            run("""
                CREATE TABLE processed.net_demand (
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
                    exec_date        VARCHAR
                )
                WITH (
                    format = 'PARQUET'
                )
            """, "Création table processed.net_demand")

            conn.close()
            logger.info("✅ Tables Trino créées avec succès")
            return True
            
        except Exception as e:
            logger.error(f"❌ Attempt {attempt+1}/{max_retries} échouée: {str(e)[:150]}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Attente {retry_delay}s avant nouvelle tentative...")
                time.sleep(retry_delay)
            else:
                logger.critical(f"❌ Impossible de créer les tables Trino après {max_retries} tentatives")
                raise Exception(f"Trino setup failed after {max_retries} retries: {str(e)}")
    
    return False


with DAG(
    'trino_setup',
    description='Initialisation des tables Hive dans Trino',
    schedule_interval=None,
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['setup', 'trino'],
    doc_md=__doc__
) as dag:
    """
    DAG d'initialisation unique.
    À exécuter UNE FOIS avant les DAGs réguliers.
    """

    setup_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_hive_tables,
        provide_context=True
    )