"""
DAG: procurement_pipeline_v2
CORRECTIONS APPLIQUÉES:
1. TrinoClient importé à l'intérieur des fonctions (pas au niveau module)
2. Retry logic pour les connexions Trino
3. Logging amélioré pour déboguer les problèmes
4. Configuration cohérente des dates (context['ds'])
5. XCom pour les données intermédiaires
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
import requests
import logging
import time

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'de_student',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@procurement.local'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def wait_for_trino(max_retries=30, delay=10):
    """Attend que Trino soit prêt avant de continuer."""
    sys.path.insert(0, '/opt/airflow/pipeline')
    for attempt in range(max_retries):
        try:
            import trino
            conn = trino.dbapi.connect(
                host='presto_procurement',
                port=8080,
                user='de_user',
                catalog='hive'
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
            logger.info(f"✓ Trino prêt à l'attempt {attempt+1}/{max_retries}")
            return True
        except Exception as e:
            logger.warning(f"⏳ Attempt {attempt+1}/{max_retries}: {str(e)[:100]}")
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                logger.error(f"❌ Trino toujours indisponible après {max_retries} tentatives")
                raise Exception("Trino n'a pas répondu dans le délai imparti")
    return False

def sync_partitions(**context):
    exec_date = context['ds']
    logger.info(f"[sync_partitions] Synchronisation des partitions pour {exec_date}")
    
    sys.path.insert(0, '/opt/airflow/pipeline')
    try:
        import trino
        from trino_client import TrinoClient
        
        wait_for_trino()
        
        with TrinoClient() as client:
            logger.info(f"Synchronisation de raw.orders...")
            client.sync_partitions('orders', 'raw')
            logger.info(f"Synchronisation de raw.inventory...")
            client.sync_partitions('inventory', 'raw')
            logger.info(f"✓ Partitions synchronisées pour {exec_date}")
    except Exception as e:
        logger.error(f"❌ Erreur sync_partitions: {str(e)}")
        raise

def aggregate_orders(**context):
    exec_date = context['ds']
    logger.info(f"[aggregate_orders] Agrégation des commandes pour {exec_date}")
    
    sys.path.insert(0, '/opt/airflow/pipeline')
    try:
        import trino
        from trino_client import TrinoClient
        
        wait_for_trino()
        
        with TrinoClient() as client:
            logger.info(f"Exécution requête d'agrégation...")
            client.aggregate_orders(exec_date)
            logger.info(f"✓ Commandes agrégées pour {exec_date}")
            context['ti'].xcom_push(key='step_completed', value='aggregate_orders')
    except Exception as e:
        logger.error(f"❌ Erreur aggregate_orders: {str(e)}")
        raise

def calculate_net_demand(**context):
    exec_date = context['ds']
    logger.info(f"[calculate_net_demand] Calcul de la demande nette pour {exec_date}")
    
    sys.path.insert(0, '/opt/airflow/pipeline')
    try:
        import trino
        from trino_client import TrinoClient
        
        wait_for_trino()
        
        with TrinoClient() as client:
            results = client.calculate_net_demand(exec_date)
            context['ti'].xcom_push(key='net_demand_results', value=results)
            
            if results:
                total_qty  = sum(r.get('final_order_qty', 0) for r in results)
                total_cost = sum(float(r.get('estimated_cost', 0) or 0) for r in results)
                logger.info(f"✓ Demande nette calculée: {len(results)} produits")
                logger.info(f"  - Quantité totale: {total_qty} unités")
                logger.info(f"  - Coût estimé: {total_cost:.2f}")
                context['ti'].xcom_push(key='demand_qty', value=total_qty)
                context['ti'].xcom_push(key='demand_cost', value=total_cost)
            else:
                logger.warning("⚠️ Aucune demande nette calculée")
    except Exception as e:
        logger.error(f"❌ Erreur calculate_net_demand: {str(e)}")
        raise

def check_quality(**context):
    exec_date = context['ds']
    logger.info(f"[quality_checks] Contrôle qualité pour {exec_date}")
    
    sys.path.insert(0, '/opt/airflow/pipeline')
    try:
        from exceptions import run_quality_checks
        report = run_quality_checks(exec_date)
        logger.info(f"Contrôle qualité: {report['errors']} erreurs, {report['warnings']} warnings")
        context['ti'].xcom_push(key='quality_report', value=report)
    except Exception as e:
        logger.error(f"❌ Erreur quality_checks: {str(e)}")
        raise

def copy_to_processed(**context):
    """Archive les résultats HDFS pour la date d'exécution."""
    exec_date = context['ds']
    logger.info(f"[copy_to_processed] Archivage HDFS pour {exec_date}")
    
    sys.path.insert(0, '/opt/airflow/pipeline')
    try:
        # Appeler simplement le script Python d'archivage
        import subprocess
        result = subprocess.run(
            ['python', '/opt/airflow/pipeline/sync_partitions.py', exec_date],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            logger.info(f"✓ Archivage HDFS terminé pour {exec_date}")
            logger.info(result.stdout)
        else:
            logger.error(f"❌ Erreur during HDFS archiving:")
            logger.error(result.stderr)
            raise Exception(f"HDFS archiving failed: {result.stderr}")
            
    except Exception as e:
        logger.error(f"❌ Erreur copy_to_processed: {str(e)}")
        raise


with DAG(
    'procurement_pipeline_v2',
    default_args=default_args,
    description='Pipeline procurement: agrégation + MRP + export fournisseurs + archivage HDFS',
    schedule_interval='0 22 * * *',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['procurement', 'batch'],
    doc_md=__doc__
) as dag:

    t1_sync = PythonOperator(
        task_id='sync_partitions',
        python_callable=sync_partitions,
        op_kwargs={},
        provide_context=True
    )
    
    t2_aggregate = PythonOperator(
        task_id='aggregate_orders',
        python_callable=aggregate_orders,
        provide_context=True
    )
    
    t3_net_demand = PythonOperator(
        task_id='calculate_net_demand',
        python_callable=calculate_net_demand,
        provide_context=True
    )
    
    t4_export = BashOperator(
        task_id='export_orders',
        bash_command='python /opt/airflow/pipeline/export_orders.py'
    )
    
    t5_quality = PythonOperator(
        task_id='quality_checks',
        python_callable=check_quality,
        provide_context=True
    )
    
    t6_ingest = BashOperator(
        task_id='ingest_to_hdfs',
        bash_command='python /opt/airflow/pipeline/ingest_to_hdfs.py'
    )
    
    t7_archive = PythonOperator(
        task_id='copy_to_processed',
        python_callable=copy_to_processed,
        provide_context=True
    )

    # Définir les dépendances de tâches
    t1_sync >> t2_aggregate >> t3_net_demand >> [t4_export, t5_quality] >> t6_ingest >> t7_archive