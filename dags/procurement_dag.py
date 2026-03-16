"""
DAG: procurement_pipeline_v2
CORRECTION: TrinoClient importé à l'intérieur des fonctions (pas au niveau module)
pour éviter l'erreur DAG Import Error au démarrage d'Airflow.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os
import requests
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'de_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def sync_partitions(**context):
    exec_date = context['ds']
    sys.path.insert(0, '/opt/airflow/pipeline')
    from trino_client import TrinoClient
    with TrinoClient() as client:
        client.sync_partitions('orders', 'raw')
        client.sync_partitions('inventory', 'raw')
        print(f"Partitions synchronisées pour {exec_date}")

def aggregate_orders(**context):
    exec_date = context['ds']
    sys.path.insert(0, '/opt/airflow/pipeline')
    from trino_client import TrinoClient
    with TrinoClient() as client:
        client.aggregate_orders(exec_date)
        print(f"Commandes agrégées pour {exec_date}")

def calculate_net_demand(**context):
    exec_date = context['ds']
    sys.path.insert(0, '/opt/airflow/pipeline')
    from trino_client import TrinoClient
    with TrinoClient() as client:
        results = client.calculate_net_demand(exec_date)
        context['ti'].xcom_push(key='net_demand_results', value=results)
        total_qty  = sum(r.get('final_order_qty', 0) for r in results)
        total_cost = sum(float(r.get('estimated_cost', 0) or 0) for r in results)
        print(f"Demande nette: {len(results)} produits, {total_qty} unités, coût={total_cost:.2f}")

def check_quality(**context):
    exec_date = context['ds']
    sys.path.insert(0, '/opt/airflow/pipeline')
    from exceptions import run_quality_checks
    report = run_quality_checks(exec_date)
    print(f"Contrôle qualité: {report['errors']} erreurs, {report['warnings']} warnings")

def copy_to_processed(**context):
    exec_date = context['ds']
    WEBHDFS   = "http://namenode:9870/webhdfs/v1"
    HDFS_USER = "root"
    base_path = "/opt/airflow/data"

    def mkdir(path):
        r = requests.put(f"{WEBHDFS}{path}?op=MKDIRS&user.name={HDFS_USER}", timeout=30)
        return r.status_code == 200

    def upload(local_path, hdfs_path):
        try:
            with open(local_path, "rb") as f:
                content = f.read()
            r = requests.put(f"{WEBHDFS}{hdfs_path}?op=CREATE&overwrite=true&user.name={HDFS_USER}",
                             allow_redirects=False, timeout=30)
            if r.status_code == 307:
                w = requests.put(r.headers["Location"], data=content, timeout=60)
                return w.status_code == 201
        except Exception as e:
            logger.warning(f"Upload échoué {hdfs_path}: {e}")
        return False

    def copy_hdfs(src, dst):
        r = requests.get(f"{WEBHDFS}{src}?op=OPEN&user.name={HDFS_USER}", allow_redirects=True, timeout=60)
        if r.status_code != 200:
            return False
        cr = requests.put(f"{WEBHDFS}{dst}?op=CREATE&overwrite=true&user.name={HDFS_USER}",
                         allow_redirects=False, timeout=30)
        if cr.status_code == 307:
            wr = requests.put(cr.headers["Location"], data=r.content, timeout=60)
            return wr.status_code == 201
        return False

    def list_hdfs(path):
        r = requests.get(f"{WEBHDFS}{path}?op=LISTSTATUS&user.name={HDFS_USER}", timeout=30)
        if r.status_code == 200:
            return r.json().get("FileStatuses", {}).get("FileStatus", [])
        return []

    total = 0
    for table, hdfs_src in [
        ("aggregated_orders", "/user/hive/warehouse/procurement_raw/aggregated_orders"),
        ("net_demand",        "/user/hive/warehouse/procurement_raw/net_demand"),
    ]:
        dst_dir = f"/procurement/processed/{table}/exec_date={exec_date}"
        mkdir(dst_dir)
        for f in list_hdfs(hdfs_src):
            fname = f.get("pathSuffix", "")
            if fname and not fname.startswith(".") and f.get("type") == "FILE":
                if copy_hdfs(f"{hdfs_src}/{fname}", f"{dst_dir}/{fname}"):
                    total += 1

    json_dir  = f"{base_path}/output/{exec_date}"
    json_hdfs = f"/procurement/output/supplier_orders/order_date={exec_date}"
    if os.path.exists(json_dir):
        mkdir(json_hdfs)
        for fname in os.listdir(json_dir):
            if fname.endswith(".json"):
                if upload(f"{json_dir}/{fname}", f"{json_hdfs}/{fname}"):
                    total += 1

    logs_dir  = f"{base_path}/logs/exceptions"
    logs_hdfs = f"/procurement/logs/exceptions/exec_date={exec_date}"
    if os.path.exists(logs_dir):
        mkdir(logs_hdfs)
        for fname in os.listdir(logs_dir):
            if fname.endswith(".json"):
                if upload(f"{logs_dir}/{fname}", f"{logs_hdfs}/{fname}"):
                    total += 1

    print(f"Archivage HDFS terminé: {total} fichiers copiés pour {exec_date}")


with DAG(
    'procurement_pipeline_v2',
    default_args=default_args,
    description='Pipeline procurement avec étapes modulaires + archivage HDFS',
    schedule_interval='0 22 * * *',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['procurement', 'batch']
) as dag:

    t1_sync = PythonOperator(task_id='sync_partitions', python_callable=sync_partitions)
    t2_aggregate = PythonOperator(task_id='aggregate_orders', python_callable=aggregate_orders)
    t3_net_demand = PythonOperator(task_id='calculate_net_demand', python_callable=calculate_net_demand)
    t4_export = BashOperator(task_id='export_orders', bash_command='python /opt/airflow/pipeline/export_orders.py')
    t5_quality = PythonOperator(task_id='quality_checks', python_callable=check_quality)
    t6_ingest = BashOperator(task_id='ingest_to_hdfs', bash_command='python /opt/airflow/pipeline/ingest_to_hdfs.py')
    t7_archive = PythonOperator(task_id='copy_to_processed', python_callable=copy_to_processed)

    t1_sync >> t2_aggregate >> t3_net_demand >> [t4_export, t5_quality] >> t6_ingest >> t7_archive