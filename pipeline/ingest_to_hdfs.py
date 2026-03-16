"""
ingest_to_hdfs.py - Version améliorée avec partitionnement Hive
"""

import os
import requests
from datetime import datetime, timedelta
import yaml
import json

class HDFSClient:
    """Client HDFS avec support du partitionnement Hive."""
    
    def __init__(self, namenode_url="http://namenode:9870/webhdfs/v1"):
        self.base_url = namenode_url
    
    def mkdir(self, path):
        """Crée un répertoire HDFS."""
        url = f"{self.base_url}{path}?op=MKDIRS&user.name=root"
        r = requests.put(url, allow_redirects=True)
        return r.status_code == 200
    
    def upload(self, local_file, hdfs_path):
        """Upload un fichier avec gestion des redirections."""
        # Étape 1: Demander l'URL du datanode
        create_url = f"{self.base_url}{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
        r = requests.put(create_url, allow_redirects=False)
        
        if r.status_code == 307:
            location = r.headers["Location"]
            # Étape 2: Uploader les données
            with open(local_file, "rb") as f:
                w = requests.put(location, data=f)
            return w.status_code == 201
        return False
    
    def upload_partitioned(self, local_dir, hdfs_base, partition_col, partition_value):
        """
        Upload des fichiers avec structure de partition Hive.
        Exemple: /procurement/raw/orders/exec_date=2026-01-15/
        """
        hdfs_dir = f"{hdfs_base}/{partition_col}={partition_value}"
        self.mkdir(hdfs_dir)
        
        success = 0
        for fname in os.listdir(local_dir):
            local_path = os.path.join(local_dir, fname)
            if os.path.isfile(local_path):
                hdfs_path = f"{hdfs_dir}/{fname}"
                if self.upload(local_path, hdfs_path):
                    print(f"  ✓ {fname}")
                    success += 1
                else:
                    print(f"  ✗ {fname}")
        return success

def upload_with_partitions():
    """Fonction principale d'upload avec partitions."""
    config_path = "/opt/airflow/config/pipeline_config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    date = config["execution_date"]
    
    client = HDFSClient()
    
    # 1. Upload des commandes avec partition par date
    print(f"\n[1/4] Upload orders pour {date}...")
    orders_dir = f"/opt/airflow/data/parquet/raw/orders/{date}"
    if os.path.exists(orders_dir):
        count = client.upload_partitioned(
            orders_dir, 
            "/procurement/raw/orders",
            "exec_date", 
            date
        )
        print(f"→ {count} fichiers uploadés")
    
    # 2. Upload de l'inventaire avec partition
    print(f"\n[2/4] Upload inventory pour {date}...")
    inventory_dir = f"/opt/airflow/data/parquet/raw/stock/{date}"
    if os.path.exists(inventory_dir):
        count = client.upload_partitioned(
            inventory_dir,
            "/procurement/raw/inventory",
            "exec_date",
            date
        )
        print(f"→ {count} fichiers uploadés")
    
    # 3. Upload master data (non partitionné)
    print("\n[3/4] Upload master data...")
    for data_type in ['products', 'suppliers']:
        dir_path = f"/opt/airflow/data/parquet/{data_type}"
        if os.path.exists(dir_path):
            client.upload_partitioned(dir_path, f"/procurement/{data_type}", "static", "latest")
    
    # 4. Upload des résultats
    print(f"\n[4/4] Upload results pour {date}...")
    results_dir = f"/opt/airflow/data/output/{date}"
    if os.path.exists(results_dir):
        client.upload_partitioned(
            results_dir,
            "/procurement/output/supplier_orders",
            "order_date",
            date
        )
    
    print(f"\n✅ Upload HDFS terminé pour {date}")

if __name__ == "__main__":
    upload_with_partitions()