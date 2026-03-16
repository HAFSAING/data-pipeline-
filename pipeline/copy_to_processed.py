"""
copy_to_processed.py
Copie les données traitées vers /processed/ dans HDFS via WebHDFS REST API.
Archive aussi les JSON fournisseurs vers /output/supplier_orders/ et les logs vers /logs/exceptions/.

Peut être appelé:
  - Depuis Airflow comme BashOperator
  - Directement: python copy_to_processed.py
"""

import os
import requests
import yaml
from datetime import datetime, timedelta

# ─── Configuration ────────────────────────────────────────────────────────────
WEBHDFS_BASE = "http://namenode:9870/webhdfs/v1"
HDFS_USER    = "root"

config_path = "/opt/airflow/config/pipeline_config.yaml"
if os.path.exists(config_path):
    with open(config_path) as f:
        config = yaml.safe_load(f)
    exec_date = config["execution_date"]
    base_path = "/opt/airflow/data"
else:
    exec_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    base_path = "../data"

# ─── Helpers WebHDFS ──────────────────────────────────────────────────────────
def webhdfs_mkdir(path: str) -> bool:
    url = f"{WEBHDFS_BASE}{path}?op=MKDIRS&user.name={HDFS_USER}"
    r = requests.put(url, allow_redirects=True, timeout=30)
    return r.status_code == 200

def webhdfs_upload(local_path: str, hdfs_path: str) -> bool:
    """Upload un fichier local vers HDFS."""
    try:
        with open(local_path, "rb") as f:
            content = f.read()
        create_url = f"{WEBHDFS_BASE}{hdfs_path}?op=CREATE&overwrite=true&user.name={HDFS_USER}"
        r = requests.put(create_url, allow_redirects=False, timeout=30)
        if r.status_code == 307:
            location = r.headers["Location"]
            w = requests.put(location, data=content, timeout=60)
            return w.status_code == 201
    except Exception as e:
        print(f"    ✗ Upload échoué ({hdfs_path}): {e}")
    return False

def webhdfs_list(path: str) -> list:
    url = f"{WEBHDFS_BASE}{path}?op=LISTSTATUS&user.name={HDFS_USER}"
    r = requests.get(url, timeout=30)
    if r.status_code == 200:
        return r.json().get("FileStatuses", {}).get("FileStatus", [])
    return []

def webhdfs_copy(src: str, dst: str) -> bool:
    """Copie un fichier dans HDFS (lecture puis écriture via WebHDFS)."""
    read_url = f"{WEBHDFS_BASE}{src}?op=OPEN&user.name={HDFS_USER}"
    read_r = requests.get(read_url, allow_redirects=True, timeout=60)
    if read_r.status_code != 200:
        return False
    create_url = f"{WEBHDFS_BASE}{dst}?op=CREATE&overwrite=true&user.name={HDFS_USER}"
    create_r = requests.put(create_url, allow_redirects=False, timeout=30)
    if create_r.status_code == 307:
        location = create_r.headers["Location"]
        write_r = requests.put(location, data=read_r.content, timeout=60)
        return write_r.status_code == 201
    return False

def upload_directory(local_dir: str, hdfs_dir: str, ext_filter: str = None) -> int:
    """Upload tous les fichiers d'un répertoire local vers HDFS."""
    if not os.path.exists(local_dir):
        print(f"  ⚠ Répertoire local absent: {local_dir}")
        return 0
    webhdfs_mkdir(hdfs_dir)
    count = 0
    for fname in os.listdir(local_dir):
        if ext_filter and not fname.endswith(ext_filter):
            continue
        local_path = os.path.join(local_dir, fname)
        if os.path.isfile(local_path):
            hdfs_path = f"{hdfs_dir}/{fname}"
            if webhdfs_upload(local_path, hdfs_path):
                print(f"    ✓ {fname}")
                count += 1
            else:
                print(f"    ✗ {fname}")
    return count

# ─── Pipeline principal ───────────────────────────────────────────────────────
def copy_to_processed(exec_date: str):
    print(f"\n{'='*60}")
    print(f"ARCHIVAGE HDFS POUR {exec_date}")
    print(f"{'='*60}\n")

    total = 0

    # 1. Parquet warehouse Hive → /processed/
    print("[1/3] Copie des Parquet Hive vers /processed/...")
    hive_sources = [
        ("/user/hive/warehouse/procurement_raw/aggregated_orders",
         f"/procurement/processed/aggregated_orders/exec_date={exec_date}"),
        ("/user/hive/warehouse/procurement_raw/net_demand",
         f"/procurement/processed/net_demand/exec_date={exec_date}"),
    ]
    for src, dst in hive_sources:
        webhdfs_mkdir(dst)
        files = webhdfs_list(src)
        for f in files:
            fname = f.get("pathSuffix", "")
            if fname and not fname.startswith(".") and f.get("type") == "FILE":
                if webhdfs_copy(f"{src}/{fname}", f"{dst}/{fname}"):
                    print(f"    ✓ {fname}")
                    total += 1
                else:
                    print(f"    ✗ {fname}")

    # 2. JSON fournisseurs → /procurement/output/supplier_orders/
    print(f"\n[2/3] Upload JSON fournisseurs vers HDFS /output/...")
    json_local = f"{base_path}/output/{exec_date}"
    json_hdfs  = f"/procurement/output/supplier_orders/order_date={exec_date}"
    n = upload_directory(json_local, json_hdfs, ext_filter=".json")
    total += n
    print(f"    → {n} fichiers JSON uploadés")

    # 3. Logs exceptions → /procurement/logs/exceptions/
    print(f"\n[3/3] Upload logs exceptions vers HDFS /logs/...")
    logs_local = f"{base_path}/logs/exceptions"
    logs_hdfs  = f"/procurement/logs/exceptions/exec_date={exec_date}"
    n = upload_directory(logs_local, logs_hdfs, ext_filter=".json")
    total += n
    print(f"    → {n} fichiers logs uploadés")

    print(f"\n✅ Archivage terminé — {total} fichiers copiés vers HDFS")
    return total

if __name__ == "__main__":
    copy_to_processed(exec_date)