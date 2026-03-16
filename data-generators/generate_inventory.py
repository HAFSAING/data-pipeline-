import csv
import os
from datetime import datetime, timedelta
import random

# CONFIGURATION---------------------
SKUS = ["SKU_1001", "SKU_1002", "SKU_1003"]
exec_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# CHEMINS---------------------------
base_path = "/opt/airflow/data/raw/stock" if os.path.exists("/opt/airflow") else "../data/raw/stock"
os.makedirs(f"{base_path}/{exec_date}", exist_ok=True)

with open(f"{base_path}/{exec_date}/inventory.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["sku", "available_stock", "reserved_stock", "warehouse"])
    writer.writeheader()
    for sku in SKUS:
        writer.writerow({
            "sku": sku,
            "available_stock": random.randint(40, 120),
            "reserved_stock": random.randint(5, 25),
            "warehouse": "WH_MAIN"
        })

print(f"Inventory snapshot generated for {exec_date}")