import os
import json
from datetime import datetime, timedelta
from faker import Faker
import random

fake = Faker()
SKUS = ["SKU_1001", "SKU_1002", "SKU_1003"]
STORES = ["STORE_01", "STORE_02", "STORE_03"]

# DATE CONFIGURATION-----------------
exec_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# CHEMINS---------------------------
base_path = "/opt/airflow/data/raw/orders" if os.path.exists("/opt/airflow") else "../data/raw/orders"
os.makedirs(f"{base_path}/{exec_date}", exist_ok=True)

for store in STORES:
    orders = []
    for _ in range(random.randint(10, 30)):
        orders.append({
            "order_id": fake.uuid4(),
            "store_id": store,
            "sku": random.choice(SKUS),
            "quantity": random.randint(1, 10),
            "timestamp": fake.date_time_this_month().isoformat()
        })
    with open(f"{base_path}/{exec_date}/{store}.json", "w") as f:
        json.dump(orders, f, indent=2)

print(f"Orders generated for {exec_date}")