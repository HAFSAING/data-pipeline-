import json
import os

suppliers = [
    {"supplier_id": "SUP_001", "name": "FreshFruit Co", "contact_email": "orders@freshfruit.com"},
    {"supplier_id": "SUP_002", "name": "DairyMax Ltd", "contact_email": "procure@dairymax.com"},
]

base_path = "/opt/airflow/data/suppliers" if os.path.exists("/opt/airflow") else "../data/suppliers"
os.makedirs(base_path, exist_ok=True)
with open(f"{base_path}/suppliers.json", "w") as f:
    json.dump(suppliers, f, indent=2)
print("Suppliers data generated")