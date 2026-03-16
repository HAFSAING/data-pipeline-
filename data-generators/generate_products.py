import json
import os

# DONNEES PRODUITS------------------
products = [
    {"sku": "SKU_1001", "name": "Bananas", "supplier_id": "SUP_001", "pack_size": 12, "moq": 24, "lead_time_days": 2, "safety_stock": 50},
    {"sku": "SKU_1002", "name": "Milk 1L", "supplier_id": "SUP_002", "pack_size": 6, "moq": 18, "lead_time_days": 1, "safety_stock": 30},
    {"sku": "SKU_1003", "name": "Yogurt", "supplier_id": "SUP_002", "pack_size": 8, "moq": 16, "lead_time_days": 2, "safety_stock": 20},
]

# CHEMINS---------------------------
base_path = "/opt/airflow/data/products" if os.path.exists("/opt/airflow") else "../data/products"
os.makedirs(base_path, exist_ok=True)
with open(f"{base_path}/products.json", "w") as f:
    json.dump(products, f, indent=2)
print("Products data generated")