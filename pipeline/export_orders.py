"""
export_orders.py
Génère les fichiers JSON de commandes fournisseurs avec:
- total_cost par article
- order_date (date de commande = J+1) vs data_date (date des données)
- total_estimated_cost global par fournisseur
"""

import trino
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
import yaml

# ─── Configuration ────────────────────────────────────────────────────────────
config_path = "/opt/airflow/config/pipeline_config.yaml"
if os.path.exists(config_path):
    with open(config_path) as f:
        config = yaml.safe_load(f)
    data_date  = config["execution_date"]
    output_dir = f"{config['output_dir']}/{data_date}"
else:
    data_date  = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    output_dir = f"../data/output/{data_date}"

# order_date = lendemain de la date des données (J+1)
order_date = (datetime.strptime(data_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

os.makedirs(output_dir, exist_ok=True)
print(f"Export des commandes — données: {data_date} | commande: {order_date}")

# ─── Connexion Trino ──────────────────────────────────────────────────────────
conn = trino.dbapi.connect(
    host='presto_procurement',
    port=8080,
    user='de_user',
    catalog='postgresql',
    schema='public'
)
cursor = conn.cursor()

# ─── Récupération des données ─────────────────────────────────────────────────
print("Récupération des commandes depuis PostgreSQL...")
cursor.execute("""
SELECT
    nd.supplier_id,
    s.name          AS supplier_name,
    s.contact_email,
    nd.sku,
    p.name          AS product_name,
    CAST(nd.final_order_qty AS INTEGER) AS quantity,
    CAST(nd.unit_cost       AS DOUBLE)  AS unit_cost,
    CAST(nd.estimated_cost  AS DOUBLE)  AS total_cost
FROM postgresql.public.net_demand nd
JOIN postgresql.public.suppliers s ON nd.supplier_id = s.supplier_id
JOIN postgresql.public.products  p ON nd.sku = p.sku
WHERE nd.final_order_qty > 0
ORDER BY nd.supplier_id, nd.sku
""")

rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]
print(f"{len(rows)} lignes récupérées")

# ─── Grouper par fournisseur ──────────────────────────────────────────────────
suppliers_orders = defaultdict(lambda: {
    "items": [],
    "supplier_name": "",
    "contact_email": "",
    "total_estimated_cost": 0.0
})

for row in rows:
    r = dict(zip(cols, row))
    sup_id = r["supplier_id"]
    suppliers_orders[sup_id]["supplier_name"]  = r["supplier_name"]
    suppliers_orders[sup_id]["contact_email"]  = r["contact_email"]
    suppliers_orders[sup_id]["total_estimated_cost"] += float(r["total_cost"] or 0)
    suppliers_orders[sup_id]["items"].append({
        "sku":        r["sku"],
        "product_name": r["product_name"],
        "quantity":   int(r["quantity"]),
        "unit_cost":  round(float(r["unit_cost"] or 0), 2),
        "total_cost": round(float(r["total_cost"] or 0), 2)
    })

print(f"{len(suppliers_orders)} fournisseurs avec des commandes")

# ─── Écriture des fichiers JSON structurés ────────────────────────────────────
for sup_id, data in suppliers_orders.items():
    order = {
        "supplier_id":           sup_id,
        "supplier_name":         data["supplier_name"],
        "contact_email":         data["contact_email"],
        "order_date":            order_date,      # date de livraison souhaitée
        "data_date":             data_date,        # date des données source
        "generated_at":          datetime.now().isoformat(),
        "items":                 data["items"],
        "total_estimated_cost":  round(data["total_estimated_cost"], 2)
    }

    filename = f"{output_dir}/{sup_id}.json"
    with open(filename, "w") as f:
        json.dump(order, f, indent=2, ensure_ascii=False)

    print(f"  ✓ {sup_id} ({data['supplier_name']}): "
          f"{len(data['items'])} articles, "
          f"coût estimé = {order['total_estimated_cost']:.2f} € → {filename}")

# ─── Flag de mise à jour pour le dashboard ────────────────────────────────────
flag_path = "/opt/airflow/data/.last_update"
os.makedirs(os.path.dirname(flag_path), exist_ok=True)
with open(flag_path, "w") as f:
    f.write(datetime.now().isoformat())

print(f"\n✅ Export terminé dans {output_dir}")