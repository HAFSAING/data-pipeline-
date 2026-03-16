"""
exceptions.py
Contrôles qualité des données du pipeline procurement.
Peut être appelé:
  - Comme script: python exceptions.py
  - Depuis Airflow: from pipeline.exceptions import run_quality_checks
"""

import os
import json
import csv
from datetime import datetime, timedelta
import yaml

# ─── Configuration ────────────────────────────────────────────────────────────
def _load_config():
    config_path = "/opt/airflow/config/pipeline_config.yaml"
    if os.path.exists(config_path):
        with open(config_path) as f:
            config = yaml.safe_load(f)
        return config["execution_date"], "/opt/airflow/data"
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"), "../data"

EXPECTED_SKUS   = {"SKU_1001", "SKU_1002", "SKU_1003"}
EXPECTED_STORES = {"STORE_01", "STORE_02", "STORE_03"}
DEMAND_SPIKE_THRESHOLD = 200

# ─── Fonction principale appelable depuis Airflow ─────────────────────────────
def run_quality_checks(exec_date: str = None) -> dict:
    """
    Exécute les 5 contrôles qualité et retourne le rapport.
    Peut être appelé depuis Airflow avec run_quality_checks(exec_date).
    """
    if exec_date is None:
        exec_date, base_path = _load_config()
    else:
        config_path = "/opt/airflow/config/pipeline_config.yaml"
        base_path = "/opt/airflow/data" if os.path.exists(config_path) else "../data"

    logs_dir = f"{base_path}/logs/exceptions"
    os.makedirs(logs_dir, exist_ok=True)

    exceptions = []

    def log(level, check, message, detail=None):
        exceptions.append({
            "timestamp": datetime.now().isoformat(),
            "level":     level,
            "check":     check,
            "message":   message,
            "detail":    detail or ""
        })
        icon = {"ERROR": "✗", "WARNING": "!", "INFO": "i"}.get(level, "?")
        print(f"[{icon}] [{level}] {check}: {message}")

    # ── Check 1 : fichiers POS manquants ──────────────────────────────────────
    print("=== Check 1 : fichiers POS ===")
    orders_dir = f"{base_path}/raw/orders/{exec_date}"
    if not os.path.exists(orders_dir):
        log("ERROR", "missing_pos_files",
            f"Répertoire commandes absent pour {exec_date}", orders_dir)
    else:
        found_stores = {
            f.replace(".json", "")
            for f in os.listdir(orders_dir) if f.endswith(".json")
        }
        for store in EXPECTED_STORES:
            if store not in found_stores:
                log("ERROR", "missing_pos_files",
                    f"Fichier POS manquant : {store}", f"{orders_dir}/{store}.json")
        if found_stores == EXPECTED_STORES:
            log("INFO", "missing_pos_files", "Tous les fichiers POS présents")

    # ── Check 2 : snapshot d'inventaire ───────────────────────────────────────
    print("=== Check 2 : snapshot inventaire ===")
    inventory_file = f"{base_path}/raw/stock/{exec_date}/inventory.csv"
    if not os.path.exists(inventory_file):
        log("ERROR", "missing_inventory",
            f"Snapshot d'inventaire absent pour {exec_date}", inventory_file)
    else:
        log("INFO", "missing_inventory", "Snapshot d'inventaire présent")

    # ── Check 3 : SKUs sans mapping fournisseur ───────────────────────────────
    print("=== Check 3 : mapping SKU → fournisseur ===")
    products_file = f"{base_path}/products/products.json"
    if not os.path.exists(products_file):
        log("ERROR", "missing_supplier_mapping",
            "Fichier products.json absent", products_file)
    else:
        with open(products_file) as f:
            products = json.load(f)
        mapped_skus = {p["sku"] for p in products}
        for sku in EXPECTED_SKUS:
            if sku not in mapped_skus:
                log("ERROR", "missing_supplier_mapping",
                    f"SKU sans fournisseur : {sku}")
        required_fields = {"sku", "supplier_id", "pack_size", "moq", "safety_stock"}
        for p in products:
            missing = required_fields - p.keys()
            if missing:
                log("ERROR", "missing_supplier_mapping",
                    f"Champs manquants pour {p.get('sku','?')} : {missing}")
        if not any(e["check"] == "missing_supplier_mapping" for e in exceptions):
            log("INFO", "missing_supplier_mapping", "Tous les SKUs ont un fournisseur valide")

    # ── Check 4 : spikes de demande ───────────────────────────────────────────
    print("=== Check 4 : spikes de demande ===")
    if os.path.exists(orders_dir):
        demand_per_sku = {}
        for fname in os.listdir(orders_dir):
            if not fname.endswith(".json"):
                continue
            with open(f"{orders_dir}/{fname}") as f:
                orders = json.load(f)
            for order in orders:
                sku = order.get("sku")
                qty = order.get("quantity", 0)
                demand_per_sku[sku] = demand_per_sku.get(sku, 0) + qty
        for sku, total in demand_per_sku.items():
            if total > DEMAND_SPIKE_THRESHOLD:
                log("WARNING", "demand_spike",
                    f"Spike détecté pour {sku} : {total} unités (seuil={DEMAND_SPIKE_THRESHOLD})",
                    f"total_demand={total}")
        if not any(e["check"] == "demand_spike" for e in exceptions):
            log("INFO", "demand_spike", "Aucun spike de demande détecté")

    # ── Check 5 : cohérence inventaire ────────────────────────────────────────
    print("=== Check 5 : cohérence inventaire ===")
    if os.path.exists(inventory_file):
        with open(inventory_file, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        inv_skus = {r["sku"] for r in rows}
        for sku in EXPECTED_SKUS:
            if sku not in inv_skus:
                log("WARNING", "inventory_inconsistency",
                    f"SKU absent du snapshot inventaire : {sku}")
        for r in rows:
            try:
                available = int(r["available_stock"])
                reserved  = int(r["reserved_stock"])
                if reserved > available:
                    log("WARNING", "inventory_inconsistency",
                        f"Stock réservé > disponible pour {r['sku']}",
                        f"available={available}, reserved={reserved}")
                if available < 0 or reserved < 0:
                    log("ERROR", "inventory_inconsistency",
                        f"Valeur négative dans l'inventaire pour {r['sku']}", str(r))
            except (ValueError, KeyError) as e:
                log("ERROR", "inventory_inconsistency",
                    f"Données invalides : {e}", str(r))
        if not any(e["check"] == "inventory_inconsistency" for e in exceptions):
            log("INFO", "inventory_inconsistency", "Inventaire cohérent")

    # ── Export du rapport ──────────────────────────────────────────────────────
    report = {
        "date":         exec_date,
        "generated_at": datetime.now().isoformat(),
        "total":        len(exceptions),
        "errors":       sum(1 for e in exceptions if e["level"] == "ERROR"),
        "warnings":     sum(1 for e in exceptions if e["level"] == "WARNING"),
        "infos":        sum(1 for e in exceptions if e["level"] == "INFO"),
        "exceptions":   exceptions
    }

    report_path = f"{logs_dir}/exceptions_{exec_date}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\n=== Rapport généré : {report_path} ===")
    print(f"  Erreurs  : {report['errors']}")
    print(f"  Warnings : {report['warnings']}")
    print(f"  Infos    : {report['infos']}")

    if report["errors"] > 0:
        print("\n[!] Des erreurs critiques ont été détectées — voir le rapport.")

    return report

# ─── Exécution directe comme script ───────────────────────────────────────────
if __name__ == "__main__":
    date, _ = _load_config()
    run_quality_checks(date)