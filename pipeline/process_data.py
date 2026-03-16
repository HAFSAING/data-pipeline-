#!/usr/bin/env python3
"""
process_data.py - Version améliorée avec TrinoClient
"""

import os
import sys
import time
import yaml
from datetime import datetime, timedelta

# Ajouter le chemin pour les imports
sys.path.append('/opt/airflow/pipeline')
from trino_client import TrinoClient

def load_config():
    """Charge la configuration."""
    config_path = "/opt/airflow/config/pipeline_config.yaml"
    if os.path.exists(config_path):
        with open(config_path) as f:
            config = yaml.safe_load(f)
        return config["execution_date"]
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def wait_for_trino(max_retries=30, delay=10):
    """Attend que Trino soit prêt."""
    for i in range(max_retries):
        try:
            with TrinoClient() as client:
                client.execute_query("SELECT 1")
                print("✓ Trino prêt")
                return True
        except Exception as e:
            print(f"Tentative {i+1}/{max_retries}: {e}")
            time.sleep(delay)
    raise Exception("Trino non disponible")

def process_pipeline(exec_date):
    """Exécute le pipeline de traitement."""
    print(f"\n{'='*60}")
    print(f"TRAITEMENT POUR LE {exec_date}")
    print(f"{'='*60}\n")
    
    with TrinoClient() as client:
        # 1. Synchroniser les partitions
        print("1. Synchronisation des partitions Hive...")
        client.sync_partitions('orders', 'raw')
        client.sync_partitions('inventory', 'raw')
        
        # 2. Agrégation des commandes
        print("\n2. Agrégation des commandes...")
        client.aggregate_orders(exec_date)
        
        # 3. Calcul de la demande nette
        print("\n3. Calcul de la demande nette...")
        results = client.calculate_net_demand(exec_date)
        
        # 4. Vérification des résultats
        total_demand = sum(r.get('final_order_qty', 0) for r in results)
        print(f"\n✓ {len(results)} produits avec demande > 0")
        print(f"✓ Quantité totale à commander: {total_demand}")
        
        return results

if __name__ == "__main__":
    print("=== DÉMARRAGE PROCESS_DATA ===")
    exec_date = load_config()
    wait_for_trino()
    results = process_pipeline(exec_date)
    print(f"\n=== TRAITEMENT TERMINÉ POUR {exec_date} ===")