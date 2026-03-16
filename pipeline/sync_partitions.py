"""
sync_partitions.py - Synchronise les partitions Hive après upload
"""

import trino
import sys
from datetime import datetime

def sync_all_partitions(exec_date):
    """Synchronise toutes les partitions pour une date."""
    conn = trino.dbapi.connect(
        host='presto_procurement',
        port=8080,
        user='de_user',
        catalog='hive'
    )
    cursor = conn.cursor()
    
    tables = [
        ('raw', 'orders'),
        ('raw', 'inventory'),
        ('processed', 'aggregated_orders'),
        ('processed', 'net_demand')
    ]
    
    for schema, table in tables:
        try:
            query = f"CALL hive.system.sync_partition_metadata('{schema}', '{table}', 'ADD')"
            print(f"Syncing {schema}.{table}...")
            cursor.execute(query)
            cursor.fetchall()
            print(f"  ✓ {schema}.{table}")
        except Exception as e:
            print(f"  ✗ {schema}.{table}: {e}")
    
    cursor.close()
    conn.close()
    print(f"\n✅ Synchronisation terminée pour {exec_date}")

if __name__ == "__main__":
    exec_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    sync_all_partitions(exec_date)