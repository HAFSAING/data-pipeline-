import subprocess
import sys
import os

def run(script):
    print(f"[RUN] {script}")
    res = subprocess.run([sys.executable, script], capture_output=True, text=True)
    if res.returncode != 0:
        print("[ERROR]", res.stderr)
        sys.exit(1)
    print("[OK]")

if __name__ == "__main__":
    # Changer vers le répertoire racine du projet
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    

    print("=== Demarrage du pipeline ===")

    run("data-generators/generate_orders.py")
    run("data-generators/generate_inventory.py")
    run("data-generators/generate_products.py")
    run("data-generators/generate_suppliers.py")
    run("pipeline/convert_to_parquet.py")
    run("pipeline/ingest_to_hdfs.py")
    run("pipeline/process_data.py")
    run("pipeline/export_orders.py")
    run("pipeline/exceptions.py")

    print("=== Pipeline termine avec succes ===")