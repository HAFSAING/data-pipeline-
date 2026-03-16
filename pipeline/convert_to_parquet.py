import pandas as pd
import json
import os
from pathlib import Path

def convert_to_parquet(input_file, output_file):
    """Convertit JSON, CSV, Excel vers Parquet"""
    ext = Path(input_file).suffix.lower()
    
    try:
        if ext == '.json':
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
        elif ext == '.csv':
            df = pd.read_csv(input_file)
        elif ext in ['.xlsx', '.xls']:
            df = pd.read_excel(input_file)
        elif ext == '.parquet':
            import shutil
            shutil.copy2(input_file, output_file)
            print(f"Copié (déjà Parquet): {input_file} → {output_file}")
            return
        else:
            raise ValueError(f"Format non supporté: {ext}")
        
        df.to_parquet(output_file, compression='snappy', index=False)
        print(f"Converti: {input_file} → {output_file}")
    except Exception as e:
        print(f"Erreur {input_file}: {e}")

def migrate_directory(input_dir, output_dir):
    """Convertit tous les fichiers d'un dossier"""
    os.makedirs(output_dir, exist_ok=True)
    
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(('.json', '.csv', '.xlsx', '.xls', '.parquet')):
                input_path = os.path.join(root, file)
                rel_path = os.path.relpath(root, input_dir)
                output_subdir = os.path.join(output_dir, rel_path)
                os.makedirs(output_subdir, exist_ok=True)
                
                output_file = os.path.join(output_subdir, Path(file).stem + '.parquet')
                convert_to_parquet(input_path, output_file)

if __name__ == "__main__":
    base_path = "/opt/airflow/data" if os.path.exists("/opt/airflow") else "../data"
    
    # Convertir raw/
    migrate_directory(f"{base_path}/raw", f"{base_path}/parquet/raw")
    
    # Convertir products/
    migrate_directory(f"{base_path}/products", f"{base_path}/parquet/products")
    
    # Convertir suppliers/
    migrate_directory(f"{base_path}/suppliers", f"{base_path}/parquet/suppliers")
    
    print("Migration terminée!")
