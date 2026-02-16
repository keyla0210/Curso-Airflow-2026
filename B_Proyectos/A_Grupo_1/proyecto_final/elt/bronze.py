import pandas as pd
from pathlib import Path

def copy_raw_to_bronze(raw_root, bronze_path, **kwargs):
    raw_file = Path(raw_root) / "products_raw.json"
    
    if not raw_file.exists():
        raise FileNotFoundError(f"No se encontró el archivo raw en {raw_file}")

    # Leer JSON y persistir en Parquet
    df = pd.read_json(raw_file)
    Path(bronze_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(bronze_path, index=False, engine='pyarrow')
    
    return bronze_path