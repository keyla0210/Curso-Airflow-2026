import pandas as pd
from pathlib import Path

def build_dim_products(silver_path, output_path, **kwargs):
    df = pd.read_parquet(silver_path)
    
    # Seleccionar solo columnas descriptivas
    dim_prod = df[['id', 'title', 'description', 'image']].copy()
    dim_prod = dim_prod.rename(columns={'id': 'product_key'})
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    dim_prod.to_parquet(output_path, index=False)