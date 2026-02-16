import pandas as pd
from pathlib import Path

def build_fact_products(silver_path, output_path, **kwargs):
    df = pd.read_parquet(silver_path)
    
    # Seleccionar métricas y llaves foráneas
    # Nota: En un proyecto real, aquí unirías con las dimensiones para obtener los IDs (Keys)
    fact_df = df[['id', 'category', 'price', 'rate', 'count']].copy()
    fact_df = fact_df.rename(columns={
        'id': 'product_key',
        'rate': 'rating_score',
        'count': 'stock_count'
    })
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fact_df.to_parquet(output_path, index=False)