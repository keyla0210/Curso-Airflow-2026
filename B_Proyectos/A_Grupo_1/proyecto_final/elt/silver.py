import pandas as pd
from pathlib import Path

def transform_bronze_to_silver(bronze_path, silver_path, **kwargs):
    df = pd.read_parquet(bronze_path)
    
    # 1. Aplanar el diccionario 'rating' (rate y count)
    if 'rating' in df.columns:
        rating_df = df['rating'].apply(pd.Series)
        df = pd.concat([df.drop(columns=['rating']), rating_df], axis=1)
    
    # 2. Limpieza básica: nombres de columnas y tipos
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    
    # 3. Guardar en Silver
    Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(silver_path, index=False)
    
    return silver_path