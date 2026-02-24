from pathlib import Path
import pandas as pd

def transform_bronze_to_silver_from_parquet(bronze_path: Path | str, silver_path: Path | str) -> str:
    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    # Leer parquet bronze
    df_bronze = pd.read_parquet(bronze_path)

    # Explode columna vehiculos
    df_exploded = df_bronze.explode("vehiculos").reset_index(drop=True)

    # Normalizar vehiculos
    df_vehiculos = pd.json_normalize(df_exploded["vehiculos"])

    # Agregar columnas claves para referencias
    df_vehiculos["fecha"] = df_exploded["fecha"]
    df_vehiculos["nro_expediente"] = df_exploded["nro_expediente"]

    # Guardar vehiculos normalizados
    df_vehiculos.to_parquet(silver_path, index=False)

    return str(silver_path)
