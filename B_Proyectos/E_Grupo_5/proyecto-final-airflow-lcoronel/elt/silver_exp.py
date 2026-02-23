from pathlib import Path
import pandas as pd
import re

def transform_bronze_to_silver_expediente(
        bronze_path: Path | str,
        silver_path: Path | str,
) -> str:

    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    # ----------------------------
    # Leer Bronze
    # ----------------------------
    df_bronze = pd.read_parquet(bronze_path)

    # ----------------------------
    # Seleccionar columnas que queremos mantener
    # ----------------------------
    cols_keep = [
        "fecha",
        "hora",
        "estado",
        "lugar",
        "nro_expediente",
        "latitud",
        "longitud",
        "fecha_finalizacion"
    ]

    df_exp = df_bronze[cols_keep].copy()

    # ----------------------------
    # Separar lugar en descripcion y detalle
    # ----------------------------
    def split_lugar(value):
        if pd.isna(value):
            return pd.Series([None, None])
        match = re.match(r"^(.*?)\s*\((.*?)\)\s*$", value)
        if match:
            return pd.Series([match.group(1).strip(), match.group(2).strip()])
        else:
            return pd.Series([value.strip(), None])

    df_exp[["lugar_descripcion", "lugar_detalle"]] = df_exp["lugar"].apply(split_lugar)
    df_exp = df_exp.drop(columns=["lugar"])

    # ----------------------------
    # Separar ciudad y país
    # ----------------------------
    def split_ciudad_pais(value):
        if pd.isna(value):
            return pd.Series([None, "Argentina"])
        parts = [p.strip() for p in str(value).split(",")]
        if len(parts) == 2:
            return pd.Series([parts[0], parts[1]])
        else:
            return pd.Series([parts[0], "Argentina"])

    df_exp[["ciudad", "pais"]] = df_exp["lugar_detalle"].apply(split_ciudad_pais)

    # ----------------------------
    # Guardar Silver
    # ----------------------------
    df_exp.to_parquet(silver_path, index=False)

    return str(silver_path)
