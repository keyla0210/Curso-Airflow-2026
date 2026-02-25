from pathlib import Path

def transform_silver_to_dim_vehiculo(
        silver_path: Path | str,
        output_path: Path | str,
) -> str:

    import pandas as pd

    silver_path = Path(silver_path)
    dim_vehiculo_path = Path(output_path)
    dim_vehiculo_path.parent.mkdir(parents=True, exist_ok=True)

    # ----------------------------
    # Leer silver vehiculos
    # ----------------------------
    df_silver_vehiculos = pd.read_parquet(silver_path)

    # ----------------------------
    # Seleccionar atributos
    # ----------------------------
    df_dim_vehiculo = df_silver_vehiculos[
        [
            "nro_expediente",
            "nombre",
            "tipo",
            "identificacion",
            "mostrar",
            "omi",
        ]
    ].drop_duplicates().reset_index(drop=True)

    # ----------------------------
    # Guardar Dim
    # ----------------------------
    df_dim_vehiculo.insert(0, "vehiculo_key", range(1, len(df_dim_vehiculo) + 1))
    df_dim_vehiculo.to_parquet(dim_vehiculo_path, index=False)

    return str(dim_vehiculo_path)
