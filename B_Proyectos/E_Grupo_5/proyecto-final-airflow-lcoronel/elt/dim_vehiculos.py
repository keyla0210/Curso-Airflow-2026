from pathlib import Path

def transform_bronze_to_silver_dim_vehiculo(
        bronze_path: Path | str,
        output_path: Path | str,
) -> str:

    import pandas as pd

    bronze_path = Path(bronze_path)
    dim_vehiculo_path = Path(output_path)
    dim_vehiculo_path.parent.mkdir(parents=True, exist_ok=True)

    # ----------------------------
    # Leer Bronze
    # ----------------------------
    df_bronze = pd.read_parquet(bronze_path)

    # Asegurarse de que la columna vehiculos exista y sea lista
    df_bronze["vehiculos"] = df_bronze["vehiculos"].apply(lambda x: x if isinstance(x, list) else [])

    # ----------------------------
    # Expandir vehículos
    # ----------------------------
    df_exploded = df_bronze.explode("vehiculos", ignore_index=True)

    # Mantener solo filas con vehículos
    df_exploded = df_exploded[df_exploded["vehiculos"].notna()].reset_index(drop=True)

    # Normalizar vehículos
    df_vehiculos = pd.json_normalize(df_exploded["vehiculos"])

    # Agregar columnas llave natural
    df_vehiculos["fecha"] = df_exploded["fecha"].values
    df_vehiculos["identificacion"] = df_exploded["identificacion"].values

    # ----------------------------
    # Seleccionar atributos de la dimensión
    # ----------------------------
    df_dim_vehiculo = df_vehiculos[
        [
            "fecha",
            "identificacion",
            "tipo",
            "nombre",
            "suceso",
            "investigacion",
            "mostrar"
        ]
    ].copy()

    # Eliminar duplicados
    df_dim_vehiculo = df_dim_vehiculo.drop_duplicates().reset_index(drop=True)

    # ----------------------------
    # Crear surrogate key
    # ----------------------------
    df_dim_vehiculo.insert(0, "vehiculo_key", range(1, len(df_dim_vehiculo) + 1))

    # ----------------------------
    # Guardar Silver
    # ----------------------------
    df_dim_vehiculo.to_parquet(dim_vehiculo_path, index=False)

    return str(dim_vehiculo_path)
