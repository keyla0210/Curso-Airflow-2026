from pathlib import Path
import pandas as pd

def transform_silver_to_fact_expedientes(
        expediente_silver_path: Path | str,
        vehiculo_silver_path: Path | str,
        fact_path: Path | str,
) -> str:

    expediente_silver_path = Path(expediente_silver_path)
    vehiculo_silver_path = Path(vehiculo_silver_path)
    fact_path = Path(fact_path)
    fact_path.parent.mkdir(parents=True, exist_ok=True)

    # ----------------------------
    # Leer tablas Silver
    # ----------------------------
    df_exp = pd.read_parquet(expediente_silver_path)
    df_vehiculos = pd.read_parquet(vehiculo_silver_path)

    # ----------------------------
    # Merge por llave natural
    # ----------------------------
    df_fact = df_vehiculos.merge(
        df_exp,
        on=["fecha", "nro_expediente"],
        how="left"
    )

    # ----------------------------
    # Seleccionar columnas finales (ordenadas)
    # ----------------------------
    cols_fact = [
        "fecha",
        "nro_expediente",
        "nombre",
        "suceso",
        "investigacion",
        "estado",
        "ciudad",
        "pais"
        #"tipo",
        #"identificacion",
        #"mostrar"
    ]

    df_fact = df_fact[cols_fact].copy()

    # ----------------------------
    # Guardar Fact
    # ----------------------------
    df_fact.to_parquet(fact_path, index=False)

    return str(fact_path)
