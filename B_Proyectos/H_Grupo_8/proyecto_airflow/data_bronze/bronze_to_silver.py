import pandas as pd

df = pd.read_parquet("data_bronze/inversion.parquet")

cols = {c.lower(): c for c in df.columns}
def col(name):
    for k,v in cols.items():
        if name in k:
            return v
    raise KeyError(name)

df = df[[col("entidad"), col("sector"), col("nivel"),
         col("codigo"), col("proyecto"),
         col("monto"), col("anio"), col("benef")]].copy()

df.columns = ["entidad","sector","nivel","codigo_proyecto",
              "proyecto","monto","anio","beneficiarios"]

df["anio"] = pd.to_numeric(df["anio"], errors="coerce")
df["monto"] = pd.to_numeric(df["monto"], errors="coerce")
df["beneficiarios"] = pd.to_numeric(df["beneficiarios"], errors="coerce")

df = df.dropna(subset=["entidad","codigo_proyecto","anio","monto"]).drop_duplicates()

df.to_parquet("data_silver/inversion.parquet", index=False)
print("BRONZE -> SILVER listo")

