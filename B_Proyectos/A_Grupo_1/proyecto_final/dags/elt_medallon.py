import sys
from pathlib import Path
import pendulum
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
import os 

# 1. Configuración de Rutas de Sistema
PROJECT_ROOT = Path("/opt/airflow")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Importaciones de tus módulos técnicos en la carpeta /elt
from elt.ingest_raw import ingest_raw  
from elt.bronze import copy_raw_to_bronze
from elt.silver import transform_bronze_to_silver 
from elt.dim_time import build_dim_time  
from elt.dim_products import build_dim_products  
from elt.dim_categories import build_dim_categories
from elt.fact_products import build_fact_products  # Cambiado de episodes a products/sales
from elt.send_email import send_completion_email

# Configuración Horaria
BOGOTA_TZ = pendulum.timezone("America/Bogota")

# 2. Definición del Data Lake (Rutas Independientes)
DATA_LAKE_ROOT = PROJECT_ROOT / "data_lake"
SOURCE_NAME = "fakestore"

RAW_ROOT = DATA_LAKE_ROOT / "raw" / SOURCE_NAME
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / SOURCE_NAME
SILVER_PATH = DATA_LAKE_ROOT / "silver" / SOURCE_NAME
GOLD_PATH = DATA_LAKE_ROOT / "gold"

# 3. Diccionarios de Parámetros para las Funciones
INGEST_PARAMS = {
    "output_dir": str(RAW_ROOT),
    "timeout": 30,
}

BRONZE_PARAMS = {
    "raw_root": str(RAW_ROOT),
    "bronze_path": str(BRONZE_PATH / f"{SOURCE_NAME}.parquet"),
}

SILVER_PARAMS = {
    "bronze_path": str(BRONZE_PATH / f"{SOURCE_NAME}.parquet"), 
    "silver_path": str(SILVER_PATH / f"{SOURCE_NAME}_silver.parquet"),
}

# Parámetros para Capa Gold (Modelo Estrella)
SILVER_FILE = str(SILVER_PATH / f"{SOURCE_NAME}_silver.parquet")

DIM_TIME_PARAMS = {"silver_path": SILVER_FILE, "output_path": str(GOLD_PATH / "dimensions" / "dim_time.parquet")}
DIM_PROD_PARAMS = {"silver_path": SILVER_FILE, "output_path": str(GOLD_PATH / "dimensions" / "dim_products.parquet")}
DIM_CAT_PARAMS  = {"silver_path": SILVER_FILE, "output_path": str(GOLD_PATH / "dimensions" / "dim_categories.parquet")}
FACT_PROD_PARAMS = {"silver_path": SILVER_FILE, "output_path": str(GOLD_PATH / "facts" / "fact_products.parquet")}

# 4. Definición del DAG
@dag(
    dag_id="elt_medallon_fakestore",
    schedule="0 5 * * *", 
    start_date=pendulum.datetime(2026, 2, 1, tz=BOGOTA_TZ),
    catchup=False,
    tags=["data_engineering", "fakestore", "parquet"],
)
def elt_medallon_dag():

    @task()
    def ingest():
        return ingest_raw(**INGEST_PARAMS)
    
    @task()
    def bronze():
        return copy_raw_to_bronze(**BRONZE_PARAMS)
    
    @task()
    def silver():   
        return transform_bronze_to_silver(**SILVER_PARAMS)  
    
    @task()
    def dim_time():  
        return build_dim_time(**DIM_TIME_PARAMS)
    
    @task()
    def dim_products():  
        return build_dim_products(**DIM_PROD_PARAMS)
    
    @task()
    def dim_categories():  
        return build_dim_categories(**DIM_CAT_PARAMS)
    
    @task()
    def fact_products():          
        return build_fact_products(**FACT_PROD_PARAMS)
    
    @task.sensor(task_id="wait_for_fact_file", poke_interval=20, timeout=600, mode="poke")
    def check_fact_file_exists():
        path = FACT_PROD_PARAMS["output_path"]
        if os.path.exists(path):
            print(f"Archivo detectado con éxito en: {path}")
            return True
        print(f"Buscando archivo en: {path} ...")
        return False

    @task()
    def notify():
        return send_completion_email()

    # 5. Orquestación y Flujo de Dependencias
    raw_data = ingest()
    bronze_data = bronze()
    silver_data = silver()
    
    # Ejecución paralela de dimensiones para optimizar el DAG
    d_time = dim_time()
    d_prod = dim_products()
    d_cat  = dim_categories()
    
    f_prod = fact_products()

    # Definición de la cadena de ejecución
    raw_data >> bronze_data >> silver_data
    silver_data >> [d_time, d_prod, d_cat] >> f_prod
    f_prod >> check_fact_file_exists() >> notify()

# Ejecución del DAG
elt_medallon = elt_medallon_dag()                       