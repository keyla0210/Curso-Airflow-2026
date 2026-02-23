import sys
from pathlib import Path
import pendulum
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor


PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from elt.ingest_raw import ingest_to_raw_simple
from elt.bronze import copy_raw_to_bronze
from elt.silver_exp import transform_bronze_to_silver_expediente 
from elt.silver_vehiculos import transform_bronze_to_silver_from_parquet 
from elt.fact_expedientes import transform_silver_to_fact_expedientes
#from elt.dim_vehiculos import transform_bronze_to_silver_dim_vehiculo  
#from elt.dim_shows import build_dim_shows  
#from elt.dim_networks import build_dim_networks
#from elt.fact_episodes import build_fact_episodes          
#from elt.send_email import send_completion_email

BOGOTA_TZ = pendulum.timezone("America/Bogota")


DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")

#Capa raw
API_URL = "https://datos.jst.gob.ar/dataset/1806747d-aec7-4917-bc96-6248b7aa1430/resource/8381d448-57c6-471f-988c-aaa97f21baef/download/expedientes_maritimo.json"
RAW_ROOT = DATA_LAKE_ROOT / "raw" / "riesgo_maritimos"

#Capa bronze
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / "riesgo_maritimos"

#Capa silver
SILVER_PATH = DATA_LAKE_ROOT / "silver" / "riesgo_maritimos"

#Dim Time
#DIM_TIME_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "time.parquet"

#Dim Vehiculos
DIM_VEHICULOS_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "vehiculos.parquet"

#Dim Network
#DIM_NETWORK_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "network.parquet"

#Fact episdodes
FACT_EXPEDIENETS_PATH = DATA_LAKE_ROOT / "gold" / "facts" / "expediente.parquet"


INGEST_PARAMS = {
    "api_url": str(API_URL),
    "output_dir": str(RAW_ROOT),
}

BRONZE_PARAMS = {
    "raw_root": str(RAW_ROOT),
    "bronze_path": str(BRONZE_PATH / "riesgo_maritimos_bronze.parquet"),
}

SILVER_EXP_PARAMS = {
    "bronze_path": str(BRONZE_PATH / "riesgo_maritimos_bronze.parquet"), 
    "silver_path": str(SILVER_PATH / "riesgo_maritimos_silver.parquet"),
}

SILVER_VEHICULOS_PARAMS = {
    "bronze_path": str(BRONZE_PATH / "riesgo_maritimos_bronze.parquet"), 
    "silver_path": str(SILVER_PATH / "vehiculos_silver.parquet"),
}

DIM_VEHICULOS_PARAMS ={
    "bronze_path": str(BRONZE_PATH / "riesgo_maritimos_bronze.parquet"),
    "output_path": str(DIM_VEHICULOS_PATH),
}

"""
DIM_TIME_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(DIM_TIME_PATH),
}

DIM_NETWORK_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(DIM_NETWORK_PATH),
}
"""

EXPEDIENTE_PARAMS ={
    "expediente_silver_path": str(SILVER_PATH / "riesgo_maritimos_silver.parquet"),
    "vehiculo_silver_path": str(SILVER_PATH / "vehiculos_silver.parquet"),
    "fact_path": str(FACT_EXPEDIENETS_PATH),
}


@dag(
    dag_id="proyecto_elt_medallon",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 10, 18, tz=BOGOTA_TZ),
    catchup=False,
    tags=["elt", "api"],
)

def elt_medallon_dag():

    @task()
    def ingest():
        return ingest_to_raw_simple(**INGEST_PARAMS)
    
    @task()
    def bronze():
        return copy_raw_to_bronze(**BRONZE_PARAMS)
    
    @task()
    def silver_exp():
        return transform_bronze_to_silver_expediente(**SILVER_EXP_PARAMS)
    
    @task()
    def silver_vehiculos():
        return transform_bronze_to_silver_from_parquet(**SILVER_VEHICULOS_PARAMS)
    
    @task()
    def fact_expedientes():
        return transform_silver_to_fact_expedientes(**EXPEDIENTE_PARAMS)

    #Orquestación
    i = ingest()
    b = bronze()
    s1 = silver_exp()  
    s2 = silver_vehiculos()  
    #time = dim_time()
    #vehiculos = dim_vehiculos()  
    fact_exp = fact_expedientes()
    #episodes = fact_episodes()
    #email = send_email_notification()

    i >>  b >> s1 >>s2 >> fact_exp#>> vehiculos #[time, show, network] >> episodes >> wait_for_fact_episodes >> email
    
elt_medallon = elt_medallon_dag()                           