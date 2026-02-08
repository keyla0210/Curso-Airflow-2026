import sys
from pathlib import Path
import pendulum
from airflow.decorators import dag, task


PROJECT_ROOT = "/opt/airflow/"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from elt.ingest_raw import ingest_to_raw
from elt.bronze import copy_raw_to_bronze
from elt.silver import transform_bronze_to_silver 
from elt.dim_time import build_dim_time  
from elt.dim_shows import build_dim_shows  
from elt.dim_networks import build_dim_networks
from elt.fact_episodes import build_fact_episodes          

BOGOTA_TZ = pendulum.timezone("America/Bogota")


DATA_LAKE_ROOT = Path("/opt/airflow/data_lake")

#Capa raw
RAW_ROOT = DATA_LAKE_ROOT / "raw" / "tvmaze"
#Capa bronze
BRONZE_PATH = DATA_LAKE_ROOT / "bronze" / "tvmaze"
#Capa silver
SILVER_PATH = DATA_LAKE_ROOT / "silver" / "tvmaze"

#Dim Time
DIM_TIME_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "time.parquet"
#Dim Show
DIM_SHOW_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "show.parquet"
#Dim Network
DIM_NETWORK_PATH = DATA_LAKE_ROOT / "gold" / "dimensions" / "network.parquet"

#Fact episdodes
FACT_EPISODES_PATH = DATA_LAKE_ROOT / "gold" / "facts" / "episodes.parquet"


INGEST_PARAMS = {
    "start_date": pendulum.date(2020, 1, 1),
    "end_date": pendulum.date(2020, 1, 31),
    "output_dir": str(RAW_ROOT),
    "timeout": 30,
}

BRONZE_PARAMS = {
    "raw_root": str(RAW_ROOT),
    "bronze_path": str(BRONZE_PATH / "tvmaze.parquet"),
}

SILVER_PARAMS = {
    "bronze_path": str(BRONZE_PATH / "tvmaze.parquet"), 
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
}

DIM_TIME_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(DIM_TIME_PATH),
}

DIM_SHOW_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(DIM_SHOW_PATH),
}

DIM_NETWORK_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(DIM_NETWORK_PATH),
}

EPISODES_PARAMS ={
    "silver_path": str(SILVER_PATH / "tvmaze_silver.parquet"),
    "output_path": str(FACT_EPISODES_PATH),
}

@dag(
    dag_id="elt_medallon",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 10, 18, tz=BOGOTA_TZ),
    catchup=False,
    tags=["elt", "api"],
)

def elt_medallon_dag():

    @task()
    def ingest():
        return ingest_to_raw(**INGEST_PARAMS)
    
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
    def dim_shows():  
        return build_dim_shows(**DIM_SHOW_PARAMS)
    
    @task()
    def dim_networks():  
        return build_dim_networks(**DIM_NETWORK_PARAMS)
    
    @task()
    def fact_episodes():          
        return build_fact_episodes(**EPISODES_PARAMS)
    
    #Orquestación
    i = ingest()
    b = bronze()
    s = silver()  
    time = dim_time()
    show = dim_shows()  
    network = dim_networks()
    episodes = fact_episodes()

    i >>  b >> s >> [time, show, network] >> episodes
    
elt_medallon = elt_medallon_dag()                           