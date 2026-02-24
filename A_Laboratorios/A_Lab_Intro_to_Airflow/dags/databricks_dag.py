from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

# Definición del DAG con decorador
@dag(
    dag_id="databricks_job_ingesta",
    start_date=datetime(2026, 2, 23),
    schedule="@daily",
    catchup=False,
    tags=["databricks", "ingesta"]
)
def databricks_job_dag():

    # Tarea para ejecutar el job en Databricks
    ingesta = DatabricksRunNowOperator(
        task_id="ingesta",
        databricks_conn_id="databricks_default",  # ID de conexión configurado en Airflow
        job_id=334730638226602,  # ID del job en Databricks
    )

    bronze_silver = DatabricksRunNowOperator(
        task_id="bronze_silver",
        databricks_conn_id="databricks_default",  # ID de conexión configurado en Airflow
        job_id=715631959073981,  # ID del job en Databricks
    )

    gold = DatabricksRunNowOperator(
        task_id="gold",
        databricks_conn_id="databricks_default",  # ID de conexión configurado en Airflow
        job_id=364159613805935,  # ID del job en Databricks
    )

    # Definición de dependencias entre tareas
    ingesta >> bronze_silver >> gold

# Instancia del DAG
databricks_job_dag()