from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="elt_inversion_medallon",
    start_date=datetime(2025,1,1),
    schedule="0 9 * * *",   # todos los días a las 09:00
    catchup=False
) as dag:

    raw_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command="python /opt/airflow/project/scripts/raw_to_bronze.py"
    )

    bronze_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="python /opt/airflow/project/scripts/bronze_to_silver.py"
    )

    silver_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="python /opt/airflow/project/scripts/silver_to_gold.py"
    )

    raw_bronze >> bronze_silver >> silver_gold