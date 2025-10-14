from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

dag_folder = os.path.dirname(os.path.abspath(__file__))  # path فولدر DAG

with DAG(
    dag_id="weather_etl_daily_00",
    start_date=datetime(2025, 10, 12),
    schedule_interval="0 3 * * *",  # كل يوم الساعة 3 صباحًا
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["weather", "etl"]
) as dag:

    run_etl = BashOperator(
        task_id="run_spark_etl_full",
        bash_command=f"spark-submit --jars {dag_folder}/postgresql-42.7.3.jar {dag_folder}/weather_etl_spark.py"
    )

    run_etl
