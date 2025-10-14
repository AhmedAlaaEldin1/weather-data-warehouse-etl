from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

dag_folder = os.path.dirname(os.path.abspath(__file__))  # path فولدر DAG

with DAG(
    dag_id="weather_etl_test_00",
    start_date=datetime(2025, 10, 12),
    schedule_interval=None,  # يدوي للتجربة
    catchup=False,
    tags=["weather", "etl_test"]
) as dag:

    t1 = BashOperator(
        task_id="run_spark_etl_test",
        bash_command=f"spark-submit --jars {dag_folder}/postgresql-42.7.3.jar {dag_folder}/weather_etl_spark.py"
    )

    t1
