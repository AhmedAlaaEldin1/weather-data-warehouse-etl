from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import json
from airflow.hooks.base import BaseHook
from botocore.client import Config
import boto3

# ======= Function Extraction =======
def extract_weather_data_from_csv(**kwargs):
    # ======= إعداد MinIO =======
    conn = BaseHook.get_connection('minio_conn')
    extras = conn.extra_dejson
    endpoint = extras.get('endpoint_url', 'http://host.docker.internal:9000')

    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # ======= قراءة CSV =======
    df = pd.read_csv('/opt/airflow/data/weather.csv')

    # ======= تحويل وحفظ البيانات =======
    for idx, row in df.iterrows():
        city = row['Station.City']
        date = row['Date.Full']

        data = {
            'avg_temp': row['Data.Temperature.Avg Temp'],
            'max_temp': row['Data.Temperature.Max Temp'],
            'min_temp': row['Data.Temperature.Min Temp'],
            'wind_speed': row['Data.Wind.Speed'],
            'precipitation': row['Data.Precipitation']
        }

        file_key = f"raw-weather/{city}/{date}.json"

        s3.put_object(
            Bucket="raw-weather",
            Key=file_key,
            Body=json.dumps(data)
        )

        print(f"✅ Weather data for {city} on {date} saved in MinIO: {file_key}")


# ======= DAG =======
with DAG(
    dag_id='weather_etl_pipeline_1',   # اسم DAG
    start_date=datetime(2025, 10, 11),
    schedule_interval=None,          # None يعني هيتشغل يدوي
    catchup=False,
    tags=['weather', 'etl'],
) as dag:

    # ======= Task =======
    t1 = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data_from_csv
    )

# لو حبيتي تضيفي Task تانية (مثلاً Spark Transformation)، هتعملي dependency:
# t1 >> t2
