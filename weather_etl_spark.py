from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month, dayofmonth, date_format, monotonically_increasing_id
from pyspark.sql.types import DateType
import boto3, json
import os

def main():
    # ====== إعداد Spark ======
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    jar_path = os.path.join(dag_folder, "postgresql-42.7.3.jar")

    spark = SparkSession.builder \
        .appName("Weather_ETL_to_DWH") \
        .config("spark.jars", jar_path) \
        .getOrCreate()

    # ====== إعداد MinIO ======
    s3_client = boto3.client(
        's3',
        endpoint_url='http://host.docker.internal:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1'
    )
    bucket_name = "raw-weather"

    # ====== قراءة ملفات JSON من MinIO ======
    response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=10000)
    file_keys = [obj['Key'] for obj in response.get('Contents', [])]

    data_list = []
    for key in file_keys:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = obj['Body'].read().decode('utf-8')
        record = json.loads(content)

        parts = key.split('/')
        city = parts[1]
        date_str = parts[2].replace('.json', '')

        record["city"] = city
        record["date"] = date_str
        data_list.append(record)

    if not data_list:
        print("❌ لم يتم العثور على ملفات صالحة للتحميل")
        return

    df = spark.createDataFrame(data_list)
    df = df.withColumn("date", col("date").cast(DateType()))
    print(f"✅ عدد السجلات بعد الدمج: {df.count()}")

    # ====== إنشاء جداول Dimensions ======
    dim_date = df.select("date").distinct() \
        .withColumn("date_id", monotonically_increasing_id()) \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date")) \
        .withColumn("day", dayofmonth("date")) \
        .withColumn("weekday", date_format(col("date"), "E"))

    dim_location = df.select("city").distinct() \
        .withColumn("location_id", monotonically_increasing_id()) \
        .withColumn("country", lit("Egypt")) \
        .withColumn("region", lit("Middle East"))

    fact_weather = df.join(dim_location, on="city", how="left") \
        .join(dim_date, on="date", how="left") \
        .select(
            col("location_id"),
            col("date_id"),
            col("avg_temp"),
            col("max_temp"),
            col("min_temp"),
            col("precipitation"),
            col("wind_speed")
        ) \
        .withColumn("weather_id", monotonically_increasing_id())

    # ====== رفع البيانات على PostgreSQL ======
    jdbc_url = "jdbc:postgresql://de_postgres:5432/airflow"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    dim_date.write.jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=properties)
    dim_location.write.jdbc(url=jdbc_url, table="dim_location", mode="overwrite", properties=properties)
    fact_weather.write.jdbc(url=jdbc_url, table="fact_weather", mode="overwrite", properties=properties)

    print("🎯 تم رفع جميع الجداول إلى PostgreSQL بنجاح!")

if __name__ == "__main__":
    main()
