from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, year, month, dayofmonth, date_format,
    row_number, to_date, trim, split
)
from pyspark.sql.types import DateType
import boto3, json, os

# ====== إعداد Spark ======
spark = SparkSession.builder \
    .appName("Weather_ETL_to_DWH_Clean") \
    .config("spark.jars", "/home/jovyan/postgresql-42.7.3.jar") \
    .getOrCreate()

# ====== إعداد اتصال PostgreSQL ======
jdbc_url = "jdbc:postgresql://de_postgres:5432/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# ====== Helper: دالة لقراءة JSONs من MinIO (مثل قبل) ======
def read_from_minio(bucket_name="raw-weather", endpoint="http://host.docker.internal:9000",
                    access_key="minioadmin", secret_key="minioadmin"):
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='us-east-1'
    )

    # pagination
    file_keys = []
    continuation_token = None
    while True:
        if continuation_token:
            resp = s3_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=continuation_token)
        else:
            resp = s3_client.list_objects_v2(Bucket=bucket_name)
        contents = resp.get("Contents", [])
        file_keys.extend([o["Key"] for o in contents])
        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break

    # قراءة الملفات
    records = []
    for idx, key in enumerate(file_keys, start=1):
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        raw = obj['Body'].read().decode('utf-8')
        try:
            record = json.loads(raw)
        except Exception:
            continue
        # filename layout: raw-weather/<city>/<date>.json
        parts = key.split('/')
        if len(parts) >= 3:
            city = parts[1]
            date_str = parts[2].replace('.json', '')
            record["city"] = city
            record["date_raw"] = date_str
        records.append(record)
        if idx % 500 == 0:
            print(f"📥 read {idx}/{len(file_keys)} from MinIO")
    if not records:
        return None
    return spark.createDataFrame(records)

# ====== أولاً: هل في ملف CSV محلي أرسلتيه؟ نفضّله لو موجود ======
local_csv_path = "/mnt/data/weather.csv"
df_raw = None

if os.path.exists(local_csv_path):
    print(f"📁 Found local CSV at {local_csv_path} — will use it as primary source.")
    # نقرأ CSV الأصلي مع infer schema (أو نعيّن dtypes إن احتاجنا)
    pdf = spark.read.option("header", True).csv(local_csv_path)  # يعود كـ DataFrame
    # نعالج أسماء الأعمدة لو فيها نقاط أو مسافات
    # (الـ CSV الأصلي فيه أعمدة مثل Data.Precipitation و Date.Full)
    # نحط أسماء قصيرة معقولة
    col_map = {}
    for c in pdf.columns:
        new = c.strip().replace(" ", "_").replace(".", "_")
        col_map[c] = new
    for old, new in col_map.items():
        pdf = pdf.withColumnRenamed(old, new)
    # الآن ensure الأعمدة المهمة موجودة
    # الأعمدة المتوقعة بعد الrename: Data_Precipitation, Date_Full, Date_Month, Date_Week_of, Date_Year,
    # Station_City, Station_Code, Station_Location, Station_State, Data_Temperature_Avg_Temp, ...
    df = pdf
    # parse date from Date_Full (format M/d/yyyy or maybe MM/dd/yyyy)
    # نستخدم to_date مع pattern مرن: أول نحاول M/d/yyyy ثم yyyy-MM-dd fallback
    df = df.withColumn("date_parsed", to_date(trim(col("Date_Full")), "M/d/yyyy"))
    df = df.withColumn("date_parsed", 
                       to_date(
                           when(col("date_parsed").isNull(), trim(col("Date_Full"))).otherwise(col("date_parsed")),
                           "MM/dd/yyyy"
                       ))
    # إذا لسه null، نححاول YYYY-MM-DD
    df = df.withColumn("date_parsed",
                       to_date(col("date_parsed"), "yyyy-MM-dd"))
    # rename parsed into date
    df = df.withColumnRenamed("date_parsed", "date")
    # ensure numeric columns casted
    numeric_cols = {
        "Data_Precipitation": "double",
        "Data_Temperature_Avg_Temp": "double",
        "Data_Temperature_Max_Temp": "double",
        "Data_Temperature_Min_Temp": "double",
        "Data_Wind_Speed": "double"
    }
    for src, dtype in numeric_cols.items():
        if src in df.columns:
            df = df.withColumn(src, col(src).cast("double"))
    # unify station columns
    if "Station_City" in df.columns:
        df = df.withColumnRenamed("Station_City", "station_city")
    if "Station_Code" in df.columns:
        df = df.withColumnRenamed("Station_Code", "station_code")
    if "Station_Location" in df.columns:
        df = df.withColumnRenamed("Station_Location", "station_location")
    if "Station_State" in df.columns:
        df = df.withColumnRenamed("Station_State", "station_state")
    # final expected measure column names (make them shorter)
    if "Data_Precipitation" in df.columns:
        df = df.withColumnRenamed("Data_Precipitation", "precipitation")
    if "Data_Temperature_Avg_Temp" in df.columns:
        df = df.withColumnRenamed("Data_Temperature_Avg_Temp", "avg_temp")
    if "Data_Temperature_Max_Temp" in df.columns:
        df = df.withColumnRenamed("Data_Temperature_Max_Temp", "max_temp")
    if "Data_Temperature_Min_Temp" in df.columns:
        df = df.withColumnRenamed("Data_Temperature_Min_Temp", "min_temp")
    if "Data_Wind_Speed" in df.columns:
        df = df.withColumnRenamed("Data_Wind_Speed", "wind_speed")
    # keep only rows with a valid date and city
    df = df.filter(col("date").isNotNull()).filter(col("station_city").isNotNull())
    df = df.select(
        col("station_city").alias("city"),
        col("station_code").alias("station_code") if "station_code" in df.columns else lit(None).alias("station_code"),
        col("station_location").alias("station_location") if "station_location" in df.columns else lit(None).alias("station_location"),
        col("station_state").alias("state") if "station_state" in df.columns else lit(None).alias("state"),
        col("date"),
        col("avg_temp"), col("max_temp"), col("min_temp"),
        col("precipitation"), col("wind_speed")
    )
    df_raw = df

else:
    # fallback to MinIO JSONs (previous pipeline)
    print("⚠️ Local CSV not found — falling back to MinIO JSONs.")
    df_minio = read_from_minio()
    if df_minio is None:
        raise RuntimeError("لم أجد بيانات في MinIO ولا ملف CSV محلي — لا توجد بيانات للمعالجة.")
    # MinIO JSONs likely already have avg_temp, max_temp, min_temp, precipitation, wind_speed, city, date
    # normalize column names if needed
    available = set(df_minio.columns)
    rename_map = {}
    if "avg_temp" not in available and "Data_Temperature_Avg_Temp" in available:
        rename_map["Data_Temperature_Avg_Temp"] = "avg_temp"
    # apply renames
    for old, new in rename_map.items():
        df_minio = df_minio.withColumnRenamed(old, new)
    # ensure date column named 'date'
    if "date_raw" in df_minio.columns:
        df_minio = df_minio.withColumnRenamed("date_raw", "date")
    df_minio = df_minio.withColumn("date", to_date(trim(col("date")), "yyyy-MM-dd"))
    df_raw = df_minio.select(
        col("city"),
        lit(None).alias("station_code"),
        lit(None).alias("station_location"),
        lit(None).alias("state"),
        col("date"),
        col("avg_temp"), col("max_temp"), col("min_temp"),
        col("precipitation"), col("wind_speed")
    )

# ====== الآن df_raw جاهزة ونقوم ببناء الأبعاد والـ fact بدون قيم افتراضية خاطئة ======
print("▶️ عدد السجلات الخام بعد التنظيف:", df_raw.count())

# ====== dim_date: unique dates with proper keys ======
date_window = Window.orderBy(col("date").asc())
dim_date = df_raw.select("date").distinct() \
    .withColumn("date_id", row_number().over(date_window) - 1) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("weekday", date_format(col("date"), "E")) \
    .orderBy("date")

# ====== dim_location: unique locations (use state as region) ======
loc_window = Window.orderBy(col("city").asc())
dim_location = df_raw.select("city", "state", "station_code", "station_location") \
    .distinct() \
    .withColumn("location_id", row_number().over(loc_window) - 1) \
    .withColumn("country", lit("USA")) \
    .withColumn("region", col("state")) \
    .select("city", "location_id", "country", "region", "station_code", "station_location")

# ====== fact_weather: link to dimension keys ======
# join to get ids
fact = df_raw.join(dim_location.select("city", "location_id"), on="city", how="left") \
    .join(dim_date.select("date", "date_id"), on="date", how="left") \
    .select(
        col("location_id"),
        col("date_id"),
        col("avg_temp"),
        col("max_temp"),
        col("min_temp"),
        col("precipitation"),
        col("wind_speed")
    )

# add a deterministic surrogate key for fact (optional)
fact = fact.withColumn("weather_id", row_number().over(Window.orderBy("location_id", "date_id")) - 1)

# ====== Final simple validation (basic checks) ======
# check for nulls in FK columns
null_locs = fact.filter(col("location_id").isNull()).count()
null_dates = fact.filter(col("date_id").isNull()).count()
print(f"⚠️ عدد صفوف fact بدون location_id: {null_locs}")
print(f"⚠️ عدد صفوف fact بدون date_id: {null_dates}")

if null_locs > 0 or null_dates > 0:
    print("هناك صفوف بدون مفاتيح أبعاد — سنحفظها إلى جدول مؤقت للتحقق بدلاً من الكتابة النهائية.")
    # save temporary and raise for inspection
    fact.write.jdbc(url=jdbc_url, table="fact_weather_temp", mode="overwrite", properties=properties)
    print("✅ حفظت fact_weather_temp لفحص المشاكل. أوقف التنفيذ أو افحص fact_weather_temp يدوياً.")
else:
    # ====== تحميل إلى PostgreSQL (نستبدل الجداول الموجودة لضمان تناسق) ======
    dim_date.write.jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=properties)
    dim_location.write.jdbc(url=jdbc_url, table="dim_location", mode="overwrite", properties=properties)
    fact.write.jdbc(url=jdbc_url, table="fact_weather", mode="overwrite", properties=properties)
    print("🎯 تم تحميل dim_date, dim_location, fact_weather إلى PostgreSQL بنجاح!")

