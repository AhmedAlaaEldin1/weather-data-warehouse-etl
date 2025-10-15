# 🌦️ Weather Data Warehouse ETL Project

### 🚀 End-to-End Data Engineering Pipeline using Airflow, PySpark, MinIO & PostgreSQL

---

## 🧩 Project Overview

This project demonstrates the complete **ETL (Extract, Transform, Load)** process for weather data — from raw JSON files to a fully structured **Data Warehouse**.
It integrates several core data engineering tools: **Apache Airflow** for orchestration, **PySpark** for data transformation, **MinIO** for data storage, and **PostgreSQL** as the Data Warehouse — all running inside **Docker containers** for scalability and portability.

---

## 🎯 Objectives

* Automate the collection and transformation of weather data.
* Design and build a **Star Schema** for analytical use.
* Practice **data pipeline orchestration** with Airflow and PySpark.
* Learn how to connect multiple tools in a real-world data engineering setup.
* Prepare data for future visualization and BI reporting.

---

## ⚙️ Architecture

The pipeline follows a layered structure:

```text
[Airflow DAG 1] --> Extract data from API --> Save JSON files in MinIO
      ↓
[Airflow DAG 2] --> Trigger PySpark job
      ↓
[PySpark ETL] --> Read + Clean + Transform data --> Load to PostgreSQL (DWH)
      ↓
[PostgreSQL] --> Star Schema (fact_weather + dim_date + dim_location)
```

Each layer is containerized with Docker to ensure consistent environments and smooth integration.

---

## 🧠 Tech Stack

| Tool                            | Purpose                                   |
| ------------------------------- | ----------------------------------------- |
| **Apache Airflow**              | Workflow orchestration for ETL automation |
| **PySpark**                     | Data transformation and schema modeling   |
| **MinIO**                       | Object storage for raw JSON data          |
| **PostgreSQL**                  | Central Data Warehouse                    |
| **Docker Compose**              | Environment setup and service management  |
| **Python (boto3, pyspark.sql)** | Scripting and data handling               |

---

## 🏗️ Project Components

### 1️⃣ **Airflow DAG 1 — Data Extraction**

* Extracts daily weather data from a public API.
* Saves each city’s data as JSON files inside MinIO.
* Folder structure:

  ```
  raw-weather/
      └── CityName/
          ├── 2024-01-01.json
          ├── 2024-01-02.json
  ```

---

### 2️⃣ **Airflow DAG 2 — ETL Trigger**

* Executes the PySpark script that performs the transformation and loading stages.
* Ensures the pipeline runs in order and is fully automated.

---

### 3️⃣ **PySpark ETL Script (`ETL_SCRIPT_pyspark.py`)**

* Reads all raw JSON files from MinIO.
* Cleans and converts data types.
* Creates three main tables:

  * `dim_date`
  * `dim_location`
  * `fact_weather`
* Loads them into PostgreSQL using JDBC connection.
* Handles large data batches and missing value validation.

---

### 4️⃣ **Data Warehouse Schema**

**Star Schema Design:**

```
           ┌──────────────┐
           │  dim_date     │
           └──────┬───────┘
                  │
           ┌──────┴───────┐
           │ fact_weather │
           └──────┬───────┘
                  │
           ┌──────┴───────┐
           │ dim_location │
           └──────────────┘
```

**Tables:**

| Table          | Description                                                             |
| -------------- | ----------------------------------------------------------------------- |
| `dim_date`     | Stores date attributes (year, month, day, weekday).                     |
| `dim_location` | Contains city, country, and region details.                             |
| `fact_weather` | Main fact table — stores temperature, wind, and precipitation measures. |

---

## 📊 Results

✅ Extracted **8,389 JSON files** from MinIO.
✅ Cleaned, transformed, and validated all records with PySpark.
✅ Loaded clean data into PostgreSQL Data Warehouse successfully.
✅ Created and tested **dim_date**, **dim_location**, and **fact_weather** tables.
✅ Verified data consistency and referential integrity between dimensions and fact tables.

---

## 💡 Key Learnings

### 🔸 Technical

* Built a complete ETL pipeline integrating **Airflow**, **Spark**, **MinIO**, and **PostgreSQL**.
* Understood how to implement a **Star Schema** for analytical data modeling.
* Practiced **data cleaning, validation**, and **batch loading** using PySpark.
* Automated workflow scheduling with Airflow.
* Improved understanding of Docker-based data systems.

### 🔸 Professional

* Learned to structure and document a full data engineering project.
* Gained experience in debugging, version control, and integration testing.
* Strengthened teamwork and project presentation skills.

---

## 🚀 Future Enhancements

* Add Power BI / Tableau dashboards on top of the PostgreSQL DWH.
* Implement logging and monitoring in Airflow for better traceability.
* Schedule daily automatic updates via Airflow DAGs.
* Extend the schema with additional dimensions (e.g., weather type, region classification).

---

## 🗂️ Repository Structure

```
├── docker-compose.yml
├── Dockerfile.airflow
├── Dockerfile.spark
├── requirements.txt
├── dags/
│   ├── Dag1.py          # Extract weather data to MinIO
│   ├── Dag2.py          # Trigger Spark ETL job
│   ├── Dagtest2.py      # DAG test version
├── scripts/
│   ├── weather_etl_spark.py    # Spark ETL logic
│   ├── ETL_SCRIPT_pyspark.py   # Main working ETL pipeline
│   ├── SQL-file_pyspark.py     # DWH table creation script
├── README.md
└── .gitignore
```

---

## 🧾 Author

**Ahmed Alaa Eldin**
Data Engineer & Analyst
📍 Passionate about building data-driven systems that turn raw information into strategic insight.

🔗 [GitHub Profile](https://github.com/AhmedAlaaEldin1)

---
