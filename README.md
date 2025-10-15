# 🌦️ Weather Data Warehouse ETL Project

## 📘 Overview

This project is a complete **ETL (Extract, Transform, Load)** pipeline designed to collect weather data, process it, and load it into a **PostgreSQL data warehouse** for further analysis.

The main goal of the project is to **build a scalable data pipeline** using modern data engineering tools like **Apache Airflow**, **PySpark**, and **PostgreSQL**, ensuring data reliability, automation, and efficiency.

---

## 🧩 Architecture

Below is the high-level architecture of the project:

1. **Airflow (Extraction Layer)** – Extracts data from a **MinIO data lake** (weather data source).
2. **PySpark (Transformation Layer)** – Cleans, transforms, and processes the extracted data.
3. **PostgreSQL (Loading Layer)** – Stores the transformed data in a structured format.
4. **Docker Compose** – Orchestrates all services in a local containerized environment.

---

## ⚙️ Tools & Technologies Used

| Tool               | Purpose                            |
| ------------------ | ---------------------------------- |
| **Apache Airflow** | Workflow automation and scheduling |
| **PySpark**        | Data transformation and processing |
| **PostgreSQL**     | Data warehouse (storage)           |
| **MinIO**          | Data lake for raw weather data     |
| **Docker Compose** | Container management               |
| **Python**         | Scripting and integration          |

---

## 🧠 What We Learned from This Project

This project helped us gain **hands-on experience in real-world data engineering concepts**, including:

* Understanding how **ETL pipelines** work end-to-end.
* Building **automated data workflows** using Apache Airflow.
* Performing large-scale **data transformations** with PySpark.
* Designing a **data warehouse** schema for efficient querying.
* Integrating multiple tools (Airflow, PySpark, PostgreSQL, MinIO) using **Docker Compose**.
* Handling errors, dependency issues, and real deployment challenges in a multi-service environment.

---

## 🚀 How It Works

1. **Airflow DAG** triggers the extraction task to fetch raw data from MinIO.
2. **PySpark job** reads this data, applies transformations (cleaning, filtering, type casting, etc.).
3. The processed data is then **loaded into PostgreSQL** for analysis.
4. The pipeline can be scheduled to run automatically at defined intervals.

---

## 🖼️ Screenshots

Below are screenshots showing key components of the project execution:

### 🌀 Airflow DAG
![Airflow DAG Screenshot](https://github.com/AhmedAlaaEldin1/weather-data-warehouse-etl/blob/main/7a014c24-bc56-4756-b820-1efd1be35186.png)

### ⚙️ ETL Execution in Jupyter (PySpark)
![PySpark ETL Screenshot](https://github.com/AhmedAlaaEldin1/weather-data-warehouse-etl/blob/main/Screenshot%202025-10-14%20182012.png)

### 🗄️ PostgreSQL Tables
![PostgreSQL Tables Screenshot](https://github.com/AhmedAlaaEldin1/weather-data-warehouse-etl/blob/main/Screenshot%202025-10-14%20182431.png)

---

## 🧩 Project Structure

```

weather-data-warehouse-etl/
│
├── airflow/                 # Airflow DAGs & configs
├── pyspark/                 # ETL PySpark scripts
├── docker-compose.yml       # Container setup
├── postgres/                # Database setup
├── minio/                   # Raw data storage
├── screenshots/             # Project images
├── requirements.txt         # Dependencies
└── README.md                # Documentation

```

---

## 📚 Future Improvements

* Automate data quality checks before loading.
* Add a **Power BI dashboard** connected to PostgreSQL.
* Deploy the entire pipeline on **AWS (S3, Redshift, and MWAA)**.

---

## 👥 Contributor

* **Ahmed Alaa** – Data Engineer

---
