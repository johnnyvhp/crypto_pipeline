# 🚀 Real-Time Cryptocurrency Data Pipeline

This project is an end-to-end **real-time data pipeline** that fetches cryptocurrency prices using the CoinGecko API, streams the data using **Kafka**, processes it with **Apache Spark**, stores it as **Parquet**, and finally loads it into **Google BigQuery** — all orchestrated using **Prefect**.

---

## 📦 Tech Stack

- **Kafka** – real-time message broker
- **Apache Spark** – streaming consumer and processor
- **Parquet** – efficient columnar storage format
- **Google BigQuery** – cloud-based data warehouse
- **Prefect** – workflow orchestration
- **Docker Compose** – containerized infrastructure

---

## ⚙️ Architecture Overview

CoinGecko API → Kafka Producer → Kafka → Spark Structured Streaming → Parquet → BigQuery
↑
Orchestrated by Prefect


## 📁 Project Structure

```
.
├── scripts/
│   ├── api_producer.py          # Fetches crypto prices and sends to Kafka
│   ├── spark_consumer.py        # Consumes messages from Kafka and writes Parquet
│   └── load_to_bigquery.py      # Loads Parquet files into BigQuery
├── prefect/
│   └── flows/
│       └── crypto_flow.py       # Prefect flow managing the full pipeline
├── output/
│   └── crypto_data/             # Folder where Parquet files are saved
├── infra/
│   └── docker-compose.yml       # Defines all services (Kafka, Prefect, etc.)
└── credentials/
    └── gcp_credentials.json     # (Git-ignored) GCP service account key
```

---

## 🧠 What I Learned

- How to build and containerize a real-time streaming data pipeline
- Connecting Kafka, Spark, and BigQuery using Prefect orchestration
- Securely handling service credentials and using `.gitignore` properly
- Resolving GitHub push protection errors due to secrets
- Structuring a clean, scalable data pipeline with logs and checkpoints

---

## 🛠️ How to Run Locally

1. **Set up environment variables**

Create a `.env` file in the root with the following:

```env
GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/gcp_credentials.json
BIGQUERY_PROJECT_ID=your_project_id
BIGQUERY_DATASET=your_dataset
BIGQUERY_TABLE=your_table

2. **Start the services**

cd infra
docker-compose up --build