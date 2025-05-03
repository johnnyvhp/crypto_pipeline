import os
import time
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))
os.environ["PREFECT_API_URL"] = "http://prefect:4200/api"
from prefect import flow, task
from scripts.api_producer import produce_five_times
from scripts.spark_consumer import main as consume_messages
from scripts.load_to_bigquery import main as load_to_bq

@task(log_prints=True)
def run_producer():
    print("🌀 Trying to fetch data from API 5 times...")
    produce_five_times()

@task
def run_consumer():
    print("⏳ Waiting for Kafka to receive data before starting Spark consumer...")
    time.sleep(15)  # Adjust timing based on needs
    consume_messages()

@task
def load_bigquery():
    load_to_bq()

@flow(name="crypto-pipeline", log_prints=True)
def crypto_pipeline():
    print("→ Prefect flow sees KAFKA_HOST:", os.getenv("KAFKA_HOST"), "KAFKA_PORT:", os.getenv("KAFKA_PORT"))
    run_producer()
    run_consumer()
    load_bigquery()

if __name__ == "__main__":
    crypto_pipeline()