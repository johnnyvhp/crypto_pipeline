import json
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os
# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define paths dynamically
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
CRYPTO_DATA_DIR = OUTPUT_DIR / "crypto_data"
CHECKPOINT_DIR = OUTPUT_DIR / "checkpoints"

# Create output folders if they don't exist
CRYPTO_DATA_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    logging.info("🚀 Starting Spark Kafka consumer...")

    # Debug: show where Parquet files will be written
    logging.info(f"🔍 Spark will write Parquet files to: {CRYPTO_DATA_DIR.resolve()}")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CryptoSparkConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define schema matching the producer
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("market_cap_rank", LongType(), True),
        StructField("fully_diluted_valuation", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("high_24h", DoubleType(), True),
        StructField("low_24h", DoubleType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("market_cap_change_24h", DoubleType(), True),
        StructField("market_cap_change_percentage_24h", DoubleType(), True),
        StructField("circulating_supply", DoubleType(), True),
        StructField("total_supply", DoubleType(), True),
        StructField("max_supply", DoubleType(), True),
        StructField("ath", DoubleType(), True),
        StructField("ath_change_percentage", DoubleType(), True),
        StructField("ath_date", StringType(), True),
        StructField("atl", DoubleType(), True),
        StructField("atl_change_percentage", DoubleType(), True),
        StructField("atl_date", StringType(), True),
        StructField("roi", StringType(), True),
        StructField("last_updated", StringType(), True),
    ])

    # Read from Kafka
    bootstrap_env = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if bootstrap_env:
        bootstrap_servers = bootstrap_env.split(",")
    else:
        host = os.getenv("KAFKA_HOST", "localhost")
        port = os.getenv("KAFKA_PORT", "9092")
        bootstrap_servers = [f"{host}:{port}"]

    logging.info(f"📡 Connecting to Kafka at: {','.join(bootstrap_servers)}")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(bootstrap_servers)) \
        .option("subscribe", "crypto_prices") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the value field (it's binary so we need to cast first)
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Write to Parquet once per run
    parquet_query = parsed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", str(CRYPTO_DATA_DIR)) \
        .option("checkpointLocation", str(CHECKPOINT_DIR)) \
        .trigger(once=True) \
        .start()

    logging.info("✅ Spark batch write triggered. Awaiting Parquet write completion...")

    # Wait for the single batch to complete
    parquet_query.awaitTermination()

if __name__ == "__main__":
    main()
