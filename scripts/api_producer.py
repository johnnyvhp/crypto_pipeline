import requests
import json
import logging
import os
import time
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("kafka").setLevel(logging.DEBUG)

# Kafka topic
topic = 'crypto_prices'

# API URL to get top 20 coins by market cap
url = 'https://api.coingecko.com/api/v3/coins/markets'
params = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 20,
    'page': 1,
    'sparkline': False
}

def produce_five_times(delay_seconds=2):
    bootstrap_env = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if bootstrap_env:
        bootstrap_servers = bootstrap_env.split(",")
    else:
        host = os.getenv("KAFKA_HOST", "localhost")
        port = os.getenv("KAFKA_PORT", "9092")
        bootstrap_servers = [f"{host}:{port}"]

    logging.info(f"🔌 Kafka bootstrap servers: {bootstrap_servers}")
    logging.info(f"📍 Kafka topic: {topic}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Wait for topic metadata to be available
    for _ in range(5):
        partitions = producer.partitions_for(topic)
        if partitions is not None:
            logging.info(f"✅ Topic '{topic}' is available with partitions: {partitions}")
            break
        logging.warning(f"⌛ Waiting for topic '{topic}' to become available...")
        time.sleep(2)
    else:
        logging.error(f"❌ Topic '{topic}' is not available after retrying.")
        return

    if producer.bootstrap_connected():
        logging.info("✅ Kafka bootstrap server connection established.")
    else:
        logging.warning("⚠️ Kafka bootstrap server connection may not be ready yet.")

    for attempt in range(5):
        logging.info(f"🔄 Fetching data from CoinGecko API... Attempt {attempt + 1}/5")
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                logging.info(f"📦 API returned {len(data)} items")
                logging.info(f"📋 Sample data: {data[:3] if len(data) >= 3 else data}")
                if data:
                    for coin in data:
                        try:
                            future = producer.send(topic, value=coin)
                            result = future.get(timeout=10)
                            logging.info(f"✅ Sent data for {coin['name']} (offset: {result.offset})")
                        except Exception as e:
                            logging.error(f"❌ Failed to send/acknowledge data for {coin.get('name', 'N/A')}: {e}")
                else:
                    logging.warning("⚠️ API returned empty data.")
                    logging.warning(f"⚠️ Response content: {response.content}")
            else:
                logging.warning(f"⚠️ API request failed with status: {response.status_code}")
                logging.warning(f"⚠️ Response content: {response.content}")
        except Exception as e:
            logging.error(f"❌ Error fetching or sending data: {e}")
        
        producer.flush()

        if attempt < 4:
            logging.info(f"⏳ Waiting {delay_seconds}s before next fetch...")
            time.sleep(delay_seconds)

if __name__ == "__main__":
    produce_five_times()