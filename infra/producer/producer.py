import os
import time
import json
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

# =========================
# API CONFIG
# =========================
API_KEY = "d5mufrhr01qj2afi757gd5mufrhr01qj2afi7580"
BASE_URL = "https://finnhub.io/api/v1/quote"

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]

# =========================
# KAFKA PRODUCER
# =========================
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=10,
    request_timeout_ms=30000
)

# =========================
# FUNCTION
# =========================
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data or data.get("c", 0) == 0:
            print(f"No valid data for {symbol}")
            return None

        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

# =========================
# MAIN LOOP
# =========================
print("🚀 Kafka producer started...")

while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            try:
                future = producer.send(
                    topic="stock-quotes",
                    key=symbol,
                    value=quote
                )
                metadata = future.get(timeout=30)
                print(
                    f"✅ Sent {symbol} → "
                    f"partition {metadata.partition}, offset {metadata.offset}"
                )

            except KafkaTimeoutError as e:
                print(f"⏱ Kafka timeout for {symbol}: {e}")

            except Exception as e:
                print(f"❌ Kafka error for {symbol}: {e}")

    producer.flush()
    time.sleep(6)
