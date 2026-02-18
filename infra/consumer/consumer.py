import json
import boto3
import time
from kafka import KafkaConsumer

# =========================
# MinIO Connection (S3 API)
# =========================
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",  # ✅ CORRECT
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    region_name="us-east-1"
)

bucket_name = "bronzes-tran1234"

# =========================
# Kafka Consumer
# =========================
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers="localhost:29092",  # ✅ same as producer
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",          # ✅ IMPORTANT
    enable_auto_commit=True,
    group_id="minio-consumer"
)

print("📥 Consuming Kafka messages and saving to MinIO...")

# =========================
# Consume & Save
# =========================
for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at", int(time.time()))

    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )

    print(f"✅ Saved → s3://{bucket_name}/{key}")
