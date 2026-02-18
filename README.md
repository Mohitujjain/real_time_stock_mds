🚀 Real-Time Stock Data Engineering Pipeline

An end-to-end real-time data engineering project built using modern data stack technologies.

🏗 Architecture Overview

<img width="595" height="364" alt="image" src="https://github.com/user-attachments/assets/e8fc3126-b3ce-4f02-ad2d-d6c1b6f6fd52" />

<img width="651" height="457" alt="image" src="https://github.com/user-attachments/assets/971562f6-53be-41bb-8cbf-03ff0c782409" />


Stock API
↓
Kafka Producer
↓
Kafka Topic (stock-quotes)
↓
Kafka Consumer
↓
MinIO (Bronze - Raw Storage)
↓
Airflow DAG
├── COPY INTO Snowflake (RAW)
└── dbt Transformations (Bronze → Silver → Gold)
↓
Snowflake Data Warehouse
↓
Power BI Dashboard


🏗 Architecture Diagram

🛠 Tech Stack

🐳 Docker
📡 Kafka
🗄 MinIO
🎯 Airflow
❄ Snowflake
🔄 dbt
📊 Power BI




🔧 Tools Used – What | Why | How

🐳 Docker
What: Containerization platform
Why: Consistent environments
How: Runs Kafka, MinIO, Airflow via docker-compose

📡 Kafka
What: Distributed streaming platform
Why: Real-time stock streaming
How: Producer → stock-quotes topic → Consumer

🗄 MinIO
What: S3-compatible object storage
Why: Bronze raw storage
How: Consumer writes JSON files into bucket

🎯 Airflow
What: Workflow orchestrator
Why: Automates data pipeline
How: DAG runs COPY INTO + dbt

❄ Snowflake
What: Cloud data warehouse
Why: Analytics-ready storage
How: RAW → Bronze → Silver → Gold

🔄 dbt
What: SQL transformation framework
Why: Modular data modeling
How: Builds staging, cleaned, KPI models

📊 Power BI
What: BI Visualization Tool
Why: Dashboard & reporting
How: Connects to Gold tables

1. Open MinIO: http://localhost:9001
2. Login using credentials from docker-compose.
3. Click 'Create Bucket'.
4. Bucket name MUST match the name defined inside consumer.py (example: stock-raw-data).
5. Create bucket before starting the consumer.


▶️ How To Run Project
1️⃣ Start Infrastructure
cd infra
docker compose up -d

2️⃣ Create Kafka Topic
Create Kafka Topic:
docker exec -it kafka kafka-topics --create --topic stock-quotes --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1


3️⃣ Start Producer
python producer.py

4️⃣ Start Consumer
python consumer.py

5️⃣ Trigger Airflow DAG

Open: http://localhost:8080

Turn ON DAG
Trigger if needed

📊 Power BI Dashboard

🎯 Key Learnings

Real-time event streaming

Medallion architecture (Bronze/Silver/Gold)

ELT vs ETL understanding

Cloud data warehousing

Workflow orchestration

👨‍💻 Author

Mohit Kumar
India 🇮🇳
