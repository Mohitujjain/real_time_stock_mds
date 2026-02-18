import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago


# =========================
# MINIO CONFIG (Docker)
# =========================
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronzes-tran1234"
LOCAL_DIR = "/opt/airflow/data/minio"

# =========================
# SNOWFLAKE CONFIG
# =========================
SNOWFLAKE_USER = "mohit"
SNOWFLAKE_PASSWORD = "Mohitkumar12345"
SNOWFLAKE_ACCOUNT = "nv36987.ap-southeast-1"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS_MDS"
SNOWFLAKE_SCHEMA = "COMMON"

# =========================
# TASK 1: DOWNLOAD
# =========================
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET)

    files = []

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Preserve folder structure locally
            local_path = os.path.join(LOCAL_DIR, key)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            s3.download_file(BUCKET, key, local_path)
            files.append(local_path)

            print(f"Downloaded {key}")

    return files


# =========================
# TASK 2: LOAD TO SNOWFLAKE
# =========================
def load_to_snowflake(**context):
    files = context["ti"].xcom_pull(task_ids="download_minio")

    if not files:
        print("No files found")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )

    try:
        cur = conn.cursor()

        # Upload files to internal stage
        for f in files:
            print(f"Uploading {f} to Snowflake stage...")
            cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw OVERWRITE=TRUE")

        # Copy into table
        print("Copying data into bronze_stock_quotes_raw table...")
        cur.execute("""
            COPY INTO bronze_stock_quotes_raw
            FROM (SELECT $1 FROM @%bronze_stock_quotes_raw)
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE'
        """)

        # Clean stage after load (VERY IMPORTANT)
        cur.execute("REMOVE @%bronze_stock_quotes_raw")

        conn.commit()
        print("Data loaded successfully.")

    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()


# =========================
# DAG
# =========================

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    load_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    download_task >> load_task
