from dotenv import load_dotenv
import os
import requests
import time
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import psycopg2
import os
import boto3

load_dotenv(dotenv_path="/opt/airflow/.env")

QDB_HOST = os.getenv("QDB_HOST")
QDB_PSYCOPG_PORT = os.getenv("QDB_PSYCOPG_PORT")
QDB_USER = os.getenv("QDB_USER")
QDB_PASSWORD = os.getenv("QDB_PASSWORD")

QDB_BRONZE_LAYER_TABLE=os.getenv("QDB_BRONZE_LAYER_TABLE")
QDB_SILVER_LAYER_TABLE=os.getenv("QDB_SILVER_LAYER_TABLE")
QDB_GOLD_VIEW_1MIN=os.getenv("QDB_GOLD_VIEW_1MIN")
QDB_GOLD_VIEW_5MIN=os.getenv("QDB_GOLD_VIEW_5MIN")
QDB_GOLD_BID_PRED_TABLE=os.getenv("QDB_GOLD_BID_PRED_TABLE")
QDB_GOLD_VOL_PRED_TABLE=os.getenv("QDB_GOLD_VOL_PRED_TABLE")

MINIO_USER_ID=os.getenv("MINIO_USER_ID")
MINIO_PASSWORD=os.getenv("MINIO_PASSWORD")
MINIO_HOST=os.getenv("MINIO_HOST")
MINIO_DATALAKE_BUCKET=os.getenv("MINIO_DATALAKE_BUCKET")
AWS_DEFAULT_REGION=os.getenv("AWS_DEFAULT_REGION")

# === Config ===
QDB_CONN = {
    "host": QDB_HOST,
    "port": QDB_PSYCOPG_PORT,
    "user": QDB_USER,
    "password": QDB_PASSWORD
}

TABLES = [
    QDB_BRONZE_LAYER_TABLE,
    QDB_SILVER_LAYER_TABLE,
    QDB_GOLD_VIEW_1MIN,
    QDB_GOLD_VIEW_5MIN,
    QDB_GOLD_BID_PRED_TABLE,
    QDB_GOLD_VOL_PRED_TABLE
]

date_string = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

MINIO_CONFIG = {
    "endpoint_url": MINIO_HOST,
    "aws_access_key_id": MINIO_USER_ID,
    "aws_secret_access_key": MINIO_PASSWORD,
    "bucket_name": MINIO_DATALAKE_BUCKET,
    "prefix": f"{date_string}/"
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'questdb_export_to_minio',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def connect_to_db(HOST, table_name, PORT=9000):
    try:
        query = f"""SELECT * FROM {table_name} WHERE timestamp >= dateadd('d', -1, now());"""
        url = f"http://{HOST}:{PORT}/exec"
        res = requests.get(url, params={"query": query})
        json_data = res.json()

        # Extract column names
        column_names = [col["name"] for col in json_data["columns"]]
        rows = json_data["dataset"]

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=column_names)
        
        logging.info(f"Fetched data from QuestDB with {len(df)} rows and {len(df.columns)} columns")
        return df
    
    except Exception as e:
        logging.error(f"Error connecting to database : {e}")
        return None

def export_table_to_minio(table_name, **kwargs):
    # Date for filename
    date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    filename = f"{table_name}_{date_str}.parquet"
    file_path = f"/tmp/{filename}"

    # Connect to QuestDB
    df = connect_to_db(QDB_HOST, table_name)
    if df is None:
        logging.error(f"Failed to fetch data for table {table_name}. Skipping export.")
        return
    # Convert to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

    # Upload to MinIO
    s3 = boto3.client('s3',
                      endpoint_url=MINIO_CONFIG['endpoint_url'],
                      aws_access_key_id=MINIO_CONFIG['aws_access_key_id'],
                      aws_secret_access_key=MINIO_CONFIG['aws_secret_access_key'])
    
    s3.upload_file(file_path,
                   MINIO_CONFIG['bucket_name'],
                   f"{MINIO_CONFIG['prefix']}{filename}")

    # Clean up
    os.remove(file_path)


for table in TABLES:
    task = PythonOperator(
        task_id=f"export_{table}",
        python_callable=export_table_to_minio,
        op_kwargs={'table_name': table},
        dag=dag
    )
