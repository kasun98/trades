from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import logging
import os

def trigger_model_retrain():
    try:
        response = requests.post("http://192.168.8.154:5050/retrain") # change to your API endpoint
        if response.status_code == 200:
            logging.info("Retrain triggered successfully!")
        else:
            logging.warning(f"Failed to trigger retrain. Status code: {response.status_code}")
        logging.info(f"Status: {response.status_code}")
        logging.info(f"Response: {response.text}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending request to retrain API: {e}")


with DAG(
    dag_id="daily_model_retrain",
    start_date=datetime(2025, 4, 4),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    retrain_task = PythonOperator(
        task_id="trigger_retrain_api",
        python_callable=trigger_model_retrain,
    )