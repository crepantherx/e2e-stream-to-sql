# dags/my_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import time
import logging

# Import functions from src
from src.data_fetch_and_send import initialize_producer, fetch_and_send_data

default_args = {
    'owner': 'Sudhir Singh',
    'start_date': datetime(2023, 9, 3, 10, 00),
    'email': ['crepantherx@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'execution_timeout': timedelta(minutes=10),
    'catchup': False,
}

dag = DAG(
    'users-stream',
    default_args=default_args,
    description='DAG using TaskFlow API, to request and stream to kafka',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sudhir', 'kafka'],
    doc_md="""
    This DAG is designed to stream data to Kafka. It performs the following steps:
    
    1. Initializes a Kafka producer.
    2. Fetches data from an API.
    3. Formats the data.
    4. Sends the formatted data to Kafka.
    """
)


@task
def stream_data_to_kafka():
    producer = initialize_producer()
    if not producer:
        logging.error("Failed to initialize Kafka producer. Exiting.")
        return

    start_time = time.time()
    end_time = start_time + 60

    fetch_and_send_data(producer, end_time)

    producer.close()
    logging.info("Kafka producer closed.")


with dag:
    stream_data_to_kafka()