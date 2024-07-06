import json
import time
import logging
from src.data_fetcher import get_data
from src.data_formatter import format_data
from src.config import KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from kafka import KafkaProducer


def initialize_producer():
    try:
        producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], max_block_ms=5000)
        return producer
    except Exception as e:
        logging.error(f"Error initializing Kafka producer: {e}")
        return None


def fetch_and_send_data(producer, end_time):
    while time.time() <= end_time:
        try:
            res = get_data()
            if not res:
                logging.warning("Received no data")
                continue

            formatted_data = format_data(res)
            if not formatted_data:
                logging.warning("Failed to format data")
                continue

            producer.send(KAFKA_TOPIC, json.dumps(formatted_data).encode('utf-8'))
            logging.info(f"Sent data to Kafka: {formatted_data}")

        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue
