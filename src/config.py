# configs/config.py
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'users')
API_URL = "https://randomuser.me/api/"