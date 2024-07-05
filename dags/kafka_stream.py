from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import json
from kafka import KafkaProducer
import time
import logging
import requests
import  uuid

default_args = {
    'owner': 'crepantherx',
    'start_date': datetime(2023, 9, 3, 10, 00)
}


def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


@task
def stream_data():

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


dag = DAG(
    'new-kafka-stream',
    default_args=default_args,
    description='A simple DAG to execute a Python script using TaskFlow API',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

with dag:
    stream_data()
