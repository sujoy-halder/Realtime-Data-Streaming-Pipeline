from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'panda',
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}

# ------------------ Fetch Data ------------------

def get_data():
    import requests
    res = requests.get('https://randomuser.me/api/')
    return res.json()['results'][0]

# ------------------ Format ------------------

def format_data(res):
    import uuid

    location = res['location']

    return {
        "id": str(uuid.uuid4()),
        "first_name": res['name']['first'],
        "last_name": res['name']['last'],
        "gender": res['gender'],
        "address": f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        "post_code": str(location['postcode']),
        "email": res['email'],
        "username": res['login']['username'],
        "registered_date": res['registered']['date'],
        "phone": res['phone'],
        "picture": res['picture']['medium']
    }

# ------------------ Kafka Producer ------------------

def stream_data():
    import json
    from kafka import KafkaProducer
    import logging

    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],  # FIXED
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        res = get_data()
        data = format_data(res)

        producer.send('users_created', data)
        logging.info(f"Sent data: {data['id']}")

    except Exception as e:
        logging.error(f"Error sending data: {e}")
        raise

# ------------------ DAG ------------------

with DAG(
    dag_id='user_automation',
    start_date=datetime(2023, 10, 30),
    schedule_interval='*/1 * * * *',  # every minute
    catchup=False
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )