from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'sujoy',
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}

# ------------------ Fetch Data ------------------

def get_data():
    import requests

    response = requests.get(
        'https://randomuser.me/api/',
        timeout=10
    )

    response.raise_for_status()

    return response.json()['results'][0]

# ------------------ Format ------------------

def format_data(res):
    import uuid

    location = res['location']

    return {
        "id": str(uuid.uuid4()),
        "first_name": res['name']['first'],
        "last_name": res['name']['last'],
        "gender": res['gender'],
        "address": (
            f"{location['street']['number']} "
            f"{location['street']['name']}, "
            f"{location['city']}, "
            f"{location['state']}, "
            f"{location['country']}"
        ),
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
    import logging
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )

    try:
        res = get_data()
        data = format_data(res)

        producer.send('users_created', value=data)

        # Ensure data is actually sent
        producer.flush()

        logging.info(f"Sent data: {data['id']}")

    except Exception as e:
        logging.error(f"Error sending data: {e}")
        raise

    finally:
        producer.close()

# ------------------ DAG ------------------

with DAG(
    dag_id='user_automation',
    start_date=datetime(2024, 1, 1),

    # New Airflow syntax
    schedule='*/1 * * * *',

    catchup=False,
    default_args=default_args,

    tags=['kafka', 'streaming', 'airflow']
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )