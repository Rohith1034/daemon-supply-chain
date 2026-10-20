from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from confluent_kafka import Consumer,KafkaError
from google.cloud import storage
import json
from datetime import datetime
from time import time
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowFailException

conf = {
    "bootstrap.servers":"pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "security.protocol":"SASL_SSL",
    "sasl.mechanisms":"PLAIN",
    "sasl.username":"XBLQVAYYNHY5KZX5",
    "sasl.password":"cfltq7uzeYUHBwT/dOAWUqL8MT7VRDMqdrF5/64khMxeSqec2aDfWQY77Psy8NlA",
    "client.id":"ccloud-python-client-998e3697-c32e-4805-9857-4381d6527253",
    "group.id": "testing_group_000",
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'enable.partition.eof': True
}

consumer = Consumer(conf)

topics = [
    'inventory-events',
    'order-events',
    'purchase-order-events',
    'shipment-events',
    'supplier-shipment-events',
    'warehouse-task-events'
]


consumer.subscribe(topics)

client = storage.Client()
bucket = client.bucket("daemon-supply-bronze")

def bytes_to_string_encoder(obj):
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serialize')


def place_it_in_GCS(msg):
    if msg is None or msg == {}:
        return

    data = msg["value"]
    year_long = datetime.today().strftime("%Y")
    current_month = datetime.now().month
    current_day = datetime.now().day
    json_value = json.loads(data)
    print(json_value)
    if 'event_type' not in json_value and json_value['eventType'] is not None and json_value['eventType'] != '':
        json_value['event_type'] = json_value['eventType']
    if 'event_type' not in json_value:
        print("event_type not found in the message value")
        return
    base_url = (
        f"raw/{msg['topic']}/"
        f"{json_value['event_type']}/"
        f"{year_long}/"
        f"{current_month}/"
        f"{current_day}/"
        f"partition={msg['partition']}/"
        f"{msg['offset']}.json"
    )
    print(base_url)
    blob = bucket.blob(base_url)
    json_string = json.dumps(msg, default=bytes_to_string_encoder, indent=4)
    blob.upload_from_string(
        data=json_string,
        content_type='application/json'
    )

    return f"Uploaded file in {base_url}"


def main_consumer():
    try:
        idle_timeout = 30
        last_message_time = time()
        while True:

            msg = consumer.poll(timeout=1.0)

            if msg is None:

                if time() - last_message_time > idle_timeout:
                    print("No new messages. Exiting consumer.")
                    break

                continue

            last_message_time = time()

            if msg.error():

                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue

                else:
                    print(msg.error())
                    continue


            else:

                result = {
                    'topic': msg.topic(),
                    'timestamp_value': str(msg.timestamp()[1]),
                    'partition': str(msg.partition()),
                    'offset': str(msg.offset()),
                    'key': msg.key().decode() if msg.key() else None,
                    'value': msg.value().decode()
                }

                upload_result = place_it_in_GCS(result)

                print(upload_result)

                if upload_result:
                    consumer.commit(message=msg)



    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

with DAG(
     dag_id="kafka_consumer",
     start_date=datetime.datetime(2021, 1, 1),
     schedule=None,
     max_active_runs=1,
     catchup=False
) as dag:
    t1 = PythonOperator(
        task_id="consumer",
        python_callable=main_consumer
    )

    t1