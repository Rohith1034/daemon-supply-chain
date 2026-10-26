from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from confluent_kafka import Consumer,KafkaError
from google.cloud import storage
from pyspark.sql import SparkSession
import os
import json
from datetime import datetime
from time import time
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowFailException
from pyspark_schemas.main import get_spark_schema
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.functions import col

conf = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
    "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN"),
    "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
    "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
    "client.id": os.getenv("KAFKA_CLIENT_ID", "daemon-supply-chain-consumer"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "daemon-supply-chain-consumer"),
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'enable.partition.eof': True
}

topics = [
    'inventory-events',
    'order-events',
    'purchase-order-events',
    'shipment-events',
    'supplier-shipment-events',
    'warehouse-task-events'
]

spark = (
        SparkSession.builder
        .appName("daemon-supply-chain")
        .master("spark://spark-master:7077")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "3")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def bytes_to_string_encoder(obj):
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serialize')

def flatten_structs(df):
    while True:
        nested = next(
            (field for field in df.schema.fields
             if isinstance(field.dataType, StructType)),
            None,
        )
        if nested is None:
            return df

        child_columns = [
            col(f"`{nested.name}`.`{child.name}`").alias(
                f"{nested.name}_{child.name}"
            )
            for child in nested.dataType.fields
        ]
        df = df.select("*", *child_columns).drop(nested.name)
        flat_df = flatten_structs(df)




def place_it_in_GCS_parse(msg,bucket):
    if msg is None or msg == {}:
        return

    data = msg["value"]
    year_long = datetime.today().strftime("%Y")
    current_month = datetime.now().month
    current_day = datetime.now().day
    json_value = json.loads(data)
    event_name = json_value.get("eventType") or json_value.get("event_type")
    if not event_name:
        raise AirflowFailException("event type not found in message value")

    event_schema = get_spark_schema(event_name=event_name)
    if isinstance(event_schema, str):
        raise AirflowFailException(event_schema)

    df = spark.createDataFrame([json_value], schema=event_schema)
    flat_df = flatten_structs(df)

    gcs_path = (
        f"gs://{bucket.name}/parsed/{msg['topic']}/{event_name}/"
        f"{year_long}/{current_month}/{current_day}/"
        f"partition={msg['partition']}/offset={msg['offset']}"
    )

    flat_df.write.mode("errorifexists").parquet(gcs_path)
    return f"Saved Parquet data to {gcs_path}"




def place_it_in_GCS(msg, bucket):
    if msg is None or msg == {}:
        return

    data = msg["value"]
    year_long = datetime.today().strftime("%Y")
    current_month = datetime.now().month
    current_day = datetime.now().day
    json_value = json.loads(data)
    # print(json_value)
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
    consumer = Consumer(conf)
    try:
        consumer.subscribe(topics)
        bucket_name = os.getenv("GCS_BUCKET", "daemon-supply-bronze")
        bucket = storage.Client().bucket(bucket_name)
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

                upload_result = place_it_in_GCS(result, bucket)
                print(upload_result)

                upload_result_of_parsed = place_it_in_GCS_parse(msg,bucket)
                print(upload_result_of_parsed)

                if upload_result:
                    consumer.commit(message=msg)



    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

with DAG(
     dag_id="kafka_consumer",
     start_date=datetime(2021, 1, 1),
     schedule=None,
     max_active_runs=1,
     catchup=False
) as dag:
    t1 = PythonOperator(
        task_id="consumer",
        python_callable=main_consumer
    )

    t1