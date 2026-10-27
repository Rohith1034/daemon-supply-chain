from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from confluent_kafka import Consumer,KafkaError
from google.cloud import storage
import os
import json
from datetime import datetime, timezone
from time import time
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowFailException

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

def bytes_to_string_encoder(obj):
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serialize')

def flatten_structs(df):
    from pyspark.sql.functions import col
    from pyspark.sql.types import StructType

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


def normalize_event_value(value, data_type, path="event"):
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructType,
        TimestampType,
    )

    if value is None:
        return None

    if isinstance(data_type, TimestampType):
        if isinstance(value, datetime):
            timestamp = value
        elif isinstance(value, str):
            try:
                timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(f"Invalid timestamp at {path}: {value!r}") from exc
        else:
            raise TypeError(
                f"Expected timestamp string at {path}, got {type(value).__name__}"
            )
        return timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp

    if isinstance(data_type, StructType):
        if not isinstance(value, dict):
            raise TypeError(f"Expected object at {path}, got {type(value).__name__}")
        fields_by_name = {field.name: field for field in data_type.fields}
        unexpected = value.keys() - fields_by_name.keys()
        if unexpected:
            raise ValueError(f"Unexpected fields at {path}: {sorted(unexpected)}")

        normalized = {}
        for field in data_type.fields:
            field_path = f"{path}.{field.name}"
            if field.name not in value:
                if not field.nullable:
                    raise ValueError(f"Missing required field {field_path}")
                field_value = None
            else:
                field_value = value[field.name]
            if field_value is None and not field.nullable:
                raise ValueError(f"Required field {field_path} cannot be null")
            normalized[field.name] = normalize_event_value(
                field_value,
                field.dataType,
                field_path,
            )
        return normalized

    if isinstance(data_type, ArrayType):
        if not isinstance(value, list):
            raise TypeError(f"Expected array at {path}, got {type(value).__name__}")
        normalized = []
        for index, item in enumerate(value):
            item_path = f"{path}[{index}]"
            if item is None and not data_type.containsNull:
                raise ValueError(f"Null array element at {item_path}")
            normalized.append(
                normalize_event_value(item, data_type.elementType, item_path)
            )
        return normalized

    if isinstance(data_type, StringType) and not isinstance(value, str):
        raise TypeError(f"Expected string at {path}, got {type(value).__name__}")
    if isinstance(data_type, IntegerType) and type(value) is not int:
        raise TypeError(f"Expected integer at {path}, got {type(value).__name__}")
    if isinstance(data_type, DoubleType):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"Expected number at {path}, got {type(value).__name__}")
        return float(value)
    if isinstance(data_type, BooleanType) and type(value) is not bool:
        raise TypeError(f"Expected boolean at {path}, got {type(value).__name__}")

    return value


def place_it_in_GCS_parse(msg, bucket, spark):
    if msg is None or msg == {}:
        return

    from pyspark_schemas.main import get_spark_schema

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

    converted_value = normalize_event_value(json_value, event_schema)
    df = spark.createDataFrame([converted_value], schema=event_schema)
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
    spark = None
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

                if spark is None:
                    from pyspark.sql import SparkSession

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

                upload_result_of_parsed = place_it_in_GCS_parse(
                    result,
                    bucket,
                    spark,
                )
                print(upload_result_of_parsed)

                if upload_result:
                    consumer.commit(message=msg)



    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        if spark is not None:
            spark.stop()

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