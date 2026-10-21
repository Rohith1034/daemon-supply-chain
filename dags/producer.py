from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from confluent_kafka import Producer
import os
import psycopg2
import json
from datetime import datetime, timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowFailException

from simulator.core.config import DB_CONFIG

purchase_order_events = [
    "PurchaseOrderCreated",
    "PurchaseOrderApproved",
]

supplier_shipment_events = [
    "SupplierShipmentCreated",
    "ASNReceived",
    "SupplierShipmentDelivered",
]

inventory_events = [
    "GoodsReceived",
    "StockIncreased",
    "InventoryReserved",
    "InventoryAllocationCreated",
    "InventoryPutaway",
]

warehouse_task_events = [
    "ReceivingTaskCreated",
    "ReceivingTaskStarted",
    "PickingTaskCreated",
    "PickingTaskStarted",
    "PickingCompleted",
    "PackingTaskCreated",
    "PackingTaskStarted",
    "PackingCompleted",
]

order_events = [
    "OrderCreated",
    "OrderItemCreated",
]

shipment_events = [
    "ShipmentReady",
    "CarrierAssigned",
    "ShipmentPickedUp",
    "ShipmentInTransit",
    "ShipmentDelivered",
]

conf = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
    "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN"),
    "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
    "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
    "client.id": os.getenv("KAFKA_CLIENT_ID", "daemon-supply-chain-producer"),
}

def onMsgSuccess(err, msg, row, conn, cur):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        current_time_utc = datetime.now(timezone.utc)
        cur.execute(
            """
            UPDATE event_outbox
            SET status = %s,
                published_at = %s
            WHERE event_id = %s
              AND id = %s
            """,
            ("COMPLETED", current_time_utc, row[1], row[0])
        )
        conn.commit()
        print(
            f"Message delivered to {msg.topic()} "
            f"[{msg.partition()}] at offset {msg.offset()}"
        )

def main_producer():
    conn = psycopg2.connect(**DB_CONFIG)
    producer = Producer(conf)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM event_outbox
                WHERE STATUS = 'PENDING'
                ORDER BY CREATED_AT;
            """)
            rows = cur.fetchall()

            for row in rows:
                print(row)
                event_type = row[2]
                key = str(row[4]).encode("utf-8")
                value = json.dumps(row[6]).encode("utf-8")
                headers = [
                    ("event_id", str(row[1]).encode("utf-8")),
                    ("event_type", str(row[2]).encode("utf-8")),
                    ("correlation_id", str(row[5]).encode("utf-8")),
                    ("aggregate_type", str(row[3]).encode("utf-8")),
                ]

                if event_type in purchase_order_events:
                    topic = "purchase-order-events"
                elif event_type in supplier_shipment_events:
                    topic = "supplier-shipment-events"
                elif event_type in inventory_events:
                    topic = "inventory-events"
                elif event_type in warehouse_task_events:
                    topic = "warehouse-task-events"
                elif event_type in order_events:
                    topic = "order-events"
                elif event_type in shipment_events:
                    topic = "shipment-events"
                else:
                    print(f"No topic mapping found for event: {event_type}")
                    continue

                producer.produce(
                    topic,
                    key=key,
                    value=value,
                    callback=lambda err, msg, row=row: onMsgSuccess(
                        err, msg, row, conn, cur
                    ),
                    headers=headers,
                )
                producer.poll(0)

            producer.flush()
    finally:
        conn.close()

with DAG(
     dag_id="kafka_producer",
     start_date=datetime(2021, 1, 1),
     # schedule="*/10 */4 * * *",
     max_active_runs=1,
     catchup=False
) as dag:

    main = PythonOperator(
        task_id="producer",
        python_callable=main_producer
    )

    trigger_consumer = TriggerDagRunOperator(
        task_id="trigger_kafka_consumer",
        trigger_dag_id="kafka_consumer",
    )

    main >> trigger_consumer