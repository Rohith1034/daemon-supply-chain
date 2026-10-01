from confluent_kafka import Producer
import psycopg2
import json
from datetime import datetime, timezone
from psycopg2.extras import RealDictCursor

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
    "bootstrap.servers":"pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "security.protocol":"SASL_SSL",
    "sasl.mechanisms":"PLAIN",
    "sasl.username":"XBLQVAYYNHY5KZX5",
    "sasl.password":"cfltq7uzeYUHBwT/dOAWUqL8MT7VRDMqdrF5/64khMxeSqec2aDfWQY77Psy8NlA",
    "client.id":"ccloud-python-client-998e3697-c32e-4805-9857-4381d6527253"
}

producer = Producer(conf)

conn = psycopg2.connect(**DB_CONFIG)

cur = conn.cursor()

cur.execute("""
    SELECT * FROM event_outbox WHERE STATUS = 'PENDING' ORDER BY CREATED_AT;
""")

rows = cur.fetchall()


def onMsgSuccess(err, msg, row):
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

for row in rows:
    event_type = row[2]
    key = str(row[4]).encode('utf-8')
    value = json.dumps(row[6]).encode('utf-8')
    headers = [
        ("event_id", str(row[1]).encode("utf-8")),
        ("event_type", str(row[2]).encode("utf-8")),
        ("correlation_id", str(row[5]).encode("utf-8")),
        ("aggregate_type", str(row[3]).encode("utf-8")),
    ]

    if event_type in purchase_order_events:
        producer.produce("purchase-order-events", key=key, value=value,
                         callback=lambda err, msg, row=row: onMsgSuccess(err, msg, row), headers=headers)

    elif event_type in supplier_shipment_events:
        producer.produce("supplier-shipment-events", key=key, value=value,
                         callback=lambda err, msg, row=row: onMsgSuccess(err, msg, row), headers=headers)

    elif event_type in inventory_events:
        producer.produce("inventory-events", key=key, value=value,
                         callback=lambda err, msg, row=row: onMsgSuccess(err, msg, row), headers=headers)

    elif event_type in warehouse_task_events:
        producer.produce("warehouse-task-events", key=key, value=value,
                         callback=lambda err, msg, row=row: onMsgSuccess(err, msg, row), headers=headers)

    elif event_type in order_events:
        producer.produce("order-events", key=key, value=value,
                         callback=lambda err, msg, row=row: onMsgSuccess(err, msg, row), headers=headers)

    elif event_type in shipment_events:
        producer.produce("shipment-events", key=key, value=value,
                         callback=lambda err, msg, row=row: onMsgSuccess(err, msg, row), headers=headers)

    else:
        print(f"No topic mapping found for event: {event_type}")

    producer.poll(0)

producer.flush()
