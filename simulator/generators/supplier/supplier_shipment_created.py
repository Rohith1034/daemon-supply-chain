from datetime import timedelta
import random


from core.db import Database

from core.ids import (
    next_shipment_id
)

from core.outbox import (
    publish_event
)

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "SupplierShipmentCreated"


def _get_shipment_base_time(po):
    """
    Shipment creation time must always happen after
    purchase order approval lifecycle.

    Priority:

    1. PO updated_at
       (approval timestamp)

    2. PO order_date

    3. Simulation clock

    This guarantees causal ordering.
    """

    simulation_now = get_simulation_now()

    candidates = [
        po.get("updated_at"),
        po.get("order_date"),
        simulation_now
    ]

    candidates = [
        value
        for value in candidates
        if value is not None
    ]

    return max(candidates)



def create_supplier_shipment(count=1):

    """
    Transportation Service

    Creates supplier shipment after
    PurchaseOrderApproved.

    Flow:

    PurchaseOrderApproved
            |
            |
            v
    SupplierShipmentCreated

    Does NOT handle:

    - ASN
    - Receiving
    - Inventory
    - Warehouse tasks
    """


    if count is None or count < 1:
        return []


    created_shipments = []


    for _ in range(count):

        with Database() as db:


            # ------------------------------------------------
            # Find approved PO without shipment
            # ------------------------------------------------

            po = db.fetch_one(
                """
                SELECT
                    po.*
                FROM purchase_orders po

                LEFT JOIN shipments s
                    ON po.po_id = s.po_id

                WHERE po.po_status='APPROVED'
                  AND s.shipment_id IS NULL

                ORDER BY po.created_at

                LIMIT 1
                """
            )


            if not po:

                raise Exception(
                    "No approved purchase order "
                    "available for shipment creation"
                )


            po_id = po["po_id"]


            # ------------------------------------------------
            # Correlation inheritance
            #
            # IMPORTANT:
            #
            # Never generate UUID here.
            #
            # PO is the root business transaction.
            # ------------------------------------------------

            correlation_id = (
                str(
                    po["correlation_id"]
                )
            )


            # ------------------------------------------------
            # Generate shipment ID
            # ------------------------------------------------

            shipment_id = (
                next_shipment_id(db)
            )



            # ------------------------------------------------
            # Shipment timeline
            # ------------------------------------------------

            shipment_date = (
                _get_shipment_base_time(po)
            )


            shipment_date = (
                shipment_date +
                timedelta(
                    hours=random.randint(
                        2,
                        24
                    )
                )
            )


            expected_delivery = (
                shipment_date +
                timedelta(
                    days=random.randint(
                        2,
                        7
                    )
                )
            )



            # ------------------------------------------------
            # Fetch PO Items
            # ------------------------------------------------

            po_items = db.fetch_all(
                """
                SELECT
                    product_id,
                    ordered_quantity,
                    unit_cost

                FROM purchase_order_items

                WHERE po_id=%s
                """,
                (
                    po_id,
                )
            )


            if not po_items:

                raise Exception(
                    f"PO {po_id} has no items"
                )



            total_quantity = sum(
                item["ordered_quantity"]
                for item in po_items
            )


            total_skus = len(
                po_items
            )



            # ------------------------------------------------
            # Create Shipment
            # ------------------------------------------------

            db.execute(
                """
                INSERT INTO shipments
                (
                    shipment_id,
                    po_id,
                    supplier_id,
                    warehouse_id,

                    shipment_status,

                    shipment_date,
                    expected_delivery,

                    total_skus,
                    total_quantity,

                    correlation_id
                )

                VALUES
                (
                    %s,%s,%s,%s,
                    %s,
                    %s,%s,
                    %s,%s,
                    %s
                )
                """,
                (
                    shipment_id,

                    po_id,

                    po["supplier_id"],

                    po["warehouse_id"],


                    "CREATED",


                    shipment_date,

                    expected_delivery,


                    total_skus,

                    total_quantity,


                    correlation_id
                )
            )



            # ------------------------------------------------
            # Create Shipment Items
            # ------------------------------------------------

            shipment_payload_items = []


            for item in po_items:


                db.execute(
                    """
                    INSERT INTO shipment_items
                    (
                        shipment_id,

                        product_id,

                        shipped_quantity
                    )

                    VALUES
                    (
                        %s,%s,%s
                    )
                    """,
                    (
                        shipment_id,

                        item["product_id"],

                        item["ordered_quantity"]
                    )
                )


                shipment_payload_items.append(
                    {
                        "product_id":
                            item["product_id"],


                        "quantity":
                            item["ordered_quantity"]
                    }
                )



            # ------------------------------------------------
            # Event Payload
            # ------------------------------------------------

            payload = {

                "event_type":
                    EVENT_NAME,


                "shipment_id":
                    shipment_id,


                "po_id":
                    po_id,


                "supplier_id":
                    po["supplier_id"],


                "warehouse_id":
                    po["warehouse_id"],


                "shipment_status":
                    "CREATED",


                "shipment_date":
                    shipment_date.isoformat(),


                "expected_delivery":
                    expected_delivery.isoformat(),


                "total_skus":
                    total_skus,


                "total_quantity":
                    total_quantity,


                "items":
                    shipment_payload_items,


                "correlation_id":
                    correlation_id
            }



            # ------------------------------------------------
            # Outbox
            #
            # Kafka topic:
            #
            # transportation-events
            #
            # ------------------------------------------------

            publish_event(

                db=db,

                event_type=EVENT_NAME,

                aggregate_type="SHIPMENT",

                aggregate_id=shipment_id,

                correlation_id=correlation_id,

                payload=payload
            )



            # ------------------------------------------------
            # Logging
            # ------------------------------------------------

            log_event_success(
                EVENT_NAME,
                {

                    "shipment_id":
                        shipment_id,


                    "po_id":
                        po_id,


                    "supplier_id":
                        po["supplier_id"],


                    "warehouse_id":
                        po["warehouse_id"],


                    "shipment_status":
                        "CREATED",


                    "shipment_date":
                        shipment_date,


                    "expected_delivery":
                        expected_delivery,


                    "quantity":
                        total_quantity,


                    "correlation_id":
                        correlation_id

                }
            )



            created_shipments.append(
                {
                    "shipment_id":
                        shipment_id,

                    "po_id":
                        po_id
                }
            )


    return created_shipments




if __name__ == "__main__":


    try:

        result = create_supplier_shipment(
            count=1
        )

        print(result)


    except Exception as e:


        log_event_failure(
            EVENT_NAME,
            e
        )

        raise