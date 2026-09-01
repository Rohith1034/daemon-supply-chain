from datetime import timedelta
import random

from core.db import Database
from core.outbox import publish_event

from core.logger import (
    log_event_success,
    log_event_failure
)

from core.simulation_clock import (
    get_simulation_now
)


EVENT_NAME = "StockIncreased"


def _get_stock_increased_time(shipment):
    """
    StockIncreased represents the inventory transaction
    being recognized after GoodsReceived.

    Therefore its timestamp must never be earlier than
    the receiving/shipment completion timestamp.

    The shipment's updated_at is the primary anchor because
    GoodsReceived updates the shipment record. The simulation
    clock is used as a fallback/reference.
    """

    simulation_now = get_simulation_now()

    shipment_updated_at = shipment.get(
        "updated_at"
    )

    actual_delivery = shipment.get(
        "actual_delivery"
    )

    shipment_date = shipment.get(
        "shipment_date"
    )

    candidates = [
        candidate
        for candidate in [
            shipment_updated_at,
            actual_delivery,
            shipment_date,
            simulation_now
        ]
        if candidate is not None
    ]

    if not candidates:
        base_time = simulation_now
    else:
        base_time = max(candidates)

    # Stock recognition happens shortly after
    # the receiving process completes.
    return (
        base_time +
        timedelta(
            minutes=random.randint(
                1,
                15
            )
        )
    )


def generate_stock_increased():

    with Database() as db:

        # ------------------------------------
        # Find latest received shipment
        #
        # GoodsReceived must already have
        # completed before StockIncreased.
        # ------------------------------------

        shipment = db.fetch_one(
            """
            SELECT
                shipment_id,
                warehouse_id,
                correlation_id,
                shipment_date,
                actual_delivery,
                updated_at
            FROM shipments
            WHERE shipment_status='RECEIVED'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )

        if not shipment:
            raise Exception(
                "No RECEIVED shipment found"
            )

        shipment_id = shipment["shipment_id"]

        warehouse_id = shipment["warehouse_id"]

        correlation_id = str(
            shipment["correlation_id"]
        )

        stock_increased_at = (
            _get_stock_increased_time(
                shipment
            )
        )

        # ------------------------------------
        # Calculate received quantity
        # ------------------------------------

        result = db.fetch_one(
            """
            SELECT
                SUM(quantity) AS total_received
            FROM inventory_transactions
            WHERE shipment_id=%s
              AND transaction_type='STOCK_RECEIVED'
            """,
            (
                shipment_id,
            )
        )

        total_received = result["total_received"]

        if not total_received:
            raise Exception(
                "No STOCK_RECEIVED transactions found"
            )

        # ------------------------------------
        # IMPORTANT:
        #
        # Do NOT change inventory status here.
        #
        # GoodsReceived leaves inventory in:
        #
        #     RECEIVED
        #
        # InventoryPutaway will later change it to:
        #
        #     AVAILABLE
        # ------------------------------------

        # ------------------------------------
        # Payload
        # ------------------------------------

        payload = {
            "eventType":
                EVENT_NAME,

            "occurredAt":
                stock_increased_at.isoformat(),

            "inventory":
            {
                "shipmentId":
                    shipment_id,

                "warehouseId":
                    warehouse_id,

                "increasedQuantity":
                    total_received,

                "status":
                    "RECEIVED"
            },

            "correlationId":
                correlation_id
        }

        # ------------------------------------
        # Outbox
        # ------------------------------------

        publish_event(
            db=db,
            event_type=EVENT_NAME,
            aggregate_type="INVENTORY",
            aggregate_id=shipment_id,
            correlation_id=correlation_id,
            payload=payload
        )

        # ------------------------------------
        # Logging
        # ------------------------------------

        log_event_success(
            EVENT_NAME,
            {
                "shipment_id":
                    shipment_id,

                "warehouse_id":
                    warehouse_id,

                "quantity":
                    total_received,

                "stock_increased_at":
                    stock_increased_at,

                "inventory_status":
                    "RECEIVED",

                "correlation_id":
                    correlation_id
            }
        )


if __name__ == "__main__":

    try:

        generate_stock_increased()

    except Exception as e:

        log_event_failure(
            EVENT_NAME,
            e
        )

        raise
