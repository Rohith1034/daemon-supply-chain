from services.po_service import POService
from services.event_service import EventService
from services.id_service import IDService


class PurchaseOrderGenerator:
    """
    Generates PurchaseOrderCreated event.

    Responsibilities:
        1. Create PO using POService
        2. Create correlation id
        3. Publish event_outbox record
    """


    def __init__(self):

        self.po_service = POService()

        self.event_service = EventService()



    def generate(self):

        # ---------------------------------
        # 1. Create Purchase Order
        # ---------------------------------

        purchase_order = (
            self.po_service
            .create_purchase_order()
        )


        po_id = purchase_order["po_id"]



        # ---------------------------------
        # 2. Create correlation id
        # ---------------------------------

        correlation_id = (
            IDService
            .generate_correlation_id(
                "PO",
                po_id
            )
        )



        # ---------------------------------
        # 3. Create Event Payload
        # ---------------------------------

        payload = {


            "po_id":
                po_id,


            "supplier_id":
                purchase_order["supplier_id"],


            "warehouse_id":
                purchase_order["warehouse_id"],


            "status":
                purchase_order["status"],


            "total_items":
                purchase_order["total_items"],


            "total_quantity":
                purchase_order["total_quantity"],


            "total_amount":
                purchase_order["total_amount"],


            "currency":
                purchase_order["currency"],


            "items":
                purchase_order["items"]

        }



        # ---------------------------------
        # 4. Write Event Outbox
        # ---------------------------------

        self.event_service.publish_event(

            event_type=
                "PurchaseOrderCreated",


            aggregate_type=
                "PURCHASE_ORDER",


            aggregate_id=
                po_id,


            correlation_id=
                correlation_id,


            payload=
                payload

        )


        return {


            "po_id":
                po_id,


            "event":
                "PurchaseOrderCreated",


            "correlation_id":
                correlation_id

        }