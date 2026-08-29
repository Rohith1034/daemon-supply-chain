from services.po_service import POService
from services.event_service import EventService
from services.id_service import IDService



class PurchaseOrderAcknowledgedGenerator:


    def __init__(self):

        self.po_service = POService()

        self.event_service = EventService()



    def generate(self, po_id):


        # 1.
        # Supplier acknowledges PO

        result = (

            self.po_service
            .acknowledge_purchase_order(
                po_id
            )

        )



        # 2.
        # Same correlation chain

        correlation_id = (

            IDService
            .generate_correlation_id(

                "PO",

                po_id

            )

        )



        # 3.
        # Event payload


        payload = {


            "po_id":
                po_id,


            "supplier_id":
                result["supplier_id"],


            "warehouse_id":
                result["warehouse_id"],


            "previous_status":
                result["previous_status"],


            "new_status":
                result["new_status"]

        }



        # 4.
        # Outbox


        self.event_service.publish_event(

            event_type=
                "PurchaseOrderAcknowledged",


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


            "event":
                "PurchaseOrderAcknowledged",


            "po_id":
                po_id,


            "correlation_id":
                correlation_id

        }