from services.po_service import POService
from services.shipment_service import ShipmentService
from services.event_service import EventService
from services.id_service import IDService



class ASNReceivedGenerator:



    def __init__(self):

        self.po_service = POService()

        self.shipment_service = ShipmentService()

        self.event_service = EventService()



    def generate(
        self,
        po_id
    ):


        # 1.
        # Validate PO


        po = (
            self.po_service
            .get_purchase_order(
                po_id
            )
        )



        if po["po_status"] != "ACKNOWLEDGED":

            raise Exception(
                f"""
                ASN cannot be created.

                PO status:
                {po["po_status"]}

                Required:
                ACKNOWLEDGED
                """
            )



        # 2.
        # Create shipment


        shipment_id = (

            self.shipment_service
            .create_shipment_from_po(
                po
            )

        )



        # 3.
        # Correlation


        correlation_id = (

            IDService
            .generate_correlation_id(
                "PO",
                po_id
            )

        )



        # 4.
        # Payload


        payload = {


            "po_id":
                po_id,


            "shipment_id":
                shipment_id,


            "supplier_id":
                po["supplier_id"],


            "warehouse_id":
                po["warehouse_id"],


            "shipment_status":
                "CREATED"

        }



        # 5.
        # Outbox event


        self.event_service.publish_event(

            event_type=
                "ASNReceived",


            aggregate_type=
                "SHIPMENT",


            aggregate_id=
                shipment_id,


            correlation_id=
                correlation_id,


            payload=
                payload

        )



        return {


            "event":
                "ASNReceived",


            "shipment_id":
                shipment_id,


            "po_id":
                po_id


        }