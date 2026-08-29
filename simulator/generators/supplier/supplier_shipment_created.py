from services.shipment_service import ShipmentService
from services.event_service import EventService
from services.id_service import IDService



class SupplierShipmentCreatedGenerator:


    def __init__(self):

        self.shipment_service = ShipmentService()

        self.event_service = EventService()



    def generate(
        self,
        shipment_id,
        items
    ):


        #
        # 1.
        # Add shipment products
        #

        count = (

            self.shipment_service
            .add_shipment_items(

                shipment_id,

                items

            )

        )


        #
        # 2.
        # Change status
        #

        self.shipment_service.update_status(

            shipment_id,

            "IN_TRANSIT"

        )


        #
        # 3.
        # Correlation

        correlation_id = (

            IDService
            .generate_correlation_id(

                "SHIP",

                shipment_id

            )

        )


        #
        # 4.
        # Event Payload

        payload = {


            "shipment_id":

                shipment_id,


            "shipment_status":

                "IN_TRANSIT",


            "items_count":

                count


        }



        #
        # 5.
        # Outbox

        self.event_service.publish_event(

            event_type=
                "SupplierShipmentCreated",


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

                "SupplierShipmentCreated",


            "shipment_id":

                shipment_id

        }