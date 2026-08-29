from services.shipment_service import ShipmentService
from services.event_service import EventService
from services.id_service import IDService


class SupplierShipmentDeliveredGenerator:


    def __init__(self):

        self.shipment_service = ShipmentService()

        self.event_service = EventService()



    def generate(
        self,
        shipment_id
    ):


        #
        # 1.
        # Update shipment status
        #

        self.shipment_service.update_status(

            shipment_id,

            "DELIVERED"

        )


        #
        # 2.
        # Update actual delivery timestamp
        #

        self.shipment_service.mark_delivered(

            shipment_id

        )


        #
        # 3.
        # Correlation ID
        #

        correlation_id = (

            IDService
            .generate_correlation_id(

                "SHIP",

                shipment_id

            )

        )


        #
        # 4.
        # Payload
        #

        payload = {


            "shipment_id":

                shipment_id,


            "shipment_status":

                "DELIVERED",


            "delivery_confirmed":

                True

        }



        #
        # 5.
        # Publish Event
        #

        self.event_service.publish_event(

            event_type=
                "SupplierShipmentDelivered",


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

                "SupplierShipmentDelivered",


            "shipment_id":

                shipment_id

        }