from services.shipment_service import ShipmentService
from services.inventory_service import InventoryService
from services.event_service import EventService
from services.id_service import IDService



class InventoryReceivedGenerator:


    def __init__(self):

        self.shipment_service = ShipmentService()

        self.inventory_service = InventoryService()

        self.event_service = EventService()



    def generate(
        self,
        shipment_id,
        warehouse_id
    ):


        #
        # 1.
        # Get shipment products
        #

        items = (

            self.shipment_service
            .get_shipment_items(

                shipment_id

            )

        )


        total_quantity = 0

        received_items = 0



        #
        # 2.
        # Receive each product
        #

        for item in items:


            product_id = item["product_id"]

            quantity = item["quantity"]



            #
            # Increase inventory
            #

            self.inventory_service.receive_inventory(

                product_id,

                warehouse_id,

                quantity

            )


            #
            # Create inventory snapshot
            #

            self.inventory_service.create_snapshot(

                product_id,

                warehouse_id

            )


            total_quantity += quantity

            received_items += 1



        #
        # 3.
        # Update shipment items received quantity
        #

        self.shipment_service.receive_items(

            shipment_id,

            [

                {

                    "product_id": item["product_id"],

                    "received_quantity": item["quantity"],

                    "damaged_quantity":0

                }

                for item in items

            ]

        )



        #
        # 4.
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
        # 5.
        # Event payload
        #

        payload = {


            "shipment_id":

                shipment_id,


            "warehouse_id":

                warehouse_id,


            "items_received":

                received_items,


            "total_quantity":

                total_quantity,


            "receiving_status":

                "COMPLETED"

        }



        #
        # 6.
        # Publish event
        #

        self.event_service.publish_event(

            event_type=
                "InventoryReceived",


            aggregate_type=
                "INVENTORY",


            aggregate_id=
                shipment_id,


            correlation_id=
                correlation_id,


            payload=
                payload

        )


        return {


            "event":

                "InventoryReceived",


            "shipment_id":

                shipment_id,


            "items_received":

                received_items

        }