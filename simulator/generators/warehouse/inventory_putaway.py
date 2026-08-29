from services.shipment_service import ShipmentService
from services.warehouse_service import WarehouseService
from services.event_service import EventService
from services.id_service import IDService



class InventoryPutawayGenerator:


    def __init__(self):

        self.shipment_service = ShipmentService()

        self.warehouse_service = WarehouseService()

        self.event_service = EventService()



    def generate(
        self,
        shipment_id,
        warehouse_id
    ):


        #
        # 1.
        # Get shipment items
        #

        items = (

            self.shipment_service
            .get_shipment_items(
                shipment_id
            )

        )


        if not items:

            return {

                "event":
                    "InventoryPutawayCreated",

                "tasks_created":
                    0

            }



        created_tasks = []



        #
        # 2.
        # Create putaway task
        #

        for item in items:


            location = (

                self.warehouse_service
                .get_available_location(

                    warehouse_id

                )

            )


            if not location:

                continue



            task_id = (

                self.warehouse_service
                .create_task(

                    task_type="PUTAWAY",

                    warehouse_id=warehouse_id,

                    shipment_id=shipment_id,

                    product_id=item["product_id"],

                    location=location["location_id"],

                    quantity=item["quantity"]

                )

            )


            created_tasks.append(

                {

                    "task_id": task_id,

                    "product_id":
                        item["product_id"],

                    "location":
                        location["location"]

                }

            )



        #
        # 3.
        # Correlation
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
        # Publish event
        #

        self.event_service.publish_event(

            event_type=
                "InventoryPutawayCreated",


            aggregate_type=
                "WAREHOUSE_TASK",


            aggregate_id=
                shipment_id,


            correlation_id=
                correlation_id,


            payload=

            {

                "shipment_id":
                    shipment_id,


                "warehouse_id":
                    warehouse_id,


                "task_type":
                    "PUTAWAY",


                "tasks_created":
                    len(created_tasks),


                "tasks":
                    created_tasks

            }

        )



        return {


            "event":
                "InventoryPutawayCreated",


            "shipment_id":
                shipment_id,


            "warehouse_id":
                warehouse_id,


            "tasks_created":
                len(created_tasks)

        }