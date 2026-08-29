from services.warehouse_service import WarehouseService
from services.inventory_service import InventoryService
from services.worker_service import WorkerService
from services.event_service import EventService
from services.id_service import IDService
import random


class TaskCompletedGenerator:


    def __init__(self):

        self.warehouse_service = WarehouseService()

        self.inventory_service = InventoryService()

        self.worker_service = WorkerService()

        self.event_service = EventService()



    def generate(
        self,
        task
    ):


        task_id = task["task_id"]

        worker_id = task["worker_id"]

        product_id = task["product_id"]

        warehouse_id = task["warehouse_id"]

        quantity = task["quantity"]



        #
        # Random execution time
        #

        actual_minutes = random.randint(
            10,
            60
        )



        #
        # 1.
        # Complete warehouse task
        #

        self.warehouse_service.complete_task(

            task_id,

            actual_minutes

        )



        #
        # 2.
        # Increase inventory
        #

        self.inventory_service.increase_inventory(

            product_id,

            warehouse_id,

            quantity

        )



        #
        # 3.
        # Worker productivity
        #

        self.worker_service.record_productivity(

            worker_id,

            "PUTAWAY",

            quantity,

            actual_minutes

        )



        #
        # 4.
        # Correlation ID
        #

        correlation_id = IDService.generate_correlation_id(

            "TASK",

            task_id

        )



        #
        # 5.
        # Publish event
        #

        self.event_service.publish_event(

            event_type="TaskCompleted",

            aggregate_type="WAREHOUSE_TASK",

            aggregate_id=task_id,

            correlation_id=correlation_id,


            payload={

                "task_id":task_id,

                "worker_id":worker_id,

                "product_id":product_id,

                "warehouse_id":warehouse_id,

                "quantity":quantity,

                "status":"COMPLETED"

            }

        )


        return {


            "event":
            "TaskCompleted",


            "task_id":
            task_id

        }