from services.worker_service import WorkerService
from services.warehouse_service import WarehouseService
from services.event_service import EventService
from services.id_service import IDService



class WorkerAssignmentGenerator:


    def __init__(self):

        self.worker_service = WorkerService()

        self.event_service = EventService()



    def generate(
        self,
        task_id,
        warehouse_id
    ):


        #
        # 1.
        # Find worker
        #

        worker_id = (

            self.worker_service
            .get_available_worker(

                warehouse_id,

                "PUTAWAY"

            )

        )


        if not worker_id:

            return {

                "event":
                    "WorkerAssignmentCreated",

                "status":
                    "NO_WORKER_AVAILABLE"

            }



        #
        # 2.
        # Assign
        #

        self.worker_service.assign_task(

            task_id,

            worker_id

        )



        #
        # 3.
        # Correlation
        #

        correlation_id = (

            IDService
            .generate_correlation_id(

                "TASK",

                task_id

            )

        )



        #
        # 4.
        # Event
        #

        self.event_service.publish_event(

            event_type=
                "WorkerAssignmentCreated",


            aggregate_type=
                "WAREHOUSE_TASK",


            aggregate_id=
                task_id,


            correlation_id=
                correlation_id,


            payload={


                "task_id":
                    task_id,


                "worker_id":
                    worker_id,


                "warehouse_id":
                    warehouse_id,


                "status":
                    "ASSIGNED"


            }

        )



        return {


            "event":
                "WorkerAssignmentCreated",


            "task_id":
                task_id,


            "worker_id":
                worker_id

        }