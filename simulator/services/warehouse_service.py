from services.database_service import DatabaseService
from services.id_service import IDService
from datetime import datetime
import random



class WarehouseService:


    def get_available_location(
        self,
        warehouse_id
    ):

        """
        Find an empty/available warehouse bin
        """

        sql = """

        SELECT

            location_id,

            zone,
            aisle,
            rack,
            shelf,
            bin

        FROM warehouse_locations

        WHERE warehouse_id=%s

        AND status='ACTIVE'

        ORDER BY current_utilization ASC

        LIMIT 1

        """


        rows = DatabaseService.fetch_all(

            sql,

            (
                warehouse_id,
            )

        )


        if not rows:
            return None


        row = rows[0]


        location_string = (

            f"{row[1]}-"
            f"{row[2]}-"
            f"{row[3]}-"
            f"{row[4]}-"
            f"{row[5]}"

        )


        return {


            "location_id":
                row[0],


            "location":
                location_string


        }

    def create_task(
            self,
            task_type,
            warehouse_id,
            shipment_id,
            product_id,
            location,
            quantity
    ):
        task_id = IDService.generate_id(
            "TASK"
        )

        sql = """

        INSERT INTO warehouse_tasks

        (

            task_id,

            task_type,

            warehouse_id,

            shipment_id,

            product_id,

            location,

            quantity,

            priority,

            status,

            estimated_minutes,

            created_at

        )


        VALUES

        (

            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s

        )

        """

        DatabaseService.insert(

            sql,

            (

                task_id,

                task_type,

                warehouse_id,

                shipment_id,

                product_id,

                location,

                quantity,

                self.calculate_priority(quantity),

                "CREATED",

                self.calculate_duration(quantity),

                datetime.utcnow()

            )

        )

        return task_id

    def calculate_priority(
            self,
            quantity
    ):

        if quantity > 500:
            return "HIGH"


        elif quantity > 100:
            return "MEDIUM"


        else:
            return "LOW"

    def calculate_duration(
            self,
            quantity
    ):

        """
        Approximate WMS labor estimation
        """

        base = 10

        handling_time = quantity // 50

        return base + handling_time

    def start_task(
            self,
            task_id
    ):

        sql = """

        UPDATE warehouse_tasks

        SET

            status='IN_PROGRESS'

        WHERE task_id=%s

        """

        DatabaseService.update(

            sql,

            (
                task_id,
            )

        )

        return True

    def complete_task(
            self,
            task_id,
            actual_minutes
    ):

        sql = """

        UPDATE warehouse_tasks

        SET

            status='COMPLETED',

            actual_minutes=%s,

            completed_at=NOW()


        WHERE task_id=%s

        """

        DatabaseService.update(

            sql,

            (
                actual_minutes,
                task_id
            )

        )

        return True

    def increase_inventory(
            self,
            product_id,
            warehouse_id,
            quantity
    ):

        sql = """

        UPDATE inventory

        SET

            on_hand_quantity =
                on_hand_quantity + %s,


            available_quantity =
                available_quantity + %s,


            last_updated_at=NOW()


        WHERE product_id=%s

        AND warehouse_id=%s

        """

        DatabaseService.update(

            sql,

            (
                quantity,
                quantity,
                product_id,
                warehouse_id
            )

        )

    def complete_task(
            self,
            task_id,
            actual_minutes
    ):

        sql = """

        UPDATE warehouse_tasks

        SET

            status='COMPLETED',

            actual_minutes=%s,

            completed_at=NOW()


        WHERE task_id=%s

        """

        DatabaseService.update(

            sql,

            (
                actual_minutes,
                task_id
            )

        )

        return True
