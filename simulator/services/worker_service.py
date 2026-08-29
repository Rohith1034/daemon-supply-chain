from services.database_service import DatabaseService



class WorkerService:


    def get_available_worker(
        self,
        warehouse_id,
        task_type
    ):


        sql = """

        SELECT

            worker_id

        FROM workers

        WHERE warehouse_id=%s

        AND employment_status='ACTIVE'

        ORDER BY productivity_rating DESC

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


        return rows[0][0]



    def assign_task(
        self,
        task_id,
        worker_id
    ):


        sql = """

        UPDATE warehouse_tasks

        SET

            assigned_worker_id=%s,

            status='ASSIGNED'

        WHERE task_id=%s

        """


        DatabaseService.update(

            sql,

            (
                worker_id,
                task_id
            )

        )


        return True

    def record_productivity(
            self,
            worker_id,
            task_type,
            quantity,
            minutes
    ):
        productivity_score = round(

            (quantity / minutes) * 10,

            2

        )

        sql = """

        INSERT INTO worker_productivity

        (
        worker_id,
        task_type,
        units_processed,
        working_minutes,
        accuracy_score,
        productivity_score
        )


        VALUES

        (%s,%s,%s,%s,%s,%s)


        """

        DatabaseService.insert(

            sql,

            (
                worker_id,
                task_type,
                quantity,
                minutes,
                100,
                productivity_score
            )

        )