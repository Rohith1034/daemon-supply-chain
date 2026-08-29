from services.database_service import DatabaseService



class InventoryService:


    def receive_inventory(
        self,
        product_id,
        warehouse_id,
        quantity
    ):


        existing = self.get_inventory(

            product_id,

            warehouse_id

        )


        if existing:


            sql = """

            UPDATE inventory

            SET

            on_hand_quantity =
                on_hand_quantity + %s,


            available_quantity =
                (
                    on_hand_quantity + %s
                )
                -
                reserved_quantity
                -
                damaged_quantity,


            last_updated_at = NOW()


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


        else:


            sql = """

            INSERT INTO inventory

            (
                product_id,
                warehouse_id,
                on_hand_quantity,
                reserved_quantity,
                damaged_quantity,
                available_quantity
            )


            VALUES

            (
                %s,%s,%s,0,0,%s
            )

            """


            DatabaseService.insert(

                sql,

                (

                    product_id,

                    warehouse_id,

                    quantity,

                    quantity

                )

            )

    def get_inventory(
            self,
            product_id,
            warehouse_id
    ):

        sql = """

        SELECT

        inventory_id,
        on_hand_quantity,
        reserved_quantity,
        damaged_quantity,
        available_quantity


        FROM inventory


        WHERE product_id=%s

        AND warehouse_id=%s

        """

        rows = DatabaseService.fetch_all(

            sql,

            (
                product_id,

                warehouse_id

            )

        )

        if not rows:
            return None

        row = rows[0]

        return {

            "inventory_id": row[0],

            "on_hand_quantity": row[1],

            "reserved_quantity": row[2],

            "damaged_quantity": row[3],

            "available_quantity": row[4]

        }

    def create_snapshot(
            self,
            product_id,
            warehouse_id
    ):

        inventory = self.get_inventory(

            product_id,

            warehouse_id

        )

        if not inventory:
            return

        sql = """

        INSERT INTO inventory_snapshots

        (
            product_id,
            warehouse_id,
            on_hand_quantity,
            reserved_quantity,
            damaged_quantity,
            available_quantity
        )


        VALUES

        (
            %s,%s,%s,%s,%s,%s
        )

        """

        DatabaseService.insert(

            sql,

            (

                product_id,

                warehouse_id,

                inventory["on_hand_quantity"],

                inventory["reserved_quantity"],

                inventory["damaged_quantity"],

                inventory["available_quantity"]

            )

        )


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
           (
               on_hand_quantity + %s
           )
           -
           reserved_quantity
           -
           damaged_quantity,


       last_updated_at = NOW()


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

    return True