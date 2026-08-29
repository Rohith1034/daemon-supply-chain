from datetime import datetime


from services.database_service import DatabaseService



class ShipmentService:


    def create_shipment_from_po(
        self,
        po
    ):


        shipment_id = (
            f"SHIP-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        )


        sql = """

        INSERT INTO shipments
        (
            shipment_id,
            po_id,
            supplier_id,
            warehouse_id,
            shipment_status,
            shipment_date,
            expected_delivery,
            total_skus,
            total_quantity
        )

        VALUES

        (
            %s,%s,%s,%s,%s,%s,%s,%s,%s
        )

        """



        DatabaseService.insert(

            sql,

            (

                shipment_id,

                po["po_id"],

                po["supplier_id"],

                po["warehouse_id"],

                "CREATED",

                datetime.utcnow(),

                po["expected_delivery"],

                po["total_items"],

                po["total_quantity"]

            )

        )


        return shipment_id

    def add_shipment_items(
            self,
            shipment_id,
            items
    ):
        sql = """

        INSERT INTO shipment_items
        (
            shipment_id,
            product_id,
            shipped_quantity,
            received_quantity,
            damaged_quantity
        )

        VALUES
        (
            %s,%s,%s,%s,%s
        )

        """

        for item in items:
            DatabaseService.insert(

                sql,

                (

                    shipment_id,

                    item["product_id"],

                    item["quantity"],

                    0,

                    0

                )

            )

        return len(items)

    def update_status(
            self,
            shipment_id,
            status
    ):
        sql = """

        UPDATE shipments

        SET

        shipment_status=%s,

        updated_at=NOW()

        WHERE shipment_id=%s

        """

        DatabaseService.update(

            sql,

            (

                status,

                shipment_id

            )

        )

    def mark_delivered(self, shipment_id):
        sql = """
        UPDATE shipments
        SET
            actual_delivery = NOW(),
            updated_at = NOW()
        WHERE shipment_id=%s
        """

        DatabaseService.update(
            sql,
            (
                shipment_id,
            )
        )

    def receive_items(
            self,
            shipment_id,
            items
    ):

        count = 0

        for item in items:
            sql = """

            UPDATE shipment_items

            SET

            received_quantity=%s,

            damaged_quantity=%s

            WHERE shipment_id=%s

            AND product_id=%s

            """

            DatabaseService.update(

                sql,

                (
                    item["received_quantity"],
                    item["damaged_quantity"],
                    shipment_id,
                    item["product_id"]
                )

            )

            count += 1

        return count

    def get_shipment_items(
            self,
            shipment_id
    ):

        sql = """

        SELECT

            product_id,

            shipped_quantity

        FROM shipment_items

        WHERE shipment_id=%s

        """

        rows = DatabaseService.fetch_all(

            sql,

            (
                shipment_id,
            )

        )

        return [

            {
                "product_id": row[0],

                "quantity": row[1]

            }

            for row in rows

        ]

    def get_shipment_items(
            self,
            shipment_id
    ):

        sql = """

        SELECT

            product_id,

            shipped_quantity


        FROM shipment_items


        WHERE shipment_id=%s

        """

        rows = DatabaseService.fetch_all(

            sql,

            (
                shipment_id,
            )

        )

        return [

            {
                "product_id": row[0],
                "quantity": row[1]
            }

            for row in rows

        ]