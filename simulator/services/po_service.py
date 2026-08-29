from datetime import datetime, timedelta
import random

from DB import db
from services.id_service import IDService


class POService:


    def create_purchase_order(self):

        """
        Creates:
            purchase_orders
            purchase_order_items

        Returns:
            created PO details
        """


        # 1. Pick supplier

        supplier = self._get_supplier()

        if not supplier:
            raise Exception(
                "No active supplier found"
            )


        supplier_id = supplier["supplier_id"]



        # 2. Pick warehouse

        warehouse = self._get_warehouse()

        if not warehouse:
            raise Exception(
                "No active warehouse found"
            )


        warehouse_id = warehouse["warehouse_id"]



        # 3. Get supplier products

        products = self._get_supplier_products(
            supplier_id
        )


        if len(products) == 0:
            raise Exception(
                f"No products found for supplier {supplier_id}"
            )


        # choose products

        selected_products = random.sample(
            products,
            min(
                len(products),
                random.randint(2,5)
            )
        )



        # 4. Generate PO ID

        sequence = random.randint(
            1,
            999999
        )


        po_id = IDService.generate_po_id(
            sequence
        )


        order_date = datetime.utcnow()


        expected_delivery = (
            order_date
            +
            timedelta(
                days=random.randint(3,10)
            )
        )



        total_quantity = 0
        total_amount = 0



        items = []


        for product in selected_products:


            quantity = random.randint(
                10,
                100
            )


            unit_cost = float(
                product["cost_price"]
            )


            total_cost = (
                quantity *
                unit_cost
            )


            total_quantity += quantity

            total_amount += total_cost



            items.append(
                {
                    "product_id":
                        product["product_id"],

                    "ordered_quantity":
                        quantity,

                    "unit_cost":
                        unit_cost,

                    "total_cost":
                        total_cost
                }
            )



        # 5. Insert PO header

        self._insert_purchase_order(

            po_id,

            supplier_id,

            warehouse_id,

            order_date,

            expected_delivery,

            len(items),

            total_quantity,

            total_amount
        )



        # 6. Insert PO items


        self._insert_purchase_order_items(
            po_id,
            items
        )



        return {

            "po_id": po_id,

            "supplier_id":
                supplier_id,

            "warehouse_id":
                warehouse_id,

            "status":
                "CREATED",

            "total_items":
                len(items),

            "total_quantity":
                total_quantity,

            "total_amount":
                round(
                    total_amount,
                    2
                ),

            "currency":
                "USD",

            "items":
                items
        }




    # ---------------------------
    # Database helpers
    # ---------------------------


    def _get_supplier(self):

        sql = """
        SELECT *
        FROM suppliers
        WHERE status='ACTIVE'
        ORDER BY RANDOM()
        LIMIT 1
        """

        return db.fetch_one(sql)



    def _get_warehouse(self):

        sql = """
        SELECT *
        FROM warehouses
        WHERE status='ACTIVE'
        ORDER BY RANDOM()
        LIMIT 1
        """

        return db.fetch_one(sql)



    def _get_supplier_products(
        self,
        supplier_id
    ):


        sql = """

        SELECT *
        FROM products

        WHERE supplier_id=%s
        AND status='ACTIVE'

        """

        return db.fetch_all(
            sql,
            (
                supplier_id,
            )
        )




    def _insert_purchase_order(

        self,

        po_id,

        supplier_id,

        warehouse_id,

        order_date,

        expected_delivery,

        total_items,

        total_quantity,

        total_amount

    ):


        sql = """

        INSERT INTO purchase_orders
        (
            po_id,
            supplier_id,
            warehouse_id,
            po_status,
            order_date,
            expected_delivery,
            total_items,
            total_quantity,
            total_amount,
            currency
        )

        VALUES

        (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )

        """



        db.insert(

            sql,

            (

                po_id,

                supplier_id,

                warehouse_id,

                "CREATED",

                order_date,

                expected_delivery,

                total_items,

                total_quantity,

                total_amount,

                "USD"

            )

        )




    def _insert_purchase_order_items(

        self,

        po_id,

        items

    ):


        sql = """

        INSERT INTO purchase_order_items
        (
            po_id,
            product_id,
            ordered_quantity,
            unit_cost,
            total_cost
        )

        VALUES
        (
            %s,%s,%s,%s,%s
        )

        """



        for item in items:


            db.insert(

                sql,

                (

                    po_id,

                    item["product_id"],

                    item["ordered_quantity"],

                    item["unit_cost"],

                    item["total_cost"]

                )

            )

    def approve_purchase_order(self, po_id):
        """
        Approves an existing purchase order.

        CREATED
           |
           v
        APPROVED
        """

        # 1. Fetch PO

        sql = """
        SELECT *
        FROM purchase_orders
        WHERE po_id=%s
        """

        po = db.fetch_one(
            sql,
            (po_id,)
        )

        if not po:
            raise Exception(
                f"Purchase order {po_id} not found"
            )

        # 2. Validate status

        if po["po_status"] != "CREATED":
            raise Exception(
                f"""
                Cannot approve PO.
                Current status:
                {po['po_status']}
                """
            )

        # 3. Update status

        update_sql = """

        UPDATE purchase_orders

        SET
            po_status=%s,
            updated_at=NOW()

        WHERE
            po_id=%s

        """

        db.update(

            update_sql,

            (
                "APPROVED",
                po_id
            )

        )

        return {

            "po_id":
                po_id,

            "old_status":
                "CREATED",

            "new_status":
                "APPROVED",

            "supplier_id":
                po["supplier_id"],

            "warehouse_id":
                po["warehouse_id"]

        }

    def acknowledge_purchase_order(self, po_id):

        """
        APPROVED
            |
            v
        ACKNOWLEDGED
        """

        sql = """
        SELECT *
        FROM purchase_orders
        WHERE po_id=%s
        """

        po = db.fetch_one(
            sql,
            (
                po_id,
            )
        )

        if not po:
            raise Exception(
                f"PO {po_id} not found"
            )

        if po["po_status"] != "APPROVED":
            raise Exception(
                f"""
                Invalid PO status transition

                Current:
                {po["po_status"]}

                Expected:
                APPROVED
                """
            )

        update_sql = """

        UPDATE purchase_orders

        SET

            po_status=%s,

            updated_at=NOW()


        WHERE

            po_id=%s

        """

        db.update(

            update_sql,

            (

                "ACKNOWLEDGED",

                po_id

            )

        )

        return {

            "po_id":
                po_id,

            "supplier_id":
                po["supplier_id"],

            "warehouse_id":
                po["warehouse_id"],

            "previous_status":
                "APPROVED",

            "new_status":
                "ACKNOWLEDGED"

        }