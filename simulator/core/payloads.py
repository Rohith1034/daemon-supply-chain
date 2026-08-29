from datetime import datetime, timezone


def utc_timestamp():
    return datetime.now(
        timezone.utc
    ).isoformat()



# =====================================
# Purchase Order Created
# =====================================

def build_purchase_order_created_payload(
        po,
        supplier,
        warehouse,
        items
):
    """
    Event payload for PurchaseOrderCreated
    """


    return {

        "eventType": "PurchaseOrderCreated",

        "occurredAt": utc_timestamp(),


        "purchaseOrder": {

            "poId": po["po_id"],

            "supplierId": supplier["supplier_id"],

            "supplierName": supplier["supplier_name"],


            "warehouseId": warehouse["warehouse_id"],

            "warehouseName": warehouse["warehouse_name"],


            "status": po["po_status"],


            "orderDate":
                po["order_date"].isoformat()
                if hasattr(po["order_date"], "isoformat")
                else po["order_date"],

            "expectedDelivery":
                po["expected_delivery"].isoformat()
                if hasattr(po["expected_delivery"], "isoformat")
                else po["expected_delivery"],


            "totalItems": po["total_items"],

            "totalQuantity": po["total_quantity"],

            "totalAmount": float(
                po["total_amount"]
            ),

            "currency": po["currency"],


            "items": [

                {

                    "productId": item["product_id"],

                    "productName": item["name"],

                    "quantity": item["ordered_quantity"],

                    "unitCost": float(
                        item["unit_cost"]
                    ),

                    "totalCost": float(
                        item["total_cost"]
                    )

                }

                for item in items

            ]

        }

    }



# =====================================
# Purchase Order Approved
# =====================================

def build_purchase_order_approved_payload(
        po
):

    return {

        "eventType":
            "PurchaseOrderApproved",


        "occurredAt":
            utc_timestamp(),


        "purchaseOrder": {

            "poId":
                po["po_id"],


            "supplierId":
                po["supplier_id"],


            "warehouseId":
                po["warehouse_id"],


            "status":
                po["po_status"]

        }

    }



# =====================================
# Supplier Shipment Created
# =====================================

def build_supplier_shipment_created_payload(
        shipment,
        po,
        supplier,
        warehouse
):

    return {

        "eventType":
            "SupplierShipmentCreated",


        "occurredAt":
            utc_timestamp(),


        "shipment": {


            "shipmentId":
                shipment["shipment_id"],


            "poId":
                po["po_id"],


            "supplierId":
                supplier["supplier_id"],


            "warehouseId":
                warehouse["warehouse_id"],


            "status":
                shipment["shipment_status"],


            "expectedDelivery":
                shipment[
                    "expected_delivery"
                ].isoformat()

        }

    }



# =====================================
# Generic Inventory Event
# =====================================

def build_inventory_event_payload(
        event_type,
        inventory
):

    return {


        "eventType":
            event_type,


        "occurredAt":
            utc_timestamp(),


        "inventory": {


            "productId":
                inventory["product_id"],


            "warehouseId":
                inventory["warehouse_id"],


            "availableQuantity":
                inventory[
                    "available_quantity"
                ]

        }

    }

def build_inventory_putaway_payload(
        warehouse_id,
        items,
        correlation_id
):

    return {

        "event_type":
            "InventoryPutaway",

        "warehouse_id":
            warehouse_id,

        "items":
            items,

        "correlation_id":
            correlation_id
    }

def build_receiving_task_payload(
        task,
        shipment
):

    return {

        "eventType":
            "ReceivingTaskCreated",

        "occurredAt":
            utc_timestamp(),

        "task":{

            "taskId":
                task["task_id"],

            "taskType":
                task["task_type"],

            "shipmentId":
                shipment["shipment_id"],

            "warehouseId":
                shipment["warehouse_id"],

            "quantity":
                task["quantity"]

        }

    }


def build_receiving_task_created_payload(
        task,
        shipment
):

    return {

        "eventType":
            "ReceivingTaskCreated",

        "occurredAt":
            utc_timestamp(),

        "task":{

            "taskId":
                task["task_id"],

            "taskType":
                task["task_type"],

            "shipmentId":
                shipment["shipment_id"],

            "warehouseId":
                shipment["warehouse_id"],

            "productId":
                task["product_id"],

            "quantity":
                task["quantity"],

            "priority":
                task["priority"]

        }

    }


def build_worker_assigned_payload(
        task,
        worker
):

    return {


        "eventType":
            "WorkerAssigned",


        "occurredAt":
            utc_timestamp(),


        "task":

        {

            "taskId":
                task["task_id"],


            "taskType":
                task["task_type"],


            "warehouseId":
                task["warehouse_id"]

        },


        "worker":

        {

            "workerId":
                worker["worker_id"],


            "role":
                worker["role"]

        }

    }