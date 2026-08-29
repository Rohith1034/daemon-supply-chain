from datetime import datetime
import uuid


class IDService:
    """
    Central service for generating
    business IDs and correlation IDs.
    """


    @staticmethod
    def generate_event_id():
        """
        Used for event_outbox.event_id
        """

        return str(uuid.uuid4())


    @staticmethod
    def generate_po_id(sequence: int):
        """
        Purchase Order ID

        Example:
        PO-20260829-000001
        """

        date = datetime.utcnow().strftime("%Y%m%d")

        return f"PO-{date}-{sequence:06d}"


    @staticmethod
    def generate_shipment_id(sequence: int):

        date = datetime.utcnow().strftime("%Y%m%d")

        return f"SHIP-{date}-{sequence:06d}"


    @staticmethod
    def generate_task_id(sequence: int):

        date = datetime.utcnow().strftime("%Y%m%d")

        return f"TASK-{date}-{sequence:06d}"


    @staticmethod
    def generate_inventory_snapshot_id(sequence: int):

        date = datetime.utcnow().strftime("%Y%m%d")

        return f"SNAP-{date}-{sequence:06d}"


    @staticmethod
    def generate_worker_assignment_id(sequence: int):

        date = datetime.utcnow().strftime("%Y%m%d")

        return f"ASSIGN-{date}-{sequence:06d}"


    @staticmethod
    def generate_correlation_id(entity_type, entity_id):

        return f"CORR-{entity_type}-{entity_id}"


    @staticmethod
    def current_timestamp():

        return datetime.utcnow()