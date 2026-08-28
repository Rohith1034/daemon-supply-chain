from datetime import datetime, timezone
import uuid

from psycopg2.extras import Json


def generate_po_id(cursor):

    return f"PO-{uuid.uuid4().hex[:8].upper()}"



def generate_event_id():

    return str(uuid.uuid4())



def generate_correlation_id(po_id):

    return f"CORR-{po_id}"