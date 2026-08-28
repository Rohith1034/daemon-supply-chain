import uuid
from datetime import datetime, timezone


def generate_event_id():

    return str(uuid.uuid4())



def generate_correlation_id(
        aggregate_id,
        prefix=None
):

    """
    Creates business transaction correlation id.

    Examples:

    PO-000001
    -> CORR-PO-000001

    ORD-000001
    -> CORR-ORD-000001

    SHIP-000001
    -> CORR-SHIP-000001

    """

    if prefix:

        return f"CORR-{prefix}-{aggregate_id}"


    return f"CORR-{aggregate_id}"



def current_timestamp():

    return datetime.now(
        timezone.utc
    ).isoformat()