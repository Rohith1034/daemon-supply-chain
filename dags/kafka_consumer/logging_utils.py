import json
import logging
from datetime import datetime, timezone


LOGGER = logging.getLogger("supply_chain.kafka_consumer")


def log_event(level, message, **fields):
    entry = {
        "message": message,
        "logged_at": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    getattr(LOGGER, level)(json.dumps(entry, default=str, sort_keys=True))
