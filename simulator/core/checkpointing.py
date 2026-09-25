import json
import os
from datetime import datetime, timezone


class SimulationCheckpoint:
    """Persist the last fully-processed simulation timestamp."""

    def __init__(self, file_path=None):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        default_path = os.path.join(base_dir, "output", "last_simulation_checkpoint.json")
        self.file_path = file_path or default_path
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

    @staticmethod
    def _normalize_datetime(value):
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            value = str(value).strip()
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def read(self):
        if not os.path.exists(self.file_path):
            return None
        with open(self.file_path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
        if not raw:
            return None
        return self._normalize_datetime(raw.get("last_processed_simulation_time"))

    def write(self, value):
        normalized = self._normalize_datetime(value)
        payload = {"last_processed_simulation_time": normalized.isoformat()}
        with open(self.file_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        return normalized

    def advance_to(self, value):
        return self.write(value)
