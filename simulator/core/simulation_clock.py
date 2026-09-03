from datetime import datetime, timezone
import os


SIMULATION_NOW_ENV = "SIMULATION_NOW"


def _parse_datetime(value: str) -> datetime:
    value = value.strip()

    # Support trailing Z
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(
            tzinfo=timezone.utc
        )
    else:
        dt = dt.astimezone(
            timezone.utc
        )

    return dt


def get_simulation_now() -> datetime:
    """
    Returns the simulation time.

    Priority:
    1. SIMULATION_NOW environment variable
    2. Real current UTC time
    """

    raw = os.getenv(
        SIMULATION_NOW_ENV
    )

    if raw:
        try:
            return _parse_datetime(
                raw
            )
        except Exception as exc:
            raise ValueError(
                f"Invalid {SIMULATION_NOW_ENV} value: {raw}"
            ) from exc

    return datetime.now(
        timezone.utc
    )