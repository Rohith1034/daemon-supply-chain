import os
from datetime import datetime, timezone


# =====================================================
# ENVIRONMENT VARIABLE
# =====================================================

SIMULATION_TIME_ENV = "SIMULATION_TIME"


# =====================================================
# GET SIMULATION TIME
# =====================================================

def get_simulation_now() -> datetime:
    """
    Return the current business/simulation timestamp.

    Priority:
        1. SIMULATION_TIME environment variable
        2. Actual current UTC time

    Expected environment variable format:

        2026-08-30T08:00:00+00:00

    A naive timestamp is also accepted and will be
    treated as UTC:

        2026-08-30T08:00:00
    """

    simulation_time = os.getenv(
        SIMULATION_TIME_ENV
    )

    # -------------------------------------------------
    # Simulation time explicitly provided
    # -------------------------------------------------

    if simulation_time:

        try:

            parsed_time = datetime.fromisoformat(
                simulation_time
            )

        except ValueError as exc:

            raise ValueError(
                f"Invalid {SIMULATION_TIME_ENV} value: "
                f"{simulation_time!r}. "
                "Expected ISO-8601 format, for example "
                "'2026-08-30T08:00:00+00:00'."
            ) from exc

        # -------------------------------------------------
        # If no timezone was supplied, treat as UTC.
        # This prevents mixing naive and timezone-aware
        # datetimes later in the generators.
        # -------------------------------------------------

        if parsed_time.tzinfo is None:

            parsed_time = parsed_time.replace(
                tzinfo=timezone.utc
            )

        # -------------------------------------------------
        # Normalize to UTC.
        # -------------------------------------------------

        return parsed_time.astimezone(
            timezone.utc
        )

    # -------------------------------------------------
    # Standalone generator execution
    #
    # When no simulation time is supplied, fall back
    # to the real current UTC time.
    # -------------------------------------------------

    return datetime.now(
        timezone.utc
    )
