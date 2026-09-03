from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


# ============================================================
# DEFAULT SIMULATION START
# ============================================================

def _default_sim_start() -> datetime:
    return datetime(
        2026,
        1,
        1,
        0,
        0,
        0,
        tzinfo=timezone.utc
    )


# ============================================================
# REQUEST
# ============================================================

class SimulationRequest(BaseModel):

    simulation_start_timestamp: datetime = Field(
        default_factory=_default_sim_start,
        description=(
            "Simulation starting timestamp in UTC. "
            "Example: 2026-01-01T00:00:00Z"
        )
    )

    reset_before_run: bool = Field(
        default=False,
        description=(
            "Reset transactional tables before running "
            "the simulation flow."
        )
    )


# ============================================================
# RESPONSE
# ============================================================

class SimulationResponse(BaseModel):

    status: Literal[
        "SUCCESS",
        "FAILED"
    ]

    message: str

    simulation_start_timestamp: datetime

    return_code: int

    report_path: Optional[str] = None

    summary: Optional[
        dict[str, Any]
    ] = None

    stdout_tail: Optional[str] = None

    stderr_tail: Optional[str] = None