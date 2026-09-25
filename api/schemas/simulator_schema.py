from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


# ============================================================
# REQUEST
# ============================================================

class SimulationRequest(BaseModel):

    simulation_start_timestamp: Optional[datetime] = Field(
        default=None,
        description=(
            "Simulation starting timestamp in UTC. "
            "When omitted, the current simulation clock is used."
        )
    )

    run_window: Literal["morning", "afternoon", "evening", "night"] = Field(
        default="morning",
        description=(
            "Contract window to execute. "
            "Each window only allows the matching business domain and event types."
        )
    )

    correlation_id: Optional[str] = Field(
        default=None,
        description=(
            "Logical execution identity. Reusing it retries or resumes the "
            "same business run; a new value starts an independent run."
        ),
    )

    duration_hours: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Optional simulation horizon in hours. "
            "When omitted, the scheduler continues until no due events remain."
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