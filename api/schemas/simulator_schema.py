from datetime import datetime, timezone
from typing import Optional, Literal, Any

from pydantic import BaseModel, Field


def _default_sim_start() -> datetime:
    return datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


class SimulationRequest(BaseModel):
    simulation_start_timestamp: Optional[datetime] = Field(
        default_factory=_default_sim_start,
        description="Simulation base timestamp in UTC"
    )
    reset_before_run: bool = Field(
        default=False,
        description="Reset transactional tables before running the flow"
    )


class SimulationResponse(BaseModel):
    status: Literal["SUCCESS", "FAILED"]
    message: str
    simulation_start_timestamp: datetime
    return_code: int
    report_path: Optional[str] = None
    stdout_tail: Optional[str] = None
    stderr_tail: Optional[str] = None
    summary: Optional[dict[str, Any]] = None