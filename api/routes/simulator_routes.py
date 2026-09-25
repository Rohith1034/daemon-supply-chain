from fastapi import APIRouter, HTTPException

from api.schemas.simulator_schema import (
    SimulationRequest,
    SimulationResponse
)

from api.services.simulator_service import (
    run_full_flow,
    get_latest_report
)
from api.services.simulation_control import simulation_manager


router = APIRouter(
    tags=["simulator"]
)


@router.post("/simulation/start")
def start_simulation(request: SimulationRequest | None = None):
    simulation_time = request.simulation_start_timestamp if request else None
    duration_hours = request.duration_hours if request else None
    run_window = request.run_window if request else "morning"
    return simulation_manager.start(simulation_time, duration_hours=duration_hours, run_window=run_window)


@router.get("/simulation/{simulation_id}/status")
def simulation_status(simulation_id: str):
    status = simulation_manager.status(simulation_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Simulation not found")
    return status


@router.post("/simulation/{simulation_id}/pause")
def pause_simulation(simulation_id: str):
    status = simulation_manager.pause(simulation_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Simulation not found")
    return status


@router.post("/simulation/{simulation_id}/resume")
def resume_simulation(simulation_id: str):
    status = simulation_manager.resume(simulation_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Simulation not found")
    return status


@router.get("/simulation/report")
def simulation_report():
    return {"status": "SUCCESS", "report": simulation_manager.report()}


# ============================================================
# HEALTH
# ============================================================

@router.get("/health")
def health():
    return {
        "service": "daemon-supply-chain-simulator",
        "status": "UP"
    }


# ============================================================
# RUN FULL SIMULATION
# ============================================================

@router.post(
    "/simulate/full-flow",
    response_model=SimulationResponse
)
def simulate_full_flow(
    request: SimulationRequest
):

    return run_full_flow(
        request
    )


# ============================================================
# GET LAST REPORT
# ============================================================

@router.get(
    "/simulate/report"
)
def simulate_report():

    report = get_latest_report()

    if report is None:
        raise HTTPException(
            status_code=404,
            detail="No simulation report found"
        )

    return {
        "status": "SUCCESS",
        "report": report
    }