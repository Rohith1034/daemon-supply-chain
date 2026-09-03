from fastapi import APIRouter, HTTPException

from api.schemas.simulator_schema import (
    SimulationRequest,
    SimulationResponse
)

from api.services.simulator_service import (
    run_full_flow,
    get_latest_report
)


router = APIRouter(
    tags=["simulator"]
)


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