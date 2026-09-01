from fastapi import APIRouter

from api.schemas.simulator_schema import SimulationRequest, SimulationResponse
from api.services.simulator_service import run_full_flow

router = APIRouter(tags=["simulator"])


@router.get("/health")
def health():
    return {
        "service": "daemon-supply-chain-simulator",
        "status": "UP"
    }


@router.post("/simulate/full-flow", response_model=SimulationResponse)
def simulate_full_flow(request: SimulationRequest):
    return run_full_flow(request)