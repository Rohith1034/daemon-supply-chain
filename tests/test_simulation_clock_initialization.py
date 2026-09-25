from datetime import datetime, timezone

from api.schemas.simulator_schema import SimulationRequest
from api.services.simulation_control import SimulationManager


def test_simulation_request_does_not_use_hardcoded_start_date():
    assert SimulationRequest().simulation_start_timestamp is None


def test_simulation_manager_uses_current_clock_when_start_is_omitted(monkeypatch):
    current = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("api.services.simulation_control.get_simulation_now", lambda: current)
    manager = SimulationManager()

    status = manager.start()

    assert status["current_simulation_time"] == current.isoformat()