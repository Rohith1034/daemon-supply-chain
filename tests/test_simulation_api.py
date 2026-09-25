from fastapi.testclient import TestClient
from pathlib import Path

from api.main import app
from api.routes import simulator_routes


class FakeSimulationManager:
    def start(self, simulation_time=None, duration_hours=None, **kwargs):
        return {
            "simulation_id": "SIM-TEST",
            "events_processed": 0,
            "queue_size": 1,
            "current_simulation_time": "2026-09-20T12:00:00+00:00",
            "status": "RUNNING",
            "error": None,
        }

    def status(self, simulation_id):
        return self.start() if simulation_id == "SIM-TEST" else None

    def pause(self, simulation_id):
        return {"simulation_id": simulation_id, "status": "PAUSED"}

    def resume(self, simulation_id):
        return {"simulation_id": simulation_id, "status": "RUNNING"}

    def report(self):
        return [self.start()]


def test_simulation_control_endpoints(monkeypatch):
    monkeypatch.setattr(simulator_routes, "simulation_manager", FakeSimulationManager())
    client = TestClient(app)

    started = client.post("/simulation/start", json={})
    assert started.status_code == 200
    assert started.json()["simulation_id"] == "SIM-TEST"

    status = client.get("/simulation/SIM-TEST/status")
    assert status.status_code == 200
    assert status.json()["status"] == "RUNNING"

    assert client.post("/simulation/SIM-TEST/pause").json()["status"] == "PAUSED"
    assert client.post("/simulation/SIM-TEST/resume").json()["status"] == "RUNNING"


def test_reset_transaction_tables_script_exists():
    script_path = (
        Path(__file__).resolve().parents[1]
        / "simulator"
        / "loading_scripts"
        / "reset_transaction_tables.py"
    )

    assert script_path.exists()