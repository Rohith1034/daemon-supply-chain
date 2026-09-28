from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import json
import subprocess
from pathlib import Path
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from api.main import app
from api.routes import simulator_routes
from api.services import simulation_control
from api.schemas.simulator_schema import (
    IndependentSimulationRequest,
    IndependentSimulationResponse,
)
from core.checkpointing import SimulationCheckpoint
from api.services.independent_simulation_service import IndependentSimulationService
from api.services import independent_simulation_service as service_module
from simulator.loading_scripts import domain_flow_runner


SIMULATION_TIME = datetime(2026, 9, 28, 6, 0, tzinfo=timezone.utc)


def _result(context):
    return {
        "domain": context.flow,
        "correlation_id": str(context.correlation_id),
        "events": [{"event": f"{context.flow}-root", "status": "SUCCESS"}],
        "order_id": None,
        "shipment_id": None,
    }


def _service(tmp_path, runner=_result, root_exists=lambda _context: False, validator=lambda _context, _result: []):
    return IndependentSimulationService(
        run_store=tmp_path,
        runner=runner,
        lock_factory=nullcontext,
        root_exists=root_exists,
        validator=validator,
    )


def test_independent_routes_dispatch_to_their_flow(monkeypatch):
    calls = []

    class FakeService:
        def run(self, flow, request):
            calls.append((flow, request))
            return IndependentSimulationResponse(
                status="SUCCESS",
                simulation_id=request.simulation_id or "generated-simulation",
                correlation_id=request.correlation_id or uuid4(),
                flow=flow,
                simulation_timestamp=request.simulation_timestamp or SIMULATION_TIME,
                message="completed",
            )

    monkeypatch.setattr(simulator_routes, "independent_simulation_service", FakeService())
    client = TestClient(app)
    for flow in ("inbound", "outbound", "transportation"):
        response = client.post(
            f"/simulation/{flow}",
            json={"simulation_timestamp": SIMULATION_TIME.isoformat()},
        )
        assert response.status_code == 200
        assert response.json()["flow"] == flow
    assert [flow for flow, _request in calls] == ["inbound", "outbound", "transportation"]


def test_independent_routes_reject_non_uuid_correlation():
    response = TestClient(app).post(
        "/simulation/inbound",
        json={"correlation_id": "TEST-CORR-001"},
    )
    assert response.status_code == 422


def test_omitted_correlations_are_fresh_and_timestamp_is_propagated(tmp_path):
    contexts = []

    def runner(context):
        contexts.append(context)
        return _result(context)

    service = _service(tmp_path, runner=runner)
    first = service.run("inbound", IndependentSimulationRequest(simulation_timestamp=SIMULATION_TIME))
    second = service.run("inbound", IndependentSimulationRequest(simulation_timestamp=SIMULATION_TIME))

    assert first.status == second.status == "SUCCESS"
    assert first.correlation_id != second.correlation_id
    assert first.simulation_id != second.simulation_id
    assert all(context.simulation_timestamp == SIMULATION_TIME for context in contexts)


def test_explicit_correlation_is_propagated_and_completed_retry_is_replayed(tmp_path):
    correlation_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    simulation_id = "simulation-explicit-1"
    contexts = []

    def runner(context):
        contexts.append(context)
        return _result(context)

    service = _service(tmp_path, runner=runner)
    request = IndependentSimulationRequest(
        simulation_id=simulation_id,
        correlation_id=correlation_id,
        simulation_timestamp=SIMULATION_TIME,
    )
    first = service.run("outbound", request)
    retry = service.run("outbound", request)

    assert first.status == retry.status == "SUCCESS"
    assert first.correlation_id == retry.correlation_id == correlation_id
    assert retry.replayed is True
    assert len(contexts) == 1
    assert first.report_path is not None
    assert Path(first.report_path).is_file()


def test_api_runner_passes_resolved_context_to_subprocess(monkeypatch):
    context = service_module.RunContext(
        simulation_id="simulation-env-test",
        correlation_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
        flow="outbound",
        simulation_timestamp=SIMULATION_TIME,
    )
    observed = {}

    def fake_run(command, *, cwd, env, capture_output, text):
        observed.update(command=command, cwd=cwd, env=env)
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps({"domain": "outbound", "correlation_id": str(context.correlation_id), "events": []}),
            "",
        )

    monkeypatch.setattr(service_module.subprocess, "run", fake_run)
    service_module._execute_runner(context)

    assert observed["env"]["SIMULATION_ID"] == context.simulation_id
    assert observed["env"]["SIMULATION_CORRELATION_ID"] == str(context.correlation_id)
    assert observed["env"]["SIMULATION_NOW"] == SIMULATION_TIME.isoformat()


def test_api_runner_extracts_json_after_progress_output():
    expected = {"domain": "transportation", "events": []}
    stdout = "Saved Vehicle: VEHICLE-1\n" + json.dumps(expected, indent=2)
    assert service_module._extract_runner_result(stdout, "transportation") == expected


def test_inbound_root_business_time_uses_po_order_date_not_event_emission_time():
    payload = {
        "occurredAt": "2026-09-28T02:52:28.277815+00:00",
        "purchaseOrder": {"orderDate": SIMULATION_TIME.isoformat()},
    }
    assert service_module._root_business_timestamp("inbound", payload) == SIMULATION_TIME.isoformat()


def test_outbound_runner_passes_correlation_to_order_root(monkeypatch):
    correlation_id = str(uuid4())
    observed = {}

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["env"] = kwargs["env"]
        return subprocess.CompletedProcess(command, 0, "root created", "")

    monkeypatch.setattr(domain_flow_runner.subprocess, "run", fake_run)
    monkeypatch.setattr(domain_flow_runner.flow, "update_context_after_static_event", lambda *_args: None)
    domain_flow_runner._execute(
        "OrderCreated",
        "generators/order/order_created.py",
        {"correlation_id": correlation_id},
        [correlation_id],
    )

    assert observed["command"][-1] == correlation_id
    assert observed["env"]["PYTHONPATH"].startswith(str(domain_flow_runner.SIMULATOR_ROOT))


def test_failed_run_with_existing_root_is_not_replayed(tmp_path):
    root_exists = [False]
    executions = []

    def runner(_context):
        executions.append("called")
        root_exists[0] = True
        raise RuntimeError("failed after root commit")

    service = _service(
        tmp_path,
        runner=runner,
        root_exists=lambda _context: root_exists[0],
    )
    request = IndependentSimulationRequest(
        simulation_id="simulation-partial-1",
        correlation_id=uuid4(),
        simulation_timestamp=SIMULATION_TIME,
    )
    first = service.run("inbound", request)
    retry = service.run("inbound", request)

    assert first.status == retry.status == "FAILED"
    assert first.retryable is False
    assert retry.replayed is True
    assert len(executions) == 1


def test_failed_post_run_validation_keeps_runner_result_artifact(tmp_path):
    root_exists = [False]

    def runner(context):
        result = _result(context)
        root_exists[0] = True
        return result

    service = _service(
        tmp_path,
        runner=runner,
        root_exists=lambda _context: root_exists[0],
        validator=lambda _context, _result: ["controlled validation failure"],
    )
    response = service.run(
        "outbound",
        IndependentSimulationRequest(correlation_id=uuid4(), simulation_timestamp=SIMULATION_TIME),
    )

    stored = json.loads(Path(response.report_path).read_text(encoding="utf-8"))
    assert response.status == "FAILED"
    assert stored["domain"] == "outbound"
    assert stored["events"][0]["status"] == "SUCCESS"


def test_retry_before_root_creation_can_run_again(tmp_path):
    attempts = []

    def runner(context):
        attempts.append(context)
        if len(attempts) == 1:
            raise RuntimeError("failed before root creation")
        return _result(context)

    service = _service(tmp_path, runner=runner)
    request = IndependentSimulationRequest(
        simulation_id="simulation-retryable-1",
        correlation_id=uuid4(),
        simulation_timestamp=SIMULATION_TIME,
    )
    failed = service.run("transportation", request)
    succeeded = service.run("transportation", request)

    assert failed.status == "FAILED" and failed.retryable is True
    assert succeeded.status == "SUCCESS"
    assert len(attempts) == 2
    assert attempts[0].correlation_id == attempts[1].correlation_id


def test_different_runs_get_separate_artifacts(tmp_path):
    service = _service(tmp_path)
    first = service.run(
        "inbound",
        IndependentSimulationRequest(simulation_id="simulation-a", correlation_id=uuid4()),
    )
    second = service.run(
        "outbound",
        IndependentSimulationRequest(simulation_id="simulation-b", correlation_id=uuid4()),
    )

    assert first.report_path != second.report_path
    assert (tmp_path / "_correlations").is_dir()
    assert first.correlation_id != second.correlation_id


def test_legacy_manager_scopes_checkpoint_report_and_queue_by_run(monkeypatch, tmp_path):
    event_rows = [
        {"event_id": "event-a", "correlation_id": "corr-a"},
        {"event_id": "event-b", "correlation_id": "corr-b"},
    ]

    class FakeQueue:
        def __init__(self, memory_mode=False):
            self.memory_mode = memory_mode

        @property
        def queue_size(self):
            return len(event_rows)

        def list_ready_events(self, *_args, **_kwargs):
            return event_rows

    class FakeThread:
        def __init__(self, target, args, daemon):
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self):
            pass

    monkeypatch.setattr(simulation_control, "RUN_STATE_ROOT", tmp_path)
    monkeypatch.setattr(simulation_control, "EventQueue", FakeQueue)
    monkeypatch.setattr(simulation_control.threading, "Thread", FakeThread)
    manager = simulation_control.SimulationManager()
    first = manager.start(correlation_id="corr-a")
    second = manager.start(correlation_id="corr-b")
    first_record = manager._runs[first["simulation_id"]]
    second_record = manager._runs[second["simulation_id"]]

    assert first_record["checkpoint_path"] != second_record["checkpoint_path"]
    assert first_record["report_path"] != second_record["report_path"]
    assert [row["correlation_id"] for row in first_record["queue"].list_ready_events()] == ["corr-a"]
    assert [row["correlation_id"] for row in second_record["queue"].list_ready_events()] == ["corr-b"]


def test_manager_restores_interrupted_run_from_its_checkpoint(monkeypatch, tmp_path):
    simulation_id = "SIM-RESTORE-1"
    run_directory = tmp_path / simulation_id
    checkpoint_path = run_directory / "checkpoint.json"
    checkpoint_time = SIMULATION_TIME + timedelta(minutes=30)
    SimulationCheckpoint(file_path=str(checkpoint_path)).write(checkpoint_time)
    report_path = run_directory / "report.json"
    report_path.write_text(json.dumps({
        "simulation_id": simulation_id,
        "status": "RUNNING",
        "events_processed": 2,
        "simulation_time": SIMULATION_TIME.isoformat(),
        "last_result": None,
        "error": None,
        "pause_requested": False,
        "initial_event_created": False,
        "seeded_events": [],
        "duration_hours": 1,
        "run_window": "morning",
        "correlation_id": None,
    }), encoding="utf-8")

    class FakeQueue:
        def __init__(self, memory_mode=False):
            self.memory_mode = memory_mode

        @property
        def queue_size(self):
            return 0

    monkeypatch.setattr(simulation_control, "RUN_STATE_ROOT", tmp_path)
    monkeypatch.setattr(simulation_control, "EventQueue", FakeQueue)
    manager = simulation_control.SimulationManager()
    status = manager.status(simulation_id)

    assert status["status"] == "PAUSED"
    assert status["current_simulation_time"] == checkpoint_time.isoformat()
    assert Path(status["report_path"]).is_file()
    assert manager._runs[simulation_id]["checkpoint_path"] == str(checkpoint_path)