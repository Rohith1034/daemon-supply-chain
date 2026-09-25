import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.checkpointing import SimulationCheckpoint
from core.event_scheduler.event_executor import EventExecutor
from core.event_scheduler.event_registry import EVENT_REGISTRY
from core.event_scheduler.event_queue import EventQueue
from core.event_state_machine import validate_transition
from simulator.master_simulator import run_simulation_cycle


def test_event_registry_contains_core_supply_chain_events():
    assert "PurchaseOrderCreated" in EVENT_REGISTRY
    assert "PurchaseOrderApproved" in EVENT_REGISTRY
    assert "SupplierShipmentCreated" in EVENT_REGISTRY
    assert "GoodsReceived" in EVENT_REGISTRY


def test_event_queue_marks_scheduled_events_ready_only_when_due():
    queue = EventQueue(memory_mode=True)

    event_id = queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-1001",
        correlation_id="corr-1001",
        payload={"po_id": "PO-1001"},
        scheduled_time="2026-09-20T10:00:00+00:00",
    )

    assert event_id is not None
    assert queue.list_ready_events("2026-09-20T09:59:00+00:00") == []
    ready = queue.list_ready_events("2026-09-20T10:00:00+00:00")
    assert len(ready) == 1
    assert ready[0]["event_id"] == event_id


def test_duplicate_event_execution_is_ignored():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)

    event_id = queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-2001",
        correlation_id="corr-2001",
        payload={
            "po_id": "PO-2001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
        scheduled_time="2026-09-20T11:00:00+00:00",
        priority="HIGH",
    )
    ready = queue.list_ready_events("2026-09-20T11:00:00+00:00")[0]

    first = executor.execute(ready, registry={"PurchaseOrderApproved": lambda context: {"status": "SUCCESS", "created_events": []}})
    second = executor.execute(ready, registry={"PurchaseOrderApproved": lambda context: {"status": "SUCCESS", "created_events": []}})

    assert first["status"] == "COMPLETED"
    assert second["status"] == "SKIPPED"
    assert executor._execution_log[event_id]["status"] == "COMPLETED"


def test_failed_event_retries_and_then_fails_after_max_retries():
    queue = EventQueue(memory_mode=True)
    executor = EventExecutor(queue=queue, memory_mode=True)

    event_id = queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-3001",
        correlation_id="corr-3001",
        payload={
            "po_id": "PO-3001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
            "retry_count": 0,
            "max_retry_count": 1,
        },
        scheduled_time="2026-09-20T12:00:00+00:00",
        priority="HIGH",
        retry_count=0,
        max_retry_count=1,
    )

    def always_fail(context):
        raise RuntimeError("temporary failure")

    result = executor.execute(queue.list_ready_events("2026-09-20T12:00:00+00:00")[0], registry={"PurchaseOrderApproved": always_fail})
    assert result["status"] == "RETRY_SCHEDULED"
    assert queue._memory_events[0]["status"] == "RETRY_SCHEDULED"

    result_2 = executor.execute(queue.list_ready_events("2026-09-20T12:20:00+00:00")[0], registry={"PurchaseOrderApproved": always_fail})
    assert result_2["status"] == "FAILED"


def test_invalid_state_transition_is_rejected():
    assert validate_transition("CREATED", "DELIVERED", "PurchaseOrder") is False
    assert validate_transition("CREATED", "APPROVED", "PurchaseOrder") is True


def test_scheduler_restart_does_not_duplicate_processing_for_checkpoint_gap():
    checkpoint = SimulationCheckpoint(file_path=str(ROOT / "simulator" / "output" / "test_checkpoint.json"))
    checkpoint.write("2026-09-20T06:00:00+00:00")

    queue = EventQueue(memory_mode=True)
    queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-4001",
        correlation_id="corr-4001",
        payload={
            "po_id": "PO-4001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
        scheduled_time="2026-09-20T12:00:00+00:00",
        priority="HIGH",
    )

    ready = queue.list_ready_events("2026-09-20T12:00:00+00:00", from_time="2026-09-20T06:00:00+00:00")
    assert len(ready) == 1
    assert ready[0]["event_id"] is not None


def test_airflow_missed_execution_catches_up_from_checkpoint():
    checkpoint = SimulationCheckpoint(file_path=str(ROOT / "simulator" / "output" / "test_checkpoint.json"))
    checkpoint.write("2026-09-20T06:00:00+00:00")

    queue = EventQueue(memory_mode=True)
    queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-5001",
        correlation_id="corr-5001",
        payload={
            "po_id": "PO-5001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
        scheduled_time="2026-09-20T09:00:00+00:00",
        priority="HIGH",
    )

    ready = queue.list_ready_events("2026-09-20T12:00:00+00:00", from_time="2026-09-20T06:00:00+00:00")
    assert len(ready) == 1
    assert ready[0]["aggregate_id"] == "PO-5001"


def test_master_simulator_executes_due_event_and_advances_checkpoint():
    checkpoint_path = str(ROOT / "simulator" / "output" / "master_simulator_checkpoint.json")
    checkpoint = SimulationCheckpoint(file_path=checkpoint_path)
    checkpoint.write("2026-09-20T06:00:00+00:00")

    queue = EventQueue(memory_mode=True)
    event_id = queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-6001",
        correlation_id="corr-6001",
        payload={
            "po_id": "PO-6001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
        scheduled_time="2026-09-20T08:00:00+00:00",
        priority="HIGH",
    )

    def success_handler(context):
        return {"status": "SUCCESS", "created_events": [{
            "event_type": "SupplierShipmentCreated",
            "aggregate_type": "shipments",
            "aggregate_id": "S-6001",
            "correlation_id": "corr-6001",
            "scheduled_time": "2026-09-20T09:00:00+00:00",
            "payload": {"shipment_id": "S-6001", "po_id": "PO-6001", "priority": "MEDIUM"},
        }]}

    result = run_simulation_cycle(
        simulation_time="2026-09-20T08:00:00+00:00",
        queue=queue,
        checkpoint_path=checkpoint_path,
        registry={"PurchaseOrderApproved": success_handler},
    )

    assert result["processed_events"][0]["status"] == "COMPLETED"
    assert len(result["scheduled_events"]) == 1
    assert result["scheduled_events"][0]["aggregate_id"] == "S-6001"
    assert queue.list_ready_events("2026-09-20T09:00:00+00:00")[0]["aggregate_id"] == "S-6001"
    assert checkpoint.read() is not None
    assert str(checkpoint.read()) == "2026-09-20 08:00:00+00:00"


def test_master_simulator_ignores_duplicate_future_event_execution():
    queue = EventQueue(memory_mode=True)
    event_id = queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-7001",
        correlation_id="corr-7001",
        payload={
            "po_id": "PO-7001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
        },
        scheduled_time="2026-09-20T10:00:00+00:00",
        priority="HIGH",
    )

    def success_handler(context):
        return {"status": "SUCCESS", "created_events": []}

    first = run_simulation_cycle(
        simulation_time="2026-09-20T10:00:00+00:00",
        queue=queue,
        checkpoint_path=str(ROOT / "simulator" / "output" / "master_duplicate_checkpoint.json"),
        registry={"PurchaseOrderApproved": success_handler},
    )
    second = run_simulation_cycle(
        simulation_time="2026-09-20T10:00:00+00:00",
        queue=queue,
        checkpoint_path=str(ROOT / "simulator" / "output" / "master_duplicate_checkpoint.json"),
        registry={"PurchaseOrderApproved": success_handler},
    )

    assert first["processed_events"][0]["status"] == "COMPLETED"
    assert second["processed_events"] == []
    assert queue._memory_events[0]["status"] == "COMPLETED"


def test_retry_scheduling_persists_due_time_and_drains_when_due():
    queue = EventQueue(memory_mode=True)
    event_id = queue.insert_future_event(
        event_type="PurchaseOrderApproved",
        aggregate_type="purchase_orders",
        aggregate_id="PO-9001",
        correlation_id="corr-9001",
        payload={
            "po_id": "PO-9001",
            "entity_type": "PurchaseOrder",
            "current_state": "CREATED",
            "next_state": "APPROVED",
            "priority": "HIGH",
            "retry_count": 1,
            "max_retry_count": 3,
        },
        scheduled_time="2026-09-20T10:00:00+00:00",
        priority="HIGH",
    )

    queue.mark_event_status(
        event_id,
        "RETRY_SCHEDULED",
        processed_at="2026-09-20T10:00:00+00:00",
        retry_count=1,
        next_retry_time="2026-09-20T10:10:00+00:00",
        priority="HIGH",
    )

    assert queue.list_ready_events("2026-09-20T10:09:00+00:00") == []
    ready = queue.list_ready_events("2026-09-20T10:10:00+00:00")
    assert len(ready) == 1
    assert ready[0]["event_id"] == event_id
    assert ready[0]["payload"]["next_retry_time"] == "2026-09-20T10:10:00+00:00"


def test_child_events_are_scheduled_after_parent_time():
    parent_time = "2026-09-20T11:00:00+00:00"
    child_time = "2026-09-20T11:00:00+00:00"

    from datetime import datetime

    from core.event_timing import get_future_event_time

    parent = datetime.fromisoformat(parent_time)
    child = get_future_event_time("PurchaseOrderApproved", base_time=parent_time)
    assert child > parent
    assert child > datetime.fromisoformat(child_time)


def test_validation_harness_reports_zero_failures_for_real_contract():
    from tests.validation.validator_utils import generate_validation_report

    report = generate_validation_report(output_path=ROOT / "output" / "validation_report.json")
    summary = report["summary"]

    assert summary["failed"] == 0, report
    assert summary["passed"] == summary["events_tested"], report


def test_run_window_contract_restricts_events_to_morning_inbound_only():
    from simulator.core.contracts import get_contract_for_window, is_event_allowed_for_window

    contract = get_contract_for_window("morning")
    assert contract["domain"] == "inbound"
    assert is_event_allowed_for_window("PurchaseOrderCreated", "morning") is True
    assert is_event_allowed_for_window("OrderCreated", "morning") is False
    assert is_event_allowed_for_window("CarrierAssigned", "morning") is False


def test_run_window_contract_restricts_events_to_evening_transportation_only():
    from simulator.core.contracts import is_event_allowed_for_window

    assert is_event_allowed_for_window("CarrierAssigned", "evening") is True
    assert is_event_allowed_for_window("ShipmentDelivered", "evening") is True
    assert is_event_allowed_for_window("OrderCreated", "evening") is False
    assert is_event_allowed_for_window("PurchaseOrderCreated", "evening") is False


def test_lms_shift_contract_requires_punch_before_punch_out():
    from simulator.core.contracts import validate_lms_sequence

    assert validate_lms_sequence(["PunchIn", "BreakStart", "BreakEnd", "PunchOut"]) is True
    assert validate_lms_sequence(["PunchOut"]) is False
    assert validate_lms_sequence(["PunchIn", "PunchIn"]) is False


def test_run_simulation_cycle_respects_window_filter_and_is_idempotent():
    from simulator.core.contracts import CONTRACT_BY_WINDOW
    from simulator.master_simulator import run_simulation_cycle

    queue = EventQueue(memory_mode=True)
    queue.insert_future_event(
        event_type="OrderCreated",
        aggregate_type="orders",
        aggregate_id="ORDER-100",
        correlation_id="corr_order_100",
        payload={"order_id": "ORDER-100", "entity_type": "Order", "current_state": "CREATED", "next_state": "ALLOCATED"},
        scheduled_time="2026-09-20T08:00:00+00:00",
        priority="HIGH",
    )
    queue.insert_future_event(
        event_type="PurchaseOrderCreated",
        aggregate_type="purchase_orders",
        aggregate_id="PO-100",
        correlation_id="corr_po_100",
        payload={"po_id": "PO-100", "entity_type": "PurchaseOrder", "current_state": "CREATED", "next_state": "APPROVED"},
        scheduled_time="2026-09-20T08:00:00+00:00",
        priority="HIGH",
    )

    first = run_simulation_cycle(
        simulation_time="2026-09-20T08:00:00+00:00",
        queue=queue,
        checkpoint_path=str(ROOT / "simulator" / "output" / "contract_window_checkpoint.json"),
        registry={
            "OrderCreated": lambda context: {"status": "SUCCESS", "created_events": []},
            "PurchaseOrderCreated": lambda context: {"status": "SUCCESS", "created_events": []},
        },
        run_window="morning",
    )
    second = run_simulation_cycle(
        simulation_time="2026-09-20T08:00:00+00:00",
        queue=queue,
        checkpoint_path=str(ROOT / "simulator" / "output" / "contract_window_checkpoint.json"),
        registry={
            "OrderCreated": lambda context: {"status": "SUCCESS", "created_events": []},
            "PurchaseOrderCreated": lambda context: {"status": "SUCCESS", "created_events": []},
        },
        run_window="morning",
    )

    assert first["processed_events"][0]["status"] == "COMPLETED"
    assert second["processed_events"] == []
    assert CONTRACT_BY_WINDOW["morning"]["allowed_events"][0] == "PurchaseOrderCreated"
