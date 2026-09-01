from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.schemas.simulator_schema import SimulationRequest, SimulationResponse


ROOT_DIR = Path(__file__).resolve().parents[2]
SIMULATOR_DIR = ROOT_DIR / "simulator"
OUTPUT_DIR = ROOT_DIR / "output"
REPORT_FILE = OUTPUT_DIR / "event_execution_report.json"

RUN_FLOW_SCRIPT = SIMULATOR_DIR / "loading_scripts" / "run_event_flow.py"
RESET_SCRIPT = SIMULATOR_DIR / "loading_scripts" / "reset_transaction_tables.py"

DEFAULT_SIM_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _ensure_utc(dt: datetime | None) -> datetime:
    if dt is None:
        return DEFAULT_SIM_START
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _tail(text: str | None, max_lines: int = 30) -> str | None:
    if not text:
        return None
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return text
    return "\n".join(lines[-max_lines:])


def _build_env(request: SimulationRequest) -> dict[str, str]:
    env = os.environ.copy()
    env["SIMULATION_START_TIMESTAMP"] = _ensure_utc(
        request.simulation_start_timestamp
    ).isoformat()

    # Keep the simulation anchored to the past unless you explicitly change it.
    env.setdefault("SIMULATION_CLOCK_MODE", "static")

    return env


def _run_script(script_path: Path, env: dict[str, str]) -> subprocess.CompletedProcess:
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    return subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(ROOT_DIR),
        env=env,
        capture_output=True,
        text=True
    )


def _read_report() -> list[dict[str, Any]] | None:
    if not REPORT_FILE.exists():
        return None

    try:
        data = json.loads(REPORT_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        return None
    except Exception:
        return None


def _build_summary(report: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    if not report:
        return None

    events = []
    failed_event = None

    for entry in report:
        if not isinstance(entry, dict):
            continue
        event_name = entry.get("event")
        if event_name:
            events.append(event_name)
        if entry.get("status") == "FAILED" and failed_event is None:
            failed_event = event_name

    last = next((x for x in reversed(report) if isinstance(x, dict)), {})
    return {
        "total_entries": len(report),
        "events": events,
        "last_event": last.get("event"),
        "last_status": last.get("status"),
        "failed_event": failed_event,
    }


def run_full_flow(request: SimulationRequest) -> SimulationResponse:
    env = _build_env(request)

    if request.reset_before_run and RESET_SCRIPT.exists():
        reset_result = _run_script(RESET_SCRIPT, env)
        if reset_result.returncode != 0:
            return SimulationResponse(
                status="FAILED",
                message="Reset script failed before flow execution",
                simulation_start_timestamp=_ensure_utc(request.simulation_start_timestamp),
                return_code=reset_result.returncode,
                stdout_tail=_tail(reset_result.stdout),
                stderr_tail=_tail(reset_result.stderr),
                report_path=str(REPORT_FILE) if REPORT_FILE.exists() else None,
                summary=_build_summary(_read_report()),
            )

    result = _run_script(RUN_FLOW_SCRIPT, env)
    report = _read_report()
    summary = _build_summary(report)

    success = result.returncode == 0

    return SimulationResponse(
        status="SUCCESS" if success else "FAILED",
        message=(
            "Full simulation flow completed successfully"
            if success
            else "Full simulation flow failed"
        ),
        simulation_start_timestamp=_ensure_utc(request.simulation_start_timestamp),
        return_code=result.returncode,
        report_path=str(REPORT_FILE) if REPORT_FILE.exists() else None,
        stdout_tail=_tail(result.stdout),
        stderr_tail=_tail(result.stderr),
        summary=summary,
    )