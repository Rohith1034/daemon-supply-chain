from __future__ import annotations

import json
import os
import subprocess
import sys

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.schemas.simulator_schema import (
    SimulationRequest,
    SimulationResponse
)


# ============================================================
# PROJECT PATHS
# ============================================================

ROOT_DIR = Path(
    __file__
).resolve().parents[2]

SIMULATOR_DIR = (
    ROOT_DIR /
    "simulator"
)

OUTPUT_DIR = (
    ROOT_DIR /
    "output"
)

REPORT_FILE = (
    OUTPUT_DIR /
    "event_execution_report.json"
)

RUN_FLOW_SCRIPT = (
    SIMULATOR_DIR /
    "loading_scripts" /
    "run_event_flow.py"
)

RESET_SCRIPT = (
    SIMULATOR_DIR /
    "loading_scripts" /
    "reset_transaction_tables.py"
)


# ============================================================
# DEFAULT SIMULATION TIMESTAMP
# ============================================================

DEFAULT_SIM_START = datetime(
    2026,
    1,
    1,
    0,
    0,
    0,
    tzinfo=timezone.utc
)


# ============================================================
# DATETIME NORMALIZATION
# ============================================================

def _ensure_utc(
    dt: datetime | None
) -> datetime:

    if dt is None:
        return DEFAULT_SIM_START

    if dt.tzinfo is None:
        return dt.replace(
            tzinfo=timezone.utc
        )

    return dt.astimezone(
        timezone.utc
    )


# ============================================================
# TEXT TAIL
# ============================================================

def _tail(
    text: str | None,
    max_lines: int = 30
) -> str | None:

    if not text:
        return None

    lines = text.splitlines()

    if len(lines) <= max_lines:
        return text

    return "\n".join(
        lines[-max_lines:]
    )


# ============================================================
# BUILD ENVIRONMENT
# ============================================================

def _build_env(
    request: SimulationRequest
) -> dict[str, str]:

    env = os.environ.copy()

    simulation_start = _ensure_utc(
        request.simulation_start_timestamp
    )

    # --------------------------------------------------------
    # Simulation clock
    # --------------------------------------------------------

    env["SIMULATION_NOW"] = (
        simulation_start.isoformat()
    )

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Add simulator directory to PYTHONPATH so generators
    # can resolve:
    #
    #     from core.db import Database
    #
    #     from core.outbox import publish_event
    #
    # etc.
    # --------------------------------------------------------

    existing_pythonpath = env.get(
        "PYTHONPATH",
        ""
    )

    simulator_pythonpath = str(
        SIMULATOR_DIR
    )

    if existing_pythonpath:

        env["PYTHONPATH"] = (
            simulator_pythonpath
            + os.pathsep
            + existing_pythonpath
        )

    else:

        env["PYTHONPATH"] = (
            simulator_pythonpath
        )

    return env


# ============================================================
# RUN SCRIPT
# ============================================================

def _run_script(
    script_path: Path,
    env: dict[str, str]
) -> subprocess.CompletedProcess:

    if not script_path.exists():

        raise FileNotFoundError(
            f"Script not found: {script_path}"
        )

    return subprocess.run(
        [
            sys.executable,
            str(script_path)
        ],
        cwd=str(ROOT_DIR),
        env=env,
        capture_output=True,
        text=True
    )


# ============================================================
# READ REPORT
# ============================================================

def get_latest_report() -> list[dict[str, Any]] | None:

    if not REPORT_FILE.exists():
        return None

    try:

        data = json.loads(
            REPORT_FILE.read_text(
                encoding="utf-8"
            )
        )

        if isinstance(
            data,
            list
        ):
            return data

        return None

    except (
        json.JSONDecodeError,
        OSError
    ):

        return None


# ============================================================
# BUILD REPORT SUMMARY
# ============================================================

def _build_summary(
    report: list[dict[str, Any]] | None
) -> dict[str, Any] | None:

    if not report:
        return None

    events = []

    failed_events = []

    successful_events = []

    for entry in report:

        if not isinstance(
            entry,
            dict
        ):
            continue

        event_name = entry.get(
            "event"
        )

        status = entry.get(
            "status"
        )

        if event_name:

            events.append(
                event_name
            )

        if status == "FAILED":

            failed_events.append(
                event_name
            )

        elif status == "SUCCESS":

            successful_events.append(
                event_name
            )

    last_entry = next(
        (
            entry
            for entry in reversed(report)
            if isinstance(entry, dict)
        ),
        {}
    )

    return {

        "total_entries":
            len(report),

        "successful_entries":
            len(successful_events),

        "failed_entries":
            len(failed_events),

        "events":
            events,

        "successful_events":
            successful_events,

        "failed_events":
            failed_events,

        "last_event":
            last_entry.get(
                "event"
            ),

        "last_status":
            last_entry.get(
                "status"
            ),

        "failed_event":
            (
                failed_events[0]
                if failed_events
                else None
            )
    }


# ============================================================
# DETERMINE FLOW SUCCESS
# ============================================================

def _is_flow_successful(
    return_code: int,
    report: list[dict[str, Any]] | None
) -> bool:

    # --------------------------------------------------------
    # Process itself must exit successfully.
    # --------------------------------------------------------

    if return_code != 0:
        return False

    # --------------------------------------------------------
    # If there is a report, check event statuses.
    # --------------------------------------------------------

    if report:

        for entry in report:

            if not isinstance(
                entry,
                dict
            ):
                continue

            if entry.get(
                "status"
            ) == "FAILED":

                return False

    return True


# ============================================================
# RUN FULL FLOW
# ============================================================

def run_full_flow(
    request: SimulationRequest
) -> SimulationResponse:

    simulation_start = _ensure_utc(
        request.simulation_start_timestamp
    )

    env = _build_env(
        request
    )

    # ========================================================
    # 1. VALIDATE FLOW SCRIPT
    # ========================================================

    if not RUN_FLOW_SCRIPT.exists():

        return SimulationResponse(

            status="FAILED",

            message=(
                "run_event_flow.py was not found"
            ),

            simulation_start_timestamp=(
                simulation_start
            ),

            return_code=1,

            report_path=None,

            summary=None,

            stdout_tail=None,

            stderr_tail=(
                f"Missing script: "
                f"{RUN_FLOW_SCRIPT}"
            )
        )

    # ========================================================
    # 2. OPTIONAL RESET
    # ========================================================

    if request.reset_before_run:

        if not RESET_SCRIPT.exists():

            return SimulationResponse(

                status="FAILED",

                message=(
                    "Reset requested but "
                    "reset_transaction_tables.py "
                    "was not found"
                ),

                simulation_start_timestamp=(
                    simulation_start
                ),

                return_code=1,

                report_path=None,

                summary=None,

                stdout_tail=None,

                stderr_tail=(
                    f"Missing script: "
                    f"{RESET_SCRIPT}"
                )
            )

        try:

            reset_result = _run_script(
                RESET_SCRIPT,
                env
            )

        except Exception as exc:

            return SimulationResponse(

                status="FAILED",

                message=(
                    "Unable to execute "
                    "reset script"
                ),

                simulation_start_timestamp=(
                    simulation_start
                ),

                return_code=1,

                report_path=None,

                summary=None,

                stdout_tail=None,

                stderr_tail=str(
                    exc
                )
            )

        if reset_result.returncode != 0:

            return SimulationResponse(

                status="FAILED",

                message=(
                    "Reset script failed "
                    "before simulation"
                ),

                simulation_start_timestamp=(
                    simulation_start
                ),

                return_code=(
                    reset_result.returncode
                ),

                report_path=(
                    str(REPORT_FILE)
                    if REPORT_FILE.exists()
                    else None
                ),

                summary=_build_summary(
                    get_latest_report()
                ),

                stdout_tail=_tail(
                    reset_result.stdout
                ),

                stderr_tail=_tail(
                    reset_result.stderr
                )
            )

    # ========================================================
    # 3. REMOVE OLD REPORT
    #
    # Prevent an old failed/successful report from being
    # mistaken for the current execution.
    # ========================================================

    try:

        if REPORT_FILE.exists():

            REPORT_FILE.unlink()

    except OSError:
        pass

    # ========================================================
    # 4. RUN COMPLETE SIMULATION FLOW
    # ========================================================

    try:

        result = _run_script(
            RUN_FLOW_SCRIPT,
            env
        )

    except Exception as exc:

        return SimulationResponse(

            status="FAILED",

            message=(
                "Unable to start "
                "simulation process"
            ),

            simulation_start_timestamp=(
                simulation_start
            ),

            return_code=1,

            report_path=None,

            summary=None,

            stdout_tail=None,

            stderr_tail=str(
                exc
            )
        )

    # ========================================================
    # 5. READ EXECUTION REPORT
    # ========================================================

    report = get_latest_report()

    summary = _build_summary(
        report
    )

    # ========================================================
    # 6. DETERMINE REAL FLOW STATUS
    # ========================================================

    success = _is_flow_successful(
        result.returncode,
        report
    )

    # ========================================================
    # 7. RETURN RESPONSE
    # ========================================================

    if success:

        return SimulationResponse(

            status="SUCCESS",

            message=(
                "Full simulation flow "
                "completed successfully"
            ),

            simulation_start_timestamp=(
                simulation_start
            ),

            return_code=(
                result.returncode
            ),

            report_path=(
                str(REPORT_FILE)
                if REPORT_FILE.exists()
                else None
            ),

            summary=summary,

            stdout_tail=_tail(
                result.stdout
            ),

            stderr_tail=_tail(
                result.stderr
            )
        )

    # ========================================================
    # FLOW FAILED
    # ========================================================

    failed_event = None

    if summary:

        failed_event = summary.get(
            "failed_event"
        )

    if failed_event:

        message = (
            f"Simulation flow failed "
            f"at {failed_event}"
        )

    else:

        message = (
            "Full simulation flow failed"
        )

    return SimulationResponse(

        status="FAILED",

        message=message,

        simulation_start_timestamp=(
            simulation_start
        ),

        return_code=(
            result.returncode
        ),

        report_path=(
            str(REPORT_FILE)
            if REPORT_FILE.exists()
            else None
        ),

        summary=summary,

        stdout_tail=_tail(
            result.stdout
        ),

        stderr_tail=_tail(
            result.stderr
        )
    )