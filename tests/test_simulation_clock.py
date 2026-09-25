import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))

from core.simulation_clock import get_simulation_now


def test_get_simulation_now_uses_override_when_present(monkeypatch):
    monkeypatch.setenv("SIMULATION_NOW", "2026-09-20T12:30:00Z")
    now = get_simulation_now()

    assert now.tzinfo is not None
    assert now == datetime(2026, 9, 20, 12, 30, tzinfo=timezone.utc)


def test_get_simulation_now_rejects_invalid_override(monkeypatch):
    monkeypatch.setenv("SIMULATION_NOW", "not-a-date")

    try:
        get_simulation_now()
        assert False, "Expected ValueError for bad SIMULATION_NOW"
    except ValueError:
        pass


def test_get_simulation_now_defaults_to_utc_timezone():
    now = get_simulation_now()

    assert now.tzinfo is not None
    assert now.utcoffset() == timezone.utc.utcoffset(now)
