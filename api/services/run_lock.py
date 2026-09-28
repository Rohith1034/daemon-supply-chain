from __future__ import annotations

import sys
import threading
from contextlib import contextmanager
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SIMULATOR_DIR = ROOT_DIR / "simulator"
if str(SIMULATOR_DIR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR_DIR))

from core.db import Database


_PROCESS_LOCK = threading.RLock()
_ADVISORY_LOCK_KEY = 7_046_267_113_409_281


@contextmanager
def simulation_execution_lock():
    """Serialize simulator executions that share database state or legacy artifacts."""
    with _PROCESS_LOCK:
        with Database() as db:
            db.fetch_one(
                "SELECT pg_advisory_lock(%s) AS acquired",
                (_ADVISORY_LOCK_KEY,),
            )
            try:
                yield
            finally:
                db.fetch_one(
                    "SELECT pg_advisory_unlock(%s) AS released",
                    (_ADVISORY_LOCK_KEY,),
                )