import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIMULATOR = ROOT / "simulator"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SIMULATOR) not in sys.path:
    sys.path.insert(0, str(SIMULATOR))


def test_event_outbox_schema_contract_matches_supported_event_store():
    schema_path = ROOT / "simulator" / "output" / "database_schema.json"
    with schema_path.open("r", encoding="utf-8") as handle:
        schema = json.load(handle)

    assert "event_outbox" in schema
    definition = schema["event_outbox"]

    required_columns = [
        "event_id",
        "event_type",
        "aggregate_type",
        "aggregate_id",
        "correlation_id",
        "payload",
        "created_at",
        "published_at",
        "status",
    ]

    assert "CREATE TABLE event_outbox" in definition
    for column in required_columns:
        assert column in definition

    assert "event_id uuid NOT NULL" in definition
    assert "event_type character varying(100) NOT NULL" in definition
    assert "payload jsonb NOT NULL" in definition
    assert "status character varying(30) NOT NULL DEFAULT 'PENDING'::character varying" in definition
