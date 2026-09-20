import json
from pathlib import Path

from bnpl_common.schemas import EXPECTED_SOURCE_COLUMNS, REQUIRED_SOURCE_COLUMNS


def test_expected_schema_contains_required_fields():
    assert REQUIRED_SOURCE_COLUMNS <= EXPECTED_SOURCE_COLUMNS
    assert {"default_30d", "default_90d"} <= EXPECTED_SOURCE_COLUMNS


def test_json_stream_contract_excludes_unknown_targets_requirement():
    schema_path = Path(__file__).resolve().parents[1] / "kafka" / "schemas" / "bnpl_event_schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert "default_30d" not in schema["properties"]
    assert "default_90d" not in schema["properties"]
    assert schema["additionalProperties"] is False
