import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "silver"
    / "phase_12_publish_exchange_calendar.py"
)


def _source() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def _tree() -> ast.Module:
    return ast.parse(_source())


def _literal_assignment(name: str) -> object:
    for node in _tree().body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == name
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"Missing literal assignment: {name}")


def test_phase_12_calendar_publisher_is_valid_databricks_source() -> None:
    source = _source()

    assert source.startswith("# Databricks notebook source")
    assert source.count("# COMMAND ----------") >= 2
    _tree()


def test_phase_12_calendar_publisher_pins_admitted_snapshot() -> None:
    source = _source()

    assert _literal_assignment("CALENDAR_SHA256") == (
        "5badba1c795de7c6c793e745240dc61999d0e04db8aa54db42918da582f9531b"
    )
    assert _literal_assignment("EXPECTED_TOTAL_ROWS") == 4384
    assert _literal_assignment("EXPECTED_ROWS_PER_EXCHANGE") == 2192
    assert _literal_assignment("EXPECTED_TRADING_DAYS_PER_EXCHANGE") == 1508
    assert 'manifest["source_id"], "EXCHANGE_CALENDARS"' in source
    assert 'manifest["source_sha256"], CALENDAR_SHA256' in source
    assert "source_record_hash(source_row)" in source


def test_phase_12_calendar_publisher_is_append_only_and_idempotent() -> None:
    source = _source()
    normalized = " ".join(source.split())

    assert 'require_equal("calendar conflicting keys", conflicting_count, 0)' in source
    assert "inserted_count in {0, EXPECTED_TOTAL_ROWS}" in source
    assert "WHEN NOT MATCHED THEN INSERT *" in normalized
    assert "UPDATE SET" not in normalized
    assert "persisted calendar hashes" in source
