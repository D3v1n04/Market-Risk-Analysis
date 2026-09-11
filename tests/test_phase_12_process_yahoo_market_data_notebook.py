import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "silver"
    / "phase_12_process_yahoo_market_data.py"
)


def _source() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def _tree() -> ast.Module:
    return ast.parse(_source())


def _function_source(name: str) -> str:
    source = _source()
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            segment = ast.get_source_segment(source, node)
            assert segment is not None
            return segment
    raise AssertionError(f"Missing function: {name}")


def test_phase_12_yahoo_writer_is_valid_databricks_source() -> None:
    source = _source()

    assert source.startswith("# Databricks notebook source")
    assert source.count("# COMMAND ----------") >= 5
    _tree()


def test_phase_12_yahoo_writer_pins_admitted_bronze_snapshots() -> None:
    source = _source()

    assert 'REAL_SOURCE_ID = "YAHOO_FINANCE"' in source
    assert (
        "7f860c0a3d8191b9b2840e5c0d784954b8b9102af51d76b24828a5c0118d7c27"
        in source
    )
    assert (
        "e6cc097f3cfe44ef325e968cf9b9c0d4869476d946fd67720854cb89d7315a83"
        in source
    )
    assert "REAL_PRICE_ROW_COUNT = 22620" in source
    assert "REAL_ACTION_ROW_COUNT = 280" in source
    assert "REAL_PRICE_SESSION_COUNT = 1508" in source
    assert '"expected_source_sha256": REAL_PRICE_SHA256' in source
    assert '"expected_source_sha256": REAL_ACTION_SHA256' in source


def test_phase_12_yahoo_writer_requires_exact_bronze_lineage() -> None:
    source = _source()

    assert '"successful Bronze source ID"' in source
    assert '"successful Bronze source hash"' in source
    assert 'row["source_id"]' in source
    assert 'row["source_sha256"]' in source
    assert 'specification["expected_source_sha256"]' in source


def test_phase_12_yahoo_dataset_gate_uses_real_calendar_not_fixture() -> None:
    function_source = _function_source("dataset_failures")

    assert 'F.col("source_id") == specification["expected_source_id"]' in (
        function_source
    )
    assert 'F.col("exchange_mic") == "XNYS"' in function_source
    assert 'F.col("is_trading_day")' in function_source
    assert "YAHOO_PRICE_CALENDAR_GRID" in function_source
    assert "YAHOO_ACTION_EXPECTED_COVERAGE" in function_source
    assert "PROJECT_GIT_FIXTURE" not in function_source
    assert "PRICE_DATES" not in function_source
