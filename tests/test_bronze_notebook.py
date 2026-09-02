from __future__ import annotations

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "bronze"
    / "phase_04_ingest_portfolios.py"
)


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def _save_as_table_targets(source: str) -> list[str]:
    tree = ast.parse(source)
    targets: list[str] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "saveAsTable" or not node.args:
            continue

        target = node.args[0]
        if isinstance(target, ast.Name):
            targets.append(target.id)

    return targets


def test_bronze_notebook_is_valid_databricks_python_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 3
    ast.parse(source)


def test_landing_validation_precedes_delta_writes() -> None:
    source = _read_notebook()

    validation_position = source.index("= load_and_validate_landing()")
    first_write_position = source.index(".saveAsTable(")

    assert validation_position < first_write_position
    assert '"source SHA-256"' in source
    assert '"source record count"' in source
    assert '"source columns"' in source
    assert '"landed object path"' in source


def test_notebook_writes_only_expected_bronze_tables() -> None:
    targets = _save_as_table_targets(_read_notebook())

    assert len(targets) == 2
    assert set(targets) == {
        "BRONZE_PORTFOLIO_TABLE",
        "BRONZE_BATCH_TABLE",
    }


def test_notebook_contains_idempotency_and_lineage_controls() -> None:
    source = _read_notebook()

    assert '"SUCCEEDED"' in source
    assert '"SUCCEEDED_WITH_WARNINGS"' in source
    assert '"SKIPPED_DUPLICATE"' in source
    assert "should_write_portfolios = False" in source
    assert '"duplicate_of_batch_id": duplicate_of_batch_id' in source

    required_lineage_fields = {
        "batch_id",
        "source_id",
        "source_object_path",
        "source_sha256",
        "source_row_number",
        "source_record_sha256",
        "raw_record",
        "ingested_at_utc",
        "contract_version",
    }

    for field in required_lineage_fields:
        assert f'"{field}"' in source

    assert ".dropDuplicates(" not in source
    assert ".fillna(" not in source
