from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "bronze"
    / "phase_06_ingest_market_data.py"
)


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def _literal_assignment(name: str) -> Any:
    tree = ast.parse(_read_notebook())

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)

    raise AssertionError(f"Missing assignment: {name}")


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


def test_market_ingestion_is_valid_databricks_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert (
        "# MAGIC %run ./phase_06_validate_market_landing\n"
        in source
    )
    assert "from typing import TYPE_CHECKING, Any" in source
    assert "if TYPE_CHECKING:" in source
    assert "from phase_06_validate_market_landing import (" in source
    assert source.count("# COMMAND ----------") == 4
    ast.parse(source)


def test_market_ingestion_pins_approved_datasets() -> None:
    approvals = _literal_assignment("APPROVED_DATASETS")

    assert approvals == {
        "DAILY_PRICES": {
            "bronze_table": (
                "workspace.devin_market_risk_dev.bronze_daily_prices"
            ),
            "expected_record_count": 60,
        },
        "CORPORATE_ACTIONS": {
            "bronze_table": (
                "workspace.devin_market_risk_dev."
                "bronze_corporate_actions"
            ),
            "expected_record_count": 2,
        },
    }


def test_market_ingestion_reuses_full_market_validation() -> None:
    source = _read_notebook()

    required_validation_calls = {
        "validate_approved_specifications()",
        "load_and_validate_landing(specification)",
        "validate_common_records(specification, source_rows)",
        "validate_daily_prices(source_rows)",
        "validate_corporate_actions(source_rows)",
        '"candidate record columns"',
        '"candidate row count"',
        '"candidate batch lineage"',
    }
    assert all(
        validation_call in source
        for validation_call in required_validation_calls
    )


def test_market_ingestion_preflights_both_before_writes() -> None:
    source = _read_notebook()

    preflight_position = source.index("prepared_sources.append(")
    plan_position = source.index("ingestion_plans = [")
    first_write_position = source.index(".saveAsTable(")

    assert preflight_position < plan_position < first_write_position
    assert "for specification in MARKET_SOURCES:" in source
    assert '"unexpected pre-existing source rows"' in source
    assert '"candidate batch count reconciliation"' in source


def test_market_ingestion_is_idempotent_and_audited() -> None:
    source = _read_notebook()

    assert 'INGESTION_CONTRACT_VERSION = "2.0.0"' in source
    assert 'batch_status = "SUCCEEDED"' in source
    assert 'batch_status = "SKIPPED_DUPLICATE"' in source
    assert "should_write_business_rows = False" in source
    assert '"duplicate_of_batch_id": duplicate_of_batch_id' in source
    assert '"received_count": source_row_count' in source
    assert '"accepted_count": accepted_count' in source
    assert '"deduplicated_count": deduplicated_count' in source
    assert '.mode("append")' in source
    assert '.dropDuplicates(' not in source
    assert '.fillna(' not in source
    assert '.mode("overwrite")' not in source


def test_market_ingestion_writes_only_approved_bronze_tables() -> None:
    source = _read_notebook()
    targets = _save_as_table_targets(source)

    assert len(targets) == 2
    assert set(targets) == {"bronze_table", "BRONZE_BATCH_TABLE"}
    assert "approved_business_tables" in source
    assert '"approved Bronze business table"' in source

    prohibited_sql = {
        "MERGE INTO",
        "UPDATE ",
        "DELETE FROM",
        "DROP TABLE",
        "TRUNCATE TABLE",
    }
    normalized = " ".join(source.upper().split())
    assert not {
        statement
        for statement in prohibited_sql
        if statement in normalized
    }


def test_market_ingestion_reconciles_persisted_evidence() -> None:
    source = _read_notebook()

    required_reconciliations = {
        '"persisted source row count"',
        '"persisted business batch lineage"',
        '"persisted candidate batch audit count"',
        '"persisted batch {field}"',
        "reconcile_persisted_batch(spark=spark, plan=plan)",
        "persisted_accepted_count=",
        '"persisted_deduplicated_count="',
    }
    assert all(
        reconciliation in source
        for reconciliation in required_reconciliations
    )

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
    assert all(f'"{field}"' in source for field in required_lineage_fields)
