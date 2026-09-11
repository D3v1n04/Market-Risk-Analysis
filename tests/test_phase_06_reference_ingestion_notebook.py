from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "bronze"
    / "phase_06_ingest_reference_data.py"
)


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def _literal_assignment(name: str) -> Any:
    tree = ast.parse(_read_notebook())

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name)
            and target.id == name
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


def test_reference_ingestion_is_valid_databricks_source() -> None:
    source = _read_notebook()

    assert source.startswith(
        "# Databricks notebook source\n"
    )
    assert source.count("# COMMAND ----------") == 3
    ast.parse(source)


def test_reference_ingestion_pins_approved_sources() -> None:
    specifications = {
        specification["dataset_name"]: specification
        for specification in _literal_assignment(
            "REFERENCE_SOURCES"
        )
    }

    assert set(specifications) == {
        "INSTRUMENTS",
        "STRESS_SCENARIOS",
        "STRESS_SCENARIO_SHOCKS",
        "TARGET_ALLOCATIONS",
    }
    assert {
        specification["bronze_table"]
        for specification in specifications.values()
    } == {
        "workspace.devin_market_risk_dev.bronze_instruments",
        (
            "workspace.devin_market_risk_dev."
            "bronze_target_allocations"
        ),
        (
            "workspace.devin_market_risk_dev."
            "bronze_stress_scenarios"
        ),
        (
            "workspace.devin_market_risk_dev."
            "bronze_stress_scenario_shocks"
        ),
    }
    assert specifications["INSTRUMENTS"]["expected_record_count"] == 15
    assert (
        specifications["TARGET_ALLOCATIONS"]["expected_record_count"]
        == 30
    )
    assert specifications["STRESS_SCENARIOS"]["source_sha256"] == (
        "3aae999b9d0cc6c8dea56f64b059d219"
        "3ac4d4b8943502c0bb58174bb811c8cb"
    )
    assert specifications["STRESS_SCENARIOS"]["manifest_sha256"] == (
        "2ee337e0ba7233e87510aab18f426b6a"
        "8a67af62fb6e6a58ab0672f9e9c4eb65"
    )
    assert specifications["STRESS_SCENARIOS"]["expected_record_count"] == 3
    assert (
        specifications["STRESS_SCENARIO_SHOCKS"]["source_sha256"]
        == "ecbc15c312f7b533b12e4433ee6fab2f"
        "c2c9a74e5c5554e6fbf006791c7f28c3"
    )
    assert (
        specifications["STRESS_SCENARIO_SHOCKS"]["manifest_sha256"]
        == "133bf6ade22987f5c33e73fb6b35a052"
        "0e6449d1b629a3e5f821073765c26dec"
    )
    assert (
        specifications["STRESS_SCENARIO_SHOCKS"]["expected_record_count"]
        == 45
    )


def test_reference_ingestion_preflights_before_writes() -> None:
    source = _read_notebook()

    preflight_position = source.index(
        "prepared_sources.append("
    )
    plan_position = source.index(
        "ingestion_plans = ["
    )
    first_write_position = source.index(
        ".saveAsTable("
    )

    assert preflight_position < plan_position < first_write_position
    assert '"manifest SHA-256"' in source
    assert '"manifest source SHA-256"' in source
    assert '"candidate record columns"' in source
    assert '"candidate row count"' in source
    assert '"unexpected pre-existing source rows"' in source


def test_reference_ingestion_is_idempotent_and_audited() -> None:
    source = _read_notebook()

    assert 'INGESTION_CONTRACT_VERSION = "2.0.0"' in source
    assert 'batch_status = "SUCCEEDED"' in source
    assert 'batch_status = "SKIPPED_DUPLICATE"' in source
    assert "should_write_business_rows = False" in source
    assert '"duplicate_of_batch_id": duplicate_of_batch_id' in source
    assert '"received_count": source_row_count' in source
    assert '"accepted_count": accepted_count' in source
    assert '"deduplicated_count": deduplicated_count' in source
    assert ".mode(\"append\")" in source
    assert ".dropDuplicates(" not in source
    assert ".fillna(" not in source
    assert ".mode(\"overwrite\")" not in source


def test_reference_ingestion_writes_only_approved_bronze_tables() -> None:
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


def test_reference_ingestion_preserves_required_lineage() -> None:
    source = _read_notebook()
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

    assert all(
        f'"{field}"' in source
        for field in required_lineage_fields
    )
    assert '"persisted business batch lineage"' in source
    assert '"persisted candidate batch audit count"' in source


def test_reference_ingestion_publishes_effective_batch_ids_for_workflow() -> None:
    source = _read_notebook()

    assert "from pyspark.dbutils import DBUtils" in source
    assert "dbutils = DBUtils(spark)" in source
    assert "WORKFLOW_TASK_VALUE_KEYS = {" in source

    expected_keys = {
        "instruments_batch_id",
        "target_allocations_batch_id",
        "stress_scenarios_batch_id",
        "stress_scenario_shocks_batch_id",
    }
    assert all(f'"{key}"' in source for key in expected_keys)

    assert "effective_reference_batch_ids: dict[str, str] = {}" in source
    assert "else plan[\"previous_successful_batch_id\"]" in source
    assert '"effective reference task-value datasets"' in source
    assert "dbutils.jobs.taskValues.set(" in source
    assert "key=task_value_key" in source
    assert "value=effective_batch_id" in source

    assert source.index("dbutils.jobs.taskValues.set(") > source.index(
        "persisted candidate batch audit count"
    )
