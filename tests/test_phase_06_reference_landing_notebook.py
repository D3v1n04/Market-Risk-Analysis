from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "bronze"
    / "phase_06_validate_reference_landing.py"
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


def test_phase_06_reference_validation_is_valid_notebook_source() -> None:
    source = _read_notebook()

    assert source.startswith(
        "# Databricks notebook source\n"
    )
    assert source.count("# COMMAND ----------") == 1
    ast.parse(source)


def test_phase_06_reference_validation_pins_landed_evidence() -> None:
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

    instruments = specifications["INSTRUMENTS"]
    assert instruments["source_sha256"] == (
        "7eb9b32d1f4ef5689404b7f88c5f685a"
        "c55fa2c27ea930e8d5de35b638200b7a"
    )
    assert instruments["manifest_sha256"] == (
        "dd40645daf887cde5d0141e939f0ed53"
        "c60f642bc450af31fad0e44fc22c8f6c"
    )
    assert instruments["expected_record_count"] == 15
    assert instruments["expected_column_count"] == 18
    assert instruments["bronze_table"].endswith(
        ".bronze_instruments"
    )

    allocations = specifications["TARGET_ALLOCATIONS"]
    assert allocations["source_sha256"] == (
        "6182be5e69859a138aa2b94c642943460"
        "aa1914439d340f68d63e4dffccb1e66"
    )
    assert allocations["manifest_sha256"] == (
        "6a71c37089a661a95aeea28ca27b77c6"
        "9ae745ea9fc14315e69d18b4828ba28a"
    )
    assert allocations["expected_record_count"] == 30
    assert allocations["expected_column_count"] == 7
    assert allocations["bronze_table"].endswith(
        ".bronze_target_allocations"
    )

    scenarios = specifications["STRESS_SCENARIOS"]
    assert scenarios["source_sha256"] == (
        "3aae999b9d0cc6c8dea56f64b059d219"
        "3ac4d4b8943502c0bb58174bb811c8cb"
    )
    assert scenarios["manifest_sha256"] == (
        "2ee337e0ba7233e87510aab18f426b6a"
        "8a67af62fb6e6a58ab0672f9e9c4eb65"
    )
    assert scenarios["expected_record_count"] == 3
    assert scenarios["expected_column_count"] == 9
    assert scenarios["bronze_table"].endswith(
        ".bronze_stress_scenarios"
    )

    shocks = specifications["STRESS_SCENARIO_SHOCKS"]
    assert shocks["source_sha256"] == (
        "ecbc15c312f7b533b12e4433ee6fab2f"
        "c2c9a74e5c5554e6fbf006791c7f28c3"
    )
    assert shocks["manifest_sha256"] == (
        "133bf6ade22987f5c33e73fb6b35a052"
        "0e6449d1b629a3e5f821073765c26dec"
    )
    assert shocks["expected_record_count"] == 45
    assert shocks["expected_column_count"] == 6
    assert shocks["bronze_table"].endswith(
        ".bronze_stress_scenario_shocks"
    )


def test_phase_06_reference_validation_precedes_candidates() -> None:
    source = _read_notebook()

    source_read_position = source.index(
        "source_payload = source_path.read_bytes()"
    )
    candidate_creation_position = source.index(
        "candidates = spark.createDataFrame("
    )

    assert source_read_position < candidate_creation_position

    required_manifest_checks = {
        '"source SHA-256"',
        '"manifest SHA-256"',
        '"manifest source SHA-256"',
        '"source size"',
        '"source record count"',
        '"source column count"',
        '"manifest source columns"',
        '"dataset name"',
        '"source identifier"',
        '"source object path"',
        '"landed object path"',
        '"source contract version"',
    }
    assert all(
        check in source
        for check in required_manifest_checks
    )

    assert 'manifest.get("source_id")' in source
    assert '"PROJECT_GIT_FIXTURE"' in source
    assert "schema=bronze_schema" in source
    assert "set(candidate_record)" in source
    assert "expected_columns" in source


def test_phase_06_reference_validation_cannot_write_data() -> None:
    source = _read_notebook()
    tree = ast.parse(source)

    prohibited_attributes = {
        "saveAsTable",
        "insertInto",
        "write",
        "writeStream",
    }
    used_prohibited_attributes = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and node.attr in prohibited_attributes
    }
    assert not used_prohibited_attributes

    normalized = " ".join(source.upper().split())
    prohibited_sql = {
        "INSERT INTO",
        "MERGE INTO",
        "UPDATE ",
        "DELETE FROM",
        "DROP TABLE",
        "TRUNCATE TABLE",
    }
    assert not {
        statement
        for statement in prohibited_sql
        if statement in normalized
    }

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

    assert ".fillna(" not in source
    assert ".dropDuplicates(" not in source
