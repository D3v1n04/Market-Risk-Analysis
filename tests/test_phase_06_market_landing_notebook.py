from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "bronze"
    / "phase_06_validate_market_landing.py"
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


def test_market_landing_validation_is_valid_notebook_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 1
    ast.parse(source)


def test_market_landing_validation_pins_complete_evidence() -> None:
    specifications = {
        specification["dataset_name"]: specification
        for specification in _literal_assignment("MARKET_SOURCES")
    }

    assert set(specifications) == {
        "DAILY_PRICES",
        "CORPORATE_ACTIONS",
    }

    prices = specifications["DAILY_PRICES"]
    assert prices["source_sha256"] == (
        "d02d4a5fdb8543a2482a354b484b2279"
        "4552cb9795942fbaf9f82e2c83d7c488"
    )
    assert prices["manifest_sha256"] == (
        "bf50d119ff5da5801c3ed31195db7ae2"
        "81516f9988129f31f5441415f3e0d1b8"
    )
    assert prices["expected_record_count"] == 60
    assert prices["expected_column_count"] == 14
    assert prices["bronze_table"].endswith(".bronze_daily_prices")

    actions = specifications["CORPORATE_ACTIONS"]
    assert actions["source_sha256"] == (
        "e4d582a1db05816e1004635975602a6a"
        "eed7f28807dde64ec4acc3c4dd8f5ffd"
    )
    assert actions["manifest_sha256"] == (
        "5c4d0f1fa2074b00dac3ec7aa05904d3"
        "9515aff1fb0d5af0b35260f798c05b3f"
    )
    assert actions["expected_record_count"] == 2
    assert actions["expected_column_count"] == 12
    assert actions["bronze_table"].endswith(
        ".bronze_corporate_actions"
    )


def test_market_landing_validation_pins_generation_lineage() -> None:
    assert _literal_assignment("GENERATION_SOURCE_OBJECT_PATH") == (
        "data/fixtures/phase_06_analytics_scenario.yml"
    )
    assert _literal_assignment("GENERATION_SOURCE_SHA256") == (
        "2fb0fe28a3d283e66d933ca6187a6869"
        "a33392f0526eab37724b549c9209cd00"
    )
    assert _literal_assignment("GENERATOR_MODULE") == (
        "market_risk_analysis.ingestion.phase_06_market_inputs"
    )
    assert _literal_assignment("GENERATOR_CODE_VERSION") == (
        "ebb82db39e16466363f82adeca3529d816345ff9"
    )

    source = _read_notebook()
    assert '"generation lineage"' in source
    assert 'manifest.get("generation")' in source


def test_market_landing_validation_checks_grid_and_rules() -> None:
    source = _read_notebook()

    required_checks = {
        '"business-key uniqueness"',
        '"source-record ID uniqueness"',
        '"controlled fixture source"',
        '"record hash at source row',
        '"price date set"',
        '"instrument count"',
        "instrument coverage",
        "date coverage",
        '"OHLC high consistency',
        '"OHLC low consistency',
        '"corporate action types"',
        '"non-unit split ratio',
    }
    assert all(check in source for check in required_checks)

    assert _literal_assignment("EXPECTED_PRICE_DATES") == (
        "2016-01-04",
        "2016-01-05",
        "2016-01-06",
        "2016-01-07",
    )
    assert '"expected_grid=15_instruments_x_4_dates"' in source
    assert "calculate_record_hash(" in source


def test_market_landing_validation_precedes_candidates() -> None:
    source = _read_notebook()

    manifest_check_position = source.index('"generation lineage"')
    record_check_position = source.index("validate_common_records(")
    candidate_creation_position = source.index(
        "candidates = spark.createDataFrame("
    )
    assert (
        manifest_check_position
        < record_check_position
        < candidate_creation_position
    )

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
    assert all(check in source for check in required_manifest_checks)
    assert "schema=bronze_schema" in source
    assert "set(candidate_record)" in source


def test_market_landing_validation_cannot_write_data() -> None:
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
    assert all(f'"{field}"' in source for field in required_lineage_fields)
    assert ".fillna(" not in source
    assert ".dropDuplicates(" not in source
