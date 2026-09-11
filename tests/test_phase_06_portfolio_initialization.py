from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "bronze"
    / "phase_06_ingest_portfolio_initialization.py"
)
SOURCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "phase_06_analytics_foundation"
    / "portfolios_initialization.csv"
)
SOURCE_SHA256 = (
    "0c36be496eed0d124fd3649f38d926d5"
    "93f10e37fa61ff3fb101f68e2f21dfbd"
)
MANIFEST_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "portfolios"
    / SOURCE_SHA256
    / "manifest.json"
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


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


def test_initialization_source_is_deterministic_and_valid() -> None:
    payload = SOURCE_PATH.read_bytes()
    assert _sha256(payload) == SOURCE_SHA256

    with SOURCE_PATH.open(newline="", encoding="utf-8") as source_file:
        rows = list(csv.DictReader(source_file))

    assert len(rows) == 2
    assert {row["portfolio_id"] for row in rows} == {
        "CORE_15_LONG",
        "LONG_SHORT_130_30",
    }
    assert {row["actual_inception_date"] for row in rows} == {"2016-01-04"}
    assert {row["config_version"] for row in rows} == {"1.1.0"}

    hash_fields = [
        "portfolio_id",
        "portfolio_name",
        "strategy_code",
        "base_currency",
        "target_inception_date",
        "actual_inception_date",
        "initial_nav",
        "target_long_ratio",
        "target_short_ratio",
        "target_gross_ratio",
        "target_net_ratio",
        "rebalance_policy",
        "cash_policy",
        "is_active",
        "config_version",
    ]
    for row in rows:
        canonical = "|".join(row[field] or "<NULL>" for field in hash_fields)
        assert _sha256(canonical.encode("utf-8")) == row["record_hash"]


def test_initialization_manifest_reconciles_to_source() -> None:
    manifest_payload = MANIFEST_PATH.read_bytes()
    manifest = json.loads(manifest_payload)
    source_payload = SOURCE_PATH.read_bytes()

    assert manifest["dataset_name"] == "PORTFOLIOS"
    assert manifest["source_id"] == "PROJECT_GIT_FIXTURE"
    assert manifest["source_contract_version"] == "1.1.0"
    assert manifest["source_sha256"] == SOURCE_SHA256
    assert manifest["source_size_bytes"] == len(source_payload)
    assert manifest["source_record_count"] == 2
    assert manifest["source_object_path"] == (
        "data/raw/phase_06_analytics_foundation/"
        "portfolios_initialization.csv"
    )
    assert manifest["landed_object_path"].endswith(
        f"/portfolios/{SOURCE_SHA256}/portfolios_initialization.csv"
    )


def test_initialization_notebook_is_valid_databricks_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 3
    ast.parse(source)


def test_initialization_notebook_pins_approved_evidence() -> None:
    source = _read_notebook()

    assert SOURCE_SHA256[:32] in source
    assert SOURCE_SHA256[32:] in source
    assert '"portfolios_initialization.csv"' in source
    assert '"source contract version"' in source
    assert '"1.1.0"' in source
    assert '"batch_type": "INCREMENTAL"' in source


def test_initialization_validates_before_writing() -> None:
    source = _read_notebook()

    assert source.index("= load_and_validate_landing()") < source.index(
        ".saveAsTable("
    )
    for evidence in [
        '"source SHA-256"',
        '"source record count"',
        '"source columns"',
        '"landed object path"',
    ]:
        assert evidence in source


def test_initialization_writes_only_approved_bronze_tables() -> None:
    targets = _save_as_table_targets(_read_notebook())

    assert len(targets) == 2
    assert set(targets) == {
        "BRONZE_PORTFOLIO_TABLE",
        "BRONZE_BATCH_TABLE",
    }


def test_initialization_ingestion_is_idempotent_and_audited() -> None:
    source = _read_notebook()

    assert '"SUCCEEDED"' in source
    assert '"SUCCEEDED_WITH_WARNINGS"' in source
    assert '"SKIPPED_DUPLICATE"' in source
    assert "should_write_portfolios = False" in source
    assert '"duplicate_of_batch_id": duplicate_of_batch_id' in source
    assert "persisted_candidate_batch_count" in source
    assert "persisted_portfolio_source_count" in source


def test_initialization_publishes_effective_batch_id_for_workflow() -> None:
    source = _read_notebook()

    assert "from pyspark.dbutils import DBUtils" in source
    assert "dbutils = DBUtils(spark)" in source
    assert "effective_portfolio_batch_id = (" in source
    assert "if should_write_portfolios" in source
    assert "else previous_successful_batch_id" in source
    assert (
        '"Effective portfolio batch ID is required after Bronze persistence"'
        in source
    )
    assert 'key="portfolio_batch_id"' in source
    assert "value=effective_portfolio_batch_id" in source
    assert (
        'workflow_task_value.portfolio_batch_id='
        in source
    )

    assert source.index("dbutils.jobs.taskValues.set(") > source.index(
        "persisted candidate batch count"
    )
