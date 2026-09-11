from __future__ import annotations

import ast
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT / "notebooks/bronze/phase_12_validate_yahoo_landing.py"
)
RESOURCE_PATH = (
    PROJECT_ROOT / "resources/phase_12_yahoo_landing_validation_job.yml"
)


def test_yahoo_landing_validation_is_read_only_and_pinned() -> None:
    source = NOTEBOOK_PATH.read_text(encoding="utf-8")

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 1
    ast.parse(source)
    assert 'SOURCE_ID = "YAHOO_FINANCE"' in source
    assert '"publication_status"' in source
    assert '"APPROVED"' in source
    assert '"shared snapshot SHA-256"' in source
    assert "hashlib.sha256(data_path.read_bytes()).hexdigest()" in source
    assert "csv.DictReader(source_file)" in source

    prohibited_attributes = {"saveAsTable", "insertInto", "write", "writeStream"}
    assert not {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and node.attr in prohibited_attributes
    }


def test_yahoo_landing_validation_job_is_manual_and_parameterized() -> None:
    resource = yaml.load(
        RESOURCE_PATH.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    job = resource["resources"]["jobs"]["yahoo_finance_landing_validation"]

    assert job["name"] == "Yahoo Finance Landing Validation"
    assert job["max_concurrent_runs"] == "1"
    assert "schedule" not in job
    tasks = {task["task_key"]: task for task in job["tasks"]}
    assert list(tasks) == ["validate_yahoo_landing", "ingest_yahoo_market_data"]
    assert all(task["max_retries"] == "0" for task in tasks.values())
    task = tasks["validate_yahoo_landing"]
    assert task["notebook_task"]["notebook_path"] == (
        "../notebooks/bronze/phase_12_validate_yahoo_landing.py"
    )
    assert task["notebook_task"]["base_parameters"] == {
        "daily_prices_sha256": "{{job.parameters.daily_prices_sha256}}",
        "corporate_actions_sha256": (
            "{{job.parameters.corporate_actions_sha256}}"
        ),
    }



def test_yahoo_bronze_writer_is_gated_by_validation() -> None:
    writer_path = (
        PROJECT_ROOT / "notebooks/bronze/phase_12_ingest_yahoo_market_data.py"
    )
    source = writer_path.read_text(encoding="utf-8")

    assert source.startswith("# Databricks notebook source\n")
    assert source.splitlines()[1] == "# MAGIC %run ./phase_12_validate_yahoo_landing"
    assert 'F.col("status").isin("SUCCEEDED", "SUCCEEDED_WITH_WARNINGS")' in source
    assert '"SKIPPED_DUPLICATE"' in source
    assert '"requested_start_date": REQUEST_START_DATE' in source
    assert "bronze_persistence=PASS" in source
    ast.parse(source)

    resource = yaml.load(
        RESOURCE_PATH.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    tasks = resource["resources"]["jobs"]["yahoo_finance_landing_validation"][
        "tasks"
    ]
    writer = tasks[1]
    assert writer["task_key"] == "ingest_yahoo_market_data"
    assert writer["depends_on"] == [{"task_key": "validate_yahoo_landing"}]
