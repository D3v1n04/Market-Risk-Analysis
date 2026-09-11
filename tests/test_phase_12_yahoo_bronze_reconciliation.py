from __future__ import annotations

import ast
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT / "notebooks/qa/phase_12_reconcile_yahoo_bronze.py"
)
RESOURCE_PATH = (
    PROJECT_ROOT / "resources/phase_12_yahoo_bronze_reconciliation_job.yml"
)


def test_yahoo_bronze_reconciliation_is_read_only_and_complete() -> None:
    source = NOTEBOOK_PATH.read_text(encoding="utf-8")

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 1
    assert 'SOURCE_ID = "YAHOO_FINANCE"' in source
    assert '"expected_count": 22620' in source
    assert '"expected_count": 280' in source
    assert '"CASH_DIVIDEND": 271' in source
    assert '"STOCK_SPLIT": 9' in source
    assert 'F.col("status") == "SUCCEEDED"' in source
    assert "bronze_reconciliation=PASS" in source
    ast.parse(source)

    prohibited_attributes = {"saveAsTable", "insertInto", "write", "writeStream"}
    assert not {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and node.attr in prohibited_attributes
    }


def test_yahoo_bronze_reconciliation_job_is_manual() -> None:
    resource = yaml.load(
        RESOURCE_PATH.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    job = resource["resources"]["jobs"]["yahoo_finance_bronze_reconciliation"]

    assert job["name"] == "Yahoo Finance Bronze Reconciliation"
    assert job["max_concurrent_runs"] == "1"
    assert "schedule" not in job
    assert job["tasks"] == [
        {
            "task_key": "reconcile_yahoo_bronze",
            "timeout_seconds": "1800",
            "max_retries": "0",
            "notebook_task": {
                "notebook_path": (
                    "../notebooks/qa/phase_12_reconcile_yahoo_bronze.py"
                )
            },
        }
    ]
