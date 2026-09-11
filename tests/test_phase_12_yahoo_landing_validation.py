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
    task = job["tasks"][0]
    assert task["task_key"] == "validate_yahoo_landing"
    assert task["max_retries"] == "0"
    assert task["notebook_task"]["notebook_path"] == (
        "../notebooks/bronze/phase_12_validate_yahoo_landing.py"
    )
    assert task["notebook_task"]["base_parameters"] == {
        "daily_prices_sha256": "{{job.parameters.daily_prices_sha256}}",
        "corporate_actions_sha256": (
            "{{job.parameters.corporate_actions_sha256}}"
        ),
    }
