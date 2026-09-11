from __future__ import annotations

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUNDLE_PATH = PROJECT_ROOT / "databricks.yml"
RESOURCE_PATH = (
    PROJECT_ROOT / "resources" / "phase_11_initialization_job.yml"
)


def test_initialization_workflow_is_safe_and_fully_wired() -> None:
    bundle = yaml.load(
        BUNDLE_PATH.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    resource = yaml.load(
        RESOURCE_PATH.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )

    assert bundle["include"] == ["resources/*.yml"]

    job = resource["resources"]["jobs"]["market_risk_initialization"]
    assert job["name"] == "Market Risk Initialization"
    assert job["max_concurrent_runs"] == "1"
    assert "schedule" not in job
    assert "run_as" not in job

    parameters = {
        parameter["name"]: parameter["default"]
        for parameter in job["parameters"]
    }
    assert parameters == {
        "code_version": "",
        "trigger_type": "MANUAL",
    }

    tasks = {task["task_key"]: task for task in job["tasks"]}
    assert list(tasks) == [
        "bronze_portfolio_initialization",
        "silver_process_portfolios",
        "bronze_reference_ingestion",
        "silver_process_instruments",
        "silver_process_target_allocations",
        "silver_process_stress_scenarios",
        "silver_process_stress_scenario_shocks",
    ]

    assert all(task["max_retries"] == "0" for task in tasks.values())
    assert all(task["timeout_seconds"] == "1800" for task in tasks.values())

    assert tasks["silver_process_portfolios"]["depends_on"] == [
        {"task_key": "bronze_portfolio_initialization"}
    ]
    assert tasks["bronze_reference_ingestion"]["depends_on"] == [
        {"task_key": "silver_process_portfolios"}
    ]
    assert tasks["silver_process_instruments"]["depends_on"] == [
        {"task_key": "bronze_reference_ingestion"}
    ]
    assert tasks["silver_process_target_allocations"]["depends_on"] == [
        {"task_key": "silver_process_instruments"}
    ]
    assert tasks["silver_process_stress_scenarios"]["depends_on"] == [
        {"task_key": "silver_process_target_allocations"}
    ]
    assert tasks["silver_process_stress_scenario_shocks"]["depends_on"] == [
        {"task_key": "silver_process_stress_scenarios"}
    ]

    assert (
        tasks["silver_process_portfolios"]["notebook_task"]["base_parameters"]
        ["source_batch_id"]
        == "{{tasks.bronze_portfolio_initialization.values.portfolio_batch_id}}"
    )

    reference_task_values = {
        "silver_process_instruments": "instruments_batch_id",
        "silver_process_target_allocations": "target_allocations_batch_id",
        "silver_process_stress_scenarios": "stress_scenarios_batch_id",
        "silver_process_stress_scenario_shocks": (
            "stress_scenario_shocks_batch_id"
        ),
    }
    for task_key, task_value_key in reference_task_values.items():
        parameters = tasks[task_key]["notebook_task"]["base_parameters"]
        assert (
            parameters["source_batch_id"]
            == f"{{{{tasks.bronze_reference_ingestion.values."
            f"{task_value_key}}}}}"
        )
        assert parameters["code_version"] == "{{job.parameters.code_version}}"
        assert parameters["trigger_type"] == "{{job.parameters.trigger_type}}"
