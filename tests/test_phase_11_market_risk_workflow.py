from __future__ import annotations

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESOURCE_PATH = (
    PROJECT_ROOT / "resources" / "phase_11_market_risk_job.yml"
)


def test_market_risk_workflow_is_safe_and_fully_wired() -> None:
    resource = yaml.load(
        RESOURCE_PATH.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    job = resource["resources"]["jobs"]["market_risk_daily_risk"]

    assert job["name"] == "Market Risk Daily Risk"
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
        "portfolio_id": "CORE_15_LONG",
        "valuation_date": "2016-01-07",
        "risk_as_of_date": "2016-12-30",
    }

    tasks = {task["task_key"]: task for task in job["tasks"]}
    assert list(tasks) == [
        "bronze_market_ingestion",
        "generate_trading_calendar",
        "silver_process_daily_prices",
        "silver_process_corporate_actions",
        "derive_positions",
        "derive_cash_balances",
        "calculate_position_market_values",
        "calculate_portfolio_daily_metrics",
        "calculate_risk_measures",
        "validate_serving_outputs",
    ]

    assert all(task["max_retries"] == "0" for task in tasks.values())
    assert all(task["timeout_seconds"] == "1800" for task in tasks.values())

    expected_dependencies = {
        "generate_trading_calendar": "bronze_market_ingestion",
        "silver_process_daily_prices": "generate_trading_calendar",
        "silver_process_corporate_actions": "silver_process_daily_prices",
        "derive_positions": "silver_process_corporate_actions",
        "derive_cash_balances": "derive_positions",
        "calculate_position_market_values": "derive_cash_balances",
        "calculate_portfolio_daily_metrics": "calculate_position_market_values",
        "calculate_risk_measures": "calculate_portfolio_daily_metrics",
        "validate_serving_outputs": "calculate_risk_measures",
    }
    for task_key, upstream_task_key in expected_dependencies.items():
        assert tasks[task_key]["depends_on"] == [
            {"task_key": upstream_task_key}
        ]

    daily_price_parameters = tasks["silver_process_daily_prices"][
        "notebook_task"
    ]["base_parameters"]
    assert daily_price_parameters["source_batch_id"] == (
        "{{tasks.bronze_market_ingestion.values.daily_prices_batch_id}}"
    )

    action_parameters = tasks["silver_process_corporate_actions"][
        "notebook_task"
    ]["base_parameters"]
    assert action_parameters["source_batch_id"] == (
        "{{tasks.bronze_market_ingestion.values."
        "corporate_actions_batch_id}}"
    )

    for task_key in [
        "derive_positions",
        "derive_cash_balances",
        "calculate_position_market_values",
        "calculate_portfolio_daily_metrics",
        "calculate_risk_measures",
    ]:
        base_parameters = tasks[task_key]["notebook_task"][
            "base_parameters"
        ]
        assert (
            base_parameters["portfolio_id"]
            == "{{job.parameters.portfolio_id}}"
        )
        assert (
            base_parameters["code_version"]
            == "{{job.parameters.code_version}}"
        )
        assert (
            base_parameters["trigger_type"]
            == "{{job.parameters.trigger_type}}"
        )

    assert tasks["derive_positions"]["notebook_task"]["base_parameters"][
        "position_date"
    ] == "{{job.parameters.valuation_date}}"
    assert tasks["derive_cash_balances"]["notebook_task"]["base_parameters"][
        "cash_date"
    ] == "{{job.parameters.valuation_date}}"
    assert tasks["calculate_position_market_values"]["notebook_task"][
        "base_parameters"
    ]["valuation_date"] == "{{job.parameters.valuation_date}}"
    assert tasks["calculate_portfolio_daily_metrics"]["notebook_task"][
        "base_parameters"
    ]["valuation_date"] == "{{job.parameters.valuation_date}}"
    assert tasks["calculate_risk_measures"]["notebook_task"][
        "base_parameters"
    ]["as_of_date"] == "{{job.parameters.risk_as_of_date}}"

    validation_parameters = tasks["validate_serving_outputs"][
    "notebook_task"
    ]["base_parameters"]

    assert tasks["validate_serving_outputs"]["notebook_task"][
        "notebook_path"
    ] == "../notebooks/qa/phase_11_validate_serving_outputs.py"

    assert validation_parameters == {
        "portfolio_id": "{{job.parameters.portfolio_id}}",
        "valuation_date": "{{job.parameters.valuation_date}}",
        "risk_as_of_date": "{{job.parameters.risk_as_of_date}}",
    }
