from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = PROJECT_ROOT / "contracts"


def _load_contract(dataset: str) -> dict[str, Any]:
    loaded = yaml.safe_load(
        (CONTRACT_DIR / f"{dataset}.yml").read_text(encoding="utf-8")
    )
    assert isinstance(loaded, dict)
    return loaded


def test_risk_run_contract_models_an_immutable_complete_bundle() -> None:
    contract = _load_contract("risk_runs")
    fields = {field["name"]: field for field in contract["fields"]}

    assert contract["contract_version"] == "1.0.0"
    assert contract["dataset_class"] == "operational_audit"
    assert contract["primary_key"] == ["risk_run_id"]
    assert contract["natural_key"] == [
        "portfolio_id",
        "as_of_date",
        "attempt_number",
    ]
    assert contract["operation_scope"] == {
        "input_trust_boundary": "TRUSTED_SILVER_AND_PUBLISHED_GOLD",
        "output_layer": "GOLD",
        "output_datasets_per_run": 3,
        "portfolios_per_run": 1,
        "as_of_dates_per_run": 1,
        "atomic_publication": False,
        "publication_visibility": "LOGICAL_SUCCESSFUL_RISK_RUN",
    }
    assert fields["risk_run_id"] == {
        "name": "risk_run_id",
        "type": "STRING",
        "nullable": False,
        "format": "UUID",
        "description": (
            "Unique immutable identifier for one complete risk bundle attempt."
        ),
    }
    assert fields["base_currency"]["allowed_values"] == ["USD"]
    assert fields["confidence_levels"]["type"] == "ARRAY<DECIMAL(3,2)>"
    assert fields["expected_historical_pnl_scenario_count"]["allowed_values"] == [
        251
    ]
    assert fields["expected_var_measure_count"]["allowed_values"] == [2]
    assert fields["expected_stress_result_count"]["allowed_values"] == [3]
    assert fields["code_version"]["format"] == "^[0-9a-f]{40}$"
    publication = contract["publication_semantics"]
    assert publication["mode"] == "LOGICAL_APPEND_COMPLETE_BUNDLE"
    assert publication["visibility_rule"] == (
        "Results are publishable only when joined to a matching risk run "
        "with successful terminal status and published=true."
    )


def test_risk_run_contract_pins_the_approved_history_and_static_exposure() -> None:
    contract = _load_contract("risk_runs")

    assert contract["methodology"] == {
        "history_window": {
            "start_date": "2016-01-04",
            "end_date": "2016-12-30",
            "trading_session_count": 252,
            "return_observation_count": 251,
            "minimum_return_observation_count": 250,
        },
        "confidence_levels": [0.95, 0.99],
        "static_exposure": {
            "source_date": "2016-01-07",
            "valuation_date": "2016-12-30",
            "policy": "BUY_AND_HOLD_NO_REBALANCE_NO_LATER_CORPORATE_ACTIONS",
        },
        "intermediate_rounding_allowed": False,
        "currency_policy": "USD_ONLY_NO_FX",
    }


def test_historical_pnl_scenario_contract_preserves_loss_sign_and_lineage() -> None:
    contract = _load_contract("historical_pnl_scenarios")
    fields = {field["name"]: field for field in contract["fields"]}
    rules = {rule["rule_id"] for rule in contract["quality_rules"]}

    assert contract["primary_key"] == ["risk_run_id", "scenario_date"]
    assert fields["base_currency"]["allowed_values"] == ["USD"]
    assert fields["simulated_portfolio_pnl"]["type"] == "DECIMAL(38,16)"
    assert "minimum_inclusive" not in fields["loss_amount"]
    assert {
        "input_static_exposure_sha256",
        "input_prior_price_partition_sha256",
        "input_scenario_price_partition_sha256",
        "record_hash",
    } <= fields.keys()

    calculation = contract["calculation"]
    assert calculation["return_formula"] == (
        "adjusted_close[t] / adjusted_close[t-1] - 1"
    )
    assert calculation["return_observation_count"] == 251
    assert calculation["minimum_return_observation_count"] == 250
    assert calculation["portfolio_pnl_formula"] == (
        "Sum signed December 30 position market value multiplied by each "
        "instrument historical return; cash return is zero."
    )
    assert calculation["loss_amount_formula"] == "-simulated_portfolio_pnl"
    assert calculation["intermediate_rounding_allowed"] is False
    assert {
        "HISTORICAL_PNL_SCENARIO_DATE_ADJACENCY",
        "HISTORICAL_PNL_SCENARIO_COUNT_COMPLETE",
        "HISTORICAL_PNL_SCENARIO_FORMULA_RECONCILES",
        "HISTORICAL_PNL_SCENARIO_LOSS_SIGN_VALID",
        "HISTORICAL_PNL_SCENARIO_RECORD_HASH_VALID",
    } <= rules


def test_var_measure_contract_pins_discrete_quantile_ranks() -> None:
    contract = _load_contract("var_measures")
    fields = {field["name"]: field for field in contract["fields"]}
    calculation = contract["calculation"]

    assert contract["primary_key"] == ["risk_run_id", "confidence_level"]
    assert fields["confidence_level"]["allowed_values"] == [0.95, 0.99]
    assert "minimum_inclusive" not in fields["var_amount"]
    assert calculation["quantile_method"] == "EMPIRICAL_DISCRETE_NO_INTERPOLATION"
    assert calculation["quantile_rank_formula"] == (
        "ceil(confidence_level * observation_count)"
    )
    assert calculation["known_quantile_ranks"] == [
        {
            "confidence_level": 0.95,
            "observation_count": 251,
            "quantile_rank": 239,
        },
        {
            "confidence_level": 0.99,
            "observation_count": 251,
            "quantile_rank": 249,
        },
    ]
    assert calculation["intermediate_rounding_allowed"] is False


def test_stress_result_contract_enforces_governed_hypothetical_scenarios() -> None:
    contract = _load_contract("stress_results")
    fields = {field["name"]: field for field in contract["fields"]}
    calculation = contract["calculation"]
    rules = {rule["rule_id"] for rule in contract["quality_rules"]}

    assert contract["primary_key"] == ["risk_run_id", "scenario_id"]
    assert fields["base_currency"]["allowed_values"] == ["USD"]
    assert fields["shock_count"]["allowed_values"] == [15]
    assert {
        "input_static_exposure_sha256",
        "input_stress_scenario_record_sha256",
        "input_shock_set_sha256",
        "record_hash",
    } <= fields.keys()
    assert calculation["scenario_type"] == "DETERMINISTIC_HYPOTHETICAL"
    assert calculation["approved_active_scenario_ids"] == [
        "BROAD_MARKET_DOWN_10",
        "TECHNOLOGY_CORRECTION",
        "GEOPOLITICAL_SUPPLY_SHOCK",
    ]
    assert calculation["required_shock_count"] == 15
    assert calculation["position_stress_pnl_formula"] == (
        "signed_market_value * shock_ratio"
    )
    assert calculation["hypothetical_not_forecast"] is True
    assert calculation["intermediate_rounding_allowed"] is False
    assert {
        "STRESS_RESULT_APPROVED_SCENARIO_SET_COMPLETE",
        "STRESS_RESULT_SHOCK_COVERAGE_VALID",
        "STRESS_RESULT_FORMULA_RECONCILES",
        "STRESS_RESULT_HYPOTHETICAL_NOT_FORECAST",
        "STRESS_RESULT_RECORD_HASH_VALID",
    } <= rules
