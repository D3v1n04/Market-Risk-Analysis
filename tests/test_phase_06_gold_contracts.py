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


def test_gold_analytics_run_contract_is_partition_atomic() -> None:
    contract = _load_contract("analytics_runs")

    assert contract["contract_version"] == "1.1.0"
    assert contract["dataset_class"] == "operational_audit"
    assert contract["operation_scope"] == {
        "input_trust_boundary": "TRUSTED_SILVER",
        "output_layer": "GOLD",
        "output_datasets_per_run": 1,
        "portfolios_per_run": 1,
        "valuation_dates_per_run": 1,
        "atomic_publication": True,
    }
    assert contract["natural_key"] == [
        "output_dataset_name",
        "portfolio_id",
        "valuation_date",
        "attempt_number",
    ]
    assert contract["publication_semantics"]["partition_key"] == [
        "portfolio_id",
        "valuation_date",
    ]


def test_position_market_value_contract_has_explicit_semantics() -> None:
    contract = _load_contract("position_market_values")
    fields = {field["name"]: field for field in contract["fields"]}

    assert contract["primary_key"] == [
        "portfolio_id",
        "instrument_id",
        "valuation_date",
    ]
    assert fields["signed_quantity"]["unit"] == "shares"
    assert fields["close_price"]["unit"] == "quote_currency_per_share"
    assert fields["signed_market_value"]["unit"] == "base_currency"
    assert fields["absolute_market_value"]["unit"] == "base_currency"
    assert fields["quote_currency"]["allowed_values"] == ["USD"]
    assert fields["base_currency"]["allowed_values"] == ["USD"]
    assert fields["position_side"]["allowed_values"] == ["LONG", "SHORT"]

    calculation = contract["calculation"]
    assert calculation["as_of"] == "MARKET_CLOSE_ON_VALUATION_DATE"
    assert calculation["price_field"] == "daily_prices.close_price"
    assert calculation["quantity_field"] == "positions.signed_quantity"
    assert calculation["currency_policy"] == "USD_ONLY_NO_FX"
    assert calculation["intermediate_rounding_allowed"] is False


def test_position_market_value_contract_is_auditable() -> None:
    contract = _load_contract("position_market_values")
    fields = {field["name"] for field in contract["fields"]}
    rule_ids = {rule["rule_id"] for rule in contract["quality_rules"]}

    assert {
        "input_position_record_sha256",
        "input_price_record_sha256",
        "analytics_run_id",
        "calculation_version",
        "calculated_at_utc",
        "contract_version",
        "record_hash",
    } <= fields
    assert {
        "POSITION_MARKET_VALUE_INPUT_COVERAGE",
        "POSITION_MARKET_VALUE_DATE_ALIGNMENT",
        "POSITION_MARKET_VALUE_CURRENCY_MATCH",
        "POSITION_MARKET_VALUE_FORMULA_RECONCILES",
        "POSITION_MARKET_VALUE_ABSOLUTE_RECONCILES",
        "POSITION_MARKET_VALUE_SIGN_RECONCILES",
        "POSITION_MARKET_VALUE_RECORD_HASH_VALID",
    } <= rule_ids
