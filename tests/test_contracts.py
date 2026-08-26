from __future__ import annotations

import csv
import hashlib
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = PROJECT_ROOT / "contracts"
FIXTURE_DIR = PROJECT_ROOT / "data" / "fixtures"

EXPECTED_CONTRACTS = {
    "corporate_actions",
    "daily_prices",
    "data_quality_violations",
    "ingestion_batches",
    "instruments",
    "portfolios",
    "positions",
    "stress_scenario_shocks",
    "stress_scenarios",
    "target_allocations",
    "trading_calendar",
}


def _load_contract(dataset: str) -> dict[str, Any]:
    path = CONTRACT_DIR / f"{dataset}.yml"
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict), f"{path} must contain a YAML mapping"
    return loaded


def _load_fixture(filename: str) -> list[dict[str, str]]:
    with (FIXTURE_DIR / filename).open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def _assert_record_hashes(
    rows: list[dict[str, str]], contract: dict[str, Any]
) -> None:
    hash_fields = contract["record_hash"]["fields"]
    null_token = contract["record_hash"].get("null_token", "<NULL>")

    for row in rows:
        canonical = "|".join(row[field] or null_token for field in hash_fields)
        expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        assert row["record_hash"] == expected, (
            f"Invalid record_hash for {contract['dataset']}: {canonical}"
        )


def _allocation_metrics(
    rows: list[dict[str, str]], portfolio_id: str
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    weights = [
        Decimal(row["target_weight"])
        for row in rows
        if row["portfolio_id"] == portfolio_id
    ]
    long_total = sum((weight for weight in weights if weight > 0), Decimal())
    short_total = sum((weight for weight in weights if weight < 0), Decimal())
    gross_total = sum((abs(weight) for weight in weights), Decimal())
    net_total = sum(weights, Decimal())
    return long_total, short_total, gross_total, net_total


def test_expected_contracts_are_parseable_and_self_describing() -> None:
    paths = sorted(CONTRACT_DIR.glob("*.yml"))

    assert {path.stem for path in paths} == EXPECTED_CONTRACTS

    for path in paths:
        contract = _load_contract(path.stem)
        assert contract["contract_version"] == "1.0.0"
        assert contract["dataset"] == path.stem
        assert contract["grain"].strip()

        fields = contract["fields"]
        field_names = [field["name"] for field in fields]
        assert len(field_names) == len(set(field_names))
        assert all(field["type"] for field in fields)
        assert all(
            field["nullable"] in {True, False, "conditional"} for field in fields
        )

        rule_ids = [rule["rule_id"] for rule in contract["quality_rules"]]
        assert rule_ids
        assert len(rule_ids) == len(set(rule_ids))


def test_declared_fixture_paths_exist() -> None:
    for dataset in EXPECTED_CONTRACTS:
        contract = _load_contract(dataset)
        fixture = contract.get("fixture")
        if fixture is not None:
            assert (PROJECT_ROOT / fixture["path"]).is_file()


def test_instrument_fixture_has_exact_active_universe_and_valid_hashes() -> None:
    contract = _load_contract("instruments")
    rows = _load_fixture("instruments.csv")

    assert len(rows) == 15
    assert len({row["instrument_id"] for row in rows}) == 15
    assert len({row["yfinance_symbol"] for row in rows}) == 15
    assert {row["quote_currency"] for row in rows} == {"USD"}
    assert all(row["is_active"] == "true" for row in rows)
    berkshire = next(row for row in rows if row["instrument_id"] == "BRK_B_US")
    assert berkshire["display_symbol"] == "BRK.B"
    assert berkshire["yfinance_symbol"] == "BRK-B"
    _assert_record_hashes(rows, contract)


def test_portfolio_fixture_reconciles_strategy_targets_and_hashes() -> None:
    contract = _load_contract("portfolios")
    rows = _load_fixture("portfolios.csv")

    assert {row["portfolio_id"] for row in rows} == {
        "CORE_15_LONG",
        "LONG_SHORT_130_30",
    }
    assert all(Decimal(row["initial_nav"]) == Decimal("1000000.00") for row in rows)
    assert all(row["actual_inception_date"] == "" for row in rows)

    expected = {
        "CORE_15_LONG": ("1.0", "0.0", "1.0", "1.0"),
        "LONG_SHORT_130_30": ("1.3", "0.3", "1.6", "1.0"),
    }
    for row in rows:
        ratios = (
            Decimal(row["target_long_ratio"]),
            Decimal(row["target_short_ratio"]),
            Decimal(row["target_gross_ratio"]),
            Decimal(row["target_net_ratio"]),
        )
        expected_ratios = tuple(
            Decimal(value) for value in expected[row["portfolio_id"]]
        )
        assert ratios == expected_ratios
        assert ratios[0] + ratios[1] == ratios[2]
        assert ratios[0] - ratios[1] == ratios[3]

    _assert_record_hashes(rows, contract)


def test_allocations_cover_both_portfolios_and_reconcile_exposure() -> None:
    contract = _load_contract("target_allocations")
    rows = _load_fixture("target_allocations.csv")
    instruments = {row["instrument_id"] for row in _load_fixture("instruments.csv")}
    portfolios = {row["portfolio_id"] for row in _load_fixture("portfolios.csv")}

    assert len(rows) == 30
    assert Counter(row["portfolio_id"] for row in rows) == {
        "CORE_15_LONG": 15,
        "LONG_SHORT_130_30": 15,
    }
    assert {row["instrument_id"] for row in rows} == instruments
    assert {row["portfolio_id"] for row in rows} == portfolios
    assert len(
        {
            (row["portfolio_id"], row["instrument_id"], row["effective_from"])
            for row in rows
        }
    ) == 30

    assert _allocation_metrics(rows, "CORE_15_LONG") == (
        Decimal("1.0"),
        Decimal("0.0"),
        Decimal("1.0"),
        Decimal("1.0"),
    )
    assert _allocation_metrics(rows, "LONG_SHORT_130_30") == (
        Decimal("1.3"),
        Decimal("-0.3"),
        Decimal("1.6"),
        Decimal("1.0"),
    )
    assert {
        row["instrument_id"]
        for row in rows
        if row["portfolio_id"] == "LONG_SHORT_130_30"
        and Decimal(row["target_weight"]) < 0
    } == {"WMT_US", "UNH_US", "TSLA_US"}

    _assert_record_hashes(rows, contract)


def test_stress_fixtures_have_complete_coverage_and_valid_hashes() -> None:
    scenario_contract = _load_contract("stress_scenarios")
    shock_contract = _load_contract("stress_scenario_shocks")
    scenarios = _load_fixture("stress_scenarios.csv")
    shocks = _load_fixture("stress_scenario_shocks.csv")
    instruments = {row["instrument_id"] for row in _load_fixture("instruments.csv")}
    scenario_ids = {row["scenario_id"] for row in scenarios}

    assert len(scenarios) == 3
    assert len(shocks) == 45
    assert Counter(row["scenario_id"] for row in shocks) == {
        scenario_id: 15 for scenario_id in scenario_ids
    }
    assert len({(row["scenario_id"], row["instrument_id"]) for row in shocks}) == 45
    assert {row["instrument_id"] for row in shocks} == instruments
    assert all(Decimal(row["shock_ratio"]) >= Decimal("-1") for row in shocks)
    assert all(row["shock_rationale"].strip() for row in shocks)

    broad_market = [
        row for row in shocks if row["scenario_id"] == "BROAD_MARKET_DOWN_10"
    ]
    assert all(Decimal(row["shock_ratio"]) == Decimal("-0.10") for row in broad_market)

    _assert_record_hashes(scenarios, scenario_contract)
    _assert_record_hashes(shocks, shock_contract)


def test_contract_artifacts_contain_no_placeholders() -> None:
    placeholder_pattern = re.compile(r"\b(?:HASH_(?:S)?\d+|TODO|TBD)\b")
    artifact_paths = [
        *CONTRACT_DIR.glob("*.yml"),
        *FIXTURE_DIR.glob("*.csv"),
    ]

    for path in artifact_paths:
        content = path.read_text(encoding="utf-8")
        assert placeholder_pattern.search(content) is None, path
