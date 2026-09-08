from __future__ import annotations

import csv
import hashlib
from decimal import Decimal
from pathlib import Path

import pytest

from market_risk_analysis.ingestion.phase_06_market_inputs import (
    CORPORATE_ACTION_HASH_FIELDS,
    DAILY_PRICE_HASH_FIELDS,
    NULL_TOKEN,
    build_corporate_action_rows,
    build_daily_price_rows,
    build_history_trading_dates,
    load_instrument_symbols,
    load_scenario,
    materialize_market_inputs,
    validate_history_anchor,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCENARIO_PATH = PROJECT_ROOT / "data/fixtures/phase_06_analytics_scenario.yml"
HISTORY_PATH = PROJECT_ROOT / "data/fixtures/phase_07_risk_history.yml"
INSTRUMENTS_PATH = PROJECT_ROOT / "data/fixtures/instruments.csv"


def _inputs() -> tuple[dict, dict[str, str]]:
    return load_scenario(SCENARIO_PATH), load_instrument_symbols(INSTRUMENTS_PATH)


def _expected_hash(row: dict[str, str], fields: tuple[str, ...]) -> str:
    canonical = "|".join(row[field] or NULL_TOKEN for field in fields)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def test_daily_prices_have_complete_deterministic_grid() -> None:
    scenario, instrument_symbols = _inputs()
    rows = build_daily_price_rows(scenario, instrument_symbols)

    assert len(rows) == 60
    assert len({(row["instrument_id"], row["price_date"]) for row in rows}) == 60
    assert {row["source_id"] for row in rows} == {"PROJECT_GIT_FIXTURE"}
    assert {row["quote_currency"] for row in rows} == {"USD"}
    assert {row["volume"] for row in rows} == {"1000000"}

    counts_by_date = {
        price_date: sum(row["price_date"] == price_date for row in rows)
        for price_date in {row["price_date"] for row in rows}
    }
    assert counts_by_date == {
        "2016-01-04": 15,
        "2016-01-05": 15,
        "2016-01-06": 15,
        "2016-01-07": 15,
    }
    assert all(
        row["open_price"]
        == row["high_price"]
        == row["low_price"]
        == row["close_price"]
        for row in rows
    )
    assert all(
        row["record_hash"] == _expected_hash(row, DAILY_PRICE_HASH_FIELDS)
        for row in rows
    )


def test_adjusted_prices_encode_dividend_and_split_treatment() -> None:
    scenario, instrument_symbols = _inputs()
    rows = build_daily_price_rows(scenario, instrument_symbols)
    by_key = {(row["instrument_id"], row["price_date"]): row for row in rows}

    wmt_dividend = by_key[("WMT_US", "2016-01-06")]
    assert wmt_dividend["close_price"] == "98.00000000"
    assert wmt_dividend["adjusted_close_price"] == "99.00000000"

    nvda_split = by_key[("NVDA_US", "2016-01-07")]
    assert nvda_split["close_price"] == "55.00000000"
    assert nvda_split["adjusted_close_price"] == "110.00000000"


def test_corporate_actions_are_complete_and_conditionally_valid() -> None:
    scenario, instrument_symbols = _inputs()
    rows = build_corporate_action_rows(scenario, instrument_symbols)

    assert len(rows) == 2
    assert len(
        {
            (
                row["instrument_id"],
                row["effective_date"],
                row["action_type"],
            )
            for row in rows
        }
    ) == 2

    by_type = {row["action_type"]: row for row in rows}
    dividend = by_type["CASH_DIVIDEND"]
    assert dividend["instrument_id"] == "WMT_US"
    assert dividend["dividend_amount_per_share"] == "1.00000000"
    assert dividend["dividend_currency"] == "USD"
    assert dividend["split_ratio"] == ""

    split = by_type["STOCK_SPLIT"]
    assert split["instrument_id"] == "NVDA_US"
    assert split["split_ratio"] == "2.0000000000"
    assert split["dividend_amount_per_share"] == ""
    assert split["dividend_currency"] == ""

    assert all(
        row["record_hash"] == _expected_hash(row, CORPORATE_ACTION_HASH_FIELDS)
        for row in rows
    )


def test_materialized_csv_bytes_are_repeatable(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"

    first = materialize_market_inputs(
        project_root=PROJECT_ROOT,
        output_root=first_root,
    )
    second = materialize_market_inputs(
        project_root=PROJECT_ROOT,
        output_root=second_root,
    )

    assert first["DAILY_PRICES"]["record_count"] == 3780
    assert first["CORPORATE_ACTIONS"]["record_count"] == 2

    assert first.keys() == second.keys()
    for dataset_name in first:
        first_path = first[dataset_name]["path"]
        second_path = second[dataset_name]["path"]
        assert first_path.read_bytes() == second_path.read_bytes()
        assert first[dataset_name]["source_sha256"] == second[dataset_name][
            "source_sha256"
        ]

        with first_path.open(newline="", encoding="utf-8") as source_file:
            assert len(list(csv.DictReader(source_file))) == first[dataset_name][
                "record_count"
            ]


def test_phase_07_history_fixture_declares_auditable_var_window() -> None:
    history = load_scenario(HISTORY_PATH)

    assert history["history_id"] == "PHASE_07_DETERMINISTIC_US_EQUITIES_2016"
    assert (
        history["anchor"]["source_fixture"]
        == "data/fixtures/phase_06_analytics_scenario.yml"
    )
    assert history["anchor"]["anchor_date"] == "2016-01-07"
    assert history["anchor"]["preserve_existing_phase_06_rows"] is True

    window = history["history_window"]
    assert window["start_date"] == "2016-01-04"
    assert window["end_date"] == "2016-12-30"
    assert window["expected_trading_date_count"] == 252
    assert window["expected_extension_trading_date_count"] == 248
    assert window["expected_instrument_count"] == 15
    assert window["expected_price_record_count"] == 3780

    policy = history["risk_history_policy"]
    assert policy["holding_period_days"] == 1
    assert policy["confidence_levels"] == [0.95, 0.99]
    assert policy["expected_return_observation_count"] == 251
    assert policy["minimum_return_observation_count"] == 250


def test_materializer_expands_phase_07_history_from_preserved_anchor() -> None:
    scenario, instrument_symbols = _inputs()
    history = load_scenario(HISTORY_PATH)

    rows = build_daily_price_rows(
        scenario,
        instrument_symbols,
        history=history,
    )

    assert len(rows) == 3780
    assert len({(row["instrument_id"], row["price_date"]) for row in rows}) == 3780
    assert len({row["price_date"] for row in rows}) == 252
    assert min(row["price_date"] for row in rows) == "2016-01-04"
    assert max(row["price_date"] for row in rows) == "2016-12-30"

    by_key = {(row["instrument_id"], row["price_date"]): row for row in rows}
    assert by_key[("NVDA_US", "2016-01-07")]["close_price"] == "55.00000000"
    assert by_key[("WMT_US", "2016-01-07")]["adjusted_close_price"] == "99.00000000"

    anchor_rows = build_daily_price_rows(scenario, instrument_symbols)
    assert rows[: len(anchor_rows)] == anchor_rows

    assert all(
        row["source_record_id"].startswith(
            "PHASE_07_DETERMINISTIC_US_EQUITIES_2016:DAILY_PRICES:"
        )
        for row in rows[len(anchor_rows) :]
    )


def test_phase_07_history_date_spine_matches_approved_2016_calendar() -> None:
    history = load_scenario(HISTORY_PATH)

    dates = build_history_trading_dates(history)

    assert len(dates) == 252
    assert dates[0] == "2016-01-04"
    assert dates[-1] == "2016-12-30"
    assert "2016-01-18" not in dates
    assert "2016-07-04" not in dates
    assert "2016-11-24" not in dates
    assert "2016-11-25" in dates


def test_phase_07_price_path_contains_varied_and_valid_returns() -> None:
    scenario, instrument_symbols = _inputs()
    history = load_scenario(HISTORY_PATH)
    rows = build_daily_price_rows(
        scenario,
        instrument_symbols,
        history=history,
    )
    dates = build_history_trading_dates(history)
    by_key = {(row["instrument_id"], row["price_date"]): row for row in rows}

    assert all(Decimal(row["close_price"]) > 0 for row in rows)

    nvda_returns = [
        (
            Decimal(by_key[("NVDA_US", current_date)]["close_price"])
            / Decimal(by_key[("NVDA_US", prior_date)]["close_price"])
            - Decimal("1")
        )
        for prior_date, current_date in zip(dates[:-1], dates[1:], strict=True)
    ]
    assert any(value > 0 for value in nvda_returns)
    assert any(value < 0 for value in nvda_returns)
    assert any(value == 0 for value in nvda_returns)

    first_extension_date = dates[4]
    first_extension_prior_date = dates[3]
    first_extension_returns = {
        Decimal(by_key[(instrument_id, first_extension_date)]["close_price"])
        / Decimal(by_key[(instrument_id, first_extension_prior_date)]["close_price"])
        - Decimal("1")
        for instrument_id in instrument_symbols
    }
    assert len(first_extension_returns) > 1


def test_phase_07_history_fixture_pins_the_phase_06_anchor() -> None:
    history = load_scenario(HISTORY_PATH)

    validate_history_anchor(
        history=history,
        anchor_source_path=SCENARIO_PATH,
    )

    invalid_history = {
        **history,
        "anchor": {
            **history["anchor"],
            "source_sha256": "0" * 64,
        },
    }
    with pytest.raises(ValueError, match="SHA-256 does not match"):
        validate_history_anchor(
            history=invalid_history,
            anchor_source_path=SCENARIO_PATH,
        )
