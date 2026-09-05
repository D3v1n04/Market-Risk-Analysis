from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from market_risk_analysis.ingestion.phase_06_market_inputs import (
    CORPORATE_ACTION_HASH_FIELDS,
    DAILY_PRICE_HASH_FIELDS,
    NULL_TOKEN,
    build_corporate_action_rows,
    build_daily_price_rows,
    load_instrument_symbols,
    load_scenario,
    materialize_market_inputs,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCENARIO_PATH = PROJECT_ROOT / "data/fixtures/phase_06_analytics_scenario.yml"
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
