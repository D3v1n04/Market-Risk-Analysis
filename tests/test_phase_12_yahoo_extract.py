from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_risk_analysis.ingestion.phase_12_yahoo_extract import (
    SOURCE_ID,
    InstrumentMapping,
    build_corporate_action_rows,
    build_daily_price_rows,
    fetch_yfinance_rows,
    load_active_instrument_mappings,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RETRIEVED_AT = datetime(2026, 9, 11, 5, 0, tzinfo=UTC)
NVDA = InstrumentMapping("NVDA_US", "NVDA", "USD")


def test_active_instrument_mapping_preserves_provider_ticker() -> None:
    mappings = load_active_instrument_mappings(
        PROJECT_ROOT / "data/fixtures/instruments.csv"
    )
    assert len(mappings) == 15
    assert (
        next(
            item for item in mappings if item.instrument_id == "BRK_B_US"
        ).provider_symbol
        == "BRK-B"
    )


def test_provider_price_rows_become_contract_shaped_records() -> None:
    rows = build_daily_price_rows(
        instrument=NVDA,
        retrieved_at_utc=RETRIEVED_AT,
        provider_rows=[
            {
                "Date": "2020-01-02",
                "Open": "100",
                "High": "102",
                "Low": "99",
                "Close": "101",
                "Adj Close": "100.5",
                "Volume": 123,
            }
        ],
    )
    assert rows[0]["source_id"] == SOURCE_ID
    assert rows[0]["price_date"] == "2020-01-02"
    assert rows[0]["source_updated_at_utc"] == "2026-09-11T05:00:00Z"
    assert rows[0]["record_hash"]
    assert (
        rows[0]["source_record_id"] == "YAHOO_FINANCE:DAILY_PRICES:NVDA_US:2020-01-02"
    )


def test_missing_or_invalid_provider_price_is_rejected() -> None:
    with pytest.raises(ValueError, match="OHLC high"):
        build_daily_price_rows(
            instrument=NVDA,
            retrieved_at_utc=RETRIEVED_AT,
            provider_rows=[
                {
                    "Date": "2020-01-02",
                    "Open": 100,
                    "High": 99,
                    "Low": 98,
                    "Close": 101,
                    "Adj Close": 101,
                    "Volume": 1,
                }
            ],
        )


def test_zero_action_event_is_valid_but_not_materialized() -> None:
    rows = build_corporate_action_rows(
        instrument=NVDA,
        retrieved_at_utc=RETRIEVED_AT,
        provider_rows=[{"Date": "2020-01-02", "Dividends": 0, "Stock Splits": 0}],
    )
    assert rows == []


def test_dividend_and_split_rows_follow_conditional_contract() -> None:
    rows = build_corporate_action_rows(
        instrument=NVDA,
        retrieved_at_utc=RETRIEVED_AT,
        provider_rows=[
            {"Date": "2020-01-02", "Dividends": "0.25", "Stock Splits": 0},
            {"Date": "2020-08-31", "Dividends": 0, "Stock Splits": 4},
        ],
    )
    assert [row["action_type"] for row in rows] == ["CASH_DIVIDEND", "STOCK_SPLIT"]
    assert rows[0]["dividend_currency"] == "USD"
    assert rows[0]["split_ratio"] == ""
    assert rows[1]["split_ratio"] == "4.0000000000"
    assert rows[1]["dividend_amount_per_share"] == ""


def test_injected_downloader_receives_provider_ticker() -> None:
    requested_symbols: list[str] = []

    class FakeFrame:
        empty = False

        def reset_index(self) -> FakeFrame:
            return self

        def to_dict(self, *, orient: str) -> list[dict[str, object]]:
            assert orient == "records"
            return [{"Date": "2020-01-02", "Close": 101}]

    def fetch(symbol: str) -> FakeFrame:
        requested_symbols.append(symbol)
        return FakeFrame()

    rows = fetch_yfinance_rows(
        instrument=NVDA,
        start_date="2020-01-01",
        end_date_exclusive="2026-01-01",
        history_fetcher=fetch,
    )

    assert requested_symbols == ["NVDA"]
    assert rows == [{"Date": "2020-01-02", "Close": 101}]


def test_empty_injected_downloader_response_is_visible() -> None:
    class EmptyFrame:
        empty = True

    assert (
        fetch_yfinance_rows(
            instrument=NVDA,
            start_date="2020-01-01",
            end_date_exclusive="2026-01-01",
            history_fetcher=lambda _symbol: EmptyFrame(),
        )
        == []
    )
