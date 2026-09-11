from datetime import UTC, date, datetime

from market_risk_analysis.ingestion.phase_12_exchange_calendar_materializer import (
    build_exchange_calendar_rows,
)


def test_pinned_xnys_calendar_includes_sessions_and_closures() -> None:
    rows = build_exchange_calendar_rows(
        exchange_mic="XNYS",
        start_date=date(2020, 1, 1),
        end_date=date(2025, 12, 31),
        generated_at_utc=datetime(2026, 9, 11, tzinfo=UTC),
    )

    by_date = {row["calendar_date"]: row for row in rows}
    assert len(rows) == 2192
    assert sum(row["is_trading_day"] == "true" for row in rows) == 1508
    assert by_date["2020-01-01"]["is_trading_day"] == "false"
    assert by_date["2020-01-02"]["is_trading_day"] == "true"
    assert by_date["2020-01-02"]["market_open_utc"]
    assert by_date["2020-01-02"]["record_hash"]
    assert all(row["calendar_version"] == "4.13.2" for row in rows)
