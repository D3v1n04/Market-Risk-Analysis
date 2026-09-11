from datetime import date

from market_risk_analysis.ingestion.phase_12_exchange_calendar import (
    build_xnys_sessions,
)


def test_pinned_xnys_calendar_matches_real_yahoo_session_spine() -> None:
    sessions = build_xnys_sessions(
        start_date=date(2020, 1, 1),
        end_date=date(2025, 12, 31),
    )

    assert len(sessions) == 1508
    assert sessions[0] == "2020-01-02"
    assert sessions[-1] == "2025-12-31"
    assert len(set(sessions)) == len(sessions)
