"""Build the pinned NYSE session spine used to admit real Yahoo data."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

import exchange_calendars


def build_xnys_sessions(
    *,
    start_date: date,
    end_date: date,
    calendar: object | None = None,
) -> tuple[str, ...]:
    """Return inclusive NYSE session dates from the pinned calendar package."""
    exchange = (
        exchange_calendars.get_calendar("XNYS")
        if calendar is None
        else calendar
    )
    sessions: Iterable[object] = exchange.sessions_in_range(
        start_date.isoformat(),
        end_date.isoformat(),
    )
    return tuple(session.date().isoformat() for session in sessions)
