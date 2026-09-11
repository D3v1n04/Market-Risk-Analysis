"""Materialize pinned US-equity exchange calendars for real-data admission."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from typing import Any

import exchange_calendars

CALENDAR_CLIENT_VERSION = "4.13.2"
EXCHANGE_MIC_CODES = ("XNAS", "XNYS")
NULL_TOKEN = "<NULL>"


def _utc_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace(
        "+00:00",
        "Z",
    )


def _record_hash(row: dict[str, str]) -> str:
    fields = (
        "exchange_mic",
        "calendar_date",
        "is_trading_day",
        "market_open_utc",
        "market_close_utc",
        "is_early_close",
        "holiday_name",
        "exchange_timezone",
        "calendar_source",
        "calendar_version",
    )
    canonical = "|".join(row[field] or NULL_TOKEN for field in fields)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_exchange_calendar_rows(
    *,
    exchange_mic: str,
    start_date: date,
    end_date: date,
    generated_at_utc: datetime,
    calendar: Any | None = None,
) -> list[dict[str, str]]:
    """Return every calendar date for one exchange with pinned-session evidence."""
    if exchange_mic not in EXCHANGE_MIC_CODES:
        raise ValueError(f"Unsupported exchange MIC: {exchange_mic}")
    if end_date < start_date:
        raise ValueError("end_date must not be before start_date")

    exchange = (
        exchange_calendars.get_calendar(exchange_mic)
        if calendar is None
        else calendar
    )
    sessions = exchange.sessions_in_range(
        start_date.isoformat(),
        end_date.isoformat(),
    )
    session_by_date = {session.date(): session for session in sessions}
    early_sessions = set(exchange.early_closes)
    rows: list[dict[str, str]] = []
    current_date = start_date

    while current_date <= end_date:
        session = session_by_date.get(current_date)
        if session is None:
            row = {
                "exchange_mic": exchange_mic,
                "calendar_date": current_date.isoformat(),
                "is_trading_day": "false",
                "market_open_utc": "",
                "market_close_utc": "",
                "is_early_close": "false",
                "holiday_name": "",
                "exchange_timezone": "America/New_York",
                "calendar_source": "exchange_calendars",
                "calendar_version": CALENDAR_CLIENT_VERSION,
                "generated_at_utc": _utc_timestamp(generated_at_utc),
                "record_hash": "",
            }
        else:
            open_time = exchange.session_open(session).to_pydatetime()
            close_time = exchange.session_close(session).to_pydatetime()
            row = {
                "exchange_mic": exchange_mic,
                "calendar_date": current_date.isoformat(),
                "is_trading_day": "true",
                "market_open_utc": _utc_timestamp(open_time),
                "market_close_utc": _utc_timestamp(close_time),
                "is_early_close": str(session in early_sessions).lower(),
                "holiday_name": "",
                "exchange_timezone": "America/New_York",
                "calendar_source": "exchange_calendars",
                "calendar_version": CALENDAR_CLIENT_VERSION,
                "generated_at_utc": _utc_timestamp(generated_at_utc),
                "record_hash": "",
            }
        row["record_hash"] = _record_hash(row)
        rows.append(row)
        current_date += timedelta(days=1)
    return rows
