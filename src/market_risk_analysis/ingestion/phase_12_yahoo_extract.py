from __future__ import annotations

import csv
import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

SOURCE_ID = "YAHOO_FINANCE"
NULL_TOKEN = "<NULL>"
PRICE_SCALE = Decimal("0.00000001")
ACTION_SCALE = Decimal("0.00000001")
SPLIT_SCALE = Decimal("0.0000000000")

DAILY_PRICE_FIELDS = (
    "instrument_id", "price_date", "source_id", "source_symbol", "open_price",
    "high_price", "low_price", "close_price", "adjusted_close_price", "volume",
    "quote_currency", "source_updated_at_utc", "source_record_id", "record_hash",
)
DAILY_PRICE_HASH_FIELDS = DAILY_PRICE_FIELDS[:11]
CORPORATE_ACTION_FIELDS = (
    "instrument_id", "effective_date", "action_type", "source_id",
    "source_symbol", "dividend_amount_per_share", "dividend_currency",
    "split_ratio", "source_action_id", "source_updated_at_utc",
    "source_record_id", "record_hash",
)
CORPORATE_ACTION_HASH_FIELDS = CORPORATE_ACTION_FIELDS[:9]


@dataclass(frozen=True)
class InstrumentMapping:
    instrument_id: str
    provider_symbol: str
    quote_currency: str


def load_active_instrument_mappings(path: Path) -> tuple[InstrumentMapping, ...]:
    with path.open(newline="", encoding="utf-8") as source_file:
        rows = list(csv.DictReader(source_file))
    mappings = tuple(
        InstrumentMapping(
            instrument_id=_required(row, "instrument_id"),
            provider_symbol=_required(row, "yfinance_symbol"),
            quote_currency=_required(row, "quote_currency"),
        )
        for row in rows
        if row.get("is_active") == "true"
    )
    if len(mappings) != 15:
        raise ValueError(f"Expected 15 active instruments; found {len(mappings)}")
    if len({item.instrument_id for item in mappings}) != len(mappings):
        raise ValueError("Active instrument IDs must be unique")
    if len({item.provider_symbol for item in mappings}) != len(mappings):
        raise ValueError("Active provider symbols must be unique")
    if {item.quote_currency for item in mappings} != {"USD"}:
        raise ValueError("The real-data MVP supports USD-quoted instruments only")
    return mappings


def build_daily_price_rows(
    *,
    instrument: InstrumentMapping,
    provider_rows: Iterable[Mapping[str, object]],
    retrieved_at_utc: datetime,
) -> list[dict[str, str]]:
    timestamp = _utc_timestamp(retrieved_at_utc)
    rows: list[dict[str, str]] = []
    for observation in provider_rows:
        price_date = _iso_date(observation.get("Date"), "Date")
        open_price = _decimal(observation.get("Open"), "Open", PRICE_SCALE)
        high_price = _decimal(observation.get("High"), "High", PRICE_SCALE)
        low_price = _decimal(observation.get("Low"), "Low", PRICE_SCALE)
        close_price = _decimal(observation.get("Close"), "Close", PRICE_SCALE)
        adjusted_close = _decimal(observation.get("Adj Close"), "Adj Close", PRICE_SCALE)
        if high_price < max(open_price, low_price, close_price):
            raise ValueError(f"OHLC high is inconsistent for {instrument.instrument_id} {price_date}")
        if low_price > min(open_price, high_price, close_price):
            raise ValueError(f"OHLC low is inconsistent for {instrument.instrument_id} {price_date}")
        volume = _volume(observation.get("Volume"))
        row = {
            "instrument_id": instrument.instrument_id,
            "price_date": price_date,
            "source_id": SOURCE_ID,
            "source_symbol": instrument.provider_symbol,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "adjusted_close_price": adjusted_close,
            "volume": volume,
            "quote_currency": instrument.quote_currency,
            "source_updated_at_utc": timestamp,
            "source_record_id": f"{SOURCE_ID}:DAILY_PRICES:{instrument.instrument_id}:{price_date}",
            "record_hash": "",
        }
        row["record_hash"] = _record_hash(row, DAILY_PRICE_HASH_FIELDS)
        rows.append(row)
    return rows


def build_corporate_action_rows(
    *,
    instrument: InstrumentMapping,
    provider_rows: Iterable[Mapping[str, object]],
    retrieved_at_utc: datetime,
) -> list[dict[str, str]]:
    timestamp = _utc_timestamp(retrieved_at_utc)
    rows: list[dict[str, str]] = []
    for event in provider_rows:
        effective_date = _iso_date(event.get("Date"), "Date")
        dividend = _nonnegative_decimal(event.get("Dividends"), "Dividends", ACTION_SCALE)
        split = _nonnegative_decimal(event.get("Stock Splits"), "Stock Splits", SPLIT_SCALE)
        if dividend > Decimal() and split > Decimal():
            raise ValueError(f"Provider event has dividend and split values for {instrument.instrument_id} {effective_date}")
        if dividend == Decimal() and split in {Decimal(), Decimal("1")}:
            continue
        if dividend > Decimal():
            action_type, amount, currency, ratio = "CASH_DIVIDEND", _format(dividend, ACTION_SCALE), instrument.quote_currency, ""
        else:
            if split == Decimal("1"):
                continue
            action_type, amount, currency, ratio = "STOCK_SPLIT", "", "", _format(split, SPLIT_SCALE)
        row = {
            "instrument_id": instrument.instrument_id,
            "effective_date": effective_date,
            "action_type": action_type,
            "source_id": SOURCE_ID,
            "source_symbol": instrument.provider_symbol,
            "dividend_amount_per_share": amount,
            "dividend_currency": currency,
            "split_ratio": ratio,
            "source_action_id": "",
            "source_updated_at_utc": timestamp,
            "source_record_id": f"{SOURCE_ID}:CORPORATE_ACTIONS:{instrument.instrument_id}:{effective_date}:{action_type}",
            "record_hash": "",
        }
        row["record_hash"] = _record_hash(row, CORPORATE_ACTION_HASH_FIELDS)
        rows.append(row)
    return rows


def write_csv(path: Path, *, fieldnames: Sequence[str], rows: Sequence[Mapping[str, str]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _required(row: Mapping[str, str | None], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be nonempty")
    return value


def _iso_date(value: object, label: str) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        return date.fromisoformat(value[:10]).isoformat()
    raise ValueError(f"{label} must be a date or ISO date string")


def _decimal(value: object, label: str, scale: Decimal) -> str:
    parsed = _nonnegative_decimal(value, label, scale)
    if parsed <= Decimal():
        raise ValueError(f"{label} must be greater than zero")
    return _format(parsed, scale)


def _nonnegative_decimal(value: object, label: str, scale: Decimal) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{label} is not a decimal") from exc
    if not parsed.is_finite() or parsed < Decimal():
        raise ValueError(f"{label} must be finite and nonnegative")
    return parsed.quantize(scale, rounding=ROUND_HALF_UP)


def _volume(value: object) -> str:
    try:
        parsed = int(str(value))
    except ValueError as exc:
        raise ValueError("Volume must be an integer") from exc
    if parsed < 0:
        raise ValueError("Volume must be nonnegative")
    return str(parsed)


def _format(value: Decimal, scale: Decimal) -> str:
    return format(value.quantize(scale, rounding=ROUND_HALF_UP), "f")


def _utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("retrieved_at_utc must be timezone-aware")
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _record_hash(row: Mapping[str, str], fields: Sequence[str]) -> str:
    canonical = "|".join(row[field] or NULL_TOKEN for field in fields)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
