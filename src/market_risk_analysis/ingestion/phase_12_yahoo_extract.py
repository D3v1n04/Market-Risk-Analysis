from __future__ import annotations

import argparse
import csv
import hashlib
import json
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
    "instrument_id",
    "price_date",
    "source_id",
    "source_symbol",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "adjusted_close_price",
    "volume",
    "quote_currency",
    "source_updated_at_utc",
    "source_record_id",
    "record_hash",
)
DAILY_PRICE_HASH_FIELDS = DAILY_PRICE_FIELDS[:11]
CORPORATE_ACTION_FIELDS = (
    "instrument_id",
    "effective_date",
    "action_type",
    "source_id",
    "source_symbol",
    "dividend_amount_per_share",
    "dividend_currency",
    "split_ratio",
    "source_action_id",
    "source_updated_at_utc",
    "source_record_id",
    "record_hash",
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
    _utc_timestamp(retrieved_at_utc)
    rows: list[dict[str, str]] = []
    for observation in provider_rows:
        price_date = _iso_date(observation.get("Date"), "Date")
        open_price = _decimal(observation.get("Open"), "Open", PRICE_SCALE)
        high_price = _decimal(observation.get("High"), "High", PRICE_SCALE)
        low_price = _decimal(observation.get("Low"), "Low", PRICE_SCALE)
        close_price = _decimal(observation.get("Close"), "Close", PRICE_SCALE)
        adjusted_close = _decimal(
            observation.get("Adj Close"), "Adj Close", PRICE_SCALE
        )
        if Decimal(high_price) < max(
            Decimal(open_price), Decimal(low_price), Decimal(close_price)
        ):
            raise ValueError(
                f"OHLC high is inconsistent for {instrument.instrument_id} {price_date}"
            )
        if Decimal(low_price) > min(
            Decimal(open_price), Decimal(high_price), Decimal(close_price)
        ):
            raise ValueError(
                f"OHLC low is inconsistent for {instrument.instrument_id} {price_date}"
            )
        volume = _volume(observation.get("Volume"))
        source_record_id = (
            f"{SOURCE_ID}:DAILY_PRICES:{instrument.instrument_id}:"
            f"{price_date}"
        )
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
            "source_updated_at_utc": "",
            "source_record_id": source_record_id,
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
        dividend = _nonnegative_decimal(
            event.get("Dividends"), "Dividends", ACTION_SCALE
        )
        split = _nonnegative_decimal(
            event.get("Stock Splits"), "Stock Splits", SPLIT_SCALE
        )
        if dividend > Decimal() and split > Decimal():
            message = (
                "Provider event has dividend and split values for "
                f"{instrument.instrument_id} {effective_date}"
            )
            raise ValueError(message)
        if dividend == Decimal() and split in {Decimal(), Decimal("1")}:
            continue
        if dividend > Decimal():
            action_type, amount, currency, ratio = (
                "CASH_DIVIDEND",
                _format(dividend, ACTION_SCALE),
                instrument.quote_currency,
                "",
            )
        else:
            if split == Decimal("1"):
                continue
            action_type, amount, currency, ratio = (
                "STOCK_SPLIT",
                "",
                "",
                _format(split, SPLIT_SCALE),
            )
        source_record_id = (
            f"{SOURCE_ID}:CORPORATE_ACTIONS:{instrument.instrument_id}:"
            f"{effective_date}:{action_type}"
        )
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
            "source_updated_at_utc": "",
            "source_record_id": source_record_id,
            "record_hash": "",
        }
        row["record_hash"] = _record_hash(row, CORPORATE_ACTION_HASH_FIELDS)
        rows.append(row)
    return rows


def write_csv(
    path: Path, *, fieldnames: Sequence[str], rows: Sequence[Mapping[str, str]]
) -> str:
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
    return (
        value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def _record_hash(row: Mapping[str, str], fields: Sequence[str]) -> str:
    canonical = "|".join(row[field] or NULL_TOKEN for field in fields)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fetch_yfinance_rows(
    *,
    instrument: InstrumentMapping,
    start_date: str,
    end_date_exclusive: str,
    history_fetcher: object | None = None,
) -> list[dict[str, object]]:
    """Fetch one provider ticker; dependency injection keeps tests network-free."""
    if history_fetcher is None:
        import yfinance

        def history_fetcher(symbol: str) -> object:
            return yfinance.Ticker(symbol).history(
                start=start_date,
                end=end_date_exclusive,
                interval="1d",
                auto_adjust=False,
                actions=True,
            )
    frame = history_fetcher(instrument.provider_symbol)
    if getattr(frame, "empty", False):
        return []
    return list(frame.reset_index().to_dict(orient="records"))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract approved Yahoo Finance history into private raw files."
    )
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/raw/yahoo_finance"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    project_root = args.project_root.resolve()
    output_root = (project_root / args.output_root).resolve()
    mappings = load_active_instrument_mappings(
        project_root / "data/fixtures/instruments.csv"
    )
    retrieved_at_utc = datetime.now(UTC)
    price_rows: list[dict[str, str]] = []
    action_rows: list[dict[str, str]] = []
    failures: list[str] = []
    for instrument in mappings:
        try:
            provider_rows = fetch_yfinance_rows(
                instrument=instrument,
                start_date="2020-01-01",
                end_date_exclusive="2026-01-01",
            )
            if not provider_rows:
                failures.append(f"{instrument.instrument_id}: EMPTY_RESPONSE")
                continue
            price_rows.extend(
                build_daily_price_rows(
                    instrument=instrument,
                    provider_rows=provider_rows,
                    retrieved_at_utc=retrieved_at_utc,
                )
            )
            action_rows.extend(
                build_corporate_action_rows(
                    instrument=instrument,
                    provider_rows=provider_rows,
                    retrieved_at_utc=retrieved_at_utc,
                )
            )
        except Exception as exc:
            failures.append(f"{instrument.instrument_id}: {type(exc).__name__}")
    staging_root = output_root / "_staging"
    price_path = staging_root / "daily_prices.csv"
    action_path = staging_root / "corporate_actions.csv"
    price_sha256 = write_csv(
        price_path, fieldnames=DAILY_PRICE_FIELDS, rows=price_rows
    )
    action_sha256 = write_csv(
        action_path, fieldnames=CORPORATE_ACTION_FIELDS, rows=action_rows
    )
    price_target = output_root / "daily_prices" / price_sha256 / "daily_prices.csv"
    action_target = (
        output_root
        / "corporate_actions"
        / action_sha256
        / "corporate_actions.csv"
    )
    price_target.parent.mkdir(parents=True, exist_ok=True)
    action_target.parent.mkdir(parents=True, exist_ok=True)
    if price_target.exists():
        price_path.unlink()
    else:
        price_path.replace(price_target)
    if action_target.exists():
        action_path.unlink()
    else:
        action_path.replace(action_target)
    mapping_path = project_root / "data/fixtures/instruments.csv"
    mapping_sha256 = hashlib.sha256(mapping_path.read_bytes()).hexdigest()
    run_sha256 = hashlib.sha256(
        f"{price_sha256}|{action_sha256}|{mapping_sha256}".encode()
    ).hexdigest()
    manifest_path = output_root / "runs" / run_sha256 / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "source_id": SOURCE_ID,
        "client_version": "1.7.0",
        "request": {
            "start_date": "2020-01-01",
            "end_date_exclusive": "2026-01-01",
            "interval": "1d",
        },
        "retrieved_at_utc": _utc_timestamp(retrieved_at_utc),
        "instrument_count": len(mappings),
        "daily_prices": {
            "path": price_target.relative_to(output_root).as_posix(),
            "record_count": len(price_rows),
            "sha256": price_sha256,
        },
        "corporate_actions": {
            "path": action_target.relative_to(output_root).as_posix(),
            "record_count": len(action_rows),
            "sha256": action_sha256,
        },
        "instrument_mapping_sha256": mapping_sha256,
        "run_sha256": run_sha256,
        "failures": failures,
        "status": "PARTIAL" if failures else "SUCCEEDED",
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"manifest_path={manifest_path}")
    print(json.dumps(manifest, sort_keys=True))
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
