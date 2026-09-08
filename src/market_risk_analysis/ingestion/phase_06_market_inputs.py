from __future__ import annotations

import argparse
import csv
import hashlib
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

import yaml

SCENARIO_PATH = Path("data/fixtures/phase_06_analytics_scenario.yml")
HISTORY_PATH = Path("data/fixtures/phase_07_risk_history.yml")
INSTRUMENTS_PATH = Path("data/fixtures/instruments.csv")
DEFAULT_OUTPUT_ROOT = Path("data/raw/phase_06_analytics_foundation")
SOURCE_ID = "PROJECT_GIT_FIXTURE"
NULL_TOKEN = "<NULL>"


US_EQUITIES_2016_HOLIDAYS = frozenset(
    {
        date(2016, 1, 1),
        date(2016, 1, 18),
        date(2016, 2, 15),
        date(2016, 3, 25),
        date(2016, 5, 30),
        date(2016, 7, 4),
        date(2016, 9, 5),
        date(2016, 11, 24),
        date(2016, 12, 26),
    }
)


def build_history_trading_dates(history: Mapping[str, Any]) -> list[str]:
    window = _require_mapping(history["history_window"], "history_window")
    calendar_version = _require_string(
        window["trading_calendar_version"],
        "history trading_calendar_version",
    )
    if calendar_version != "US_EQUITIES_2016_V1":
        raise ValueError(f"Unsupported history calendar version: {calendar_version}")

    start_date = date.fromisoformat(
        _require_string(window["start_date"], "history start_date")
    )
    end_date = date.fromisoformat(
        _require_string(window["end_date"], "history end_date")
    )
    if end_date < start_date:
        raise ValueError("History end_date must be on or after start_date")

    dates: list[str] = []
    current_date = start_date
    while current_date <= end_date:
        if (
            current_date.weekday() < 5
            and current_date not in US_EQUITIES_2016_HOLIDAYS
        ):
            dates.append(current_date.isoformat())
        current_date += timedelta(days=1)

    expected_count = int(window["expected_trading_date_count"])
    if len(dates) != expected_count:
        raise ValueError(
            "Unexpected history trading-date count: "
            f"actual={len(dates)} expected={expected_count}"
        )
    return dates


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

DAILY_PRICE_HASH_FIELDS = (
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
)

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

CORPORATE_ACTION_HASH_FIELDS = (
    "instrument_id",
    "effective_date",
    "action_type",
    "source_id",
    "source_symbol",
    "dividend_amount_per_share",
    "dividend_currency",
    "split_ratio",
    "source_action_id",
)


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a mapping")
    return value


def _require_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a nonempty string")
    return value


def validate_history_anchor(
    *,
    history: Mapping[str, Any],
    anchor_source_path: Path,
) -> None:
    anchor = _require_mapping(history["anchor"], "history anchor")
    source_fixture = _require_string(
        anchor["source_fixture"],
        "history anchor source_fixture",
    )
    if source_fixture != SCENARIO_PATH.as_posix():
        raise ValueError("History anchor source fixture is unsupported")

    expected_sha256 = _require_string(
        anchor["source_sha256"],
        "history anchor source_sha256",
    )
    actual_sha256 = hashlib.sha256(anchor_source_path.read_bytes()).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError(
            "History anchor source SHA-256 does not match the pinned fixture"
        )


def _record_hash(row: Mapping[str, str], fields: Sequence[str]) -> str:
    canonical = "|".join(row[field] or NULL_TOKEN for field in fields)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load_scenario(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("Phase 06 scenario must contain one YAML mapping")
    return loaded


def load_instrument_symbols(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8") as source_file:
        rows = list(csv.DictReader(source_file))

    symbols: dict[str, str] = {}
    for row in rows:
        instrument_id = _require_string(row.get("instrument_id"), "instrument_id")
        source_symbol = _require_string(
            row.get("yfinance_symbol"), "yfinance_symbol"
        )
        if instrument_id in symbols:
            raise ValueError(f"Duplicate instrument_id: {instrument_id}")
        symbols[instrument_id] = source_symbol
    return symbols


def _resolve_series(
    *,
    specifications: Mapping[str, Any],
    ordered_dates: Sequence[str],
    instrument_ids: Sequence[str],
    raw_series: Mapping[str, Mapping[str, str]] | None = None,
) -> dict[str, dict[str, str]]:
    resolved: dict[str, dict[str, str]] = {}
    expected_instruments = set(instrument_ids)

    for price_date in ordered_dates:
        date_specification = _require_mapping(
            specifications.get(price_date), f"price specification for {price_date}"
        )

        if date_specification.get("equals_raw_close") is True:
            if raw_series is None or price_date not in raw_series:
                raise ValueError(f"Raw close is unavailable for {price_date}")
            values = dict(raw_series[price_date])
        elif "all_instruments" in date_specification:
            value = _require_string(
                date_specification["all_instruments"],
                f"all-instrument price for {price_date}",
            )
            values = {instrument_id: value for instrument_id in instrument_ids}
        elif "carry_forward_from" in date_specification:
            prior_date = _require_string(
                date_specification["carry_forward_from"],
                f"carry-forward date for {price_date}",
            )
            if prior_date not in resolved:
                raise ValueError(
                    f"Carry-forward source {prior_date} is unavailable for {price_date}"
                )
            values = dict(resolved[prior_date])
        else:
            values = {
                instrument_id: _require_string(
                    date_specification.get(instrument_id),
                    f"{instrument_id} price for {price_date}",
                )
                for instrument_id in instrument_ids
            }

        overrides = date_specification.get("overrides", {})
        override_mapping = _require_mapping(overrides, f"overrides for {price_date}")
        unknown_overrides = set(override_mapping) - expected_instruments
        if unknown_overrides:
            raise ValueError(
                f"Unknown price overrides for {price_date}: {sorted(unknown_overrides)}"
            )
        values.update(
            {
                instrument_id: _require_string(
                    value, f"{instrument_id} override for {price_date}"
                )
                for instrument_id, value in override_mapping.items()
            }
        )

        if set(values) != expected_instruments:
            raise ValueError(f"Incomplete instrument coverage for {price_date}")
        resolved[price_date] = values

    return resolved


def build_daily_price_rows(
    scenario: Mapping[str, Any],
    instrument_symbols: Mapping[str, str],
    *,
    history: Mapping[str, Any] | None = None,
) -> list[dict[str, str]]:
    instrument_ids = [
        _require_string(value, "scenario instrument_id")
        for value in scenario["instrument_ids"]
    ]
    if set(instrument_ids) != set(instrument_symbols):
        raise ValueError("Scenario and instrument fixture universes differ")

    business_dates = _require_mapping(scenario["business_dates"], "business_dates")
    ordered_dates = [
        _require_string(value, "business date") for value in business_dates.values()
    ]
    if len(ordered_dates) != len(set(ordered_dates)):
        raise ValueError("Scenario business dates must be unique")

    generation = _require_mapping(scenario["price_generation"], "price_generation")
    for flag in ("open_equals_close", "high_equals_close", "low_equals_close"):
        if generation.get(flag) is not True:
            raise ValueError(f"The deterministic fixture requires {flag}=true")

    raw_close = _resolve_series(
        specifications=_require_mapping(
            generation["raw_close_by_date"], "raw_close_by_date"
        ),
        ordered_dates=ordered_dates,
        instrument_ids=instrument_ids,
    )
    adjusted_close = _resolve_series(
        specifications=_require_mapping(
            generation["adjusted_close_by_date"], "adjusted_close_by_date"
        ),
        ordered_dates=ordered_dates,
        instrument_ids=instrument_ids,
        raw_series=raw_close,
    )

    scenario_id = _require_string(scenario["scenario_id"], "scenario_id")
    volume = str(generation["default_volume"])
    quote_currency = _require_string(
        generation["quote_currency"], "quote_currency"
    )
    rows: list[dict[str, str]] = []

    for price_date in ordered_dates:
        for instrument_id in instrument_ids:
            close_price = raw_close[price_date][instrument_id]
            row = {
                "instrument_id": instrument_id,
                "price_date": price_date,
                "source_id": SOURCE_ID,
                "source_symbol": instrument_symbols[instrument_id],
                "open_price": close_price,
                "high_price": close_price,
                "low_price": close_price,
                "close_price": close_price,
                "adjusted_close_price": adjusted_close[price_date][instrument_id],
                "volume": volume,
                "quote_currency": quote_currency,
                "source_updated_at_utc": "",
                "source_record_id": (
                    f"{scenario_id}:DAILY_PRICES:{instrument_id}:{price_date}"
                ),
                "record_hash": "",
            }
            row["record_hash"] = _record_hash(row, DAILY_PRICE_HASH_FIELDS)
            rows.append(row)

    if history is None:
        return rows

    history_currency = _require_string(history["currency"], "history currency")
    if history_currency != quote_currency:
        raise ValueError("History and Phase 06 quote currencies differ")

    anchor = _require_mapping(history["anchor"], "history anchor")
    anchor_date = _require_string(anchor["anchor_date"], "history anchor_date")
    if anchor_date != ordered_dates[-1]:
        raise ValueError("History anchor_date must equal the final Phase 06 date")

    price_path = _require_mapping(history["price_path"], "history price_path")
    if price_path.get("generator") != "DETERMINISTIC_FACTOR_PATH_V1":
        raise ValueError("Unsupported deterministic history generator")
    seed = _require_string(price_path["seed"], "history price-path seed")
    history_id = _require_string(history["history_id"], "history_id")

    history_dates = build_history_trading_dates(history)
    if set(ordered_dates) - set(history_dates):
        raise ValueError("Phase 06 dates are absent from the history date spine")

    extension_dates = [
        price_date for price_date in history_dates if price_date not in raw_close
    ]
    expected_extension_count = int(
        _require_mapping(history["history_window"], "history_window")[
            "expected_extension_trading_date_count"
        ]
    )
    if len(extension_dates) != expected_extension_count:
        raise ValueError("Unexpected deterministic history extension-date count")

    expected_price_count = int(
        _require_mapping(history["history_window"], "history_window")[
            "expected_price_record_count"
        ]
    )

    raw_prices = {
        instrument_id: Decimal(raw_close[anchor_date][instrument_id])
        for instrument_id in instrument_ids
    }
    adjusted_multipliers = {
        instrument_id: (
            Decimal(adjusted_close[anchor_date][instrument_id])
            / Decimal(raw_close[anchor_date][instrument_id])
        )
        for instrument_id in instrument_ids
    }

    for date_index, price_date in enumerate(extension_dates, start=1):
        for instrument_id in instrument_ids:
            return_ratio = _deterministic_return_ratio(
                seed=seed,
                instrument_id=instrument_id,
                price_date=price_date,
                date_index=date_index,
            )
            raw_prices[instrument_id] = (
                raw_prices[instrument_id] * (Decimal("1") + return_ratio)
            ).quantize(PRICE_SCALE, rounding=ROUND_HALF_UP)

            close_price = _format_price(raw_prices[instrument_id])
            adjusted_close_price = _format_price(
                raw_prices[instrument_id] * adjusted_multipliers[instrument_id]
            )
            row = {
                "instrument_id": instrument_id,
                "price_date": price_date,
                "source_id": SOURCE_ID,
                "source_symbol": instrument_symbols[instrument_id],
                "open_price": close_price,
                "high_price": close_price,
                "low_price": close_price,
                "close_price": close_price,
                "adjusted_close_price": adjusted_close_price,
                "volume": volume,
                "quote_currency": quote_currency,
                "source_updated_at_utc": "",
                "source_record_id": (
                    f"{history_id}:DAILY_PRICES:{instrument_id}:{price_date}"
                ),
                "record_hash": "",
            }
            row["record_hash"] = _record_hash(row, DAILY_PRICE_HASH_FIELDS)
            rows.append(row)

    if len(rows) != expected_price_count:
        raise ValueError(
            f"Unexpected daily-price count: actual={len(rows)} "
            f"expected={expected_price_count}"
        )
    return rows


PRICE_SCALE = Decimal("0.00000001")

CLUSTER_BY_INSTRUMENT = {
    "NVDA_US": "TECH",
    "AAPL_US": "TECH",
    "GOOGL_US": "TECH",
    "MSFT_US": "TECH",
    "TSM_US": "SEMIS",
    "AVGO_US": "SEMIS",
    "AMZN_US": "CONSUMER",
    "WMT_US": "CONSUMER",
    "LLY_US": "HEALTHCARE",
    "UNH_US": "HEALTHCARE",
    "JPM_US": "FINANCIALS",
    "BRK_B_US": "FINANCIALS",
    "XOM_US": "MACRO",
    "TSLA_US": "MACRO",
    "LMT_US": "MACRO",
}


def _signed_basis_points(*, seed: str, label: str, magnitude: int) -> int:
    digest = hashlib.sha256(f"{seed}|{label}".encode()).hexdigest()
    return int(digest[:8], 16) % (2 * magnitude + 1) - magnitude


def _deterministic_return_ratio(
    *,
    seed: str,
    instrument_id: str,
    price_date: str,
    date_index: int,
) -> Decimal:
    if date_index % 29 == 0:
        return Decimal("0")

    cluster = CLUSTER_BY_INSTRUMENT[instrument_id]
    market_bps = _signed_basis_points(
        seed=seed,
        label=f"MARKET|{price_date}",
        magnitude=120,
    )
    cluster_bps = _signed_basis_points(
        seed=seed,
        label=f"CLUSTER|{cluster}|{price_date}",
        magnitude=55,
    )
    instrument_bps = _signed_basis_points(
        seed=seed,
        label=f"INSTRUMENT|{instrument_id}|{price_date}",
        magnitude=35,
    )
    return Decimal(market_bps + cluster_bps + instrument_bps) / Decimal("10000")


def _format_price(value: Decimal) -> str:
    return format(
        value.quantize(PRICE_SCALE, rounding=ROUND_HALF_UP),
        "f",
    )


def build_corporate_action_rows(
    scenario: Mapping[str, Any],
    instrument_symbols: Mapping[str, str],
) -> list[dict[str, str]]:
    scenario_id = _require_string(scenario["scenario_id"], "scenario_id")
    rows: list[dict[str, str]] = []

    for action_value in scenario["corporate_actions"]:
        action = _require_mapping(action_value, "corporate action")
        instrument_id = _require_string(action["instrument_id"], "instrument_id")
        if instrument_id not in instrument_symbols:
            raise ValueError(f"Unknown corporate-action instrument: {instrument_id}")
        effective_date = _require_string(action["effective_date"], "effective_date")
        action_type = _require_string(action["action_type"], "action_type")
        row = {
            "instrument_id": instrument_id,
            "effective_date": effective_date,
            "action_type": action_type,
            "source_id": SOURCE_ID,
            "source_symbol": instrument_symbols[instrument_id],
            "dividend_amount_per_share": str(
                action.get("dividend_amount_per_share") or ""
            ),
            "dividend_currency": str(action.get("dividend_currency") or ""),
            "split_ratio": str(action.get("split_ratio") or ""),
            "source_action_id": "",
            "source_updated_at_utc": "",
            "source_record_id": (
                f"{scenario_id}:CORPORATE_ACTIONS:{instrument_id}:"
                f"{effective_date}:{action_type}"
            ),
            "record_hash": "",
        }
        row["record_hash"] = _record_hash(row, CORPORATE_ACTION_HASH_FIELDS)
        rows.append(row)

    return rows


def write_csv(
    path: Path,
    *,
    fieldnames: Sequence[str],
    rows: Sequence[Mapping[str, str]],
) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=fieldnames,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def materialize_market_inputs(
    *,
    project_root: Path,
    output_root: Path,
) -> dict[str, dict[str, Any]]:
    scenario = load_scenario(project_root / SCENARIO_PATH)
    history = load_scenario(project_root / HISTORY_PATH)
    validate_history_anchor(
        history=history,
        anchor_source_path=project_root / SCENARIO_PATH,
    )
    instrument_symbols = load_instrument_symbols(project_root / INSTRUMENTS_PATH)
    price_rows = build_daily_price_rows(
        scenario,
        instrument_symbols,
        history=history,
    )
    action_rows = build_corporate_action_rows(scenario, instrument_symbols)

    specifications = {
        "DAILY_PRICES": {
            "path": output_root / "daily_prices.csv",
            "fieldnames": DAILY_PRICE_FIELDS,
            "rows": price_rows,
        },
        "CORPORATE_ACTIONS": {
            "path": output_root / "corporate_actions.csv",
            "fieldnames": CORPORATE_ACTION_FIELDS,
            "rows": action_rows,
        },
    }
    results: dict[str, dict[str, Any]] = {}
    for dataset_name, specification in specifications.items():
        output_path = specification["path"]
        rows = specification["rows"]
        source_sha256 = write_csv(
            output_path,
            fieldnames=specification["fieldnames"],
            rows=rows,
        )
        results[dataset_name] = {
            "path": output_path,
            "record_count": len(rows),
            "source_sha256": source_sha256,
        }
    return results


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize deterministic Phase 06 market-input fixtures."
    )
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--output-root", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    project_root = args.project_root.resolve()
    output_root = (
        args.output_root.resolve()
        if args.output_root is not None
        else project_root / DEFAULT_OUTPUT_ROOT
    )
    results = materialize_market_inputs(
        project_root=project_root,
        output_root=output_root,
    )
    for dataset_name, result in results.items():
        print(f"dataset_name={dataset_name}")
        print(f"source_path={result['path']}")
        print(f"source_record_count={result['record_count']}")
        print(f"source_sha256={result['source_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
