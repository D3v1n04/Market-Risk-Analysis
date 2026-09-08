# Databricks notebook source
"""Validate generated Phase 06 market inputs before Bronze ingestion."""

import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from io import StringIO
from pathlib import Path
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession

LANDING_ROOT = Path(
    "/Volumes/workspace/devin_market_risk_dev/bronze_landing"
)
SOURCE_ID = "PROJECT_GIT_FIXTURE"
EXPECTED_GENERATION_BY_DATASET = {
    "DAILY_PRICES": {
        "generator_code_version": (
            "5f31a75fc04afd63fd28224d663dc5cc7de2cac6"
        ),
        "generator_module": (
            "market_risk_analysis.ingestion.phase_06_market_inputs"
        ),
        "source_object_path": (
            "data/fixtures/phase_07_risk_history.yml"
        ),
        "source_sha256": (
            "9fb768b79dcdd74dd9b76b0408fae4c3d88a40635e440746064214043e6b2a1e"
        ),
    },
    "CORPORATE_ACTIONS": {
        "generator_code_version": (
            "ebb82db39e16466363f82adeca3529d816345ff9"
        ),
        "generator_module": (
            "market_risk_analysis.ingestion.phase_06_market_inputs"
        ),
        "source_object_path": (
            "data/fixtures/phase_06_analytics_scenario.yml"
        ),
        "source_sha256": (
            "2fb0fe28a3d283e66d933ca6187a6869a33392f0526eab37724b549c9209cd00"
        ),
    },
}
EXPECTED_PRICE_START_DATE = date(2016, 1, 4)
EXPECTED_PRICE_END_DATE = date(2016, 12, 30)
EXPECTED_PRICE_DATE_COUNT = 252

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


def build_expected_price_dates() -> tuple[str, ...]:
    """Return the approved 2016 US-equities trading-date spine."""
    dates: list[str] = []
    current_date = EXPECTED_PRICE_START_DATE

    while current_date <= EXPECTED_PRICE_END_DATE:
        if (
            current_date.weekday() < 5
            and current_date not in US_EQUITIES_2016_HOLIDAYS
        ):
            dates.append(current_date.isoformat())
        current_date += timedelta(days=1)

    if len(dates) != EXPECTED_PRICE_DATE_COUNT:
        raise ValueError("Unexpected approved price-date count")
    return tuple(dates)


EXPECTED_PRICE_DATES = build_expected_price_dates()

MARKET_SOURCES = (
    {
        "dataset_name": "DAILY_PRICES",
        "dataset_slug": "daily_prices",
        "source_filename": "daily_prices.csv",
        "source_object_path": (
            "data/raw/phase_06_analytics_foundation/daily_prices.csv"
        ),
        "source_sha256": "219e106b3d8c027965d317ea992c3405881ebbc9eee0678c9ed66270bd81a5a3",  # noqa: E501
        "manifest_sha256": "65069509e6cc95203174a5f2fcdc61fa66f2ea3504be138fbb566d11eab3689a",  # noqa: E501
        "source_contract_version": "1.1.0",
        "expected_record_count": 3780,
        "expected_column_count": 14,
        "bronze_table": (
            "workspace.devin_market_risk_dev.bronze_daily_prices"
        ),
        "business_key": [
            "instrument_id",
            "price_date",
            "source_id",
        ],
        "record_hash_fields": [
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
        ],
        "preview_columns": [
            "instrument_id",
            "price_date",
            "close_price",
            "adjusted_close_price",
        ],
    },
    {
        "dataset_name": "CORPORATE_ACTIONS",
        "dataset_slug": "corporate_actions",
        "source_filename": "corporate_actions.csv",
        "source_object_path": (
            "data/raw/phase_06_analytics_foundation/corporate_actions.csv"
        ),
        "source_sha256": (
            "e4d582a1db05816e1004635975602a6a"
            "eed7f28807dde64ec4acc3c4dd8f5ffd"
        ),
        "manifest_sha256": (
            "5c4d0f1fa2074b00dac3ec7aa05904d3"
            "9515aff1fb0d5af0b35260f798c05b3f"
        ),
        "source_contract_version": "1.1.0",
        "expected_record_count": 2,
        "expected_column_count": 12,
        "bronze_table": (
            "workspace.devin_market_risk_dev.bronze_corporate_actions"
        ),
        "business_key": [
            "instrument_id",
            "effective_date",
            "action_type",
            "source_id",
        ],
        "record_hash_fields": [
            "instrument_id",
            "effective_date",
            "action_type",
            "source_id",
            "source_symbol",
            "dividend_amount_per_share",
            "dividend_currency",
            "split_ratio",
            "source_action_id",
        ],
        "preview_columns": [
            "instrument_id",
            "effective_date",
            "action_type",
        ],
    },
)


def calculate_bytes_sha256(payload: bytes) -> str:
    """Return the SHA-256 digest of bytes already in memory."""
    return hashlib.sha256(payload).hexdigest()


def require_equal(label: str, actual: Any, expected: Any) -> None:
    """Raise a clear error when evidence differs."""
    if actual != expected:
        raise ValueError(
            f"{label} mismatch: expected {expected!r}, found {actual!r}"
        )


def require_true(label: str, condition: bool) -> None:
    """Raise a clear error when a validation rule is false."""
    if not condition:
        raise ValueError(f"{label} validation failed")


def calculate_record_hash(
    row: dict[str, str],
    fields: list[str],
) -> str:
    """Reproduce the contract-defined record hash."""
    canonical = "|".join(row[field] or "<NULL>" for field in fields)
    return calculate_bytes_sha256(canonical.encode("utf-8"))


def load_and_validate_landing(
    specification: dict[str, Any],
) -> tuple[
    dict[str, Any],
    list[str],
    list[dict[str, str]],
    list[str],
]:
    """Validate one landed CSV and its complete manifest lineage."""
    landing_directory = (
        LANDING_ROOT
        / specification["dataset_slug"]
        / specification["source_sha256"]
    )
    source_path = landing_directory / specification["source_filename"]
    manifest_path = landing_directory / "manifest.json"

    source_payload = source_path.read_bytes()
    manifest_payload = manifest_path.read_bytes()

    try:
        source_text = source_payload.decode("utf-8")
        manifest = json.loads(manifest_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "Landed source or manifest is not valid UTF-8"
        ) from exc

    if not isinstance(manifest, dict):
        raise ValueError("Manifest must contain one JSON object")

    reader = csv.DictReader(StringIO(source_text))
    source_columns = reader.fieldnames
    if source_columns is None:
        raise ValueError("Landed CSV source is empty")
    source_rows = list(reader)

    require_true(
        "CSV record width",
        all(
            None not in row
            and all(value is not None for value in row.values())
            for row in source_rows
        ),
    )

    source_lines = source_text.splitlines()
    require_equal(
        "physical CSV line count",
        len(source_lines),
        len(source_rows) + 1,
    )
    raw_records = source_lines[1:]

    source_sha256 = calculate_bytes_sha256(source_payload)
    manifest_sha256 = calculate_bytes_sha256(manifest_payload)

    require_equal(
        "source SHA-256",
        source_sha256,
        specification["source_sha256"],
    )
    require_equal(
        "manifest SHA-256",
        manifest_sha256,
        specification["manifest_sha256"],
    )
    require_equal(
        "manifest source SHA-256",
        manifest.get("source_sha256"),
        source_sha256,
    )
    require_equal(
        "source size",
        manifest.get("source_size_bytes"),
        len(source_payload),
    )
    require_equal(
        "source record count",
        manifest.get("source_record_count"),
        specification["expected_record_count"],
    )
    require_equal(
        "parsed source record count",
        len(source_rows),
        specification["expected_record_count"],
    )
    require_equal(
        "source column count",
        len(source_columns),
        specification["expected_column_count"],
    )
    require_equal(
        "manifest source columns",
        manifest.get("source_columns"),
        source_columns,
    )
    require_equal(
        "dataset name",
        manifest.get("dataset_name"),
        specification["dataset_name"],
    )
    require_equal("source identifier", manifest.get("source_id"), SOURCE_ID)
    require_equal(
        "source object path",
        manifest.get("source_object_path"),
        specification["source_object_path"],
    )
    require_equal(
        "landed object path",
        manifest.get("landed_object_path"),
        source_path.as_posix(),
    )
    require_equal(
        "source contract version",
        manifest.get("source_contract_version"),
        specification["source_contract_version"],
    )
    require_equal("source format", manifest.get("source_format"), "CSV")
    require_equal("manifest has header", manifest.get("has_header"), True)
    require_equal(
        "generation lineage",
        manifest.get("generation"),
        EXPECTED_GENERATION_BY_DATASET[
            specification["dataset_name"]
        ],
    )

    print(f"{specification['dataset_name']} landing_validation=PASS")
    print(f"source_sha256={source_sha256}")
    print(f"manifest_sha256={manifest_sha256}")
    print(f"source_record_count={len(source_rows)}")

    return manifest, source_columns, source_rows, raw_records


def validate_common_records(
    specification: dict[str, Any],
    source_rows: list[dict[str, str]],
) -> None:
    """Validate keys, controlled origin, and row-level hashes."""
    business_key = specification["business_key"]
    key_counts = Counter(
        tuple(row[column] for column in business_key) for row in source_rows
    )
    require_true(
        "business-key uniqueness",
        all(count == 1 for count in key_counts.values()),
    )
    require_equal(
        "source-record ID uniqueness",
        len({row["source_record_id"] for row in source_rows}),
        len(source_rows),
    )
    require_true(
        "source-record IDs present",
        all(row["source_record_id"] for row in source_rows),
    )
    require_true(
        "controlled fixture source",
        all(row["source_id"] == SOURCE_ID for row in source_rows),
    )

    for source_row_number, row in enumerate(source_rows, start=1):
        expected_hash = calculate_record_hash(
            row,
            specification["record_hash_fields"],
        )
        require_equal(
            f"record hash at source row {source_row_number}",
            row["record_hash"],
            expected_hash,
        )


def parse_positive_decimal(value: str, label: str) -> Decimal:
    """Parse one required positive decimal fixture value."""
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{label} is not a valid decimal") from exc
    require_true(label, parsed > 0)
    return parsed


def validate_daily_prices(source_rows: list[dict[str, str]]) -> None:
    """Prove complete price coverage and valid market-value fields."""
    rows_by_date: dict[str, list[dict[str, str]]] = defaultdict(list)
    dates_by_instrument: dict[str, set[str]] = defaultdict(set)

    for source_row_number, row in enumerate(source_rows, start=1):
        rows_by_date[row["price_date"]].append(row)
        dates_by_instrument[row["instrument_id"]].add(row["price_date"])

        open_price = parse_positive_decimal(
            row["open_price"],
            f"open price at source row {source_row_number}",
        )
        high_price = parse_positive_decimal(
            row["high_price"],
            f"high price at source row {source_row_number}",
        )
        low_price = parse_positive_decimal(
            row["low_price"],
            f"low price at source row {source_row_number}",
        )
        close_price = parse_positive_decimal(
            row["close_price"],
            f"close price at source row {source_row_number}",
        )
        parse_positive_decimal(
            row["adjusted_close_price"],
            f"adjusted close at source row {source_row_number}",
        )
        require_true(
            f"OHLC high consistency at source row {source_row_number}",
            high_price >= max(open_price, low_price, close_price),
        )
        require_true(
            f"OHLC low consistency at source row {source_row_number}",
            low_price <= min(open_price, high_price, close_price),
        )
        try:
            volume = int(row["volume"])
        except ValueError as exc:
            raise ValueError(
                f"volume at source row {source_row_number} is not an integer"
            ) from exc
        require_true(
            f"volume at source row {source_row_number}",
            volume >= 0,
        )
        require_equal(
            f"quote currency at source row {source_row_number}",
            row["quote_currency"],
            "USD",
        )
        require_equal(
            f"provider timestamp at source row {source_row_number}",
            row["source_updated_at_utc"],
            "",
        )

    require_equal(
        "price date set",
        set(rows_by_date),
        set(EXPECTED_PRICE_DATES),
    )
    require_equal("instrument count", len(dates_by_instrument), 15)
    instrument_universe = set(dates_by_instrument)

    for price_date in EXPECTED_PRICE_DATES:
        date_rows = rows_by_date[price_date]
        require_equal(f"{price_date} price count", len(date_rows), 15)
        require_equal(
            f"{price_date} instrument coverage",
            {row["instrument_id"] for row in date_rows},
            instrument_universe,
        )

    for instrument_id, instrument_dates in dates_by_instrument.items():
        require_equal(
            f"{instrument_id} date coverage",
            instrument_dates,
            set(EXPECTED_PRICE_DATES),
        )

    print("DAILY_PRICES record_validation=PASS")
    print(
        "expected_grid="
        f"15_instruments_x_{EXPECTED_PRICE_DATE_COUNT}_dates"
    )
    print(f"validated_grid_count={len(source_rows)}")


def validate_corporate_actions(
    source_rows: list[dict[str, str]],
) -> None:
    """Validate action-specific conditional fields."""
    require_equal(
        "corporate action types",
        Counter(row["action_type"] for row in source_rows),
        Counter({"CASH_DIVIDEND": 1, "STOCK_SPLIT": 1}),
    )

    for source_row_number, row in enumerate(source_rows, start=1):
        require_true(
            f"effective date at source row {source_row_number}",
            row["effective_date"] in EXPECTED_PRICE_DATES,
        )
        require_equal(
            f"provider action ID at source row {source_row_number}",
            row["source_action_id"],
            "",
        )
        require_equal(
            f"provider timestamp at source row {source_row_number}",
            row["source_updated_at_utc"],
            "",
        )

        if row["action_type"] == "CASH_DIVIDEND":
            parse_positive_decimal(
                row["dividend_amount_per_share"],
                f"dividend amount at source row {source_row_number}",
            )
            require_equal(
                f"dividend currency at source row {source_row_number}",
                row["dividend_currency"],
                "USD",
            )
            require_equal(
                f"split ratio at source row {source_row_number}",
                row["split_ratio"],
                "",
            )
        elif row["action_type"] == "STOCK_SPLIT":
            split_ratio = parse_positive_decimal(
                row["split_ratio"],
                f"split ratio at source row {source_row_number}",
            )
            require_true(
                f"non-unit split ratio at source row {source_row_number}",
                split_ratio != Decimal("1"),
            )
            require_equal(
                f"dividend amount at source row {source_row_number}",
                row["dividend_amount_per_share"],
                "",
            )
            require_equal(
                f"dividend currency at source row {source_row_number}",
                row["dividend_currency"],
                "",
            )

    print("CORPORATE_ACTIONS record_validation=PASS")
    print(f"validated_action_count={len(source_rows)}")


def build_bronze_candidates(
    *,
    spark: SparkSession,
    specification: dict[str, Any],
    manifest: dict[str, Any],
    source_columns: list[str],
    source_rows: list[dict[str, str]],
    raw_records: list[str],
) -> DataFrame:
    """Create a write-free, source-aligned Bronze candidate DataFrame."""
    candidate_batch_id = str(uuid4())
    validation_time = datetime.now(UTC).replace(tzinfo=None)
    bronze_schema = spark.table(specification["bronze_table"]).schema
    expected_columns = set(bronze_schema.fieldNames())
    candidate_records: list[dict[str, Any]] = []

    for source_row_number, (source_values, raw_record) in enumerate(
        zip(source_rows, raw_records, strict=True),
        start=1,
    ):
        lineage_values = {
            "batch_id": candidate_batch_id,
            "source_id": manifest["source_id"],
            "source_object_path": manifest["source_object_path"],
            "source_sha256": manifest["source_sha256"],
            "source_row_number": source_row_number,
            "source_record_sha256": calculate_bytes_sha256(
                raw_record.encode("utf-8")
            ),
            "raw_record": raw_record,
            "ingested_at_utc": validation_time,
            "contract_version": manifest["source_contract_version"],
        }
        candidate_record = source_values | lineage_values
        require_equal(
            "candidate record columns",
            set(candidate_record),
            expected_columns,
        )
        candidate_records.append(candidate_record)

    candidates = spark.createDataFrame(candidate_records, schema=bronze_schema)
    require_equal(
        "candidate row count",
        candidates.count(),
        specification["expected_record_count"],
    )
    require_equal(
        "candidate DataFrame columns",
        set(candidates.columns),
        expected_columns,
    )

    print(f"{specification['dataset_name']} candidate_validation=PASS")
    print(f"candidate_batch_id={candidate_batch_id}")
    print(f"candidate_row_count={candidates.count()}")
    return candidates


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

for specification in MARKET_SOURCES:
    manifest, source_columns, source_rows, raw_records = (
        load_and_validate_landing(specification)
    )
    validate_common_records(specification, source_rows)

    if specification["dataset_name"] == "DAILY_PRICES":
        validate_daily_prices(source_rows)
    elif specification["dataset_name"] == "CORPORATE_ACTIONS":
        validate_corporate_actions(source_rows)
    else:
        raise ValueError(
            f"Unsupported market dataset: {specification['dataset_name']}"
        )

    candidates = build_bronze_candidates(
        spark=spark,
        specification=specification,
        manifest=manifest,
        source_columns=source_columns,
        source_rows=source_rows,
        raw_records=raw_records,
    )
    candidates.select(
        *specification["preview_columns"],
        "batch_id",
        "source_row_number",
        "source_record_sha256",
    ).show(5, truncate=False)
