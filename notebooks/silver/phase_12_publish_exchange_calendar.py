# Databricks notebook source
"""Publish the admitted 2020–2025 exchange-calendar snapshot into Silver."""

import csv
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TRADING_CALENDAR_TABLE = (
    "workspace.devin_market_risk_dev.silver_trading_calendar"
)
VOLUME_ROOT = Path(
    "/Volumes/workspace/devin_market_risk_dev/bronze_landing/exchange_calendars"
)
CALENDAR_SHA256 = (
    "5badba1c795de7c6c793e745240dc61999d0e04db8aa54db42918da582f9531b"
)
CALENDAR_PATH = (
    VOLUME_ROOT / CALENDAR_SHA256 / "us_equities_2020_2025.csv"
)
MANIFEST_PATH = VOLUME_ROOT / CALENDAR_SHA256 / "manifest.json"

EXCHANGES = ("XNAS", "XNYS")
CALENDAR_START = date(2020, 1, 1)
CALENDAR_END = date(2025, 12, 31)
EXPECTED_ROWS_PER_EXCHANGE = 2192
EXPECTED_TOTAL_ROWS = 4384
EXPECTED_TRADING_DAYS_PER_EXCHANGE = 1508
RECORD_HASH_FIELDS = (
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


def require_equal(label: str, actual: object, expected: object) -> None:
    if actual != expected:
        raise ValueError(f"{label}: expected {expected!r}; found {actual!r}")


def date_range(start: date, end: date) -> set[date]:
    return {
        start + timedelta(days=offset)
        for offset in range((end - start).days + 1)
    }


def source_record_hash(row: dict[str, str]) -> str:
    canonical = "|".join(
        row[field_name] if row[field_name] else "<NULL>"
        for field_name in RECORD_HASH_FIELDS
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_boolean(value: str) -> bool:
    if value not in {"true", "false"}:
        raise ValueError(f"Calendar boolean must be true or false; found {value!r}")
    return value == "true"


def parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    return (
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        .astimezone(UTC)
        .replace(tzinfo=None)
    )


# COMMAND ----------

manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
require_equal("calendar source ID", manifest["source_id"], "EXCHANGE_CALENDARS")
require_equal("calendar source hash", manifest["source_sha256"], CALENDAR_SHA256)
require_equal(
    "calendar payload hash",
    hashlib.sha256(CALENDAR_PATH.read_bytes()).hexdigest(),
    CALENDAR_SHA256,
)
require_equal(
    "calendar manifest record count",
    manifest["record_count"],
    EXPECTED_TOTAL_ROWS,
)

with CALENDAR_PATH.open(encoding="utf-8", newline="") as source_file:
    source_rows = list(csv.DictReader(source_file))

require_equal("calendar source row count", len(source_rows), EXPECTED_TOTAL_ROWS)

typed_rows: list[dict[str, object]] = []
for source_row in source_rows:
    require_equal(
        "calendar source record hash",
        source_row["record_hash"],
        source_record_hash(source_row),
    )
    require_equal(
        "calendar source",
        source_row["calendar_source"],
        "exchange_calendars",
    )
    require_equal("calendar version", source_row["calendar_version"], "4.13.2")

    typed_rows.append(
        {
            "exchange_mic": source_row["exchange_mic"],
            "calendar_date": date.fromisoformat(source_row["calendar_date"]),
            "is_trading_day": parse_boolean(source_row["is_trading_day"]),
            "market_open_utc": parse_timestamp(source_row["market_open_utc"]),
            "market_close_utc": parse_timestamp(source_row["market_close_utc"]),
            "is_early_close": parse_boolean(source_row["is_early_close"]),
            "holiday_name": source_row["holiday_name"] or None,
            "exchange_timezone": source_row["exchange_timezone"],
            "calendar_source": source_row["calendar_source"],
            "calendar_version": source_row["calendar_version"],
            "generated_at_utc": parse_timestamp(source_row["generated_at_utc"]),
            "record_hash": source_row["record_hash"],
        }
    )

expected_dates = date_range(CALENDAR_START, CALENDAR_END)
for exchange_mic in EXCHANGES:
    exchange_rows = [
        row for row in typed_rows if row["exchange_mic"] == exchange_mic
    ]
    require_equal(
        f"{exchange_mic} calendar row count",
        len(exchange_rows),
        EXPECTED_ROWS_PER_EXCHANGE,
    )
    require_equal(
        f"{exchange_mic} calendar date coverage",
        {row["calendar_date"] for row in exchange_rows},
        expected_dates,
    )
    require_equal(
        f"{exchange_mic} trading-session count",
        sum(bool(row["is_trading_day"]) for row in exchange_rows),
        EXPECTED_TRADING_DAYS_PER_EXCHANGE,
    )

require_equal(
    "calendar exchange coverage",
    {row["exchange_mic"] for row in typed_rows},
    set(EXCHANGES),
)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

target_schema = spark.table(TRADING_CALENDAR_TABLE).schema
candidate_df = spark.createDataFrame(typed_rows, schema=target_schema)
require_equal(
    "Spark calendar candidate count",
    candidate_df.count(),
    EXPECTED_TOTAL_ROWS,
)
candidate_df.createOrReplaceTempView("phase_12_calendar_candidates")

target_df = spark.table(TRADING_CALENDAR_TABLE)
target_duplicate_count = (
    target_df.groupBy("exchange_mic", "calendar_date")
    .count()
    .where(F.col("count") != 1)
    .count()
)
require_equal("existing Silver calendar key uniqueness", target_duplicate_count, 0)

target_hashes = target_df.select(
    "exchange_mic",
    "calendar_date",
    F.col("record_hash").alias("target_record_hash"),
)
comparison_df = candidate_df.join(
    target_hashes,
    ["exchange_mic", "calendar_date"],
    "left",
)

inserted_count = comparison_df.where(F.col("target_record_hash").isNull()).count()
unchanged_count = comparison_df.where(
    F.col("target_record_hash") == F.col("record_hash")
).count()
conflicting_count = comparison_df.where(
    F.col("target_record_hash").isNotNull()
    & (F.col("target_record_hash") != F.col("record_hash"))
).count()

require_equal("calendar conflicting keys", conflicting_count, 0)
require_equal(
    "calendar publication is all-new or all-unchanged",
    inserted_count in {0, EXPECTED_TOTAL_ROWS},
    True,
)
require_equal(
    "calendar publication counts reconcile",
    inserted_count + unchanged_count,
    EXPECTED_TOTAL_ROWS,
)

if inserted_count:
    spark.sql(
        f"""
        MERGE INTO {TRADING_CALENDAR_TABLE} AS target
        USING phase_12_calendar_candidates AS source
        ON target.exchange_mic = source.exchange_mic
        AND target.calendar_date = source.calendar_date
        WHEN NOT MATCHED THEN INSERT *
        """
    )

persisted = (
    spark.table(TRADING_CALENDAR_TABLE)
    .where(F.col("exchange_mic").isin(*EXCHANGES))
    .where(F.col("calendar_date").between(CALENDAR_START, CALENDAR_END))
)
require_equal("persisted calendar row count", persisted.count(), EXPECTED_TOTAL_ROWS)

persisted_mismatches = (
    candidate_df.select("exchange_mic", "calendar_date", "record_hash")
    .alias("candidate")
    .join(
        persisted.select(
            "exchange_mic",
            "calendar_date",
            F.col("record_hash").alias("persisted_record_hash"),
        ).alias("persisted"),
        ["exchange_mic", "calendar_date"],
        "left",
    )
    .where(F.col("candidate.record_hash") != F.col("persisted_record_hash"))
    .count()
)
require_equal("persisted calendar hashes", persisted_mismatches, 0)

print(
    "phase_12_calendar_publication=PASS "
    f"calendar_sha256={CALENDAR_SHA256} "
    f"inserted_count={inserted_count} "
    f"unchanged_count={unchanged_count} "
    f"persisted_count={EXPECTED_TOTAL_ROWS}"
)
