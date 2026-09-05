# Databricks notebook source
import hashlib
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

TRADING_CALENDAR_TABLE = (
    "workspace.devin_market_risk_dev.silver_trading_calendar"
)
INSTRUMENT_TABLE = "workspace.devin_market_risk_dev.silver_instruments"

CONTRACT_VERSION = "1.0.0"
CALENDAR_SOURCE = "PROJECT_GIT_RULESET"
CALENDAR_VERSION = "US_EQUITIES_2016_V1"
EXCHANGE_TIMEZONE = "America/New_York"
EXCHANGES = ("XNAS", "XNYS")
CALENDAR_START = date(2016, 1, 1)
CALENDAR_END = date(2016, 12, 31)
EXPECTED_DATE_COUNT = 366
EXPECTED_ROW_COUNT = 732
EXPECTED_TRADING_DAYS_PER_EXCHANGE = 252
EXPECTED_NONTRADING_DAYS_PER_EXCHANGE = 114
EXPECTED_EARLY_CLOSES_PER_EXCHANGE = 1

HOLIDAYS = {
    date(2016, 1, 1): "New Year's Day",
    date(2016, 1, 18): "Martin Luther King Jr. Day",
    date(2016, 2, 15): "Washington's Birthday",
    date(2016, 3, 25): "Good Friday",
    date(2016, 5, 30): "Memorial Day",
    date(2016, 7, 4): "Independence Day",
    date(2016, 9, 5): "Labor Day",
    date(2016, 11, 24): "Thanksgiving Day",
    date(2016, 12, 26): "Christmas Day (Observed)",
}
EARLY_CLOSES = {
    date(2016, 11, 25): "Day After Thanksgiving",
}
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
RECONCILIATION_COLUMNS = (*RECORD_HASH_FIELDS, "record_hash")


# COMMAND ----------

def require_true(rule_name: str, condition: bool) -> None:
    if not condition:
        raise ValueError(f"Trading-calendar rule failed: {rule_name}")


def require_equal(rule_name: str, actual: object, expected: object) -> None:
    if actual != expected:
        raise ValueError(
            f"Trading-calendar rule failed: {rule_name}; "
            f"actual={actual!r}; expected={expected!r}"
        )


def date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def local_session_timestamp(calendar_date: date, session_time: time) -> datetime:
    local_timestamp = datetime.combine(
        calendar_date,
        session_time,
        tzinfo=ZoneInfo(EXCHANGE_TIMEZONE),
    )
    return local_timestamp.astimezone(UTC).replace(tzinfo=None)


def canonical_hash_value(value: object) -> str:
    if value is None:
        return "<NULL>"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC).isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        )
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def calculate_record_hash(row: dict[str, object]) -> str:
    canonical_record = "|".join(
        canonical_hash_value(row[field_name]) for field_name in RECORD_HASH_FIELDS
    )
    return hashlib.sha256(canonical_record.encode("utf-8")).hexdigest()


def build_calendar_rows(generated_at_utc: datetime) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for exchange_mic in EXCHANGES:
        for calendar_date in date_range(CALENDAR_START, CALENDAR_END):
            is_weekday = calendar_date.weekday() < 5
            holiday_name = HOLIDAYS.get(calendar_date)
            is_trading_day = is_weekday and holiday_name is None
            is_early_close = is_trading_day and calendar_date in EARLY_CLOSES

            if is_trading_day:
                market_open_utc = local_session_timestamp(calendar_date, time(9, 30))
                close_time = time(13, 0) if is_early_close else time(16, 0)
                market_close_utc = local_session_timestamp(calendar_date, close_time)
                holiday_name = EARLY_CLOSES.get(calendar_date)
            else:
                market_open_utc = None
                market_close_utc = None

            row: dict[str, object] = {
                "exchange_mic": exchange_mic,
                "calendar_date": calendar_date,
                "is_trading_day": is_trading_day,
                "market_open_utc": market_open_utc,
                "market_close_utc": market_close_utc,
                "is_early_close": is_early_close,
                "holiday_name": holiday_name,
                "exchange_timezone": EXCHANGE_TIMEZONE,
                "calendar_source": CALENDAR_SOURCE,
                "calendar_version": CALENDAR_VERSION,
                "generated_at_utc": generated_at_utc,
            }
            row["record_hash"] = calculate_record_hash(row)
            rows.append(row)
    return rows


# COMMAND ----------

def validate_calendar_rows(rows: list[dict[str, object]]) -> None:
    expected_dates = date_range(CALENDAR_START, CALENDAR_END)
    expected_date_set = set(expected_dates)
    require_equal("candidate row count", len(rows), EXPECTED_ROW_COUNT)
    require_equal("contiguous date count", len(expected_dates), EXPECTED_DATE_COUNT)

    key_counts = Counter(
        (row["exchange_mic"], row["calendar_date"]) for row in rows
    )
    duplicate_keys = [key for key, count in key_counts.items() if count != 1]
    require_equal("unique exchange-date keys", duplicate_keys, [])

    require_equal(
        "pinned exchange coverage",
        {row["exchange_mic"] for row in rows},
        set(EXCHANGES),
    )
    for exchange_mic in EXCHANGES:
        exchange_rows = [row for row in rows if row["exchange_mic"] == exchange_mic]
        exchange_dates = {row["calendar_date"] for row in exchange_rows}
        require_equal(
            f"{exchange_mic} contiguous dates", exchange_dates, expected_date_set
        )
        require_equal(
            f"{exchange_mic} trading-day count",
            sum(bool(row["is_trading_day"]) for row in exchange_rows),
            EXPECTED_TRADING_DAYS_PER_EXCHANGE,
        )
        require_equal(
            f"{exchange_mic} nontrading-day count",
            sum(not bool(row["is_trading_day"]) for row in exchange_rows),
            EXPECTED_NONTRADING_DAYS_PER_EXCHANGE,
        )
        require_equal(
            f"{exchange_mic} early-close count",
            sum(bool(row["is_early_close"]) for row in exchange_rows),
            EXPECTED_EARLY_CLOSES_PER_EXCHANGE,
        )

    for row in rows:
        calendar_date = row["calendar_date"]
        is_trading_day = bool(row["is_trading_day"])
        is_early_close = bool(row["is_early_close"])
        require_true(
            "weekends are nontrading",
            calendar_date.weekday() < 5 or not is_trading_day,
        )
        require_equal(
            "calendar source is pinned", row["calendar_source"], CALENDAR_SOURCE
        )
        require_equal(
            "calendar version is pinned", row["calendar_version"], CALENDAR_VERSION
        )
        require_equal(
            "exchange timezone is pinned",
            row["exchange_timezone"],
            EXCHANGE_TIMEZONE,
        )
        require_equal(
            "record hash",
            row["record_hash"],
            calculate_record_hash(row),
        )

        if is_trading_day:
            require_true("trading-day open exists", row["market_open_utc"] is not None)
            require_true(
                "trading-day close exists", row["market_close_utc"] is not None
            )
            require_true(
                "session close follows open",
                row["market_close_utc"] > row["market_open_utc"],
            )
            if is_early_close:
                expected_duration = timedelta(hours=3, minutes=30)
            else:
                expected_duration = timedelta(hours=6, minutes=30)
            require_equal(
                "session duration",
                row["market_close_utc"] - row["market_open_utc"],
                expected_duration,
            )
        else:
            require_equal("nontrading open is null", row["market_open_utc"], None)
            require_equal("nontrading close is null", row["market_close_utc"], None)
            require_true("nontrading day is not early close", not is_early_close)
            if calendar_date.weekday() < 5:
                require_true(
                    "weekday closure has holiday name",
                    row["holiday_name"] is not None,
                )

    require_equal(
        "standard-time UTC open",
        local_session_timestamp(date(2016, 1, 4), time(9, 30)),
        datetime(2016, 1, 4, 14, 30),
    )
    require_equal(
        "daylight-time UTC open",
        local_session_timestamp(date(2016, 7, 5), time(9, 30)),
        datetime(2016, 7, 5, 13, 30),
    )
    require_equal(
        "early-close UTC close",
        local_session_timestamp(date(2016, 11, 25), time(13, 0)),
        datetime(2016, 11, 25, 18, 0),
    )


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

generated_at_utc = datetime.now(UTC).replace(tzinfo=None)
calendar_rows = build_calendar_rows(generated_at_utc)
validate_calendar_rows(calendar_rows)

active_instrument_exchanges = {
    row["exchange_mic"]
    for row in (
        spark.table(INSTRUMENT_TABLE)
        .where(F.col("is_active"))
        .select("exchange_mic")
        .distinct()
        .collect()
    )
}
require_true("active instrument exchanges exist", bool(active_instrument_exchanges))
require_true(
    "active instrument exchanges are covered",
    active_instrument_exchanges.issubset(set(EXCHANGES)),
)

target_schema = spark.table(TRADING_CALENDAR_TABLE).schema
candidate_df = spark.createDataFrame(calendar_rows, schema=target_schema)
require_equal("Spark candidate count", candidate_df.count(), EXPECTED_ROW_COUNT)
candidate_df.createOrReplaceTempView("phase_06_trading_calendar_candidates")

print(
    "trading_calendar_generation_validation=PASS "
    f"contract_version={CONTRACT_VERSION} "
    f"calendar_source={CALENDAR_SOURCE} "
    f"calendar_version={CALENDAR_VERSION} "
    f"candidate_count={EXPECTED_ROW_COUNT} "
    f"exchange_count={len(EXCHANGES)} "
    f"date_count={EXPECTED_DATE_COUNT}"
)


# COMMAND ----------

target_df = spark.table(TRADING_CALENDAR_TABLE)
target_duplicate_count = (
    target_df.groupBy("exchange_mic", "calendar_date")
    .count()
    .where(F.col("count") != 1)
    .count()
)
require_equal("existing target has unique keys", target_duplicate_count, 0)

target_hashes = target_df.select(
    "exchange_mic",
    "calendar_date",
    F.col("record_hash").alias("target_record_hash"),
)
comparison_df = candidate_df.alias("source").join(
    target_hashes.alias("target"),
    ["exchange_mic", "calendar_date"],
    "left",
)
inserted_count = comparison_df.where(F.col("target_record_hash").isNull()).count()
updated_count = comparison_df.where(
    F.col("target_record_hash").isNotNull()
    & (F.col("source.record_hash") != F.col("target_record_hash"))
).count()
unchanged_count = comparison_df.where(
    F.col("source.record_hash") == F.col("target_record_hash")
).count()
require_equal(
    "publication counts reconcile",
    inserted_count + updated_count + unchanged_count,
    EXPECTED_ROW_COUNT,
)
published = inserted_count + updated_count > 0

print(
    "trading_calendar_publication_gate=PASS "
    f"inserted_count={inserted_count} "
    f"updated_count={updated_count} "
    f"unchanged_count={unchanged_count} "
    f"published={published}"
)


# COMMAND ----------

merge_sql = f"""
MERGE INTO {TRADING_CALENDAR_TABLE} AS target
USING phase_06_trading_calendar_candidates AS source
ON target.exchange_mic = source.exchange_mic
AND target.calendar_date = source.calendar_date
WHEN MATCHED AND target.record_hash <> source.record_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

if published:
    spark.sql(merge_sql)


# COMMAND ----------

persisted_df: DataFrame = (
    spark.table(TRADING_CALENDAR_TABLE)
    .where(F.col("exchange_mic").isin(*EXCHANGES))
    .where(F.col("calendar_date").between(CALENDAR_START, CALENDAR_END))
)
require_equal("persisted row count", persisted_df.count(), EXPECTED_ROW_COUNT)
require_equal(
    "persisted generated timestamps",
    persisted_df.where(F.col("generated_at_utc").isNotNull()).count(),
    EXPECTED_ROW_COUNT,
)
require_equal(
    "candidate rows missing from persisted target",
    candidate_df.select(*RECONCILIATION_COLUMNS)
    .exceptAll(persisted_df.select(*RECONCILIATION_COLUMNS))
    .count(),
    0,
)
require_equal(
    "unexpected persisted rows",
    persisted_df.select(*RECONCILIATION_COLUMNS)
    .exceptAll(candidate_df.select(*RECONCILIATION_COLUMNS))
    .count(),
    0,
)
require_equal(
    "persisted calendar lineage",
    {
        (row["calendar_source"], row["calendar_version"])
        for row in persisted_df.select(
            "calendar_source", "calendar_version"
        ).distinct().collect()
    },
    {(CALENDAR_SOURCE, CALENDAR_VERSION)},
)

print(
    "trading_calendar_persistence=PASS "
    f"persisted_count={EXPECTED_ROW_COUNT} "
    f"published={published} "
    f"inserted_count={inserted_count} "
    f"updated_count={updated_count} "
    f"unchanged_count={unchanged_count}"
)
