# Databricks notebook source
"""Read-only admission gate for real Yahoo Bronze data entering Silver."""

import hashlib
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
BRONZE_DAILY_PRICE_TABLE = (
    "workspace.devin_market_risk_dev.bronze_daily_prices"
)
BRONZE_ACTION_TABLE = (
    "workspace.devin_market_risk_dev.bronze_corporate_actions"
)
PRICE_SHA256 = (
    "7f860c0a3d8191b9b2840e5c0d784954b8b9102af51d76b24828a5c0118d7c27"
)
ACTION_SHA256 = (
    "e6cc097f3cfe44ef325e968cf9b9c0d4869476d946fd67720854cb89d7315a83"
)


def require_equal(label: str, actual: object, expected: object) -> None:
    if actual != expected:
        raise ValueError(f"{label}: expected {expected!r}; found {actual!r}")


# COMMAND ----------

manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
require_equal("calendar source ID", manifest["source_id"], "EXCHANGE_CALENDARS")
require_equal("calendar source hash", manifest["source_sha256"], CALENDAR_SHA256)
require_equal(
    "calendar payload hash",
    hashlib.sha256(CALENDAR_PATH.read_bytes()).hexdigest(),
    CALENDAR_SHA256,
)
require_equal("calendar record count", manifest["record_count"], 4384)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
calendar = (
    spark.read.option("header", True).csv(str(CALENDAR_PATH))
    .where(F.col("is_trading_day") == "true")
    .select("exchange_mic", "calendar_date")
)
calendar_counts = {
    row["exchange_mic"]: row["count"]
    for row in calendar.groupBy("exchange_mic").count().collect()
}
require_equal(
    "calendar sessions by exchange",
    calendar_counts,
    {"XNAS": 1508, "XNYS": 1508},
)

prices = (
    spark.table(BRONZE_DAILY_PRICE_TABLE)
    .where(
        (F.col("source_id") == "YAHOO_FINANCE")
        & (F.col("source_sha256") == PRICE_SHA256)
    )
)
require_equal("price row count", prices.count(), 22620)
price_dates = prices.select("price_date").distinct()
require_equal("price date count", price_dates.count(), 1508)
require_equal(
    "prices per date",
    {
        row["count"]
        for row in prices.groupBy("price_date").count().collect()
    },
    {15},
)
for exchange_mic in ("XNAS", "XNYS"):
    session_dates = calendar.where(
        F.col("exchange_mic") == exchange_mic
    ).select("calendar_date")
    require_equal(
        f"{exchange_mic} price-calendar difference",
        price_dates.exceptAll(session_dates).count()
        + session_dates.exceptAll(price_dates).count(),
        0,
    )

actions = (
    spark.table(BRONZE_ACTION_TABLE)
    .where(
        (F.col("source_id") == "YAHOO_FINANCE")
        & (F.col("source_sha256") == ACTION_SHA256)
    )
)
require_equal("action row count", actions.count(), 280)
require_equal(
    "actions outside admitted price sessions",
    actions.select(F.col("effective_date").alias("price_date"))
    .distinct()
    .exceptAll(price_dates)
    .count(),
    0,
)

print("real_silver_admission=PASS")
print("calendar_session_count=1508")
print("price_row_count=22620")
print("action_row_count=280")
