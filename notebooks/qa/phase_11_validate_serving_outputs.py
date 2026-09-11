# Databricks notebook source
"""Read-only validation of consumer-facing Phase 08 serving outputs."""

import re
from datetime import date

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DAILY_VIEW = (
    "workspace.devin_market_risk_dev.vw_portfolio_daily_analytics"
)
POSITION_VIEW = (
    "workspace.devin_market_risk_dev.vw_position_exposure_detail"
)
RISK_RUN_VIEW = (
    "workspace.devin_market_risk_dev.vw_latest_published_risk_runs"
)
VAR_VIEW = "workspace.devin_market_risk_dev.vw_latest_published_var"
STRESS_VIEW = (
    "workspace.devin_market_risk_dev.vw_latest_published_stress_results"
)

GOLD_DAILY_TABLE = (
    "workspace.devin_market_risk_dev.gold_portfolio_daily_metrics"
)
GOLD_POSITION_TABLE = (
    "workspace.devin_market_risk_dev.gold_position_market_values"
)

PORTFOLIO_ID_PATTERN = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$")
EXPECTED_INSTRUMENT_COUNT = 15
EXPECTED_VAR_MEASURE_COUNT = 2
EXPECTED_STRESS_RESULT_COUNT = 3


def require_equal(label: str, actual: object, expected: object) -> None:
    """Raise a clear error when a serving validation rule fails."""
    if actual != expected:
        raise ValueError(
            f"{label} mismatch: expected {expected!r}, found {actual!r}"
        )


def require_parameter(name: str, value: str, pattern: re.Pattern[str]) -> str:
    """Require one non-empty governed workflow parameter."""
    if not pattern.fullmatch(value):
        raise ValueError(f"{name} has an invalid value: {value!r}")
    return value


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils = DBUtils(spark)

dbutils.widgets.text("portfolio_id", "", "Portfolio ID")
dbutils.widgets.text("valuation_date", "", "Valuation date (YYYY-MM-DD)")
dbutils.widgets.text("risk_as_of_date", "", "Risk as-of date (YYYY-MM-DD)")

portfolio_id = require_parameter(
    "portfolio_id",
    dbutils.widgets.get("portfolio_id").strip().upper(),
    PORTFOLIO_ID_PATTERN,
)
valuation_date = date.fromisoformat(
    dbutils.widgets.get("valuation_date").strip()
)
risk_as_of_date = date.fromisoformat(
    dbutils.widgets.get("risk_as_of_date").strip()
)

# COMMAND ----------

daily_served = (
    spark.table(DAILY_VIEW)
    .where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("valuation_date") == valuation_date)
    )
)
daily_gold = (
    spark.table(GOLD_DAILY_TABLE)
    .where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("valuation_date") == valuation_date)
    )
)

require_equal("served daily analytics row count", daily_served.count(), 1)
require_equal("canonical Gold daily metrics row count", daily_gold.count(), 1)

served_daily_row = daily_served.select("analytics_run_id").first()
gold_daily_row = daily_gold.select("analytics_run_id").first()

require_equal(
    "served daily analytics run ID",
    served_daily_row["analytics_run_id"],
    gold_daily_row["analytics_run_id"],
)

position_served = (
    spark.table(POSITION_VIEW)
    .where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("valuation_date") == valuation_date)
    )
)
position_gold = (
    spark.table(GOLD_POSITION_TABLE)
    .where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("valuation_date") == valuation_date)
    )
)

require_equal(
    "served position exposure row count",
    position_served.count(),
    EXPECTED_INSTRUMENT_COUNT,
)
require_equal(
    "canonical Gold position row count",
    position_gold.count(),
    EXPECTED_INSTRUMENT_COUNT,
)
require_equal(
    "served distinct position instrument count",
    position_served.select("instrument_id").distinct().count(),
    EXPECTED_INSTRUMENT_COUNT,
)
require_equal(
    "canonical distinct position instrument count",
    position_gold.select("instrument_id").distinct().count(),
    EXPECTED_INSTRUMENT_COUNT,
)

served_instrument_ids = {
    row["instrument_id"]
    for row in position_served.select("instrument_id").collect()
}
gold_instrument_ids = {
    row["instrument_id"]
    for row in position_gold.select("instrument_id").collect()
}

require_equal(
    "served position instrument set",
    served_instrument_ids,
    gold_instrument_ids,
)

# COMMAND ----------

selected_risk_runs = (
    spark.table(RISK_RUN_VIEW)
    .where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("as_of_date") == risk_as_of_date)
    )
)

require_equal("selected published risk run count", selected_risk_runs.count(), 1)

selected_risk_run = selected_risk_runs.select("risk_run_id").first()
risk_run_id = selected_risk_run["risk_run_id"]

served_var = spark.table(VAR_VIEW).where(
    F.col("risk_run_id") == risk_run_id
)
served_stress = spark.table(STRESS_VIEW).where(
    F.col("risk_run_id") == risk_run_id
)

require_equal(
    "served VaR measure count",
    served_var.count(),
    EXPECTED_VAR_MEASURE_COUNT,
)
require_equal(
    "served distinct VaR confidence count",
    served_var.select("confidence_level").distinct().count(),
    EXPECTED_VAR_MEASURE_COUNT,
)
require_equal(
    "served stress-result count",
    served_stress.count(),
    EXPECTED_STRESS_RESULT_COUNT,
)
require_equal(
    "served distinct stress scenario count",
    served_stress.select("scenario_id").distinct().count(),
    EXPECTED_STRESS_RESULT_COUNT,
)

print("serving_validation=PASS")
print(f"validated_portfolio_id={portfolio_id}")
print(f"validated_valuation_date={valuation_date.isoformat()}")
print(f"validated_risk_as_of_date={risk_as_of_date.isoformat()}")
print(f"validated_risk_run_id={risk_run_id}")
