# Databricks notebook source
"""Calculate one complete, audited Gold position-market-value partition."""

import hashlib
import json
import re
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.dbutils import DBUtils
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

PORTFOLIO_TABLE = "workspace.devin_market_risk_dev.silver_portfolios"
INSTRUMENT_TABLE = "workspace.devin_market_risk_dev.silver_instruments"
PRICE_TABLE = "workspace.devin_market_risk_dev.silver_daily_prices"
CALENDAR_TABLE = "workspace.devin_market_risk_dev.silver_trading_calendar"
POSITION_TABLE = "workspace.devin_market_risk_dev.silver_positions"
MARKET_VALUE_TABLE = (
    "workspace.devin_market_risk_dev.gold_position_market_values"
)
ANALYTICS_RUN_TABLE = (
    "workspace.devin_market_risk_dev.gold_analytics_runs"
)

MARKET_VALUE_CONTRACT_VERSION = "1.0.0"
ANALYTICS_RUN_CONTRACT_VERSION = "1.0.0"
CALCULATION_VERSION = "1.0.0"
EXPECTED_POSITION_COUNT = 15
EMPTY_SET_SHA256 = hashlib.sha256(b"").hexdigest()

PORTFOLIO_ID_PATTERN = re.compile(
    r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$"
)
CODE_VERSION_PATTERN = re.compile(r"^[0-9a-f]{40}$")

MARKET_VALUE_COLUMNS = [
    "portfolio_id",
    "instrument_id",
    "valuation_date",
    "signed_quantity",
    "close_price",
    "quote_currency",
    "base_currency",
    "position_side",
    "signed_market_value",
    "absolute_market_value",
    "input_position_record_sha256",
    "input_price_record_sha256",
    "analytics_run_id",
    "calculation_version",
    "calculated_at_utc",
    "contract_version",
    "record_hash",
]

MARKET_VALUE_HASH_FIELDS = [
    "portfolio_id",
    "instrument_id",
    "valuation_date",
    "signed_quantity",
    "close_price",
    "quote_currency",
    "base_currency",
    "position_side",
    "signed_market_value",
    "absolute_market_value",
    "input_position_record_sha256",
    "input_price_record_sha256",
    "calculation_version",
]

INPUT_CONTRACT_VERSIONS = {
    "PORTFOLIOS": "1.1.0",
    "INSTRUMENTS": "1.1.0",
    "DAILY_PRICES": "1.1.0",
    "TRADING_CALENDAR": "1.0.0",
    "POSITIONS": "1.1.0",
}


def require_equal(label: str, actual: Any, expected: Any) -> None:
    """Raise a clear reconciliation error."""
    if actual != expected:
        raise ValueError(
            f"{label} mismatch: expected {expected!r}, found {actual!r}"
        )


def require_parameter(
    label: str,
    value: str,
    pattern: re.Pattern[str],
) -> str:
    """Require a nonempty parameter in its approved format."""
    if pattern.fullmatch(value) is None:
        raise ValueError(f"{label} has an invalid format")
    return value


def ordered_set_sha256(
    frame: DataFrame,
    *,
    order_columns: list[str],
    value_columns: list[str],
) -> str:
    """Hash an ordered set of trusted rows."""
    if frame.count() == 0:
        return EMPTY_SET_SHA256

    ordered_rows = frame.select(
        F.struct(
            *[
                F.col(column_name).alias(f"order_{index}")
                for index, column_name in enumerate(order_columns)
            ],
            F.concat_ws(
                "|",
                *[
                    F.coalesce(
                        F.col(column_name).cast("string"),
                        F.lit("<NULL>"),
                    )
                    for column_name in value_columns
                ],
            ).alias("canonical_row"),
        ).alias("ordered_row")
    )
    digest = (
        ordered_rows.agg(
            F.sha2(
                F.concat_ws(
                    "\n",
                    F.transform(
                        F.array_sort(F.collect_list("ordered_row")),
                        lambda row: row["canonical_row"],
                    ),
                ),
                256,
            ).alias("set_sha256")
        ).first()
    )
    return digest["set_sha256"]


def market_value_partition_sha256(frame: DataFrame) -> str | None:
    """Hash one canonical Gold portfolio-date partition."""
    if frame.count() == 0:
        return None
    return ordered_set_sha256(
        frame,
        order_columns=["portfolio_id", "instrument_id", "valuation_date"],
        value_columns=[
            "portfolio_id",
            "instrument_id",
            "valuation_date",
            "record_hash",
        ],
    )


def canonical_hash_expression(fields: list[str]) -> Column:
    """Build the contract-defined Gold record hash."""
    return F.sha2(
        F.concat_ws(
            "|",
            *[
                F.coalesce(
                    F.col(field).cast("string"),
                    F.lit("<NULL>"),
                )
                for field in fields
            ],
        ),
        256,
    )


def freeze_small_dataframe(
    frame: DataFrame,
    *,
    label: str,
    max_rows: int,
) -> DataFrame:
    """Detach bounded evidence without cache or persist commands."""
    rows = frame.limit(max_rows + 1).collect()
    if len(rows) > max_rows:
        raise ValueError(
            f"{label} exceeded safe materialization limit {max_rows}"
        )
    return spark.createDataFrame(rows, schema=frame.schema)


def add_failure(
    failures: list[str],
    condition: bool,
    rule_id: str,
) -> None:
    """Append a stable rule ID when its assertion fails."""
    if condition:
        failures.append(rule_id)


def build_manifest_entry(
    *,
    dataset_name: str,
    frame: DataFrame,
    order_columns: list[str],
    value_columns: list[str],
    selection_scope: str,
) -> dict[str, Any]:
    """Build one deterministic trusted-Silver manifest entry."""
    return {
        "dataset_name": dataset_name,
        "dataset_contract_version": INPUT_CONTRACT_VERSIONS[dataset_name],
        "snapshot_reference": "SILVER_CURRENT",
        "selection_scope": selection_scope,
        "record_count": frame.count(),
        "record_set_sha256": ordered_set_sha256(
            frame,
            order_columns=order_columns,
            value_columns=value_columns,
        ),
    }


def input_manifest_sha256(entries: list[dict[str, Any]]) -> str:
    """Hash sorted dataset-level input-manifest entries."""
    canonical_rows = []
    for entry in sorted(entries, key=lambda item: item["dataset_name"]):
        canonical_rows.append(
            "|".join(
                [
                    entry["dataset_name"],
                    entry["dataset_contract_version"],
                    entry["snapshot_reference"],
                    entry["selection_scope"],
                    str(entry["record_count"]),
                    entry["record_set_sha256"],
                ]
            )
        )
    return hashlib.sha256("\n".join(canonical_rows).encode()).hexdigest()


def merge_market_value_partition(
    candidate: DataFrame,
    *,
    portfolio_id: str,
    valuation_date: date,
) -> None:
    """Atomically replace one complete Gold portfolio-date partition."""
    target = DeltaTable.forName(spark, MARKET_VALUE_TABLE)
    match_condition = (
        "target.portfolio_id = source.portfolio_id "
        "AND target.instrument_id = source.instrument_id "
        "AND target.valuation_date = source.valuation_date"
    )
    partition_condition = (
        f"target.portfolio_id = '{portfolio_id}' "
        f"AND target.valuation_date = DATE '{valuation_date.isoformat()}'"
    )
    (
        target.alias("target")
        .merge(candidate.alias("source"), match_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=partition_condition)
        .execute()
    )


def insert_analytics_audit(frame: DataFrame) -> None:
    """Insert immutable Gold analytics evidence."""
    target = DeltaTable.forName(spark, ANALYTICS_RUN_TABLE)
    (
        target.alias("target")
        .merge(
            frame.alias("source"),
            "target.analytics_run_id = source.analytics_run_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils = DBUtils(spark)

dbutils.widgets.text("portfolio_id", "", "Portfolio ID")
dbutils.widgets.text("valuation_date", "", "Valuation date (YYYY-MM-DD)")
dbutils.widgets.text("code_version", "", "Git code version")
dbutils.widgets.text("trigger_type", "MANUAL", "Analytics trigger type")

portfolio_id = require_parameter(
    "portfolio_id",
    dbutils.widgets.get("portfolio_id").strip().upper(),
    PORTFOLIO_ID_PATTERN,
)
valuation_date = date.fromisoformat(
    dbutils.widgets.get("valuation_date").strip()
)
code_version = require_parameter(
    "code_version",
    dbutils.widgets.get("code_version").strip(),
    CODE_VERSION_PATTERN,
)
trigger_type = dbutils.widgets.get("trigger_type").strip().upper()
if trigger_type not in {"MANUAL", "SCHEDULED", "RECOVERY"}:
    raise ValueError(
        "trigger_type must be MANUAL, SCHEDULED, or RECOVERY"
    )

analytics_run_id = str(uuid4())
started_at_utc = datetime.now(UTC).replace(tzinfo=None)

portfolio_rows = (
    spark.table(PORTFOLIO_TABLE)
    .where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("is_active") == F.lit(True))
    )
    .collect()
)
require_equal("active canonical portfolio count", len(portfolio_rows), 1)
portfolio = portfolio_rows[0]
if portfolio["actual_inception_date"] is None:
    raise ValueError("actual_inception_date must be governed before Gold")
if valuation_date < portfolio["actual_inception_date"]:
    raise ValueError("valuation_date cannot precede actual_inception_date")

previous_runs = (
    spark.table(ANALYTICS_RUN_TABLE)
    .where(
        (F.col("output_dataset_name") == "POSITION_MARKET_VALUES")
        & (F.col("portfolio_id") == portfolio_id)
        & (F.col("valuation_date") == F.lit(valuation_date))
    )
    .orderBy(F.col("attempt_number").desc())
    .select("analytics_run_id", "attempt_number")
    .limit(1)
    .collect()
)
if previous_runs:
    attempt_number = previous_runs[0]["attempt_number"] + 1
    reprocess_of_analytics_run_id = previous_runs[0]["analytics_run_id"]
else:
    attempt_number = 1
    reprocess_of_analytics_run_id = None

position_inputs = spark.table(POSITION_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("position_date") == F.lit(valuation_date))
)
position_instruments = position_inputs.select("instrument_id").distinct()
instruments = (
    spark.table(INSTRUMENT_TABLE)
    .join(position_instruments, "instrument_id", "inner")
    .where(
        (F.col("active_from") <= F.lit(valuation_date))
        & (
            F.col("active_to").isNull()
            | (F.col("active_to") >= F.lit(valuation_date))
        )
        & F.col("is_active")
    )
)
prices = (
    spark.table(PRICE_TABLE)
    .join(position_instruments, "instrument_id", "inner")
    .where(F.col("price_date") == F.lit(valuation_date))
)
exchange_ids = instruments.select("exchange_mic").distinct()
calendar = (
    spark.table(CALENDAR_TABLE)
    .join(exchange_ids, "exchange_mic", "inner")
    .where(F.col("calendar_date") == F.lit(valuation_date))
)

portfolio_frame = spark.createDataFrame(
    [portfolio],
    schema=spark.table(PORTFOLIO_TABLE).schema,
)

print(
    "market_value_parameters=PASS "
    f"analytics_run_id={analytics_run_id} "
    f"portfolio_id={portfolio_id} "
    f"valuation_date={valuation_date} "
    f"attempt_number={attempt_number} "
    "reprocess_of_analytics_run_id="
    f"{reprocess_of_analytics_run_id}"
)


# COMMAND ----------

failures: list[str] = []
position_count = position_inputs.count()
position_distinct_count = position_instruments.count()
instrument_count = instruments.count()
price_count = prices.count()
price_distinct_count = prices.select("instrument_id").distinct().count()
exchange_count = exchange_ids.count()
open_exchange_count = (
    calendar.where(
        F.col("is_trading_day")
        & F.col("market_open_utc").isNotNull()
        & F.col("market_close_utc").isNotNull()
    )
    .select("exchange_mic")
    .distinct()
    .count()
)

invalid_position_count = position_inputs.where(
    F.col("signed_quantity").isNull()
    | (F.col("signed_quantity") == 0)
    | (F.col("position_date") != F.lit(valuation_date))
).count()
invalid_price_count = prices.where(
    F.col("close_price").isNull()
    | (F.col("close_price") <= 0)
    | (F.col("price_date") != F.lit(valuation_date))
).count()
invalid_currency_count = (
    prices.alias("price")
    .join(
        instruments.select("instrument_id", "quote_currency").alias(
            "instrument"
        ),
        "instrument_id",
        "inner",
    )
    .where(
        (F.col("price.quote_currency") != "USD")
        | (F.col("instrument.quote_currency") != "USD")
        | (F.lit(portfolio["base_currency"]) != "USD")
        | (
            F.col("price.quote_currency")
            != F.col("instrument.quote_currency")
        )
        | (
            F.col("price.quote_currency")
            != F.lit(portfolio["base_currency"])
        )
    )
    .count()
)

add_failure(
    failures,
    position_count != EXPECTED_POSITION_COUNT
    or position_distinct_count != EXPECTED_POSITION_COUNT
    or instrument_count != EXPECTED_POSITION_COUNT
    or price_count != EXPECTED_POSITION_COUNT
    or price_distinct_count != EXPECTED_POSITION_COUNT,
    "POSITION_MARKET_VALUE_INPUT_COVERAGE",
)
add_failure(
    failures,
    exchange_count == 0 or open_exchange_count != exchange_count,
    "POSITION_MARKET_VALUE_INPUT_COVERAGE",
)
add_failure(
    failures,
    invalid_position_count != 0 or invalid_price_count != 0,
    "POSITION_MARKET_VALUE_DATE_ALIGNMENT",
)
add_failure(
    failures,
    invalid_currency_count != 0 or portfolio["base_currency"] != "USD",
    "POSITION_MARKET_VALUE_CURRENCY_MATCH",
)

signed_market_value_expression = (
    F.col("position.signed_quantity").cast("decimal(38,16)")
    * F.col("price.close_price").cast("decimal(20,8)")
).cast("decimal(38,16)")

calculated_at_utc = datetime.now(UTC).replace(tzinfo=None)
market_value_candidate = (
    position_inputs.alias("position")
    .join(
        prices.alias("price"),
        F.col("position.instrument_id") == F.col("price.instrument_id"),
        "inner",
    )
    .select(
        F.col("position.portfolio_id").alias("portfolio_id"),
        F.col("position.instrument_id").alias("instrument_id"),
        F.lit(valuation_date).cast("date").alias("valuation_date"),
        F.col("position.signed_quantity")
        .cast("decimal(38,16)")
        .alias("signed_quantity"),
        F.col("price.close_price")
        .cast("decimal(20,8)")
        .alias("close_price"),
        F.col("price.quote_currency").alias("quote_currency"),
        F.lit(portfolio["base_currency"]).alias("base_currency"),
        F.when(F.col("position.signed_quantity") > 0, F.lit("LONG"))
        .otherwise(F.lit("SHORT"))
        .alias("position_side"),
        signed_market_value_expression.alias("signed_market_value"),
        F.abs(signed_market_value_expression)
        .cast("decimal(38,16)")
        .alias("absolute_market_value"),
        F.col("position.record_hash").alias(
            "input_position_record_sha256"
        ),
        F.col("price.record_hash").alias("input_price_record_sha256"),
        F.lit(analytics_run_id).alias("analytics_run_id"),
        F.lit(CALCULATION_VERSION).alias("calculation_version"),
        F.lit(calculated_at_utc)
        .cast("timestamp")
        .alias("calculated_at_utc"),
        F.lit(MARKET_VALUE_CONTRACT_VERSION).alias("contract_version"),
        F.lit(EMPTY_SET_SHA256).alias("record_hash"),
    )
    .withColumn(
        "record_hash",
        canonical_hash_expression(MARKET_VALUE_HASH_FIELDS),
    )
    .select(*MARKET_VALUE_COLUMNS)
)
market_value_candidate = freeze_small_dataframe(
    market_value_candidate,
    label="Gold market-value candidate partition",
    max_rows=EXPECTED_POSITION_COUNT * 4,
)

calculated_output_count = market_value_candidate.count()
candidate_distinct_count = market_value_candidate.select(
    "portfolio_id", "instrument_id", "valuation_date"
).distinct().count()
formula_failure_count = market_value_candidate.where(
    F.col("signed_market_value")
    != (
        F.col("signed_quantity").cast("decimal(38,16)")
        * F.col("close_price").cast("decimal(20,8)")
    ).cast("decimal(38,16)")
).count()
absolute_failure_count = market_value_candidate.where(
    F.col("absolute_market_value")
    != F.abs(F.col("signed_market_value")).cast("decimal(38,16)")
).count()
sign_failure_count = market_value_candidate.where(
    ((F.col("position_side") == "LONG") & (F.col("signed_market_value") <= 0))
    | ((F.col("position_side") == "SHORT") & (F.col("signed_market_value") >= 0))
).count()
hash_failure_count = market_value_candidate.withColumn(
    "recalculated_hash",
    canonical_hash_expression(MARKET_VALUE_HASH_FIELDS),
).where(F.col("record_hash") != F.col("recalculated_hash")).count()

add_failure(
    failures,
    calculated_output_count != EXPECTED_POSITION_COUNT
    or candidate_distinct_count != EXPECTED_POSITION_COUNT,
    "POSITION_MARKET_VALUE_KEY_UNIQUE",
)
add_failure(
    failures,
    formula_failure_count != 0,
    "POSITION_MARKET_VALUE_FORMULA_RECONCILES",
)
add_failure(
    failures,
    absolute_failure_count != 0,
    "POSITION_MARKET_VALUE_ABSOLUTE_RECONCILES",
)
add_failure(
    failures,
    sign_failure_count != 0,
    "POSITION_MARKET_VALUE_SIGN_RECONCILES",
)
add_failure(
    failures,
    hash_failure_count != 0,
    "POSITION_MARKET_VALUE_RECORD_HASH_VALID",
)

failed_rule_ids = sorted(set(failures))

print(
    "market_value_validation=PASS "
    f"expected_output_count={EXPECTED_POSITION_COUNT} "
    f"calculated_output_count={calculated_output_count} "
    f"failed_rule_ids={failed_rule_ids}"
)
market_value_candidate.select(
    "portfolio_id",
    "instrument_id",
    "valuation_date",
    "signed_quantity",
    "close_price",
    "position_side",
    "signed_market_value",
    "absolute_market_value",
).orderBy("instrument_id").show(truncate=False)


# COMMAND ----------

manifest_entries = [
    build_manifest_entry(
        dataset_name="PORTFOLIOS",
        frame=portfolio_frame,
        order_columns=["portfolio_id"],
        value_columns=["portfolio_id", "record_hash"],
        selection_scope=f"portfolio_id={portfolio_id}",
    ),
    build_manifest_entry(
        dataset_name="INSTRUMENTS",
        frame=instruments,
        order_columns=["instrument_id"],
        value_columns=["instrument_id", "record_hash"],
        selection_scope=(
            f"portfolio_id={portfolio_id};valuation_date={valuation_date}"
        ),
    ),
    build_manifest_entry(
        dataset_name="DAILY_PRICES",
        frame=prices,
        order_columns=["instrument_id", "price_date", "source_id"],
        value_columns=[
            "instrument_id",
            "price_date",
            "source_id",
            "record_hash",
        ],
        selection_scope=f"price_date={valuation_date}",
    ),
    build_manifest_entry(
        dataset_name="TRADING_CALENDAR",
        frame=calendar,
        order_columns=["exchange_mic", "calendar_date"],
        value_columns=["exchange_mic", "calendar_date", "record_hash"],
        selection_scope=f"calendar_date={valuation_date}",
    ),
    build_manifest_entry(
        dataset_name="POSITIONS",
        frame=position_inputs,
        order_columns=["portfolio_id", "instrument_id", "position_date"],
        value_columns=[
            "portfolio_id",
            "instrument_id",
            "position_date",
            "record_hash",
        ],
        selection_scope=(
            f"portfolio_id={portfolio_id};position_date={valuation_date}"
        ),
    ),
]

input_dataset_names = sorted(
    entry["dataset_name"] for entry in manifest_entries
)
input_record_count = sum(
    entry["record_count"] for entry in manifest_entries
)
input_manifest_digest = input_manifest_sha256(manifest_entries)

print(
    "market_value_input_manifest=PASS "
    f"input_dataset_names={input_dataset_names} "
    f"input_record_count={input_record_count} "
    f"input_manifest_sha256={input_manifest_digest} "
    "manifest_entries="
    f"{json.dumps(manifest_entries, sort_keys=True)}"
)

current_partition = spark.table(MARKET_VALUE_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("valuation_date") == F.lit(valuation_date))
)
canonical_before_count = current_partition.count()
canonical_before_sha256 = market_value_partition_sha256(current_partition)
candidate_sha256 = market_value_partition_sha256(market_value_candidate)

publish_allowed = (
    not failed_rule_ids
    and calculated_output_count == EXPECTED_POSITION_COUNT
)
published = publish_allowed and candidate_sha256 != canonical_before_sha256
if publish_allowed:
    canonical_after_count = EXPECTED_POSITION_COUNT
    canonical_after_sha256 = candidate_sha256
else:
    canonical_after_count = canonical_before_count
    canonical_after_sha256 = canonical_before_sha256

run_status = "SUCCEEDED" if publish_allowed else "FAILED"
completed_at_utc = datetime.now(UTC).replace(tzinfo=None)
published_at_utc = completed_at_utc if published else None

print(
    "market_value_publication_gate=PASS "
    f"canonical_before_count={canonical_before_count} "
    f"candidate_count={calculated_output_count} "
    f"publish_allowed={publish_allowed} "
    f"published={published} "
    f"canonical_after_count={canonical_after_count} "
    f"canonical_before_sha256={canonical_before_sha256} "
    f"canonical_after_sha256={canonical_after_sha256}"
)

audit_values = {
    "analytics_run_id": analytics_run_id,
    "output_dataset_name": "POSITION_MARKET_VALUES",
    "portfolio_id": portfolio_id,
    "valuation_date": valuation_date,
    "attempt_number": attempt_number,
    "reprocess_of_analytics_run_id": reprocess_of_analytics_run_id,
    "trigger_type": trigger_type,
    "started_at_utc": started_at_utc,
    "completed_at_utc": completed_at_utc,
    "status": run_status,
    "input_dataset_names": input_dataset_names,
    "input_record_count": input_record_count,
    "input_manifest_sha256": input_manifest_digest,
    "expected_output_count": EXPECTED_POSITION_COUNT,
    "calculated_output_count": calculated_output_count,
    "canonical_before_count": canonical_before_count,
    "canonical_after_count": canonical_after_count,
    "canonical_before_sha256": canonical_before_sha256,
    "canonical_after_sha256": canonical_after_sha256,
    "published": published,
    "published_at_utc": published_at_utc,
    "warning_count": 0,
    "failed_rule_ids": failed_rule_ids,
    "error_code": None if publish_allowed else "VALIDATION_FAILED",
    "error_message": (
        None
        if publish_allowed
        else "Gold market-value validation prevented atomic publication."
    ),
    "output_contract_version": MARKET_VALUE_CONTRACT_VERSION,
    "calculation_version": CALCULATION_VERSION,
    "code_version": code_version,
    "contract_version": ANALYTICS_RUN_CONTRACT_VERSION,
}
audit_candidate = spark.createDataFrame(
    [audit_values],
    schema=spark.table(ANALYTICS_RUN_TABLE).schema,
)

if published:
    merge_market_value_partition(
        market_value_candidate,
        portfolio_id=portfolio_id,
        valuation_date=valuation_date,
    )
insert_analytics_audit(audit_candidate)

persisted_partition = spark.table(MARKET_VALUE_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("valuation_date") == F.lit(valuation_date))
)
require_equal(
    "persisted market-value count",
    persisted_partition.count(),
    canonical_after_count,
)
require_equal(
    "persisted market-value SHA-256",
    market_value_partition_sha256(persisted_partition),
    canonical_after_sha256,
)
require_equal(
    "persisted analytics audit count",
    spark.table(ANALYTICS_RUN_TABLE)
    .where(F.col("analytics_run_id") == analytics_run_id)
    .count(),
    1,
)

print(
    "market_value_persistence=PASS "
    f"analytics_run_id={analytics_run_id} "
    f"portfolio_id={portfolio_id} "
    f"valuation_date={valuation_date} "
    f"run_status={run_status} "
    f"published={published} "
    f"persisted_market_value_count={persisted_partition.count()} "
    f"persisted_partition_sha256={canonical_after_sha256}"
)

if not publish_allowed:
    raise ValueError(
        "Gold market-value calculation failed rules: "
        + ", ".join(failed_rule_ids)
    )
