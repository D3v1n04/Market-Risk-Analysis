# Databricks notebook source
"""Derive one complete, audited Silver cash-balance partition."""

import hashlib
import json
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.dbutils import DBUtils
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

PORTFOLIO_TABLE = "workspace.devin_market_risk_dev.silver_portfolios"
INSTRUMENT_TABLE = "workspace.devin_market_risk_dev.silver_instruments"
PRICE_TABLE = "workspace.devin_market_risk_dev.silver_daily_prices"
ACTION_TABLE = "workspace.devin_market_risk_dev.silver_corporate_actions"
CALENDAR_TABLE = "workspace.devin_market_risk_dev.silver_trading_calendar"
POSITION_TABLE = "workspace.devin_market_risk_dev.silver_positions"
CASH_BALANCE_TABLE = (
    "workspace.devin_market_risk_dev.silver_cash_balances"
)
DERIVATION_RUN_TABLE = (
    "workspace.devin_market_risk_dev.silver_derivation_runs"
)

CASH_BALANCE_CONTRACT_VERSION = "1.1.0"
DERIVATION_RUN_CONTRACT_VERSION = "1.0.0"
CALCULATION_VERSION = "1.0.0"
EMPTY_SET_SHA256 = hashlib.sha256(b"").hexdigest()
ZERO_CASH = Decimal("0.0000000000000000")

PORTFOLIO_ID_PATTERN = re.compile(
    r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$"
)
CODE_VERSION_PATTERN = re.compile(r"^[0-9a-f]{7,40}$")

CASH_BALANCE_COLUMNS = [
    "portfolio_id",
    "cash_date",
    "prior_cash_date",
    "position_input_date",
    "base_currency",
    "opening_cash_balance",
    "dividend_cash_flow",
    "closing_cash_balance",
    "corporate_action_count",
    "input_position_set_sha256",
    "input_corporate_action_set_sha256",
    "derivation_run_id",
    "calculation_version",
    "derived_at_utc",
    "contract_version",
    "record_hash",
]

CASH_BALANCE_HASH_FIELDS = [
    "portfolio_id",
    "cash_date",
    "prior_cash_date",
    "position_input_date",
    "base_currency",
    "opening_cash_balance",
    "dividend_cash_flow",
    "closing_cash_balance",
    "corporate_action_count",
    "input_position_set_sha256",
    "input_corporate_action_set_sha256",
    "calculation_version",
]

INPUT_CONTRACT_VERSIONS = {
    "PORTFOLIOS": "1.1.0",
    "INSTRUMENTS": "1.1.0",
    "DAILY_PRICES": "1.1.0",
    "CORPORATE_ACTIONS": "1.1.0",
    "TRADING_CALENDAR": "1.0.0",
    "POSITIONS": "1.1.0",
    "CASH_BALANCES": CASH_BALANCE_CONTRACT_VERSION,
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


def cash_partition_sha256(frame: DataFrame) -> str | None:
    """Hash one canonical cash partition."""
    if frame.count() == 0:
        return None
    return ordered_set_sha256(
        frame,
        order_columns=["portfolio_id", "cash_date"],
        value_columns=["portfolio_id", "cash_date", "record_hash"],
    )


def canonical_hash_expression(fields: list[str]) -> Column:
    """Build the contract-defined cash record hash."""
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
    snapshot_reference: str,
    selection_scope: str,
) -> dict[str, Any]:
    """Build one deterministic trusted-input manifest entry."""
    return {
        "dataset_name": dataset_name,
        "dataset_contract_version": INPUT_CONTRACT_VERSIONS[dataset_name],
        "snapshot_reference": snapshot_reference,
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


def merge_cash_partition(
    candidate: DataFrame,
    *,
    portfolio_id: str,
    cash_date: date,
) -> None:
    """Atomically replace one complete portfolio-date cash partition."""
    target = DeltaTable.forName(spark, CASH_BALANCE_TABLE)
    match_condition = (
        "target.portfolio_id = source.portfolio_id "
        "AND target.cash_date = source.cash_date"
    )
    partition_condition = (
        f"target.portfolio_id = '{portfolio_id}' "
        f"AND target.cash_date = DATE '{cash_date.isoformat()}'"
    )
    (
        target.alias("target")
        .merge(candidate.alias("source"), match_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=partition_condition)
        .execute()
    )


def insert_derivation_audit(frame: DataFrame) -> None:
    """Insert immutable derivation evidence."""
    target = DeltaTable.forName(spark, DERIVATION_RUN_TABLE)
    (
        target.alias("target")
        .merge(
            frame.alias("source"),
            "target.derivation_run_id = source.derivation_run_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def scalar_decimal(frame: DataFrame, column_name: str) -> Decimal | None:
    """Collect one bounded decimal aggregate."""
    row = frame.select(column_name).limit(1).collect()
    return row[0][column_name] if row else None


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils = DBUtils(spark)

dbutils.widgets.text("portfolio_id", "", "Portfolio ID")
dbutils.widgets.text("cash_date", "", "Cash date (YYYY-MM-DD)")
dbutils.widgets.text("code_version", "", "Git code version")
dbutils.widgets.text(
    "trigger_type",
    "MANUAL",
    "Cash derivation trigger type",
)

portfolio_id = require_parameter(
    "portfolio_id",
    dbutils.widgets.get("portfolio_id").strip().upper(),
    PORTFOLIO_ID_PATTERN,
)
cash_date = date.fromisoformat(dbutils.widgets.get("cash_date").strip())
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

derivation_run_id = str(uuid4())
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
    raise ValueError("actual_inception_date must be governed before cash")
if cash_date < portfolio["actual_inception_date"]:
    raise ValueError("cash_date cannot precede actual_inception_date")

previous_runs = (
    spark.table(DERIVATION_RUN_TABLE)
    .where(
        (F.col("output_dataset_name") == "CASH_BALANCES")
        & (F.col("portfolio_id") == portfolio_id)
        & (F.col("business_date") == F.lit(cash_date))
    )
    .orderBy(F.col("attempt_number").desc())
    .select("derivation_run_id", "attempt_number")
    .limit(1)
    .collect()
)
if previous_runs:
    attempt_number = previous_runs[0]["attempt_number"] + 1
    reprocess_of_derivation_run_id = previous_runs[0][
        "derivation_run_id"
    ]
else:
    attempt_number = 1
    reprocess_of_derivation_run_id = None

portfolio_position_dates = (
    spark.table(POSITION_TABLE)
    .where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("position_date") <= F.lit(cash_date))
    )
    .groupBy("position_date")
    .agg(F.countDistinct("instrument_id").alias("instrument_count"))
    .where(F.col("instrument_count") == 15)
    .select("position_date")
)
prior_date_rows = (
    portfolio_position_dates.where(F.col("position_date") < F.lit(cash_date))
    .agg(F.max("position_date").alias("prior_cash_date"))
    .collect()
)
prior_cash_date = (
    prior_date_rows[0]["prior_cash_date"] if prior_date_rows else None
)
is_inception = cash_date == portfolio["actual_inception_date"]
position_input_date = cash_date if is_inception else prior_cash_date

position_inputs = spark.table(POSITION_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (
        F.col("position_date")
        == F.lit(position_input_date).cast("date")
    )
)
position_instruments = position_inputs.select("instrument_id").distinct()
instruments = (
    spark.table(INSTRUMENT_TABLE)
    .join(position_instruments, "instrument_id", "inner")
    .where(
        (F.col("active_from") <= F.lit(cash_date))
        & (
            F.col("active_to").isNull()
            | (F.col("active_to") >= F.lit(cash_date))
        )
        & F.col("is_active")
    )
)
prices = (
    spark.table(PRICE_TABLE)
    .join(position_instruments, "instrument_id", "inner")
    .where(F.col("price_date") == F.lit(cash_date))
)
exchange_ids = instruments.select("exchange_mic").distinct()
calendar = (
    spark.table(CALENDAR_TABLE)
    .join(exchange_ids, "exchange_mic", "inner")
    .where(F.col("calendar_date") == F.lit(cash_date))
)
actions_on_date = (
    spark.table(ACTION_TABLE)
    .join(position_instruments, "instrument_id", "inner")
    .where(F.col("effective_date") == F.lit(cash_date))
)
dividend_actions = actions_on_date.where(
    F.col("action_type") == "CASH_DIVIDEND"
)
prior_cash_rows = spark.table(CASH_BALANCE_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("cash_date") == F.lit(prior_cash_date).cast("date"))
)

print(
    "cash_parameters=PASS "
    f"derivation_run_id={derivation_run_id} "
    f"portfolio_id={portfolio_id} "
    f"cash_date={cash_date} "
    f"attempt_number={attempt_number} "
    "reprocess_of_derivation_run_id="
    f"{reprocess_of_derivation_run_id} "
    f"is_inception={is_inception} "
    f"prior_cash_date={prior_cash_date} "
    f"position_input_date={position_input_date}"
)


# COMMAND ----------

failures: list[str] = []
position_count = position_inputs.select("instrument_id").distinct().count()
instrument_count = instruments.select("instrument_id").distinct().count()
price_count = prices.select("instrument_id").distinct().count()
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
invalid_price_count = prices.where(
    F.col("close_price").isNull()
    | (F.col("close_price") <= 0)
    | (F.col("quote_currency") != portfolio["base_currency"])
).count()
invalid_dividend_count = dividend_actions.where(
    F.col("dividend_amount_per_share").isNull()
    | (F.col("dividend_amount_per_share") <= 0)
    | (F.col("dividend_currency") != portfolio["base_currency"])
    | F.col("split_ratio").isNotNull()
).count()
prior_cash_count = prior_cash_rows.count()

add_failure(
    failures,
    position_count != 15
    or instrument_count != 15
    or price_count != 15
    or invalid_price_count != 0,
    "CASH_BALANCE_INPUT_COVERAGE",
)
add_failure(
    failures,
    exchange_count == 0 or open_exchange_count != exchange_count,
    "CASH_BALANCE_INPUT_COVERAGE",
)
add_failure(
    failures,
    invalid_dividend_count != 0,
    "CASH_BALANCE_INPUT_COVERAGE",
)
add_failure(
    failures,
    is_inception
    and (prior_cash_date is not None or position_input_date != cash_date),
    "CASH_BALANCE_DATE_SEQUENCE",
)
add_failure(
    failures,
    not is_inception
    and (
        prior_cash_date is None
        or position_input_date != prior_cash_date
        or prior_cash_count != 1
    ),
    "CASH_BALANCE_DATE_SEQUENCE",
)

signed_market_value = scalar_decimal(
    position_inputs.alias("position")
    .join(
        prices.select("instrument_id", "close_price").alias("price"),
        "instrument_id",
        "inner",
    )
    .agg(
        F.sum(
            (
                F.col("position.signed_quantity").cast("decimal(38,16)")
                * F.col("price.close_price").cast("decimal(20,8)")
            ).cast("decimal(38,16)")
        )
        .cast("decimal(38,16)")
        .alias("signed_market_value")
    ),
    "signed_market_value",
)
prior_closing_cash = scalar_decimal(
    prior_cash_rows.select(
        F.col("closing_cash_balance").cast("decimal(38,16)").alias(
            "prior_closing_cash"
        )
    ),
    "prior_closing_cash",
)

if is_inception:
    expected_opening_cash = (
        Decimal(str(portfolio["initial_nav"])) - signed_market_value
        if signed_market_value is not None
        else None
    )
else:
    expected_opening_cash = prior_closing_cash
opening_cash_balance = (
    expected_opening_cash
    if expected_opening_cash is not None
    else ZERO_CASH
)

dividend_cash_value = scalar_decimal(
    position_inputs.alias("position")
    .join(
        dividend_actions.select(
            "instrument_id", "dividend_amount_per_share"
        ).alias("action"),
        "instrument_id",
        "inner",
    )
    .agg(
        F.coalesce(
            F.sum(
                (
                    F.col("position.signed_quantity").cast(
                        "decimal(38,16)"
                    )
                    * F.col("action.dividend_amount_per_share").cast(
                        "decimal(20,8)"
                    )
                ).cast("decimal(38,16)")
            ),
            F.lit(ZERO_CASH).cast("decimal(38,16)"),
        )
        .cast("decimal(38,16)")
        .alias("dividend_cash_flow")
    ),
    "dividend_cash_flow",
)
dividend_cash_flow = dividend_cash_value or ZERO_CASH
closing_cash_balance = opening_cash_balance + dividend_cash_flow
corporate_action_count = dividend_actions.count()

position_set_sha256 = ordered_set_sha256(
    position_inputs,
    order_columns=["portfolio_id", "instrument_id", "position_date"],
    value_columns=[
        "portfolio_id",
        "instrument_id",
        "position_date",
        "record_hash",
    ],
)
dividend_action_set_sha256 = ordered_set_sha256(
    dividend_actions,
    order_columns=["instrument_id", "effective_date", "action_type"],
    value_columns=[
        "instrument_id",
        "effective_date",
        "action_type",
        "record_hash",
    ],
)

derived_at_utc = datetime.now(UTC).replace(tzinfo=None)
candidate_values = {
    "portfolio_id": portfolio_id,
    "cash_date": cash_date,
    "prior_cash_date": prior_cash_date,
    "position_input_date": position_input_date or cash_date,
    "base_currency": portfolio["base_currency"],
    "opening_cash_balance": opening_cash_balance,
    "dividend_cash_flow": dividend_cash_flow,
    "closing_cash_balance": closing_cash_balance,
    "corporate_action_count": corporate_action_count,
    "input_position_set_sha256": position_set_sha256,
    "input_corporate_action_set_sha256": dividend_action_set_sha256,
    "derivation_run_id": derivation_run_id,
    "calculation_version": CALCULATION_VERSION,
    "derived_at_utc": derived_at_utc,
    "contract_version": CASH_BALANCE_CONTRACT_VERSION,
    "record_hash": EMPTY_SET_SHA256,
}
cash_candidate = spark.createDataFrame(
    [candidate_values],
    schema=spark.table(CASH_BALANCE_TABLE).schema,
).withColumn(
    "record_hash",
    canonical_hash_expression(CASH_BALANCE_HASH_FIELDS),
).select(*CASH_BALANCE_COLUMNS)
cash_candidate = freeze_small_dataframe(
    cash_candidate,
    label="cash candidate partition",
    max_rows=1,
)

derived_output_count = cash_candidate.count()
candidate_distinct_count = (
    cash_candidate.select("portfolio_id", "cash_date").distinct().count()
)
candidate = cash_candidate.first()
hash_failure_count = cash_candidate.withColumn(
    "recalculated_hash",
    canonical_hash_expression(CASH_BALANCE_HASH_FIELDS),
).where(F.col("record_hash") != F.col("recalculated_hash")).count()

add_failure(
    failures,
    derived_output_count != 1 or candidate_distinct_count != 1,
    "CASH_BALANCE_BUSINESS_KEY_UNIQUE",
)
add_failure(
    failures,
    portfolio["base_currency"] != "USD"
    or candidate["base_currency"] != portfolio["base_currency"],
    "CASH_BALANCE_BASE_CURRENCY_MATCH",
)
add_failure(
    failures,
    expected_opening_cash is None,
    (
        "CASH_BALANCE_INCEPTION_RECONCILIATION"
        if is_inception
        else "CASH_BALANCE_OPENING_CONTINUITY"
    ),
)
add_failure(
    failures,
    expected_opening_cash is not None
    and candidate["opening_cash_balance"] != expected_opening_cash,
    (
        "CASH_BALANCE_INCEPTION_RECONCILIATION"
        if is_inception
        else "CASH_BALANCE_OPENING_CONTINUITY"
    ),
)
add_failure(
    failures,
    candidate["dividend_cash_flow"] != dividend_cash_flow,
    "CASH_BALANCE_DIVIDEND_RECONCILIATION",
)
add_failure(
    failures,
    candidate["closing_cash_balance"]
    != candidate["opening_cash_balance"]
    + candidate["dividend_cash_flow"],
    "CASH_BALANCE_CLOSING_RECONCILIATION",
)
add_failure(
    failures,
    candidate["corporate_action_count"] != corporate_action_count,
    "CASH_BALANCE_ACTION_COUNT_CONSISTENT",
)
add_failure(
    failures,
    hash_failure_count != 0,
    "CASH_BALANCE_RECORD_HASH_VALID",
)
add_failure(
    failures,
    len(portfolio_rows) != 1 or instrument_count != position_count,
    "CASH_BALANCE_REFERENCES_VALID",
)

failed_rule_ids = sorted(set(failures))

print(
    "cash_validation=PASS "
    "expected_output_count=1 "
    f"derived_output_count={derived_output_count} "
    f"failed_rule_ids={failed_rule_ids}"
)
cash_candidate.select(
    "portfolio_id",
    "cash_date",
    "prior_cash_date",
    "position_input_date",
    "opening_cash_balance",
    "dividend_cash_flow",
    "closing_cash_balance",
    "corporate_action_count",
).show(truncate=False)


# COMMAND ----------

portfolio_frame = spark.createDataFrame(
    [portfolio],
    schema=spark.table(PORTFOLIO_TABLE).schema,
)
manifest_entries = [
    build_manifest_entry(
        dataset_name="PORTFOLIOS",
        frame=portfolio_frame,
        order_columns=["portfolio_id"],
        value_columns=["portfolio_id", "record_hash"],
        snapshot_reference="SILVER_CURRENT",
        selection_scope=f"portfolio_id={portfolio_id}",
    ),
    build_manifest_entry(
        dataset_name="INSTRUMENTS",
        frame=instruments,
        order_columns=["instrument_id"],
        value_columns=["instrument_id", "record_hash"],
        snapshot_reference="SILVER_CURRENT",
        selection_scope=(
            f"portfolio_id={portfolio_id};cash_date={cash_date}"
        ),
    ),
    build_manifest_entry(
        dataset_name="DAILY_PRICES",
        frame=prices,
        order_columns=["instrument_id", "price_date"],
        value_columns=["instrument_id", "price_date", "record_hash"],
        snapshot_reference="SILVER_CURRENT",
        selection_scope=f"price_date={cash_date}",
    ),
    build_manifest_entry(
        dataset_name="CORPORATE_ACTIONS",
        frame=actions_on_date,
        order_columns=["instrument_id", "effective_date", "action_type"],
        value_columns=[
            "instrument_id",
            "effective_date",
            "action_type",
            "record_hash",
        ],
        snapshot_reference="SILVER_CURRENT",
        selection_scope=f"effective_date={cash_date}",
    ),
    build_manifest_entry(
        dataset_name="TRADING_CALENDAR",
        frame=calendar,
        order_columns=["exchange_mic", "calendar_date"],
        value_columns=["exchange_mic", "calendar_date", "record_hash"],
        snapshot_reference="SILVER_CURRENT",
        selection_scope=f"calendar_date={cash_date}",
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
        snapshot_reference="SILVER_CURRENT",
        selection_scope=(
            f"portfolio_id={portfolio_id};"
            f"position_date={position_input_date}"
        ),
    ),
]
if not is_inception:
    manifest_entries.append(
        build_manifest_entry(
            dataset_name="CASH_BALANCES",
            frame=prior_cash_rows,
            order_columns=["portfolio_id", "cash_date"],
            value_columns=["portfolio_id", "cash_date", "record_hash"],
            snapshot_reference="SILVER_CURRENT",
            selection_scope=(
                f"portfolio_id={portfolio_id};cash_date={prior_cash_date}"
            ),
        )
    )

input_dataset_names = sorted(
    entry["dataset_name"] for entry in manifest_entries
)
input_record_count = sum(
    entry["record_count"] for entry in manifest_entries
)
input_manifest_digest = input_manifest_sha256(manifest_entries)

print(
    "cash_input_manifest=PASS "
    f"input_dataset_names={input_dataset_names} "
    f"input_record_count={input_record_count} "
    f"input_manifest_sha256={input_manifest_digest} "
    "manifest_entries="
    f"{json.dumps(manifest_entries, sort_keys=True)}"
)

current_partition = spark.table(CASH_BALANCE_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("cash_date") == F.lit(cash_date))
)
canonical_before_count = current_partition.count()
canonical_before_sha256 = cash_partition_sha256(current_partition)
candidate_sha256 = cash_partition_sha256(cash_candidate)

publish_allowed = not failed_rule_ids and derived_output_count == 1
published = publish_allowed and candidate_sha256 != canonical_before_sha256
if publish_allowed:
    canonical_after_count = 1
    canonical_after_sha256 = candidate_sha256
else:
    canonical_after_count = canonical_before_count
    canonical_after_sha256 = canonical_before_sha256

run_status = "SUCCEEDED" if publish_allowed else "FAILED"
completed_at_utc = datetime.now(UTC).replace(tzinfo=None)
published_at_utc = completed_at_utc if published else None

print(
    "cash_publication_gate=PASS "
    f"canonical_before_count={canonical_before_count} "
    f"candidate_count={derived_output_count} "
    f"publish_allowed={publish_allowed} "
    f"published={published} "
    f"canonical_after_count={canonical_after_count} "
    f"canonical_before_sha256={canonical_before_sha256} "
    f"canonical_after_sha256={canonical_after_sha256}"
)

audit_values = {
    "derivation_run_id": derivation_run_id,
    "output_dataset_name": "CASH_BALANCES",
    "portfolio_id": portfolio_id,
    "business_date": cash_date,
    "attempt_number": attempt_number,
    "reprocess_of_derivation_run_id": reprocess_of_derivation_run_id,
    "trigger_type": trigger_type,
    "started_at_utc": started_at_utc,
    "completed_at_utc": completed_at_utc,
    "status": run_status,
    "input_dataset_names": input_dataset_names,
    "input_record_count": input_record_count,
    "input_manifest_sha256": input_manifest_digest,
    "expected_output_count": 1,
    "derived_output_count": derived_output_count,
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
        else "Cash validation prevented atomic publication."
    ),
    "output_contract_version": CASH_BALANCE_CONTRACT_VERSION,
    "calculation_version": CALCULATION_VERSION,
    "code_version": code_version,
    "contract_version": DERIVATION_RUN_CONTRACT_VERSION,
}
audit_candidate = spark.createDataFrame(
    [audit_values],
    schema=spark.table(DERIVATION_RUN_TABLE).schema,
)

if published:
    merge_cash_partition(
        cash_candidate,
        portfolio_id=portfolio_id,
        cash_date=cash_date,
    )
insert_derivation_audit(audit_candidate)

persisted_partition = spark.table(CASH_BALANCE_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("cash_date") == F.lit(cash_date))
)
require_equal(
    "persisted cash count",
    persisted_partition.count(),
    canonical_after_count,
)
require_equal(
    "persisted cash SHA-256",
    cash_partition_sha256(persisted_partition),
    canonical_after_sha256,
)
require_equal(
    "persisted derivation audit count",
    spark.table(DERIVATION_RUN_TABLE)
    .where(F.col("derivation_run_id") == derivation_run_id)
    .count(),
    1,
)

print(
    "cash_persistence=PASS "
    f"derivation_run_id={derivation_run_id} "
    f"portfolio_id={portfolio_id} "
    f"cash_date={cash_date} "
    f"run_status={run_status} "
    f"published={published} "
    f"persisted_cash_count={persisted_partition.count()} "
    f"persisted_partition_sha256={canonical_after_sha256}"
)

if not publish_allowed:
    raise ValueError(
        "Cash derivation failed rules: " + ", ".join(failed_rule_ids)
    )
