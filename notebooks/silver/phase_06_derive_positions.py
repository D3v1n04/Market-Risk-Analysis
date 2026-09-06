# Databricks notebook source
"""Derive one complete, audited Silver position partition."""

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
ALLOCATION_TABLE = (
    "workspace.devin_market_risk_dev.silver_target_allocations"
)
PRICE_TABLE = "workspace.devin_market_risk_dev.silver_daily_prices"
ACTION_TABLE = "workspace.devin_market_risk_dev.silver_corporate_actions"
CALENDAR_TABLE = "workspace.devin_market_risk_dev.silver_trading_calendar"
POSITION_TABLE = "workspace.devin_market_risk_dev.silver_positions"
DERIVATION_RUN_TABLE = (
    "workspace.devin_market_risk_dev.silver_derivation_runs"
)

POSITION_CONTRACT_VERSION = "1.1.0"
DERIVATION_RUN_CONTRACT_VERSION = "1.0.0"
CALCULATION_VERSION = "1.0.0"
EMPTY_SET_SHA256 = hashlib.sha256(b"").hexdigest()

PORTFOLIO_ID_PATTERN = re.compile(
    r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$"
)
CODE_VERSION_PATTERN = re.compile(r"^[0-9a-f]{7,40}$")

POSITION_COLUMNS = [
    "portfolio_id",
    "instrument_id",
    "position_date",
    "signed_quantity",
    "position_basis",
    "allocation_effective_from",
    "prior_position_date",
    "input_allocation_record_sha256",
    "input_price_record_sha256",
    "input_prior_position_record_sha256",
    "input_corporate_action_set_sha256",
    "derivation_run_id",
    "calculation_version",
    "derived_at_utc",
    "contract_version",
    "record_hash",
]

POSITION_HASH_FIELDS = [
    "portfolio_id",
    "instrument_id",
    "position_date",
    "signed_quantity",
    "position_basis",
    "allocation_effective_from",
    "prior_position_date",
    "input_allocation_record_sha256",
    "input_price_record_sha256",
    "input_prior_position_record_sha256",
    "input_corporate_action_set_sha256",
    "calculation_version",
]

INPUT_CONTRACT_VERSIONS = {
    "PORTFOLIOS": "1.1.0",
    "INSTRUMENTS": "1.1.0",
    "TARGET_ALLOCATIONS": "1.1.0",
    "DAILY_PRICES": "1.1.0",
    "CORPORATE_ACTIONS": "1.1.0",
    "TRADING_CALENDAR": "1.0.0",
    "POSITIONS": POSITION_CONTRACT_VERSION,
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


def partition_sha256(frame: DataFrame) -> str | None:
    """Hash one canonical position partition."""
    if frame.count() == 0:
        return None
    return ordered_set_sha256(
        frame,
        order_columns=["portfolio_id", "instrument_id", "position_date"],
        value_columns=[
            "portfolio_id",
            "instrument_id",
            "position_date",
            "record_hash",
        ],
    )


def canonical_hash_expression(fields: list[str]) -> Column:
    """Build the contract-defined position record hash."""
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


def merge_position_partition(
    candidates: DataFrame,
    *,
    portfolio_id: str,
    position_date: date,
) -> None:
    """Atomically replace one complete portfolio-date partition."""
    target = DeltaTable.forName(spark, POSITION_TABLE)
    match_condition = (
        "target.portfolio_id = source.portfolio_id "
        "AND target.instrument_id = source.instrument_id "
        "AND target.position_date = source.position_date"
    )
    partition_condition = (
        f"target.portfolio_id = '{portfolio_id}' "
        f"AND target.position_date = DATE '{position_date.isoformat()}'"
    )
    (
        target.alias("target")
        .merge(candidates.alias("source"), match_condition)
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
            (
                "target.derivation_run_id = "
                "source.derivation_run_id"
            ),
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils = DBUtils(spark)

dbutils.widgets.text("portfolio_id", "", "Portfolio ID")
dbutils.widgets.text("position_date", "", "Position date (YYYY-MM-DD)")
dbutils.widgets.text("code_version", "", "Git code version")
dbutils.widgets.text(
    "trigger_type",
    "MANUAL",
    "Position derivation trigger type",
)

portfolio_id = require_parameter(
    "portfolio_id",
    dbutils.widgets.get("portfolio_id").strip().upper(),
    PORTFOLIO_ID_PATTERN,
)
position_date = date.fromisoformat(
    dbutils.widgets.get("position_date").strip()
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
    raise ValueError("actual_inception_date must be governed before positions")
if position_date < portfolio["actual_inception_date"]:
    raise ValueError("position_date cannot precede actual_inception_date")

previous_runs = (
    spark.table(DERIVATION_RUN_TABLE)
    .where(
        (F.col("output_dataset_name") == "POSITIONS")
        & (F.col("portfolio_id") == portfolio_id)
        & (F.col("business_date") == F.lit(position_date))
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

allocations = (
    spark.table(ALLOCATION_TABLE)
    .where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("effective_from") <= F.lit(position_date))
        & (
            F.col("effective_to").isNull()
            | (F.col("effective_to") >= F.lit(position_date))
        )
    )
)
expected_output_count = allocations.count()
allocation_instruments = allocations.select("instrument_id")

instruments = (
    spark.table(INSTRUMENT_TABLE)
    .join(allocation_instruments, "instrument_id", "inner")
    .where(
        (F.col("active_from") <= F.lit(position_date))
        & (
            F.col("active_to").isNull()
            | (F.col("active_to") >= F.lit(position_date))
        )
        & (F.col("is_active") == F.lit(True))
    )
)
prices = (
    spark.table(PRICE_TABLE)
    .join(allocation_instruments, "instrument_id", "inner")
    .where(F.col("price_date") == F.lit(position_date))
)
exchange_ids = instruments.select("exchange_mic").distinct()
calendar = (
    spark.table(CALENDAR_TABLE)
    .join(exchange_ids, "exchange_mic", "inner")
    .where(F.col("calendar_date") == F.lit(position_date))
)
actions_on_date = (
    spark.table(ACTION_TABLE)
    .join(allocation_instruments, "instrument_id", "inner")
    .where(F.col("effective_date") == F.lit(position_date))
)
split_actions = actions_on_date.where(
    F.col("action_type") == "STOCK_SPLIT"
)
dividend_actions = actions_on_date.where(
    F.col("action_type") == "CASH_DIVIDEND"
)

complete_price_dates = (
    spark.table(PRICE_TABLE)
    .join(allocation_instruments, "instrument_id", "inner")
    .where(
        (F.col("price_date") >= F.lit(portfolio["actual_inception_date"]))
        & (F.col("price_date") <= F.lit(position_date))
        & (F.col("close_price") > 0)
        & (F.col("quote_currency") == portfolio["base_currency"])
    )
    .groupBy("price_date")
    .agg(
        F.countDistinct("instrument_id").alias("instrument_count")
    )
    .where(F.col("instrument_count") == expected_output_count)
    .select("price_date")
)
prior_date_rows = (
    complete_price_dates.where(F.col("price_date") < F.lit(position_date))
    .agg(F.max("price_date").alias("prior_position_date"))
    .collect()
)
prior_position_date = (
    prior_date_rows[0]["prior_position_date"]
    if prior_date_rows
    else None
)
is_inception = position_date == portfolio["actual_inception_date"]
prior_positions = (
    spark.table(POSITION_TABLE)
    .where(
        (F.col("portfolio_id") == portfolio_id)
        & (
            F.col("position_date")
            == F.lit(prior_position_date).cast("date")
        )
    )
)

print(
    "position_parameters=PASS "
    f"derivation_run_id={derivation_run_id} "
    f"portfolio_id={portfolio_id} "
    f"position_date={position_date} "
    f"attempt_number={attempt_number} "
    "reprocess_of_derivation_run_id="
    f"{reprocess_of_derivation_run_id} "
    f"is_inception={is_inception} "
    f"prior_position_date={prior_position_date}"
)


# COMMAND ----------

failures: list[str] = []
allocation_distinct_count = allocations.select("instrument_id").distinct().count()
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
invalid_split_count = split_actions.where(
    F.col("split_ratio").isNull() | (F.col("split_ratio") <= 0)
).count()

add_failure(
    failures,
    expected_output_count != 15
    or allocation_distinct_count != expected_output_count,
    "POSITION_DAILY_COVERAGE",
)
add_failure(
    failures,
    instrument_count != expected_output_count,
    "POSITION_REFERENCES_VALID",
)
add_failure(
    failures,
    price_count != expected_output_count or invalid_price_count != 0,
    "POSITION_INPUT_COVERAGE",
)
add_failure(
    failures,
    exchange_count == 0 or open_exchange_count != exchange_count,
    "POSITION_VALID_SESSION",
)
add_failure(
    failures,
    invalid_split_count != 0,
    "POSITION_INPUT_COVERAGE",
)
add_failure(
    failures,
    position_date
    not in {
        row["price_date"]
        for row in complete_price_dates.collect()
    },
    "POSITION_INPUT_COVERAGE",
)
add_failure(
    failures,
    is_inception and prior_position_date is not None,
    "POSITION_DATE_SEQUENCE",
)
add_failure(
    failures,
    not is_inception and prior_position_date is None,
    "POSITION_DATE_SEQUENCE",
)
add_failure(
    failures,
    not is_inception
    and prior_positions.select("instrument_id").distinct().count()
    != expected_output_count,
    "POSITION_INPUT_COVERAGE",
)

split_statistics = split_actions.groupBy("instrument_id").agg(
    F.count("*").cast("bigint").alias("split_action_count"),
    F.aggregate(
        F.array_sort(F.collect_list("split_ratio")),
        F.lit(Decimal("1.0000000000000000")).cast("decimal(38,16)"),
        lambda product, ratio: (
            product * ratio.cast("decimal(38,16)")
        ).cast("decimal(38,16)"),
    ).alias("split_multiplier"),
    F.sha2(
        F.concat_ws(
            "\n",
            F.array_sort(F.collect_list("record_hash")),
        ),
        256,
    ).alias("split_action_set_sha256"),
)

derived_at_utc = datetime.now(UTC).replace(tzinfo=None)
if is_inception:
    position_candidates = (
        allocations.alias("allocation")
        .join(
            prices.select(
                "instrument_id",
                "close_price",
                F.col("record_hash").alias("price_record_hash"),
            ).alias("price"),
            "instrument_id",
            "inner",
        )
        .select(
            F.lit(portfolio_id).alias("portfolio_id"),
            "instrument_id",
            F.lit(position_date).cast("date").alias("position_date"),
            (
                (
                    F.col("target_weight").cast("decimal(18,10)")
                    * F.lit(portfolio["initial_nav"]).cast("decimal(18,2)")
                )
                / F.col("close_price").cast("decimal(20,8)")
            )
            .cast("decimal(38,16)")
            .alias("signed_quantity"),
            F.lit("INITIAL_ALLOCATION").alias("position_basis"),
            F.col("effective_from").alias("allocation_effective_from"),
            F.lit(None).cast("date").alias("prior_position_date"),
            F.col("allocation.record_hash").alias(
                "input_allocation_record_sha256"
            ),
            F.col("price_record_hash").alias(
                "input_price_record_sha256"
            ),
            F.lit(None).cast("string").alias(
                "input_prior_position_record_sha256"
            ),
            F.lit(EMPTY_SET_SHA256).alias(
                "input_corporate_action_set_sha256"
            ),
            F.lit(derivation_run_id).alias("derivation_run_id"),
            F.lit(CALCULATION_VERSION).alias("calculation_version"),
            F.lit(derived_at_utc).cast("timestamp").alias(
                "derived_at_utc"
            ),
            F.lit(POSITION_CONTRACT_VERSION).alias("contract_version"),
        )
    )
else:
    position_candidates = (
        allocations.alias("allocation")
        .join(
            prior_positions.select(
                "instrument_id",
                F.col("signed_quantity").alias("prior_signed_quantity"),
                F.col("record_hash").alias("prior_record_hash"),
            ).alias("prior"),
            "instrument_id",
            "inner",
        )
        .join(split_statistics, "instrument_id", "left")
        .select(
            F.lit(portfolio_id).alias("portfolio_id"),
            "instrument_id",
            F.lit(position_date).cast("date").alias("position_date"),
            (
                F.col("prior_signed_quantity").cast("decimal(38,16)")
                * F.coalesce(
                    F.col("split_multiplier"),
                    F.lit(Decimal("1.0000000000000000")).cast(
                        "decimal(38,16)"
                    ),
                )
            )
            .cast("decimal(38,16)")
            .alias("signed_quantity"),
            F.when(
                F.coalesce(F.col("split_action_count"), F.lit(0)) > 0,
                F.lit("SPLIT_ADJUSTED_CARRY_FORWARD"),
            )
            .otherwise(F.lit("BUY_AND_HOLD_CARRY_FORWARD"))
            .alias("position_basis"),
            F.col("effective_from").alias("allocation_effective_from"),
            F.lit(prior_position_date).cast("date").alias(
                "prior_position_date"
            ),
            F.col("allocation.record_hash").alias(
                "input_allocation_record_sha256"
            ),
            F.lit(None).cast("string").alias(
                "input_price_record_sha256"
            ),
            F.col("prior_record_hash").alias(
                "input_prior_position_record_sha256"
            ),
            F.coalesce(
                F.col("split_action_set_sha256"),
                F.lit(EMPTY_SET_SHA256),
            ).alias("input_corporate_action_set_sha256"),
            F.lit(derivation_run_id).alias("derivation_run_id"),
            F.lit(CALCULATION_VERSION).alias("calculation_version"),
            F.lit(derived_at_utc).cast("timestamp").alias(
                "derived_at_utc"
            ),
            F.lit(POSITION_CONTRACT_VERSION).alias("contract_version"),
        )
    )

position_candidates = (
    position_candidates.withColumn(
        "record_hash",
        canonical_hash_expression(POSITION_HASH_FIELDS),
    )
    .select(*POSITION_COLUMNS)
)
position_candidates = freeze_small_dataframe(
    position_candidates,
    label="position candidate partition",
    max_rows=50,
)

derived_output_count = position_candidates.count()
candidate_distinct_count = (
    position_candidates.select(
        "portfolio_id",
        "instrument_id",
        "position_date",
    )
    .distinct()
    .count()
)
nonzero_failure_count = position_candidates.where(
    F.col("signed_quantity").isNull()
    | (F.col("signed_quantity") == 0)
).count()
sign_failure_count = (
    position_candidates.alias("position")
    .join(
        allocations.select("instrument_id", "target_weight").alias(
            "allocation"
        ),
        "instrument_id",
        "inner",
    )
    .where(
        (
            (F.col("allocation.target_weight") > 0)
            & (F.col("position.signed_quantity") <= 0)
        )
        | (
            (F.col("allocation.target_weight") < 0)
            & (F.col("position.signed_quantity") >= 0)
        )
    )
    .count()
)
hash_failure_count = position_candidates.withColumn(
    "recalculated_hash",
    canonical_hash_expression(POSITION_HASH_FIELDS),
).where(F.col("record_hash") != F.col("recalculated_hash")).count()

add_failure(
    failures,
    derived_output_count != expected_output_count
    or candidate_distinct_count != derived_output_count,
    "POSITION_BUSINESS_KEY_UNIQUE",
)
add_failure(
    failures,
    nonzero_failure_count != 0,
    "POSITION_NONZERO_QUANTITY",
)
add_failure(
    failures,
    sign_failure_count != 0,
    "POSITION_STRATEGY_SIGN",
)
add_failure(
    failures,
    hash_failure_count != 0,
    "POSITION_RECORD_HASH_VALID",
)

if is_inception:
    inception_lineage_failures = position_candidates.where(
        F.col("prior_position_date").isNotNull()
        | F.col("input_prior_position_record_sha256").isNotNull()
        | F.col("input_price_record_sha256").isNull()
        | (F.col("position_basis") != "INITIAL_ALLOCATION")
    ).count()
    add_failure(
        failures,
        inception_lineage_failures != 0,
        "POSITION_INCEPTION_RECONCILIATION",
    )
else:
    carry_lineage_failures = position_candidates.where(
        F.col("prior_position_date").isNull()
        | F.col("input_prior_position_record_sha256").isNull()
        | F.col("input_price_record_sha256").isNotNull()
    ).count()
    add_failure(
        failures,
        carry_lineage_failures != 0,
        "POSITION_BUY_AND_HOLD_CONTINUITY",
    )

    candidate_and_prior = position_candidates.alias("candidate").join(
        prior_positions.select(
            "instrument_id",
            F.col("signed_quantity").alias("expected_prior_quantity"),
        ).alias("prior"),
        "instrument_id",
        "inner",
    )
    ordinary_continuity_failures = candidate_and_prior.where(
        (F.col("candidate.position_basis") == "BUY_AND_HOLD_CARRY_FORWARD")
        & (
            F.col("candidate.signed_quantity")
            != F.col("prior.expected_prior_quantity")
        )
    ).count()
    add_failure(
        failures,
        ordinary_continuity_failures != 0,
        "POSITION_BUY_AND_HOLD_CONTINUITY",
    )

    split_reconciliation_failures = (
        candidate_and_prior.join(
            split_statistics.select(
                "instrument_id",
                "split_multiplier",
            ),
            "instrument_id",
            "inner",
        )
        .where(
            (
                F.col("prior.expected_prior_quantity")
                * F.col("split_multiplier")
            ).cast("decimal(38,16)")
            != F.col("candidate.signed_quantity")
        )
        .count()
    )
    add_failure(
        failures,
        split_reconciliation_failures != 0,
        "POSITION_SPLIT_RECONCILIATION",
    )

    dividend_quantity_failures = (
        candidate_and_prior.join(
            dividend_actions.select("instrument_id").distinct(),
            "instrument_id",
            "inner",
        )
        .join(
            split_statistics.select(
                "instrument_id",
                "split_multiplier",
            ),
            "instrument_id",
            "left",
        )
        .where(
            (
                F.col("prior.expected_prior_quantity")
                * F.coalesce(
                    F.col("split_multiplier"),
                    F.lit(Decimal("1.0000000000000000")).cast(
                        "decimal(38,16)"
                    ),
                )
            ).cast("decimal(38,16)")
            != F.col("candidate.signed_quantity")
        )
        .count()
    )
    add_failure(
        failures,
        dividend_quantity_failures != 0,
        "POSITION_DIVIDEND_QUANTITY_UNCHANGED",
    )

failed_rule_ids = sorted(set(failures))

print(
    "position_validation=PASS "
    f"expected_output_count={expected_output_count} "
    f"derived_output_count={derived_output_count} "
    f"failed_rule_ids={failed_rule_ids}"
)
position_candidates.select(
    "portfolio_id",
    "instrument_id",
    "position_date",
    "signed_quantity",
    "position_basis",
    "prior_position_date",
).orderBy("instrument_id").show(20, truncate=False)


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
            f"portfolio_id={portfolio_id};position_date={position_date}"
        ),
    ),
    build_manifest_entry(
        dataset_name="TARGET_ALLOCATIONS",
        frame=allocations,
        order_columns=["portfolio_id", "instrument_id", "effective_from"],
        value_columns=[
            "portfolio_id",
            "instrument_id",
            "effective_from",
            "record_hash",
        ],
        snapshot_reference="SILVER_CURRENT",
        selection_scope=(
            f"portfolio_id={portfolio_id};position_date={position_date}"
        ),
    ),
    build_manifest_entry(
        dataset_name="DAILY_PRICES",
        frame=prices,
        order_columns=["instrument_id", "price_date"],
        value_columns=["instrument_id", "price_date", "record_hash"],
        snapshot_reference="SILVER_CURRENT",
        selection_scope=f"price_date={position_date}",
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
        selection_scope=(
            f"effective_date={position_date}"
        ),
    ),
    build_manifest_entry(
        dataset_name="TRADING_CALENDAR",
        frame=calendar,
        order_columns=["exchange_mic", "calendar_date"],
        value_columns=["exchange_mic", "calendar_date", "record_hash"],
        snapshot_reference="SILVER_CURRENT",
        selection_scope=f"calendar_date={position_date}",
    ),
]
if not is_inception:
    manifest_entries.append(
        build_manifest_entry(
            dataset_name="POSITIONS",
            frame=prior_positions,
            order_columns=[
                "portfolio_id",
                "instrument_id",
                "position_date",
            ],
            value_columns=[
                "portfolio_id",
                "instrument_id",
                "position_date",
                "record_hash",
            ],
            snapshot_reference="SILVER_CURRENT",
            selection_scope=(
                f"portfolio_id={portfolio_id};"
                f"position_date={prior_position_date}"
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
    "position_input_manifest=PASS "
    f"input_dataset_names={input_dataset_names} "
    f"input_record_count={input_record_count} "
    f"input_manifest_sha256={input_manifest_digest} "
    "manifest_entries="
    f"{json.dumps(manifest_entries, sort_keys=True)}"
)

current_partition = spark.table(POSITION_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("position_date") == F.lit(position_date))
)
canonical_before_count = current_partition.count()
canonical_before_sha256 = partition_sha256(current_partition)
candidate_sha256 = partition_sha256(position_candidates)

publish_allowed = (
    not failed_rule_ids
    and expected_output_count > 0
    and derived_output_count == expected_output_count
)
published = (
    publish_allowed
    and candidate_sha256 != canonical_before_sha256
)
if publish_allowed:
    canonical_after_count = derived_output_count
    canonical_after_sha256 = candidate_sha256
else:
    canonical_after_count = canonical_before_count
    canonical_after_sha256 = canonical_before_sha256

run_status = "SUCCEEDED" if publish_allowed else "FAILED"
completed_at_utc = datetime.now(UTC).replace(tzinfo=None)
published_at_utc = completed_at_utc if published else None

print(
    "position_publication_gate=PASS "
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
    "output_dataset_name": "POSITIONS",
    "portfolio_id": portfolio_id,
    "business_date": position_date,
    "attempt_number": attempt_number,
    "reprocess_of_derivation_run_id": (
        reprocess_of_derivation_run_id
    ),
    "trigger_type": trigger_type,
    "started_at_utc": started_at_utc,
    "completed_at_utc": completed_at_utc,
    "status": run_status,
    "input_dataset_names": input_dataset_names,
    "input_record_count": input_record_count,
    "input_manifest_sha256": input_manifest_digest,
    "expected_output_count": expected_output_count,
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
        else "Position validation prevented atomic publication."
    ),
    "output_contract_version": POSITION_CONTRACT_VERSION,
    "calculation_version": CALCULATION_VERSION,
    "code_version": code_version,
    "contract_version": DERIVATION_RUN_CONTRACT_VERSION,
}
audit_candidate = spark.createDataFrame(
    [audit_values],
    schema=spark.table(DERIVATION_RUN_TABLE).schema,
)

if published:
    merge_position_partition(
        position_candidates,
        portfolio_id=portfolio_id,
        position_date=position_date,
    )
insert_derivation_audit(audit_candidate)

persisted_partition = spark.table(POSITION_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("position_date") == F.lit(position_date))
)
require_equal(
    "persisted position count",
    persisted_partition.count(),
    canonical_after_count,
)
require_equal(
    "persisted position SHA-256",
    partition_sha256(persisted_partition),
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
    "position_persistence=PASS "
    f"derivation_run_id={derivation_run_id} "
    f"portfolio_id={portfolio_id} "
    f"position_date={position_date} "
    f"run_status={run_status} "
    f"published={published} "
    f"persisted_position_count={persisted_partition.count()} "
    f"persisted_partition_sha256={canonical_after_sha256}"
)

if not publish_allowed:
    raise ValueError(
        "Position derivation failed rules: "
        + ", ".join(failed_rule_ids)
    )
