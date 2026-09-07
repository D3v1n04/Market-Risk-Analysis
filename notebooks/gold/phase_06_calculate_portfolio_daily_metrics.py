# Databricks notebook source
"""Calculate one complete, audited Gold portfolio-daily-metrics partition."""
import hashlib
import json
import re
from datetime import UTC, date, datetime
from decimal import Decimal, localcontext
from typing import Any
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.dbutils import DBUtils
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

PORTFOLIO_TABLE = "workspace.devin_market_risk_dev.silver_portfolios"
CASH_BALANCE_TABLE = (
    "workspace.devin_market_risk_dev.silver_cash_balances"
)
MARKET_VALUE_TABLE = (
    "workspace.devin_market_risk_dev.gold_position_market_values"
)
PORTFOLIO_DAILY_TABLE = (
    "workspace.devin_market_risk_dev.gold_portfolio_daily_metrics"
)
ANALYTICS_RUN_TABLE = (
    "workspace.devin_market_risk_dev.gold_analytics_runs"
)

PORTFOLIO_DAILY_CONTRACT_VERSION = "1.0.0"
ANALYTICS_RUN_CONTRACT_VERSION = "1.2.0"
CALCULATION_VERSION = "1.0.0"
EXPECTED_POSITION_COUNT = 15
EMPTY_SET_SHA256 = hashlib.sha256(b"").hexdigest()

PORTFOLIO_ID_PATTERN = re.compile(
    r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$"
)
CODE_VERSION_PATTERN = re.compile(r"^[0-9a-f]{40}$")

PORTFOLIO_DAILY_COLUMNS = [
    "portfolio_id",
    "valuation_date",
    "base_currency",
    "position_count",
    "long_position_count",
    "short_position_count",
    "long_market_value",
    "short_market_value",
    "gross_market_value",
    "net_security_market_value",
    "closing_cash_balance",
    "closing_nav",
    "baseline_nav",
    "baseline_source",
    "prior_valuation_date",
    "daily_pnl",
    "daily_return",
    "long_exposure_ratio",
    "short_exposure_ratio",
    "gross_exposure_ratio",
    "net_exposure_ratio",
    "input_market_value_partition_sha256",
    "input_cash_balance_record_sha256",
    "input_portfolio_record_sha256",
    "input_prior_metric_record_sha256",
    "analytics_run_id",
    "calculation_version",
    "calculated_at_utc",
    "contract_version",
    "record_hash",
]

PORTFOLIO_DAILY_HASH_FIELDS = [
    "portfolio_id",
    "valuation_date",
    "base_currency",
    "position_count",
    "long_position_count",
    "short_position_count",
    "long_market_value",
    "short_market_value",
    "gross_market_value",
    "net_security_market_value",
    "closing_cash_balance",
    "closing_nav",
    "baseline_nav",
    "baseline_source",
    "prior_valuation_date",
    "daily_pnl",
    "daily_return",
    "long_exposure_ratio",
    "short_exposure_ratio",
    "gross_exposure_ratio",
    "net_exposure_ratio",
    "input_market_value_partition_sha256",
    "input_cash_balance_record_sha256",
    "input_portfolio_record_sha256",
    "input_prior_metric_record_sha256",
    "calculation_version",
]

BASELINE_SOURCE_INITIAL = "INITIAL_NAV"
BASELINE_SOURCE_PRIOR = "PRIOR_PUBLISHED_NAV"

# Contract calculations prohibit intermediate rounding.
INPUT_CONTRACT_VERSIONS = {
    "PORTFOLIOS": "1.1.0",
    "CASH_BALANCES": "1.1.0",
    "POSITION_MARKET_VALUES": "1.0.0",
    "PORTFOLIO_DAILY_METRICS": "1.0.0",
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


def portfolio_daily_partition_sha256(frame: DataFrame) -> str | None:
    """Hash one canonical Gold portfolio-date metric row."""
    if frame.count() == 0:
        return None
    return ordered_set_sha256(
        frame,
        order_columns=["portfolio_id", "valuation_date"],
        value_columns=[
            "portfolio_id",
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
    snapshot_reference: str,
    selection_scope: str,
) -> dict[str, Any]:
    """Build one deterministic governed-input manifest entry."""
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


def merge_portfolio_daily_partition(
    candidate: DataFrame,
    *,
    portfolio_id: str,
    valuation_date: date,
) -> None:
    """Atomically replace one Gold portfolio-date metric partition."""
    target = DeltaTable.forName(spark, PORTFOLIO_DAILY_TABLE)
    match_condition = (
        "target.portfolio_id = source.portfolio_id "
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
        (F.col("output_dataset_name") == "PORTFOLIO_DAILY_METRICS")
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

portfolio_frame = spark.createDataFrame(
    [portfolio],
    schema=spark.table(PORTFOLIO_TABLE).schema,
)

market_value_inputs = freeze_small_dataframe(
    spark.table(MARKET_VALUE_TABLE).where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("valuation_date") == F.lit(valuation_date))
    ),
    label="published Gold market-value input partition",
    max_rows=EXPECTED_POSITION_COUNT * 4,
)

cash_inputs = freeze_small_dataframe(
    spark.table(CASH_BALANCE_TABLE).where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("cash_date") == F.lit(valuation_date))
    ),
    label="trusted Silver cash input",
    max_rows=4,
)
cash_rows = cash_inputs.collect()
cash = cash_rows[0] if len(cash_rows) == 1 else None
prior_cash_date = cash["prior_cash_date"] if cash is not None else None

prior_metric_source = spark.table(PORTFOLIO_DAILY_TABLE)
if prior_cash_date is None:
    prior_metric_source = prior_metric_source.where(F.lit(False))
else:
    prior_metric_source = prior_metric_source.where(
        (F.col("portfolio_id") == portfolio_id)
        & (F.col("valuation_date") == F.lit(prior_cash_date))
    )

prior_metric_inputs = freeze_small_dataframe(
    prior_metric_source,
    label="prior published Gold portfolio metric input",
    max_rows=4,
)
prior_metric_rows = prior_metric_inputs.collect()
prior_metric = (
    prior_metric_rows[0]
    if len(prior_metric_rows) == 1
    else None
)

print(
    "portfolio_daily_parameters=PASS "
    f"analytics_run_id={analytics_run_id} "
    f"portfolio_id={portfolio_id} "
    f"valuation_date={valuation_date} "
    f"prior_cash_date={prior_cash_date} "
    f"attempt_number={attempt_number} "
    "reprocess_of_analytics_run_id="
    f"{reprocess_of_analytics_run_id}"
)


# COMMAND ----------

failures: list[str] = []

market_value_count = market_value_inputs.count()
market_value_distinct_count = (
    market_value_inputs.select("instrument_id").distinct().count()
)
cash_count = len(cash_rows)
prior_metric_count = len(prior_metric_rows)
is_inception = valuation_date == portfolio["actual_inception_date"]

aggregates = market_value_inputs.agg(
    F.sum(
        F.when(F.col("position_side") == "LONG", F.lit(1)).otherwise(
            F.lit(0)
        )
    ).alias("long_position_count"),
    F.sum(
        F.when(F.col("position_side") == "SHORT", F.lit(1)).otherwise(
            F.lit(0)
        )
    ).alias("short_position_count"),
    F.sum(
        F.when(
            F.col("position_side") == "LONG",
            F.col("signed_market_value"),
        ).otherwise(F.lit(0))
    )
    .cast("decimal(38,16)")
    .alias("long_market_value"),
    F.sum(
        F.when(
            F.col("position_side") == "SHORT",
            F.col("absolute_market_value"),
        ).otherwise(F.lit(0))
    )
    .cast("decimal(38,16)")
    .alias("short_market_value"),
    F.sum(F.col("absolute_market_value"))
    .cast("decimal(38,16)")
    .alias("gross_market_value"),
    F.sum(F.col("signed_market_value"))
    .cast("decimal(38,16)")
    .alias("net_security_market_value"),
).first()

zero_money = Decimal("0").quantize(Decimal("0.0000000000000000"))

long_position_count = int(aggregates["long_position_count"] or 0)
short_position_count = int(aggregates["short_position_count"] or 0)
long_market_value = aggregates["long_market_value"] or zero_money
short_market_value = aggregates["short_market_value"] or zero_money
gross_market_value = aggregates["gross_market_value"] or zero_money
net_security_market_value = (
    aggregates["net_security_market_value"] or zero_money
)
closing_cash_balance = (
    cash["closing_cash_balance"]
    if cash is not None
    else zero_money
)
closing_nav = net_security_market_value + closing_cash_balance

if is_inception:
    baseline_nav = portfolio["initial_nav"]
    baseline_source = BASELINE_SOURCE_INITIAL
    prior_valuation_date = None
    input_prior_metric_record_sha256 = None
else:
    baseline_nav = (
        prior_metric["closing_nav"]
        if prior_metric is not None
        else zero_money
    )
    baseline_source = BASELINE_SOURCE_PRIOR
    prior_valuation_date = prior_cash_date
    input_prior_metric_record_sha256 = (
        prior_metric["record_hash"]
        if prior_metric is not None
        else None
    )

daily_pnl = closing_nav - baseline_nav


def divide_without_intermediate_rounding(
    numerator: Decimal,
    denominator: Decimal,
) -> Decimal:
    """Calculate at higher precision than the declared output scale."""
    if denominator <= 0:
        return Decimal("0")
    with localcontext() as decimal_context:
        decimal_context.prec = 50
        return numerator / denominator


daily_return = divide_without_intermediate_rounding(
    daily_pnl,
    baseline_nav,
)
long_exposure_ratio = divide_without_intermediate_rounding(
    long_market_value,
    closing_nav,
)
short_exposure_ratio = divide_without_intermediate_rounding(
    short_market_value,
    closing_nav,
)
gross_exposure_ratio = divide_without_intermediate_rounding(
    gross_market_value,
    closing_nav,
)
net_exposure_ratio = divide_without_intermediate_rounding(
    net_security_market_value,
    closing_nav,
)

input_market_value_partition_sha256 = ordered_set_sha256(
    market_value_inputs,
    order_columns=["portfolio_id", "instrument_id", "valuation_date"],
    value_columns=[
        "portfolio_id",
        "instrument_id",
        "valuation_date",
        "record_hash",
    ],
)
input_cash_balance_record_sha256 = (
    cash["record_hash"] if cash is not None else EMPTY_SET_SHA256
)
input_portfolio_record_sha256 = portfolio["record_hash"]

calculated_at_utc = datetime.now(UTC).replace(tzinfo=None)
candidate_values = {
    "portfolio_id": portfolio_id,
    "valuation_date": valuation_date,
    "base_currency": portfolio["base_currency"],
    "position_count": market_value_count,
    "long_position_count": long_position_count,
    "short_position_count": short_position_count,
    "long_market_value": long_market_value,
    "short_market_value": short_market_value,
    "gross_market_value": gross_market_value,
    "net_security_market_value": net_security_market_value,
    "closing_cash_balance": closing_cash_balance,
    "closing_nav": closing_nav,
    "baseline_nav": baseline_nav,
    "baseline_source": baseline_source,
    "prior_valuation_date": prior_valuation_date,
    "daily_pnl": daily_pnl,
    "daily_return": daily_return,
    "long_exposure_ratio": long_exposure_ratio,
    "short_exposure_ratio": short_exposure_ratio,
    "gross_exposure_ratio": gross_exposure_ratio,
    "net_exposure_ratio": net_exposure_ratio,
    "input_market_value_partition_sha256": (
        input_market_value_partition_sha256
    ),
    "input_cash_balance_record_sha256": (
        input_cash_balance_record_sha256
    ),
    "input_portfolio_record_sha256": input_portfolio_record_sha256,
    "input_prior_metric_record_sha256": (
        input_prior_metric_record_sha256
    ),
    "analytics_run_id": analytics_run_id,
    "calculation_version": CALCULATION_VERSION,
    "calculated_at_utc": calculated_at_utc,
    "contract_version": PORTFOLIO_DAILY_CONTRACT_VERSION,
    "record_hash": EMPTY_SET_SHA256,
}

portfolio_daily_candidate = spark.createDataFrame(
    [candidate_values],
    schema=spark.table(PORTFOLIO_DAILY_TABLE).schema,
)
portfolio_daily_candidate = (
    portfolio_daily_candidate.withColumn(
        "record_hash",
        canonical_hash_expression(PORTFOLIO_DAILY_HASH_FIELDS),
    )
    .select(*PORTFOLIO_DAILY_COLUMNS)
)
portfolio_daily_candidate = freeze_small_dataframe(
    portfolio_daily_candidate,
    label="Gold portfolio-daily candidate partition",
    max_rows=4,
)

candidate_row = portfolio_daily_candidate.collect()[0]
calculated_output_count = portfolio_daily_candidate.count()
candidate_distinct_count = (
    portfolio_daily_candidate.select(
        "portfolio_id",
        "valuation_date",
    )
    .distinct()
    .count()
)

invalid_market_value_date_count = market_value_inputs.where(
    F.col("valuation_date") != F.lit(valuation_date)
).count()
invalid_market_value_currency_count = market_value_inputs.where(
    F.col("base_currency") != F.lit("USD")
).count()

date_alignment_failed = (
    invalid_market_value_date_count != 0
    or (
        cash is not None
        and cash["cash_date"] != valuation_date
    )
)
currency_failed = (
    portfolio["base_currency"] != "USD"
    or invalid_market_value_currency_count != 0
    or (
        cash is not None
        and cash["base_currency"] != "USD"
    )
)

expected_prior_count = 0 if is_inception else 1
input_coverage_failed = (
    market_value_count != EXPECTED_POSITION_COUNT
    or market_value_distinct_count != EXPECTED_POSITION_COUNT
    or cash_count != 1
    or prior_metric_count != expected_prior_count
)

counts_failed = (
    market_value_count
    != long_position_count + short_position_count
    or market_value_count != EXPECTED_POSITION_COUNT
)

market_values_failed = (
    gross_market_value
    != long_market_value + short_market_value
    or net_security_market_value
    != long_market_value - short_market_value
)

nav_failed = (
    candidate_row["closing_nav"]
    != candidate_row["net_security_market_value"]
    + candidate_row["closing_cash_balance"]
)
nav_positive_failed = (
    candidate_row["closing_nav"] <= 0
    or candidate_row["baseline_nav"] <= 0
)

if is_inception:
    baseline_failed = (
        prior_cash_date is not None
        or prior_metric_count != 0
        or candidate_row["baseline_source"]
        != BASELINE_SOURCE_INITIAL
        or candidate_row["prior_valuation_date"] is not None
        or candidate_row["baseline_nav"] != portfolio["initial_nav"]
        or candidate_row["input_prior_metric_record_sha256"] is not None
    )
else:
    baseline_failed = (
        prior_cash_date is None
        or prior_cash_date >= valuation_date
        or prior_metric_count != 1
        or prior_metric is None
        or candidate_row["baseline_source"]
        != BASELINE_SOURCE_PRIOR
        or candidate_row["prior_valuation_date"] != prior_cash_date
        or (
            prior_metric is not None
            and candidate_row["baseline_nav"]
            != prior_metric["closing_nav"]
        )
        or (
            prior_metric is not None
            and candidate_row["input_prior_metric_record_sha256"]
            != prior_metric["record_hash"]
        )
    )

pnl_failed = (
    candidate_row["daily_pnl"]
    != candidate_row["closing_nav"] - candidate_row["baseline_nav"]
)

ratio_tolerance = Decimal("0.000000000000000001")
expected_return = divide_without_intermediate_rounding(
    candidate_row["daily_pnl"],
    candidate_row["baseline_nav"],
)
return_failed = (
    abs(candidate_row["daily_return"] - expected_return)
    > ratio_tolerance
)

expected_ratios = {
    "long_exposure_ratio": divide_without_intermediate_rounding(
        candidate_row["long_market_value"],
        candidate_row["closing_nav"],
    ),
    "short_exposure_ratio": divide_without_intermediate_rounding(
        candidate_row["short_market_value"],
        candidate_row["closing_nav"],
    ),
    "gross_exposure_ratio": divide_without_intermediate_rounding(
        candidate_row["gross_market_value"],
        candidate_row["closing_nav"],
    ),
    "net_exposure_ratio": divide_without_intermediate_rounding(
        candidate_row["net_security_market_value"],
        candidate_row["closing_nav"],
    ),
}
exposure_ratios_failed = any(
    abs(candidate_row[field_name] - expected_value)
    > ratio_tolerance
    for field_name, expected_value in expected_ratios.items()
)

hash_failure_count = portfolio_daily_candidate.withColumn(
    "recalculated_hash",
    canonical_hash_expression(PORTFOLIO_DAILY_HASH_FIELDS),
).where(
    F.col("record_hash") != F.col("recalculated_hash")
).count()

add_failure(
    failures,
    calculated_output_count != 1 or candidate_distinct_count != 1,
    "PORTFOLIO_DAILY_KEY_UNIQUE",
)
add_failure(
    failures,
    input_coverage_failed,
    "PORTFOLIO_DAILY_INPUT_COVERAGE",
)
add_failure(
    failures,
    date_alignment_failed,
    "PORTFOLIO_DAILY_DATE_ALIGNMENT",
)
add_failure(
    failures,
    counts_failed,
    "PORTFOLIO_DAILY_POSITION_COUNTS_RECONCILE",
)
add_failure(
    failures,
    market_values_failed,
    "PORTFOLIO_DAILY_MARKET_VALUES_RECONCILE",
)
add_failure(
    failures,
    nav_failed,
    "PORTFOLIO_DAILY_NAV_RECONCILES",
)
add_failure(
    failures,
    nav_positive_failed,
    "PORTFOLIO_DAILY_NAV_POSITIVE",
)
add_failure(
    failures,
    baseline_failed,
    "PORTFOLIO_DAILY_BASELINE_RECONCILES",
)
add_failure(
    failures,
    pnl_failed,
    "PORTFOLIO_DAILY_PNL_RECONCILES",
)
add_failure(
    failures,
    return_failed,
    "PORTFOLIO_DAILY_RETURN_RECONCILES",
)
add_failure(
    failures,
    exposure_ratios_failed,
    "PORTFOLIO_DAILY_EXPOSURE_RATIOS_RECONCILE",
)
add_failure(
    failures,
    currency_failed,
    "PORTFOLIO_DAILY_CURRENCY_MATCH",
)
add_failure(
    failures,
    hash_failure_count != 0,
    "PORTFOLIO_DAILY_RECORD_HASH_VALID",
)

failed_rule_ids = sorted(set(failures))

print(
    "portfolio_daily_validation=PASS "
    "expected_output_count=1 "
    f"calculated_output_count={calculated_output_count} "
    f"failed_rule_ids={failed_rule_ids}"
)
portfolio_daily_candidate.select(
    "portfolio_id",
    "valuation_date",
    "long_market_value",
    "short_market_value",
    "gross_market_value",
    "net_security_market_value",
    "closing_cash_balance",
    "closing_nav",
    "baseline_nav",
    "daily_pnl",
    "daily_return",
    "long_exposure_ratio",
    "short_exposure_ratio",
    "gross_exposure_ratio",
    "net_exposure_ratio",
).show(truncate=False)


# COMMAND ----------

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
        dataset_name="CASH_BALANCES",
        frame=cash_inputs,
        order_columns=["portfolio_id", "cash_date"],
        value_columns=["portfolio_id", "cash_date", "record_hash"],
        snapshot_reference="SILVER_CURRENT",
        selection_scope=(
            f"portfolio_id={portfolio_id};cash_date={valuation_date}"
        ),
    ),
    build_manifest_entry(
        dataset_name="POSITION_MARKET_VALUES",
        frame=market_value_inputs,
        order_columns=[
            "portfolio_id",
            "instrument_id",
            "valuation_date",
        ],
        value_columns=[
            "portfolio_id",
            "instrument_id",
            "valuation_date",
            "record_hash",
        ],
        snapshot_reference="PUBLISHED_GOLD",
        selection_scope=(
            f"portfolio_id={portfolio_id};valuation_date={valuation_date}"
        ),
    ),
    build_manifest_entry(
        dataset_name="PORTFOLIO_DAILY_METRICS",
        frame=prior_metric_inputs,
        order_columns=["portfolio_id", "valuation_date"],
        value_columns=[
            "portfolio_id",
            "valuation_date",
            "record_hash",
        ],
        snapshot_reference="PUBLISHED_GOLD",
        selection_scope=(
            f"portfolio_id={portfolio_id};"
            f"valuation_date={prior_cash_date}"
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
    "portfolio_daily_input_manifest=PASS "
    f"input_dataset_names={input_dataset_names} "
    f"input_record_count={input_record_count} "
    f"input_manifest_sha256={input_manifest_digest} "
    "manifest_entries="
    f"{json.dumps(manifest_entries, sort_keys=True)}"
)

current_partition = spark.table(PORTFOLIO_DAILY_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("valuation_date") == F.lit(valuation_date))
)
canonical_before_count = current_partition.count()
canonical_before_sha256 = portfolio_daily_partition_sha256(
    current_partition
)
candidate_sha256 = portfolio_daily_partition_sha256(
    portfolio_daily_candidate
)

publish_allowed = (
    not failed_rule_ids
    and calculated_output_count == 1
)
published = (
    publish_allowed
    and candidate_sha256 != canonical_before_sha256
)

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
    "portfolio_daily_publication_gate=PASS "
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
    "output_dataset_name": "PORTFOLIO_DAILY_METRICS",
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
    "expected_output_count": 1,
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
        else (
            "Gold portfolio-daily validation prevented "
            "atomic publication."
        )
    ),
    "output_contract_version": PORTFOLIO_DAILY_CONTRACT_VERSION,
    "calculation_version": CALCULATION_VERSION,
    "code_version": code_version,
    "contract_version": ANALYTICS_RUN_CONTRACT_VERSION,
}
audit_candidate = spark.createDataFrame(
    [audit_values],
    schema=spark.table(ANALYTICS_RUN_TABLE).schema,
)

if published:
    merge_portfolio_daily_partition(
        portfolio_daily_candidate,
        portfolio_id=portfolio_id,
        valuation_date=valuation_date,
    )

insert_analytics_audit(audit_candidate)

persisted_partition = spark.table(PORTFOLIO_DAILY_TABLE).where(
    (F.col("portfolio_id") == portfolio_id)
    & (F.col("valuation_date") == F.lit(valuation_date))
)
require_equal(
    "persisted portfolio-daily count",
    persisted_partition.count(),
    canonical_after_count,
)
require_equal(
    "persisted portfolio-daily SHA-256",
    portfolio_daily_partition_sha256(persisted_partition),
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
    "portfolio_daily_persistence=PASS "
    f"analytics_run_id={analytics_run_id} "
    f"portfolio_id={portfolio_id} "
    f"valuation_date={valuation_date} "
    f"run_status={run_status} "
    f"published={published} "
    "persisted_portfolio_daily_count="
    f"{persisted_partition.count()} "
    f"persisted_partition_sha256={canonical_after_sha256}"
)

if not publish_allowed:
    raise ValueError(
        "Gold portfolio-daily calculation failed rules: "
        + ", ".join(failed_rule_ids)
    )
