# Databricks notebook source
"""Calculate one immutable, audited Phase 07 historical-risk bundle.

The bundle is deliberately bounded to the approved 2016 universe.  It uses
trusted Silver only, preserves Decimal arithmetic until Delta casts the
contract fields, and publishes no result rows unless every preflight rule
passes.  Stress scenarios are deterministic hypothetical assumptions, never
forecasts.
"""

import hashlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

from pyspark.dbutils import DBUtils
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

PORTFOLIO_TABLE = "workspace.devin_market_risk_dev.silver_portfolios"
INSTRUMENT_TABLE = "workspace.devin_market_risk_dev.silver_instruments"
PRICE_TABLE = "workspace.devin_market_risk_dev.silver_daily_prices"
POSITION_TABLE = "workspace.devin_market_risk_dev.silver_positions"
CASH_TABLE = "workspace.devin_market_risk_dev.silver_cash_balances"
STRESS_SCENARIO_TABLE = "workspace.devin_market_risk_dev.silver_stress_scenarios"
STRESS_SHOCK_TABLE = "workspace.devin_market_risk_dev.silver_stress_scenario_shocks"

RISK_RUN_TABLE = "workspace.devin_market_risk_dev.gold_risk_runs"
HISTORICAL_PNL_TABLE = "workspace.devin_market_risk_dev.gold_historical_pnl_scenarios"
VAR_MEASURE_TABLE = "workspace.devin_market_risk_dev.gold_var_measures"
STRESS_RESULT_TABLE = "workspace.devin_market_risk_dev.gold_stress_results"

RISK_RUN_CONTRACT_VERSION = "1.0.0"
HISTORICAL_PNL_CONTRACT_VERSION = "1.0.0"
VAR_MEASURE_CONTRACT_VERSION = "1.0.0"
STRESS_RESULT_CONTRACT_VERSION = "1.0.0"
CALCULATION_VERSION = "1.0.0"

EXPECTED_HISTORY_START_DATE = date(2016, 1, 4)
EXPECTED_AS_OF_DATE = date(2016, 12, 30)
STATIC_EXPOSURE_SOURCE_DATE = date(2016, 1, 7)
EXPECTED_INSTRUMENT_COUNT = 15
EXPECTED_SESSION_COUNT = 252
EXPECTED_RETURN_COUNT = 251
MINIMUM_RETURN_COUNT = 250
EXPECTED_STRESS_SCENARIO_COUNT = 3
EXPECTED_SHOCK_COUNT = 15
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
APPROVED_SCENARIO_IDS = frozenset(
    {
        "BROAD_MARKET_DOWN_10",
        "TECHNOLOGY_CORRECTION",
        "GEOPOLITICAL_SUPPLY_SHOCK",
    }
)
CONFIDENCE_RANKS = {
    Decimal("0.95"): 239,
    Decimal("0.99"): 249,
}
EMPTY_SET_SHA256 = hashlib.sha256(b"").hexdigest()
SHA256_PATTERN = r"^[0-9a-f]{64}$"
PORTFOLIO_ID_PATTERN = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$")
CODE_VERSION_PATTERN = re.compile(r"^[0-9a-f]{40}$")

INPUT_CONTRACT_VERSIONS = {
    "PORTFOLIOS": "1.1.0",
    "INSTRUMENTS": "1.0.0",
    "DAILY_PRICES": "1.0.0",
    "POSITIONS": "1.1.0",
    "CASH_BALANCES": "1.1.0",
    "STRESS_SCENARIOS": "1.0.0",
    "STRESS_SCENARIO_SHOCKS": "1.0.0",
}

HISTORICAL_HASH_FIELDS = [
    "portfolio_id",
    "prior_price_date",
    "scenario_date",
    "return_observation_number",
    "base_currency",
    "simulated_portfolio_pnl",
    "loss_amount",
    "input_static_exposure_sha256",
    "input_prior_price_partition_sha256",
    "input_scenario_price_partition_sha256",
    "calculation_version",
]
VAR_HASH_FIELDS = [
    "portfolio_id",
    "as_of_date",
    "base_currency",
    "confidence_level",
    "observation_count",
    "quantile_rank",
    "var_amount",
    "input_historical_pnl_scenario_set_sha256",
    "input_static_exposure_sha256",
    "calculation_version",
]
STRESS_HASH_FIELDS = [
    "portfolio_id",
    "as_of_date",
    "base_currency",
    "scenario_id",
    "scenario_version",
    "shock_count",
    "stress_pnl",
    "stressed_nav",
    "input_static_exposure_sha256",
    "input_stress_scenario_record_sha256",
    "input_shock_set_sha256",
    "calculation_version",
]


def build_expected_price_dates() -> tuple[date, ...]:
    """Return the approved 2016 US-equities trading-date spine."""
    dates: list[date] = []
    current_date = EXPECTED_HISTORY_START_DATE
    while current_date <= EXPECTED_AS_OF_DATE:
        if current_date.weekday() < 5 and current_date not in US_EQUITIES_2016_HOLIDAYS:
            dates.append(current_date)
        current_date += timedelta(days=1)
    if len(dates) != EXPECTED_SESSION_COUNT:
        raise ValueError("Unexpected approved price-date count")
    return tuple(dates)


EXPECTED_PRICE_DATES = build_expected_price_dates()


def build_expected_price_date_pairs(
    price_dates: tuple[date, ...],
) -> tuple[tuple[date, date], ...]:
    """Return ordered adjacent pairs without requiring equal-length inputs."""
    return tuple(
        (price_dates[index], price_dates[index + 1])
        for index in range(len(price_dates) - 1)
    )


EXPECTED_PRICE_DATE_PAIRS = frozenset(
    build_expected_price_date_pairs(EXPECTED_PRICE_DATES)
)


def require_equal(label: str, actual: Any, expected: Any) -> None:
    """Raise a clear preflight reconciliation error."""
    if actual != expected:
        raise ValueError(f"{label} mismatch: expected {expected!r}, found {actual!r}")


def require_parameter(label: str, value: str, pattern: re.Pattern[str]) -> str:
    """Require a nonempty parameter in its approved format."""
    if pattern.fullmatch(value) is None:
        raise ValueError(f"{label} has an invalid format")
    return value


def freeze_small_dataframe(frame: DataFrame, *, label: str, max_rows: int) -> DataFrame:
    """Detach bounded evidence without cache or persist commands."""
    rows = frame.limit(max_rows + 1).collect()
    if len(rows) > max_rows:
        input_bound_failures.append(label)
    return spark.createDataFrame(rows[:max_rows], schema=frame.schema)


def canonical_hash_expression(fields: list[str]) -> Column:
    """Build the contract-defined SHA-256 hash for one candidate record."""
    return F.sha2(
        F.concat_ws(
            "|",
            *[
                F.coalesce(F.col(field).cast("string"), F.lit("<NULL>"))
                for field in fields
            ],
        ),
        256,
    )


def ordered_set_sha256(
    frame: DataFrame, *, order_columns: list[str], value_columns: list[str]
) -> str:
    """Return an ordered-set SHA-256 digest for trusted bounded evidence."""
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
                    F.coalesce(F.col(column_name).cast("string"), F.lit("<NULL>"))
                    for column_name in value_columns
                ],
            ).alias("canonical_row"),
        ).alias("ordered_row")
    )
    row = ordered_rows.agg(
        F.sha2(
            F.concat_ws(
                "\n",
                F.transform(
                    F.array_sort(F.collect_list("ordered_row")),
                    lambda item: item["canonical_row"],
                ),
            ),
            256,
        ).alias("set_sha256")
    ).first()
    return row["set_sha256"]


def build_manifest_entry(
    *,
    dataset_name: str,
    frame: DataFrame,
    order_columns: list[str],
    value_columns: list[str],
    selection_scope: str,
) -> dict[str, Any]:
    """Build one deterministic trusted-Silver input-manifest entry."""
    return {
        "dataset_name": dataset_name,
        "dataset_contract_version": INPUT_CONTRACT_VERSIONS[dataset_name],
        "snapshot_reference": "TRUSTED_SILVER",
        "selection_scope": selection_scope,
        "record_count": frame.count(),
        "record_set_sha256": ordered_set_sha256(
            frame, order_columns=order_columns, value_columns=value_columns
        ),
    }


def input_manifest_sha256(entries: list[dict[str, Any]]) -> str:
    """Hash the sorted manifest entries without any nondeterministic state."""
    canonical_rows = [
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
        for entry in sorted(entries, key=lambda entry: entry["dataset_name"])
    ]
    return hashlib.sha256("\n".join(canonical_rows).encode()).hexdigest()


def add_failure(failures: list[str], condition: bool, rule_id: str) -> None:
    """Add one stable contract rule identifier only once at final sorting."""
    if condition:
        failures.append(rule_id)


def append_immutable(frame: DataFrame, table_name: str) -> None:
    """Append a validated immutable Delta evidence set; never mutate history."""
    frame.write.format("delta").mode("append").saveAsTable(table_name)


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils = DBUtils(spark)

dbutils.widgets.text("portfolio_id", "", "Portfolio ID")
dbutils.widgets.text("as_of_date", "", "As-of date (YYYY-MM-DD)")
dbutils.widgets.text("code_version", "", "Git code version")
dbutils.widgets.text("trigger_type", "MANUAL", "Risk trigger type")

portfolio_id = require_parameter(
    "portfolio_id",
    dbutils.widgets.get("portfolio_id").strip().upper(),
    PORTFOLIO_ID_PATTERN,
)
as_of_date = date.fromisoformat(dbutils.widgets.get("as_of_date").strip())
require_equal("approved risk as_of_date", as_of_date, EXPECTED_AS_OF_DATE)
code_version = require_parameter(
    "code_version", dbutils.widgets.get("code_version").strip(), CODE_VERSION_PATTERN
)
trigger_type = dbutils.widgets.get("trigger_type").strip().upper()
if trigger_type not in {"MANUAL", "SCHEDULED", "RECOVERY"}:
    raise ValueError("trigger_type must be MANUAL, SCHEDULED, or RECOVERY")

risk_run_id = str(uuid4())
started_at_utc = datetime.now(UTC).replace(tzinfo=None)
input_bound_failures: list[str] = []
attempt_number = 1
reprocess_of_risk_run_id: str | None = None
risk_audit_written = False


def append_failed_risk_audit(error: BaseException) -> None:
    """Append one failed audit row for any post-parameter terminal error."""
    global risk_audit_written
    if risk_audit_written:
        return

    completed_at_utc = datetime.now(UTC).replace(tzinfo=None)
    failure_values = {
        "risk_run_id": risk_run_id,
        "portfolio_id": portfolio_id,
        "as_of_date": as_of_date,
        "base_currency": "USD",
        "attempt_number": attempt_number,
        "reprocess_of_risk_run_id": reprocess_of_risk_run_id,
        "trigger_type": trigger_type,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "status": "FAILED",
        "methodology": "ONE_DAY_HISTORICAL_SIMULATION",
        "holding_period_days": 1,
        "return_price_field": "daily_prices.adjusted_close_price",
        "return_formula": "adjusted_close[t] / adjusted_close[t-1] - 1",
        "price_history_start_date": EXPECTED_HISTORY_START_DATE,
        "price_history_end_date": as_of_date,
        "trading_session_count": globals().get("session_count", 0),
        "return_observation_count": globals().get("return_observation_count", 0),
        "minimum_return_observation_count": MINIMUM_RETURN_COUNT,
        "confidence_levels": [Decimal("0.95"), Decimal("0.99")],
        "static_exposure_source_date": STATIC_EXPOSURE_SOURCE_DATE,
        "static_exposure_valuation_date": as_of_date,
        "static_exposure_policy": (
            "BUY_AND_HOLD_NO_REBALANCE_NO_LATER_CORPORATE_ACTIONS"
        ),
        "input_dataset_names": globals().get("input_dataset_names", []),
        "input_record_count": globals().get("input_record_count", 0),
        "input_manifest_sha256": globals().get(
            "input_manifest_digest", EMPTY_SET_SHA256
        ),
        "input_static_exposure_sha256": globals().get(
            "static_exposure_sha256", EMPTY_SET_SHA256
        ),
        "input_price_history_sha256": globals().get(
            "price_history_sha256", EMPTY_SET_SHA256
        ),
        "input_stress_scenario_set_sha256": globals().get(
            "scenario_set_sha256", EMPTY_SET_SHA256
        ),
        "input_stress_shock_set_sha256": globals().get(
            "shock_set_sha256", EMPTY_SET_SHA256
        ),
        "expected_historical_pnl_scenario_count": EXPECTED_RETURN_COUNT,
        "calculated_historical_pnl_scenario_count": globals().get(
            "historical_count", 0
        ),
        "expected_var_measure_count": len(CONFIDENCE_RANKS),
        "calculated_var_measure_count": globals().get("var_count", 0),
        "expected_stress_result_count": EXPECTED_STRESS_SCENARIO_COUNT,
        "calculated_stress_result_count": globals().get("stress_count", 0),
        "published": False,
        "published_at_utc": None,
        "warning_count": 0,
        "failed_rule_ids": ["RISK_RUN_UNHANDLED_FAILURE"],
        "error_code": "UNHANDLED_EXCEPTION",
        "error_message": str(error)[:4000],
        "historical_pnl_scenarios_contract_version": (HISTORICAL_PNL_CONTRACT_VERSION),
        "var_measures_contract_version": VAR_MEASURE_CONTRACT_VERSION,
        "stress_results_contract_version": STRESS_RESULT_CONTRACT_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "code_version": code_version,
        "contract_version": RISK_RUN_CONTRACT_VERSION,
    }
    append_immutable(
        spark.createDataFrame(
            [failure_values], schema=spark.table(RISK_RUN_TABLE).schema
        ),
        RISK_RUN_TABLE,
    )
    risk_audit_written = True


def execute_with_failed_audit(execute, on_failure) -> None:
    """Run the complete post-parameter path and audit one terminal failure."""
    try:
        execute()
    except Exception as error:
        on_failure(error)
        raise


def execute_risk_run() -> None:
    """Execute every risky operation after valid parameters establish a run ID."""
    global attempt_number
    global reprocess_of_risk_run_id
    global session_count
    global return_observation_count
    global input_dataset_names
    global input_record_count
    global input_manifest_digest
    global static_exposure_sha256
    global price_history_sha256
    global scenario_set_sha256
    global shock_set_sha256
    global historical_count
    global var_count
    global stress_count
    global risk_audit_written

    previous_runs = (
        spark.table(RISK_RUN_TABLE)
        .where(
            (F.col("portfolio_id") == portfolio_id)
            & (F.col("as_of_date") == F.lit(as_of_date))
        )
        .orderBy(F.col("attempt_number").desc())
        .select("risk_run_id", "attempt_number")
        .limit(1)
        .collect()
    )
    if previous_runs:
        attempt_number = previous_runs[0]["attempt_number"] + 1
        reprocess_of_risk_run_id = previous_runs[0]["risk_run_id"]
    else:
        attempt_number = 1
        reprocess_of_risk_run_id = None

    # All inputs are bounded by the approved fixed 2016 universe before derivation.
    portfolio_inputs = freeze_small_dataframe(
        spark.table(PORTFOLIO_TABLE).where(
            (F.col("portfolio_id") == portfolio_id)
            & (F.col("is_active") == F.lit(True))
        ),
        label="active trusted portfolio",
        max_rows=2,
    )
    instrument_inputs = freeze_small_dataframe(
        spark.table(INSTRUMENT_TABLE).where(
            (F.col("is_active") == F.lit(True))
            & (F.col("quote_currency") == F.lit("USD"))
        ),
        label="active trusted instrument universe",
        max_rows=EXPECTED_INSTRUMENT_COUNT + 1,
    )
    position_inputs = freeze_small_dataframe(
        spark.table(POSITION_TABLE).where(
            (F.col("portfolio_id") == portfolio_id)
            & (F.col("position_date") == F.lit(STATIC_EXPOSURE_SOURCE_DATE))
        ),
        label="January 7 trusted static positions",
        max_rows=EXPECTED_INSTRUMENT_COUNT + 1,
    )
    cash_inputs = freeze_small_dataframe(
        spark.table(CASH_TABLE).where(
            (F.col("portfolio_id") == portfolio_id)
            & (F.col("cash_date") == F.lit(STATIC_EXPOSURE_SOURCE_DATE))
        ),
        label="January 7 trusted static cash",
        max_rows=2,
    )
    scenario_inputs = freeze_small_dataframe(
        spark.table(STRESS_SCENARIO_TABLE).where(F.col("is_active") == F.lit(True)),
        label="active trusted stress scenarios",
        max_rows=EXPECTED_STRESS_SCENARIO_COUNT + 1,
    )
    shock_inputs = freeze_small_dataframe(
        spark.table(STRESS_SHOCK_TABLE),
        label="trusted stress shocks",
        max_rows=EXPECTED_STRESS_SCENARIO_COUNT * EXPECTED_SHOCK_COUNT + 1,
    )
    active_instrument_ids = instrument_inputs.select("instrument_id")
    price_inputs = freeze_small_dataframe(
        spark.table(PRICE_TABLE)
        .join(active_instrument_ids, "instrument_id", "inner")
        .where(
            (F.col("price_date") >= F.lit(EXPECTED_HISTORY_START_DATE))
            & (F.col("price_date") <= F.lit(as_of_date))
        ),
        label="approved 2016 trusted price history",
        max_rows=EXPECTED_INSTRUMENT_COUNT * EXPECTED_SESSION_COUNT + 1,
    )

    print(
        "risk_parameters=PASS "
        f"risk_run_id={risk_run_id} portfolio_id={portfolio_id} "
        f"as_of_date={as_of_date} attempt_number={attempt_number} "
        "static_exposure_policy=BUY_AND_HOLD_NO_REBALANCE_NO_LATER_CORPORATE_ACTIONS"
    )

    failures: list[str] = []
    add_failure(
        failures,
        bool(input_bound_failures),
        "RISK_RUN_INPUT_BOUND_VALID",
    )
    portfolio_rows = portfolio_inputs.collect()
    portfolio = portfolio_rows[0] if len(portfolio_rows) == 1 else None
    cash_rows = cash_inputs.collect()
    cash = cash_rows[0] if len(cash_rows) == 1 else None

    add_failure(
        failures, len(portfolio_rows) != 1, "RISK_RUN_PORTFOLIO_REFERENCE_VALID"
    )
    add_failure(
        failures,
        portfolio is None or portfolio["base_currency"] != "USD",
        "RISK_RUN_CURRENCY_VALID",
    )
    add_failure(
        failures,
        instrument_inputs.count() != EXPECTED_INSTRUMENT_COUNT
        or instrument_inputs.select("instrument_id").distinct().count()
        != EXPECTED_INSTRUMENT_COUNT,
        "RISK_RUN_ACTIVE_INSTRUMENT_UNIVERSE_COMPLETE",
    )
    add_failure(
        failures,
        position_inputs.count() != EXPECTED_INSTRUMENT_COUNT
        or position_inputs.select("instrument_id").distinct().count()
        != EXPECTED_INSTRUMENT_COUNT
        or position_inputs.where(F.col("signed_quantity") == F.lit(0)).count() != 0,
        "RISK_RUN_STATIC_EXPOSURE_COMPLETE",
    )
    add_failure(
        failures,
        cash is None or cash["base_currency"] != "USD",
        "RISK_RUN_STATIC_CASH_VALID",
    )

    position_coverage_missing = active_instrument_ids.join(
        position_inputs.select("instrument_id"), "instrument_id", "left_anti"
    ).count()
    add_failure(
        failures,
        position_coverage_missing != 0,
        "RISK_RUN_STATIC_EXPOSURE_INSTRUMENT_COVERAGE",
    )

    session_count = price_inputs.select("price_date").distinct().count()
    price_count = price_inputs.count()
    price_key_count = (
        price_inputs.select("instrument_id", "price_date").distinct().count()
    )
    instrument_session_counts = price_inputs.groupBy("instrument_id").agg(
        F.countDistinct("price_date").alias("session_count")
    )
    missing_price_coverage = (
        active_instrument_ids.join(instrument_session_counts, "instrument_id", "left")
        .where(F.coalesce(F.col("session_count"), F.lit(0)) != EXPECTED_SESSION_COUNT)
        .count()
    )
    price_dates = [
        row["price_date"]
        for row in price_inputs.select("price_date")
        .distinct()
        .orderBy("price_date")
        .collect()
    ]
    add_failure(
        failures,
        session_count != EXPECTED_SESSION_COUNT
        or price_count != EXPECTED_INSTRUMENT_COUNT * EXPECTED_SESSION_COUNT
        or price_key_count != price_count
        or missing_price_coverage != 0
        or price_dates != list(EXPECTED_PRICE_DATES),
        "RISK_RUN_PRICE_HISTORY_COVERAGE_COMPLETE",
    )
    add_failure(
        failures,
        price_inputs.where(
            (F.col("quote_currency") != F.lit("USD"))
            | F.col("close_price").isNull()
            | F.col("adjusted_close_price").isNull()
            | (F.col("close_price") <= F.lit(0))
            | (F.col("adjusted_close_price") <= F.lit(0))
        ).count()
        != 0,
        "RISK_RUN_PRICE_INPUTS_VALID",
    )

    for label, frame in [
        ("portfolio", portfolio_inputs),
        ("instruments", instrument_inputs),
        ("positions", position_inputs),
        ("cash", cash_inputs),
        ("prices", price_inputs),
        ("stress_scenarios", scenario_inputs),
        ("stress_shocks", shock_inputs),
    ]:
        add_failure(
            failures,
            frame.where(~F.col("record_hash").rlike(SHA256_PATTERN)).count() != 0,
            f"RISK_RUN_{label.upper()}_RECORD_HASH_VALID",
        )

    scenario_ids = {
        row["scenario_id"] for row in scenario_inputs.select("scenario_id").collect()
    }
    scenario_key_count = scenario_inputs.select("scenario_id").distinct().count()
    add_failure(
        failures,
        scenario_inputs.count() != EXPECTED_STRESS_SCENARIO_COUNT
        or scenario_key_count != EXPECTED_STRESS_SCENARIO_COUNT
        or scenario_ids != APPROVED_SCENARIO_IDS
        or scenario_inputs.where(
            (F.col("scenario_type") != F.lit("DETERMINISTIC_HYPOTHETICAL"))
            | (F.col("scenario_description").contains("forecast"))
        ).count()
        != 0,
        "STRESS_RESULT_APPROVED_SCENARIO_SET_COMPLETE",
    )
    scenario_versions = scenario_inputs.select(
        "scenario_id", "scenario_version", "record_hash"
    ).alias("scenario")
    shocks_with_references = (
        shock_inputs.alias("shock")
        .join(
            scenario_versions,
            (F.col("shock.scenario_id") == F.col("scenario.scenario_id"))
            & (F.col("shock.scenario_version") == F.col("scenario.scenario_version")),
            "inner",
        )
        .join(
            active_instrument_ids.alias("instrument"),
            F.col("shock.instrument_id") == F.col("instrument.instrument_id"),
            "inner",
        )
        .select("shock.*", F.col("scenario.record_hash").alias("scenario_record_hash"))
    )
    shock_key_count = (
        shock_inputs.select("scenario_id", "instrument_id").distinct().count()
    )
    shock_coverage = shocks_with_references.groupBy("scenario_id").agg(
        F.count("instrument_id").alias("shock_count"),
        F.countDistinct("instrument_id").alias("instrument_count"),
    )
    missing_shock_coverage = (
        scenario_inputs.select("scenario_id")
        .join(shock_coverage, "scenario_id", "left")
        .where(
            (F.coalesce(F.col("shock_count"), F.lit(0)) != EXPECTED_SHOCK_COUNT)
            | (
                F.coalesce(F.col("instrument_count"), F.lit(0))
                != EXPECTED_INSTRUMENT_COUNT
            )
        )
        .count()
    )
    add_failure(
        failures,
        shock_inputs.count() != EXPECTED_STRESS_SCENARIO_COUNT * EXPECTED_SHOCK_COUNT
        or shock_key_count != shock_inputs.count()
        or shocks_with_references.count() != shock_inputs.count()
        or missing_shock_coverage != 0
        or shock_inputs.where(
            (F.col("shock_ratio") < F.lit(-1))
            | F.col("shock_rationale").isNull()
            | (F.length(F.trim(F.col("shock_rationale"))) == 0)
        ).count()
        != 0,
        "STRESS_RESULT_SHOCK_COVERAGE_VALID",
    )
    add_failure(
        failures,
        shock_inputs.where(
            (F.col("scenario_id") == F.lit("BROAD_MARKET_DOWN_10"))
            & (F.col("shock_ratio") != F.lit(Decimal("-0.1000000000")))
        ).count()
        != 0,
        "STRESS_BROAD_MARKET_BASELINE",
    )

    # Static January 7 signed quantities are carried unchanged, then repriced only
    # on December 30.  No later rebalances or corporate actions are consulted.
    as_of_prices = price_inputs.where(F.col("price_date") == F.lit(as_of_date)).select(
        "instrument_id",
        F.col("close_price").alias("as_of_close_price"),
        F.col("record_hash").alias("as_of_price_record_hash"),
    )
    static_exposure = (
        position_inputs.alias("position")
        .join(as_of_prices.alias("price"), "instrument_id", "inner")
        .select(
            F.col("position.instrument_id"),
            F.col("position.signed_quantity")
            .cast("decimal(38,16)")
            .alias("signed_quantity"),
            F.col("price.as_of_close_price")
            .cast("decimal(20,8)")
            .alias("as_of_close_price"),
            (
                F.col("position.signed_quantity").cast("decimal(38,16)")
                * F.col("price.as_of_close_price").cast("decimal(20,8)")
            )
            .cast("decimal(38,16)")
            .alias("signed_market_value"),
            F.col("position.record_hash").alias("position_record_hash"),
            F.col("price.as_of_price_record_hash"),
        )
    )
    static_exposure = freeze_small_dataframe(
        static_exposure,
        label="static December 30 exposure",
        max_rows=EXPECTED_INSTRUMENT_COUNT,
    )
    static_exposure_sha256 = ordered_set_sha256(
        static_exposure,
        order_columns=["instrument_id"],
        value_columns=[
            "instrument_id",
            "signed_quantity",
            "as_of_close_price",
            "signed_market_value",
            "position_record_hash",
            "as_of_price_record_hash",
        ],
    )
    static_security_value = static_exposure.agg(
        F.sum("signed_market_value").cast("decimal(38,16)").alias("value")
    ).first()["value"] or Decimal("0")
    static_cash_balance = (
        cash["closing_cash_balance"] if cash is not None else Decimal("0")
    )
    static_nav = static_security_value + static_cash_balance
    add_failure(
        failures,
        static_exposure.count() != EXPECTED_INSTRUMENT_COUNT,
        "RISK_RUN_STATIC_EXPOSURE_REPRICE_COMPLETE",
    )

    price_window = Window.partitionBy("instrument_id").orderBy("price_date")
    instrument_returns = (
        price_inputs.select(
            "instrument_id", "price_date", "adjusted_close_price", "record_hash"
        )
        .withColumn("prior_price_date", F.lag("price_date").over(price_window))
        .withColumn(
            "prior_adjusted_close_price",
            F.lag("adjusted_close_price").over(price_window),
        )
        .withColumn("prior_price_record_hash", F.lag("record_hash").over(price_window))
        .where(F.col("prior_price_date").isNotNull())
        .withColumn(
            "instrument_return",
            (
                F.col("adjusted_close_price").cast("decimal(38,16)")
                / F.col("prior_adjusted_close_price").cast("decimal(38,16)")
                - F.lit(1)
            ).cast("decimal(38,16)"),
        )
    )
    return_observation_count = (
        instrument_returns.select("price_date").distinct().count()
    )
    add_failure(
        failures,
        return_observation_count != EXPECTED_RETURN_COUNT
        or return_observation_count < MINIMUM_RETURN_COUNT
        or instrument_returns.count()
        != EXPECTED_INSTRUMENT_COUNT * EXPECTED_RETURN_COUNT,
        "HISTORICAL_PNL_SCENARIO_COUNT_COMPLETE",
    )

    historical_base = (
        instrument_returns.alias("returns")
        .join(
            static_exposure.select("instrument_id", "signed_market_value").alias(
                "exposure"
            ),
            "instrument_id",
            "inner",
        )
        .groupBy("prior_price_date", "price_date")
        .agg(
            F.sum(
                F.col("signed_market_value").cast("decimal(38,16)")
                * F.col("instrument_return").cast("decimal(38,16)")
            )
            .cast("decimal(38,16)")
            .alias("simulated_portfolio_pnl")
        )
        .withColumn("scenario_date", F.col("price_date"))
        .drop("price_date")
        .withColumn(
            "return_observation_number",
            F.row_number().over(Window.orderBy("scenario_date")),
        )
        .withColumn(
            "loss_amount", (-F.col("simulated_portfolio_pnl")).cast("decimal(38,16)")
        )
    )

    price_partition_hashes = price_inputs.groupBy("price_date").agg(
        F.sha2(
            F.concat_ws(
                "\n",
                F.transform(
                    F.array_sort(
                        F.collect_list(F.struct("instrument_id", "record_hash"))
                    ),
                    lambda item: F.concat_ws(
                        "|", item["instrument_id"], item["record_hash"]
                    ),
                ),
            ),
            256,
        ).alias("price_partition_sha256")
    )
    historical_base = (
        historical_base.alias("candidate")
        .join(
            price_partition_hashes.alias("prior_hash"),
            F.col("candidate.prior_price_date") == F.col("prior_hash.price_date"),
            "inner",
        )
        .join(
            price_partition_hashes.alias("scenario_hash"),
            F.col("candidate.scenario_date") == F.col("scenario_hash.price_date"),
            "inner",
        )
        .select(
            "candidate.*",
            F.col("prior_hash.price_partition_sha256").alias(
                "input_prior_price_partition_sha256"
            ),
            F.col("scenario_hash.price_partition_sha256").alias(
                "input_scenario_price_partition_sha256"
            ),
        )
    )
    calculated_at_utc = datetime.now(UTC).replace(tzinfo=None)
    historical_values = historical_base.select(
        F.lit(risk_run_id).alias("risk_run_id"),
        F.lit(portfolio_id).alias("portfolio_id"),
        "prior_price_date",
        "scenario_date",
        F.col("return_observation_number")
        .cast("bigint")
        .alias("return_observation_number"),
        F.lit("USD").alias("base_currency"),
        "simulated_portfolio_pnl",
        "loss_amount",
        F.lit(static_exposure_sha256).alias("input_static_exposure_sha256"),
        "input_prior_price_partition_sha256",
        "input_scenario_price_partition_sha256",
        F.lit(CALCULATION_VERSION).alias("calculation_version"),
        F.lit(calculated_at_utc).alias("calculated_at_utc"),
        F.lit(HISTORICAL_PNL_CONTRACT_VERSION).alias("contract_version"),
        F.lit(EMPTY_SET_SHA256).alias("record_hash"),
    )
    historical_candidate = historical_values.withColumn(
        "record_hash", canonical_hash_expression(HISTORICAL_HASH_FIELDS)
    ).select(*spark.table(HISTORICAL_PNL_TABLE).columns)
    historical_candidate = freeze_small_dataframe(
        historical_candidate,
        label="historical PnL candidate",
        max_rows=EXPECTED_RETURN_COUNT,
    )

    historical_set_sha256 = ordered_set_sha256(
        historical_candidate,
        order_columns=["scenario_date"],
        value_columns=["scenario_date", "record_hash"],
    )
    ranked_losses = historical_candidate.withColumn(
        "loss_rank",
        F.row_number().over(
            Window.orderBy(F.col("loss_amount").asc(), F.col("scenario_date").asc())
        ),
    )
    confidence_frame = spark.createDataFrame(
        [(confidence, rank) for confidence, rank in CONFIDENCE_RANKS.items()],
        ["confidence_level", "quantile_rank"],
    ).withColumn("confidence_level", F.col("confidence_level").cast("decimal(3,2)"))
    var_values = confidence_frame.join(
        ranked_losses,
        F.col("quantile_rank") == F.col("loss_rank"),
        "inner",
    ).select(
        F.lit(risk_run_id).alias("risk_run_id"),
        F.lit(portfolio_id).alias("portfolio_id"),
        F.lit(as_of_date).alias("as_of_date"),
        F.lit("USD").alias("base_currency"),
        "confidence_level",
        F.lit(EXPECTED_RETURN_COUNT).cast("bigint").alias("observation_count"),
        F.col("quantile_rank").cast("bigint").alias("quantile_rank"),
        F.col("loss_amount").cast("decimal(38,16)").alias("var_amount"),
        F.lit(historical_set_sha256).alias("input_historical_pnl_scenario_set_sha256"),
        F.lit(static_exposure_sha256).alias("input_static_exposure_sha256"),
        F.lit(CALCULATION_VERSION).alias("calculation_version"),
        F.lit(calculated_at_utc).alias("calculated_at_utc"),
        F.lit(VAR_MEASURE_CONTRACT_VERSION).alias("contract_version"),
        F.lit(EMPTY_SET_SHA256).alias("record_hash"),
    )
    var_candidate = var_values.withColumn(
        "record_hash", canonical_hash_expression(VAR_HASH_FIELDS)
    ).select(*spark.table(VAR_MEASURE_TABLE).columns)
    var_candidate = freeze_small_dataframe(
        var_candidate, label="VaR candidate", max_rows=2
    )

    shock_set_hashes = shocks_with_references.groupBy("scenario_id").agg(
        F.sha2(
            F.concat_ws(
                "\n",
                F.transform(
                    F.array_sort(
                        F.collect_list(F.struct("instrument_id", "record_hash"))
                    ),
                    lambda item: F.concat_ws(
                        "|", item["instrument_id"], item["record_hash"]
                    ),
                ),
            ),
            256,
        ).alias("input_shock_set_sha256")
    )
    stress_values = (
        shocks_with_references.alias("shock")
        .join(
            static_exposure.select("instrument_id", "signed_market_value").alias(
                "exposure"
            ),
            F.col("shock.instrument_id") == F.col("exposure.instrument_id"),
            "inner",
        )
        .groupBy(
            F.col("shock.scenario_id").alias("scenario_id"),
            F.col("shock.scenario_version").alias("scenario_version"),
            F.col("shock.scenario_record_hash").alias("scenario_record_hash"),
        )
        .agg(
            F.count("shock.instrument_id").cast("bigint").alias("shock_count"),
            F.sum(
                F.col("exposure.signed_market_value").cast("decimal(38,16)")
                * F.col("shock.shock_ratio").cast("decimal(38,16)")
            )
            .cast("decimal(38,16)")
            .alias("stress_pnl"),
        )
        .join(shock_set_hashes, "scenario_id", "inner")
        .withColumn(
            "stressed_nav",
            (F.lit(static_nav).cast("decimal(38,16)") + F.col("stress_pnl")).cast(
                "decimal(38,16)"
            ),
        )
        .select(
            F.lit(risk_run_id).alias("risk_run_id"),
            F.lit(portfolio_id).alias("portfolio_id"),
            F.lit(as_of_date).alias("as_of_date"),
            F.lit("USD").alias("base_currency"),
            "scenario_id",
            "scenario_version",
            "shock_count",
            "stress_pnl",
            "stressed_nav",
            F.lit(static_exposure_sha256).alias("input_static_exposure_sha256"),
            F.col("scenario_record_hash").alias("input_stress_scenario_record_sha256"),
            "input_shock_set_sha256",
            F.lit(CALCULATION_VERSION).alias("calculation_version"),
            F.lit(calculated_at_utc).alias("calculated_at_utc"),
            F.lit(STRESS_RESULT_CONTRACT_VERSION).alias("contract_version"),
            F.lit(EMPTY_SET_SHA256).alias("record_hash"),
        )
    )
    stress_candidate = stress_values.withColumn(
        "record_hash", canonical_hash_expression(STRESS_HASH_FIELDS)
    ).select(*spark.table(STRESS_RESULT_TABLE).columns)
    stress_candidate = freeze_small_dataframe(
        stress_candidate,
        label="stress-result candidate",
        max_rows=EXPECTED_STRESS_SCENARIO_COUNT,
    )

    # Validate every output count, key, formula, rank, hash, and static-NAV tie-out
    # before any append.  A signed loss is intentionally allowed to be negative.
    historical_count = historical_candidate.count()
    var_count = var_candidate.count()
    stress_count = stress_candidate.count()
    historical_date_pairs = {
        (row["prior_price_date"], row["scenario_date"])
        for row in historical_candidate.select(
            "prior_price_date", "scenario_date"
        ).collect()
    }
    add_failure(
        failures,
        historical_count != EXPECTED_RETURN_COUNT
        or historical_candidate.select("scenario_date").distinct().count()
        != historical_count
        or historical_date_pairs != EXPECTED_PRICE_DATE_PAIRS
        or historical_candidate.where(
            F.col("loss_amount") != -F.col("simulated_portfolio_pnl")
        ).count()
        != 0,
        "HISTORICAL_PNL_SCENARIO_FORMULA_RECONCILES",
    )
    add_failure(
        failures,
        var_count != len(CONFIDENCE_RANKS)
        or var_candidate.select("confidence_level").distinct().count()
        != len(CONFIDENCE_RANKS)
        or var_candidate.where(
            (
                (F.col("confidence_level") == F.lit(Decimal("0.95")))
                & (F.col("quantile_rank") != 239)
            )
            | (
                (F.col("confidence_level") == F.lit(Decimal("0.99")))
                & (F.col("quantile_rank") != 249)
            )
        ).count()
        != 0,
        "VAR_MEASURE_DISCRETE_QUANTILE_RECONCILES",
    )
    var_reconciliation_failures = (
        var_candidate.alias("var")
        .join(
            ranked_losses.select(
                F.col("loss_rank").alias("expected_rank"),
                F.col("loss_amount").alias("expected_var_amount"),
            ).alias("ranked_loss"),
            F.col("var.quantile_rank") == F.col("ranked_loss.expected_rank"),
            "left",
        )
        .where(
            F.col("ranked_loss.expected_var_amount").isNull()
            | (F.col("var.var_amount") != F.col("ranked_loss.expected_var_amount"))
        )
        .count()
    )
    add_failure(
        failures,
        var_reconciliation_failures != 0,
        "VAR_MEASURE_AMOUNT_RECONCILES",
    )
    expected_stress_reconciliation = (
        shocks_with_references.alias("shock")
        .join(
            static_exposure.select("instrument_id", "signed_market_value").alias(
                "exposure"
            ),
            F.col("shock.instrument_id") == F.col("exposure.instrument_id"),
            "inner",
        )
        .groupBy(F.col("shock.scenario_id").alias("scenario_id"))
        .agg(
            F.sum(
                F.col("exposure.signed_market_value").cast("decimal(38,16)")
                * F.col("shock.shock_ratio").cast("decimal(38,16)")
            )
            .cast("decimal(38,16)")
            .alias("expected_stress_pnl"),
        )
    )
    stress_reconciliation_failures = (
        stress_candidate.alias("result")
        .join(
            expected_stress_reconciliation.alias("expected"),
            F.col("result.scenario_id") == F.col("expected.scenario_id"),
            "left",
        )
        .where(
            F.col("expected.expected_stress_pnl").isNull()
            | (F.col("result.stress_pnl") != F.col("expected.expected_stress_pnl"))
            | (
                F.col("result.stressed_nav")
                != F.lit(static_nav).cast("decimal(38,16)")
                + F.col("expected.expected_stress_pnl")
            )
        )
        .count()
    )
    add_failure(
        failures,
        stress_count != EXPECTED_STRESS_SCENARIO_COUNT
        or stress_candidate.select("scenario_id").distinct().count() != stress_count
        or stress_reconciliation_failures != 0
        or stress_candidate.where(
            (F.col("shock_count") != EXPECTED_SHOCK_COUNT)
            | (
                F.col("stressed_nav")
                != F.lit(static_nav).cast("decimal(38,16)") + F.col("stress_pnl")
            )
        ).count()
        != 0,
        "STRESS_RESULT_FORMULA_RECONCILES",
    )
    for candidate, hash_fields, rule_id in [
        (
            historical_candidate,
            HISTORICAL_HASH_FIELDS,
            "HISTORICAL_PNL_SCENARIO_RECORD_HASH_VALID",
        ),
        (var_candidate, VAR_HASH_FIELDS, "VAR_MEASURE_RECORD_HASH_VALID"),
        (stress_candidate, STRESS_HASH_FIELDS, "STRESS_RESULT_RECORD_HASH_VALID"),
    ]:
        add_failure(
            failures,
            candidate.withColumn(
                "expected_record_hash", canonical_hash_expression(hash_fields)
            )
            .where(
                (~F.col("record_hash").rlike(SHA256_PATTERN))
                | (F.col("record_hash") != F.col("expected_record_hash"))
            )
            .count()
            != 0,
            rule_id,
        )

    manifest_entries = [
        build_manifest_entry(
            dataset_name="PORTFOLIOS",
            frame=portfolio_inputs,
            order_columns=["portfolio_id"],
            value_columns=["portfolio_id", "record_hash"],
            selection_scope=f"portfolio_id={portfolio_id}",
        ),
        build_manifest_entry(
            dataset_name="INSTRUMENTS",
            frame=instrument_inputs,
            order_columns=["instrument_id"],
            value_columns=["instrument_id", "record_hash"],
            selection_scope="is_active=true;quote_currency=USD",
        ),
        build_manifest_entry(
            dataset_name="DAILY_PRICES",
            frame=price_inputs,
            order_columns=["instrument_id", "price_date"],
            value_columns=["instrument_id", "price_date", "record_hash"],
            selection_scope=f"2016-01-04..{as_of_date}",
        ),
        build_manifest_entry(
            dataset_name="POSITIONS",
            frame=position_inputs,
            order_columns=["portfolio_id", "instrument_id"],
            value_columns=["portfolio_id", "instrument_id", "record_hash"],
            selection_scope=(f"portfolio_id={portfolio_id};position_date=2016-01-07"),
        ),
        build_manifest_entry(
            dataset_name="CASH_BALANCES",
            frame=cash_inputs,
            order_columns=["portfolio_id", "cash_date"],
            value_columns=["portfolio_id", "cash_date", "record_hash"],
            selection_scope=(f"portfolio_id={portfolio_id};cash_date=2016-01-07"),
        ),
        build_manifest_entry(
            dataset_name="STRESS_SCENARIOS",
            frame=scenario_inputs,
            order_columns=["scenario_id"],
            value_columns=["scenario_id", "record_hash"],
            selection_scope="is_active=true",
        ),
        build_manifest_entry(
            dataset_name="STRESS_SCENARIO_SHOCKS",
            frame=shock_inputs,
            order_columns=["scenario_id", "instrument_id"],
            value_columns=["scenario_id", "instrument_id", "record_hash"],
            selection_scope="approved active scenarios",
        ),
    ]
    input_dataset_names = sorted(entry["dataset_name"] for entry in manifest_entries)
    input_record_count = sum(entry["record_count"] for entry in manifest_entries)
    input_manifest_digest = input_manifest_sha256(manifest_entries)
    price_history_sha256 = ordered_set_sha256(
        price_inputs,
        order_columns=["instrument_id", "price_date"],
        value_columns=["instrument_id", "price_date", "record_hash"],
    )
    scenario_set_sha256 = ordered_set_sha256(
        scenario_inputs,
        order_columns=["scenario_id"],
        value_columns=["scenario_id", "record_hash"],
    )
    shock_set_sha256 = ordered_set_sha256(
        shock_inputs,
        order_columns=["scenario_id", "instrument_id"],
        value_columns=["scenario_id", "instrument_id", "record_hash"],
    )

    failed_rule_ids = sorted(set(failures))
    publish_allowed = not failed_rule_ids
    run_status = "SUCCEEDED" if publish_allowed else "FAILED"
    completed_at_utc = datetime.now(UTC).replace(tzinfo=None)
    published_at_utc = completed_at_utc if publish_allowed else None

    print(
        "risk_validation=PASS "
        f"historical_count={historical_count} var_count={var_count} "
        f"stress_count={stress_count} failed_rule_ids={failed_rule_ids}"
    )
    print(
        "risk_input_manifest=PASS "
        f"input_dataset_names={input_dataset_names} "
        f"input_record_count={input_record_count} "
        f"input_manifest_sha256={input_manifest_digest} "
        f"manifest_entries={json.dumps(manifest_entries, sort_keys=True)}"
    )

    audit_values = {
        "risk_run_id": risk_run_id,
        "portfolio_id": portfolio_id,
        "as_of_date": as_of_date,
        "base_currency": "USD",
        "attempt_number": attempt_number,
        "reprocess_of_risk_run_id": reprocess_of_risk_run_id,
        "trigger_type": trigger_type,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "status": run_status,
        "methodology": "ONE_DAY_HISTORICAL_SIMULATION",
        "holding_period_days": 1,
        "return_price_field": "daily_prices.adjusted_close_price",
        "return_formula": "adjusted_close[t] / adjusted_close[t-1] - 1",
        "price_history_start_date": EXPECTED_HISTORY_START_DATE,
        "price_history_end_date": as_of_date,
        "trading_session_count": session_count,
        "return_observation_count": return_observation_count,
        "minimum_return_observation_count": MINIMUM_RETURN_COUNT,
        "confidence_levels": [Decimal("0.95"), Decimal("0.99")],
        "static_exposure_source_date": STATIC_EXPOSURE_SOURCE_DATE,
        "static_exposure_valuation_date": as_of_date,
        "static_exposure_policy": (
            "BUY_AND_HOLD_NO_REBALANCE_NO_LATER_CORPORATE_ACTIONS"
        ),
        "input_dataset_names": input_dataset_names,
        "input_record_count": input_record_count,
        "input_manifest_sha256": input_manifest_digest,
        "input_static_exposure_sha256": static_exposure_sha256,
        "input_price_history_sha256": price_history_sha256,
        "input_stress_scenario_set_sha256": scenario_set_sha256,
        "input_stress_shock_set_sha256": shock_set_sha256,
        "expected_historical_pnl_scenario_count": EXPECTED_RETURN_COUNT,
        "calculated_historical_pnl_scenario_count": historical_count,
        "expected_var_measure_count": len(CONFIDENCE_RANKS),
        "calculated_var_measure_count": var_count,
        "expected_stress_result_count": EXPECTED_STRESS_SCENARIO_COUNT,
        "calculated_stress_result_count": stress_count,
        "published": publish_allowed,
        "published_at_utc": published_at_utc,
        "warning_count": 0,
        "failed_rule_ids": failed_rule_ids,
        "error_code": None if publish_allowed else "VALIDATION_FAILED",
        "error_message": (
            None if publish_allowed else "Risk validation prevented bundle publication."
        ),
        "historical_pnl_scenarios_contract_version": HISTORICAL_PNL_CONTRACT_VERSION,
        "var_measures_contract_version": VAR_MEASURE_CONTRACT_VERSION,
        "stress_results_contract_version": STRESS_RESULT_CONTRACT_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "code_version": code_version,
        "contract_version": RISK_RUN_CONTRACT_VERSION,
    }
    audit_candidate = spark.createDataFrame(
        [audit_values], schema=spark.table(RISK_RUN_TABLE).schema
    )

    # The output writes occur only after the complete preflight gate.  Delta is
    # atomic per table, so this is a logical publication: consumers must join
    # result rows to this successful, published terminal audit record.
    if publish_allowed:
        try:
            append_immutable(historical_candidate, HISTORICAL_PNL_TABLE)
            append_immutable(var_candidate, VAR_MEASURE_TABLE)
            append_immutable(stress_candidate, STRESS_RESULT_TABLE)
        except Exception as error:
            append_failed_risk_audit(error)
            raise

    append_immutable(audit_candidate, RISK_RUN_TABLE)
    risk_audit_written = True

    require_equal(
        "persisted risk audit count",
        spark.table(RISK_RUN_TABLE).where(F.col("risk_run_id") == risk_run_id).count(),
        1,
    )
    if publish_allowed:
        require_equal(
            "persisted historical scenario count",
            spark.table(HISTORICAL_PNL_TABLE)
            .where(F.col("risk_run_id") == risk_run_id)
            .count(),
            EXPECTED_RETURN_COUNT,
        )
        require_equal(
            "persisted VaR measure count",
            spark.table(VAR_MEASURE_TABLE)
            .where(F.col("risk_run_id") == risk_run_id)
            .count(),
            len(CONFIDENCE_RANKS),
        )
        require_equal(
            "persisted stress result count",
            spark.table(STRESS_RESULT_TABLE)
            .where(F.col("risk_run_id") == risk_run_id)
            .count(),
            EXPECTED_STRESS_SCENARIO_COUNT,
        )

    print(
        "risk_persistence=PASS "
        f"risk_run_id={risk_run_id} status={run_status} published={publish_allowed}"
    )
    if not publish_allowed:
        raise ValueError("Risk calculation failed rules: " + ", ".join(failed_rule_ids))


execute_with_failed_audit(execute_risk_run, append_failed_risk_audit)
