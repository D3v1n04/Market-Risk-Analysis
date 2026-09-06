# Databricks notebook source
"""Validate and canonicalize one Phase 06 Bronze market-data batch."""

import re
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID, uuid4

from delta.tables import DeltaTable
from pyspark.dbutils import DBUtils
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROCESSING_RUN_CONTRACT_VERSION = "2.0.0"
OUTCOME_CONTRACT_VERSION = "1.0.0"
VIOLATION_CONTRACT_VERSION = "2.0.0"

BRONZE_BATCH_TABLE = (
    "workspace.devin_market_risk_dev.bronze_ingestion_batches"
)
SILVER_PROCESSING_RUN_TABLE = (
    "workspace.devin_market_risk_dev.silver_processing_runs"
)
SILVER_OUTCOME_TABLE = (
    "workspace.devin_market_risk_dev.silver_source_record_outcomes"
)
SILVER_VIOLATION_TABLE = (
    "workspace.devin_market_risk_dev.silver_data_quality_violations"
)
SILVER_INSTRUMENT_TABLE = (
    "workspace.devin_market_risk_dev.silver_instruments"
)
SILVER_CALENDAR_TABLE = (
    "workspace.devin_market_risk_dev.silver_trading_calendar"
)

PRICE_DATES = (
    date(2016, 1, 4),
    date(2016, 1, 5),
    date(2016, 1, 6),
    date(2016, 1, 7),
)

DATASET_SPECS = {
    "DAILY_PRICES": {
        "contract_version": "1.1.0",
        "bronze_table": (
            "workspace.devin_market_risk_dev.bronze_daily_prices"
        ),
        "silver_table": (
            "workspace.devin_market_risk_dev.silver_daily_prices"
        ),
        "expected_source_count": 60,
        "key_columns": ["instrument_id", "price_date", "source_id"],
        "output_columns": [
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
            "source_updated_at_utc",
            "source_record_id",
            "batch_id",
            "ingested_at_utc",
            "contract_version",
            "record_hash",
        ],
        "hash_fields": [
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
    },
    "CORPORATE_ACTIONS": {
        "contract_version": "1.1.0",
        "bronze_table": (
            "workspace.devin_market_risk_dev.bronze_corporate_actions"
        ),
        "silver_table": (
            "workspace.devin_market_risk_dev.silver_corporate_actions"
        ),
        "expected_source_count": 2,
        "key_columns": [
            "instrument_id",
            "effective_date",
            "action_type",
            "source_id",
        ],
        "output_columns": [
            "instrument_id",
            "effective_date",
            "action_type",
            "source_id",
            "source_symbol",
            "dividend_amount_per_share",
            "dividend_currency",
            "split_ratio",
            "source_action_id",
            "source_updated_at_utc",
            "source_record_id",
            "batch_id",
            "ingested_at_utc",
            "contract_version",
            "record_hash",
        ],
        "hash_fields": [
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
    },
}

CODE_VERSION_PATTERN = re.compile(r"^[0-9a-f]{7,40}$")


def require_equal(label: str, actual: Any, expected: Any) -> None:
    """Raise a clear error when two values differ."""
    if actual != expected:
        raise ValueError(
            f"{label} mismatch: expected {expected!r}, found {actual!r}"
        )


def require_uuid(label: str, value: str) -> str:
    """Require a canonical UUID string."""
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be a valid UUID") from exc
    canonical = str(parsed)
    if value.lower() != canonical:
        raise ValueError(f"{label} must use canonical UUID formatting")
    return canonical


def require_code_version(value: str) -> str:
    """Require a lowercase abbreviated or complete Git SHA."""
    if CODE_VERSION_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "code_version must be a lowercase 7-to-40-character Git SHA"
        )
    return value


def null_if_blank(column_name: str) -> Column:
    """Normalize a blank source string to null without changing nonblank text."""
    return F.when(
        F.trim(F.col(column_name)) == "",
        F.lit(None).cast("string"),
    ).otherwise(F.trim(F.col(column_name)))


def calculate_ordered_set_sha256(
    frame: DataFrame,
    *,
    order_columns: list[str],
    value_columns: list[str],
) -> str:
    """Hash a deterministically ordered set of rows."""
    ordering_fields = [
        F.col(column_name).alias(f"order_{index}")
        for index, column_name in enumerate(order_columns)
    ]
    value_fields = [
        F.coalesce(F.col(column_name).cast("string"), F.lit("<NULL>"))
        for column_name in value_columns
    ]
    digest = (
        frame.select(
            F.struct(
                *ordering_fields,
                F.concat_ws("|", *value_fields).alias("canonical_row"),
            ).alias("ordered_row")
        )
        .agg(
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
        )
        .first()
    )
    return digest["set_sha256"]


def snapshot_sha256(
    frame: DataFrame,
    *,
    key_columns: list[str],
) -> str | None:
    """Return null for an empty snapshot, otherwise its digest."""
    if frame.count() == 0:
        return None
    return calculate_ordered_set_sha256(
        frame,
        order_columns=key_columns,
        value_columns=[*key_columns, "record_hash"],
    )


def freeze_small_dataframe(
    frame: DataFrame,
    *,
    label: str,
    max_rows: int,
) -> DataFrame:
    """Detach bounded evidence from source tables without cache or persist."""
    rows = frame.limit(max_rows + 1).collect()
    if len(rows) > max_rows:
        raise ValueError(
            f"{label} exceeded the safe materialization limit of {max_rows}"
        )
    return spark.createDataFrame(rows, schema=frame.schema)


def insert_only_audit_records(
    frame: DataFrame,
    target_table: str,
    key_column: str,
) -> None:
    """Insert immutable audit evidence without updating existing rows."""
    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("target")
        .merge(
            frame.alias("source"),
            f"target.{key_column} = source.{key_column}",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def canonical_hash_expression(fields: list[str]) -> Column:
    """Build the contract-defined fixed-type record hash."""
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


def business_key_expression(key_columns: list[str]) -> Column:
    """Build the contract business key and null malformed keys."""
    missing_key = None
    for column_name in key_columns:
        missing = F.col(column_name).isNull() | (
            F.trim(F.col(column_name).cast("string")) == ""
        )
        missing_key = missing if missing_key is None else missing_key | missing
    return F.when(missing_key, F.lit(None).cast("string")).otherwise(
        F.concat_ws(
            "|",
            *[F.col(column_name).cast("string") for column_name in key_columns],
        )
    )


def violation_struct(
    *,
    rule_id: str,
    severity: str,
    disposition: str,
    affected_field: str | None,
    observed_value: Column,
    expected_condition: str,
    message: str,
) -> Column:
    """Create a uniform record-level violation value."""
    return F.struct(
        F.lit(rule_id).alias("rule_id"),
        F.lit(severity).alias("severity"),
        F.lit(disposition).alias("disposition"),
        F.lit(affected_field).cast("string").alias("affected_field"),
        observed_value.cast("string").alias("observed_value"),
        F.lit(expected_condition).alias("expected_condition"),
        F.lit(message).alias("message"),
    )


def conditional_violation(condition: Column, violation: Column) -> Column:
    """Return a violation only when its rule fails."""
    return F.when(condition, violation)


def audit_source_record_id() -> Column:
    """Create the audit identity required by source-record outcomes."""
    return F.concat_ws(
        ":",
        F.col("batch_id"),
        F.col("source_row_number").cast("string"),
    )


def typed_daily_price_candidates(bronze_source: DataFrame) -> DataFrame:
    """Cast source-aligned daily-price strings to contract types."""
    return bronze_source.select(
        null_if_blank("instrument_id").alias("instrument_id"),
        F.to_date(F.col("price_date"), "yyyy-MM-dd").alias("price_date"),
        null_if_blank("source_id").alias("source_id"),
        null_if_blank("source_symbol").alias("source_symbol"),
        F.col("open_price").cast("decimal(20,8)").alias("open_price"),
        F.col("high_price").cast("decimal(20,8)").alias("high_price"),
        F.col("low_price").cast("decimal(20,8)").alias("low_price"),
        F.col("close_price").cast("decimal(20,8)").alias("close_price"),
        F.col("adjusted_close_price")
        .cast("decimal(20,8)")
        .alias("adjusted_close_price"),
        null_if_blank("volume").cast("bigint").alias("volume"),
        null_if_blank("quote_currency").alias("quote_currency"),
        F.to_timestamp(null_if_blank("source_updated_at_utc")).alias(
            "source_updated_at_utc"
        ),
        null_if_blank("source_record_id").alias("source_record_id"),
        F.col("batch_id"),
        F.col("ingested_at_utc"),
        F.col("contract_version"),
        null_if_blank("record_hash").alias("record_hash"),
        F.col("source_row_number"),
        F.col("source_record_sha256"),
        audit_source_record_id().alias("audit_source_record_id"),
        F.col("price_date").alias("raw_price_date"),
        F.col("open_price").alias("raw_open_price"),
        F.col("high_price").alias("raw_high_price"),
        F.col("low_price").alias("raw_low_price"),
        F.col("close_price").alias("raw_close_price"),
        F.col("adjusted_close_price").alias("raw_adjusted_close_price"),
        F.col("volume").alias("raw_volume"),
        F.col("source_updated_at_utc").alias("raw_source_updated_at_utc"),
    ).withColumn(
        "calculated_record_hash",
        canonical_hash_expression(
            DATASET_SPECS["DAILY_PRICES"]["hash_fields"]
        ),
    )


def typed_action_candidates(bronze_source: DataFrame) -> DataFrame:
    """Cast source-aligned corporate-action strings to contract types."""
    return bronze_source.select(
        null_if_blank("instrument_id").alias("instrument_id"),
        F.to_date(F.col("effective_date"), "yyyy-MM-dd").alias(
            "effective_date"
        ),
        F.upper(null_if_blank("action_type")).alias("action_type"),
        null_if_blank("source_id").alias("source_id"),
        null_if_blank("source_symbol").alias("source_symbol"),
        null_if_blank("dividend_amount_per_share")
        .cast("decimal(20,8)")
        .alias("dividend_amount_per_share"),
        null_if_blank("dividend_currency").alias("dividend_currency"),
        null_if_blank("split_ratio")
        .cast("decimal(20,10)")
        .alias("split_ratio"),
        null_if_blank("source_action_id").alias("source_action_id"),
        F.to_timestamp(null_if_blank("source_updated_at_utc")).alias(
            "source_updated_at_utc"
        ),
        null_if_blank("source_record_id").alias("source_record_id"),
        F.col("batch_id"),
        F.col("ingested_at_utc"),
        F.col("contract_version"),
        null_if_blank("record_hash").alias("record_hash"),
        F.col("source_row_number"),
        F.col("source_record_sha256"),
        audit_source_record_id().alias("audit_source_record_id"),
        F.col("effective_date").alias("raw_effective_date"),
        F.col("dividend_amount_per_share").alias("raw_dividend_amount"),
        F.col("split_ratio").alias("raw_split_ratio"),
        F.col("source_updated_at_utc").alias("raw_source_updated_at_utc"),
    ).withColumn(
        "calculated_record_hash",
        canonical_hash_expression(
            DATASET_SPECS["CORPORATE_ACTIONS"]["hash_fields"]
        ),
    )


def reference_enriched_candidates(
    candidates: DataFrame,
    *,
    date_column: str,
) -> DataFrame:
    """Attach reviewed instrument and trading-session evidence."""
    instruments = (
        spark.table(SILVER_INSTRUMENT_TABLE)
        .where(F.col("is_active"))
        .select(
            F.col("instrument_id").alias("reference_instrument_id"),
            F.col("yfinance_symbol").alias("reference_source_symbol"),
            F.col("quote_currency").alias("reference_quote_currency"),
            F.col("exchange_mic").alias("reference_exchange_mic"),
        )
    )
    calendar = spark.table(SILVER_CALENDAR_TABLE).select(
        F.col("exchange_mic").alias("calendar_exchange_mic"),
        F.col("calendar_date").alias("reference_calendar_date"),
        F.col("is_trading_day").alias("reference_is_trading_day"),
    )
    with_instrument = candidates.alias("candidate").join(
        instruments.alias("instrument"),
        F.col("candidate.instrument_id")
        == F.col("instrument.reference_instrument_id"),
        "left",
    )
    return (
        with_instrument.alias("mapped")
        .join(
            calendar.alias("calendar"),
            (
                F.col("mapped.reference_exchange_mic")
                == F.col("calendar.calendar_exchange_mic")
            )
            & (
                F.col(f"mapped.{date_column}")
                == F.col("calendar.reference_calendar_date")
            ),
            "left",
        )
        .select(
            "mapped.*",
            "calendar.reference_calendar_date",
            "calendar.reference_is_trading_day",
        )
    )


def explode_violations(frame: DataFrame) -> DataFrame:
    """Explode non-null violation values with immutable Bronze lineage."""
    return (
        frame.select(
            F.col("audit_source_record_id").alias("source_record_id"),
            "batch_id",
            "source_row_number",
            "source_record_sha256",
            F.explode(
                F.filter(
                    F.col("violations"),
                    lambda violation: violation.isNotNull(),
                )
            ).alias("violation"),
        )
        .select(
            "source_record_id",
            "batch_id",
            "source_row_number",
            "source_record_sha256",
            "violation.*",
        )
    )


def daily_price_violations(candidates: DataFrame) -> DataFrame:
    """Evaluate daily-price record and reference-integrity rules."""
    enriched = reference_enriched_candidates(
        candidates,
        date_column="price_date",
    )
    required_missing = (
        F.col("instrument_id").isNull()
        | F.col("price_date").isNull()
        | F.col("source_id").isNull()
        | F.col("source_symbol").isNull()
        | F.col("open_price").isNull()
        | F.col("high_price").isNull()
        | F.col("low_price").isNull()
        | F.col("close_price").isNull()
        | F.col("adjusted_close_price").isNull()
        | F.col("quote_currency").isNull()
        | F.col("source_record_id").isNull()
        | F.col("record_hash").isNull()
    )
    type_failure = (
        (null_if_blank("raw_price_date").isNotNull() & F.col("price_date").isNull())
        | (null_if_blank("raw_open_price").isNotNull() & F.col("open_price").isNull())
        | (null_if_blank("raw_high_price").isNotNull() & F.col("high_price").isNull())
        | (null_if_blank("raw_low_price").isNotNull() & F.col("low_price").isNull())
        | (null_if_blank("raw_close_price").isNotNull() & F.col("close_price").isNull())
        | (
            null_if_blank("raw_adjusted_close_price").isNotNull()
            & F.col("adjusted_close_price").isNull()
        )
        | (null_if_blank("raw_volume").isNotNull() & F.col("volume").isNull())
        | (
            null_if_blank("raw_source_updated_at_utc").isNotNull()
            & F.col("source_updated_at_utc").isNull()
        )
    )
    nonpositive_price = (
        (F.col("open_price") <= 0)
        | (F.col("high_price") <= 0)
        | (F.col("low_price") <= 0)
        | (F.col("close_price") <= 0)
        | (F.col("adjusted_close_price") <= 0)
    )
    invalid_ohlc = (
        (F.col("high_price") < F.col("open_price"))
        | (F.col("high_price") < F.col("low_price"))
        | (F.col("high_price") < F.col("close_price"))
        | (F.col("low_price") > F.col("open_price"))
        | (F.col("low_price") > F.col("high_price"))
        | (F.col("low_price") > F.col("close_price"))
    )
    return explode_violations(
        enriched.withColumn(
            "violations",
            F.array(
                conditional_violation(
                    required_missing,
                    violation_struct(
                        rule_id="PRICE_REQUIRED_FIELDS",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field=None,
                        observed_value=F.lit(None),
                        expected_condition="All required price fields are present.",
                        message="Required price data is missing.",
                    ),
                ),
                conditional_violation(
                    type_failure,
                    violation_struct(
                        rule_id="PRICE_TYPES_CASTABLE",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field=None,
                        observed_value=F.lit(None),
                        expected_condition="Typed price fields parse exactly.",
                        message="A supplied price value cannot be typed.",
                    ),
                ),
                conditional_violation(
                    ~F.col("source_id").isin(
                        "YAHOO_FINANCE", "PROJECT_GIT_FIXTURE"
                    ),
                    violation_struct(
                        rule_id="PRICE_SOURCE_PROVENANCE_VALID",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field="source_id",
                        observed_value=F.col("source_id"),
                        expected_condition="Source is an approved price origin.",
                        message="Price provenance is not approved.",
                    ),
                ),
                conditional_violation(
                    F.col("reference_instrument_id").isNull()
                    | (
                        F.col("source_symbol")
                        != F.col("reference_source_symbol")
                    ),
                    violation_struct(
                        rule_id="PRICE_INSTRUMENT_MAPPING",
                        severity="ERROR",
                        disposition="QUARANTINE",
                        affected_field="instrument_id",
                        observed_value=F.col("instrument_id"),
                        expected_condition="Instrument and source symbol map.",
                        message="Price instrument mapping is not trusted.",
                    ),
                ),
                conditional_violation(
                    nonpositive_price,
                    violation_struct(
                        rule_id="PRICE_POSITIVE_VALUES",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field=None,
                        observed_value=F.col("close_price"),
                        expected_condition="All prices are greater than zero.",
                        message="At least one price is nonpositive.",
                    ),
                ),
                conditional_violation(
                    invalid_ohlc,
                    violation_struct(
                        rule_id="PRICE_OHLC_CONSISTENCY",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field=None,
                        observed_value=F.col("close_price"),
                        expected_condition="High and low contain open and close.",
                        message="OHLC values are internally inconsistent.",
                    ),
                ),
                conditional_violation(
                    F.col("volume").isNull() | (F.col("volume") == 0),
                    violation_struct(
                        rule_id="PRICE_VOLUME_MISSING_OR_ZERO",
                        severity="WARNING",
                        disposition="WARN_AND_ACCEPT",
                        affected_field="volume",
                        observed_value=F.col("volume"),
                        expected_condition="Volume is positive when available.",
                        message="Price is usable but volume needs review.",
                    ),
                ),
                conditional_violation(
                    F.col("volume") < 0,
                    violation_struct(
                        rule_id="PRICE_VOLUME_NEGATIVE",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field="volume",
                        observed_value=F.col("volume"),
                        expected_condition="Volume is nonnegative.",
                        message="Negative volume is invalid.",
                    ),
                ),
                conditional_violation(
                    F.col("reference_calendar_date").isNull()
                    | ~F.coalesce(
                        F.col("reference_is_trading_day"), F.lit(False)
                    ),
                    violation_struct(
                        rule_id="PRICE_VALID_SESSION",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field="price_date",
                        observed_value=F.col("price_date"),
                        expected_condition="Price date is a verified session.",
                        message="Price date is not a trading session.",
                    ),
                ),
                conditional_violation(
                    (F.col("quote_currency") != "USD")
                    | (
                        F.col("quote_currency")
                        != F.col("reference_quote_currency")
                    ),
                    violation_struct(
                        rule_id="PRICE_SUPPORTED_CURRENCY",
                        severity="ERROR",
                        disposition="QUARANTINE",
                        affected_field="quote_currency",
                        observed_value=F.col("quote_currency"),
                        expected_condition="Price currency is mapped USD.",
                        message="Price currency cannot be trusted.",
                    ),
                ),
                conditional_violation(
                    F.col("contract_version") != "1.1.0",
                    violation_struct(
                        rule_id="PRICE_CONTRACT_VERSION_VALID",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field="contract_version",
                        observed_value=F.col("contract_version"),
                        expected_condition="Daily-price contract is 1.1.0.",
                        message="Unexpected daily-price contract version.",
                    ),
                ),
                conditional_violation(
                    F.col("record_hash") != F.col("calculated_record_hash"),
                    violation_struct(
                        rule_id="PRICE_RECORD_HASH_VALID",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field="record_hash",
                        observed_value=F.col("record_hash"),
                        expected_condition="Hash matches canonical values.",
                        message="Price record hash does not reconcile.",
                    ),
                ),
            ),
        )
    )


def action_violations(candidates: DataFrame) -> DataFrame:
    """Evaluate corporate-action record and reference-integrity rules."""
    enriched = reference_enriched_candidates(
        candidates,
        date_column="effective_date",
    )
    required_missing = (
        F.col("instrument_id").isNull()
        | F.col("effective_date").isNull()
        | F.col("action_type").isNull()
        | F.col("source_id").isNull()
        | F.col("source_symbol").isNull()
        | F.col("source_record_id").isNull()
        | F.col("record_hash").isNull()
    )
    type_failure = (
        (
            null_if_blank("raw_effective_date").isNotNull()
            & F.col("effective_date").isNull()
        )
        | (
            null_if_blank("raw_dividend_amount").isNotNull()
            & F.col("dividend_amount_per_share").isNull()
        )
        | (
            null_if_blank("raw_split_ratio").isNotNull()
            & F.col("split_ratio").isNull()
        )
        | (
            null_if_blank("raw_source_updated_at_utc").isNotNull()
            & F.col("source_updated_at_utc").isNull()
        )
    )
    invalid_dividend = (F.col("action_type") == "CASH_DIVIDEND") & (
        F.col("dividend_amount_per_share").isNull()
        | (F.col("dividend_amount_per_share") <= 0)
        | (F.col("dividend_currency") != "USD")
        | F.col("split_ratio").isNotNull()
    )
    invalid_split = (F.col("action_type") == "STOCK_SPLIT") & (
        F.col("split_ratio").isNull()
        | (F.col("split_ratio") <= 0)
        | (F.col("split_ratio") == 1)
        | F.col("dividend_amount_per_share").isNotNull()
        | F.col("dividend_currency").isNotNull()
    )
    return explode_violations(
        enriched.withColumn(
            "violations",
            F.array(
                conditional_violation(
                    required_missing,
                    violation_struct(
                        rule_id="ACTION_REQUIRED_FIELDS",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field=None,
                        observed_value=F.lit(None),
                        expected_condition="Required action fields are present.",
                        message="Required corporate-action data is missing.",
                    ),
                ),
                conditional_violation(
                    type_failure,
                    violation_struct(
                        rule_id="ACTION_TYPES_CASTABLE",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field=None,
                        observed_value=F.lit(None),
                        expected_condition="Typed action fields parse exactly.",
                        message="A supplied action value cannot be typed.",
                    ),
                ),
                conditional_violation(
                    ~F.col("source_id").isin(
                        "YAHOO_FINANCE", "PROJECT_GIT_FIXTURE"
                    ),
                    violation_struct(
                        rule_id="ACTION_SOURCE_PROVENANCE_VALID",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field="source_id",
                        observed_value=F.col("source_id"),
                        expected_condition="Source is an approved action origin.",
                        message="Corporate-action provenance is not approved.",
                    ),
                ),
                conditional_violation(
                    F.col("reference_instrument_id").isNull()
                    | (
                        F.col("source_symbol")
                        != F.col("reference_source_symbol")
                    ),
                    violation_struct(
                        rule_id="ACTION_INSTRUMENT_MAPPING",
                        severity="ERROR",
                        disposition="QUARANTINE",
                        affected_field="instrument_id",
                        observed_value=F.col("instrument_id"),
                        expected_condition="Instrument and source symbol map.",
                        message="Action instrument mapping is not trusted.",
                    ),
                ),
                conditional_violation(
                    ~F.col("action_type").isin(
                        "CASH_DIVIDEND", "STOCK_SPLIT"
                    ),
                    violation_struct(
                        rule_id="ACTION_TYPE_ALLOWED",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field="action_type",
                        observed_value=F.col("action_type"),
                        expected_condition="Action type is approved.",
                        message="Corporate-action type is unsupported.",
                    ),
                ),
                conditional_violation(
                    invalid_dividend,
                    violation_struct(
                        rule_id="ACTION_DIVIDEND_FIELDS",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field=None,
                        observed_value=F.col("dividend_amount_per_share"),
                        expected_condition="Dividend fields are positive USD.",
                        message="Cash-dividend fields are inconsistent.",
                    ),
                ),
                conditional_violation(
                    invalid_split,
                    violation_struct(
                        rule_id="ACTION_SPLIT_FIELDS",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field=None,
                        observed_value=F.col("split_ratio"),
                        expected_condition="Split ratio is positive and not one.",
                        message="Stock-split fields are inconsistent.",
                    ),
                ),
                conditional_violation(
                    F.col("reference_calendar_date").isNull()
                    | ~F.coalesce(
                        F.col("reference_is_trading_day"), F.lit(False)
                    ),
                    violation_struct(
                        rule_id="ACTION_EFFECTIVE_SESSION",
                        severity="ERROR",
                        disposition="QUARANTINE",
                        affected_field="effective_date",
                        observed_value=F.col("effective_date"),
                        expected_condition="Action date is a verified session.",
                        message="Action date lacks verified session evidence.",
                    ),
                ),
                conditional_violation(
                    F.col("contract_version") != "1.1.0",
                    violation_struct(
                        rule_id="ACTION_CONTRACT_VERSION_VALID",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field="contract_version",
                        observed_value=F.col("contract_version"),
                        expected_condition="Action contract is 1.1.0.",
                        message="Unexpected action contract version.",
                    ),
                ),
                conditional_violation(
                    F.col("record_hash") != F.col("calculated_record_hash"),
                    violation_struct(
                        rule_id="ACTION_RECORD_HASH_VALID",
                        severity="ERROR",
                        disposition="REJECT",
                        affected_field="record_hash",
                        observed_value=F.col("record_hash"),
                        expected_condition="Hash matches canonical values.",
                        message="Action record hash does not reconcile.",
                    ),
                ),
            ),
        )
    )


def add_same_batch_evidence(
    candidates: DataFrame,
    key_columns: list[str],
) -> DataFrame:
    """Add deterministic duplicate and conflict evidence."""
    key_window = Window.partitionBy(*key_columns)
    hash_window = Window.partitionBy(*key_columns, "record_hash")
    winner_window = hash_window.orderBy(F.col("source_row_number").asc())
    return (
        candidates.withColumn("business_key", business_key_expression(key_columns))
        .withColumn("same_key_count", F.count("*").over(key_window))
        .withColumn("minimum_key_hash", F.min("record_hash").over(key_window))
        .withColumn("maximum_key_hash", F.max("record_hash").over(key_window))
        .withColumn("same_key_hash_count", F.count("*").over(hash_window))
        .withColumn("duplicate_rank", F.row_number().over(winner_window))
        .withColumn(
            "deduplicated_to_source_record_id",
            F.first("audit_source_record_id").over(winner_window),
        )
        .withColumn(
            "has_same_batch_conflict",
            (F.col("same_key_count") > 1)
            & (F.col("minimum_key_hash") != F.col("maximum_key_hash")),
        )
        .withColumn(
            "is_identical_duplicate_nonwinner",
            (F.col("same_key_hash_count") > 1)
            & (F.col("duplicate_rank") > 1),
        )
    )


def same_batch_violations(
    candidates: DataFrame,
    dataset_name: str,
) -> DataFrame:
    """Create conflict and identical-duplicate evidence."""
    prefix = "PRICE" if dataset_name == "DAILY_PRICES" else "ACTION"
    return explode_violations(
        candidates.withColumn(
            "violations",
            F.array(
                conditional_violation(
                    F.col("has_same_batch_conflict"),
                    violation_struct(
                        rule_id=f"{prefix}_SAME_BATCH_CONFLICT",
                        severity="ERROR",
                        disposition="QUARANTINE",
                        affected_field=None,
                        observed_value=F.col("business_key"),
                        expected_condition="One hash exists per batch key.",
                        message="Conflicting same-batch records are quarantined.",
                    ),
                ),
                conditional_violation(
                    F.col("is_identical_duplicate_nonwinner"),
                    violation_struct(
                        rule_id=f"{prefix}_SAME_BATCH_IDENTICAL_DUPLICATE",
                        severity="WARNING",
                        disposition="WARN_AND_DEDUPLICATE",
                        affected_field=None,
                        observed_value=F.col("business_key"),
                        expected_condition="One winner exists per exact duplicate.",
                        message="Identical nonwinning source row was deduplicated.",
                    ),
                ),
            ),
        )
    )


def add_current_comparison(
    candidates: DataFrame,
    specification: dict[str, Any],
) -> DataFrame:
    """Compare each candidate with its current canonical key."""
    key_columns = specification["key_columns"]
    target_hashes = spark.table(specification["silver_table"]).select(
        *key_columns,
        F.col("record_hash").alias("current_record_hash"),
    )
    return candidates.join(target_hashes, key_columns, "left")


def final_outcomes(
    *,
    candidates: DataFrame,
    violations: DataFrame,
) -> DataFrame:
    """Assign exactly one outcome using the contract precedence."""
    summaries = violations.groupBy("source_record_id").agg(
        F.count("*").cast("bigint").alias("violation_count"),
        F.sum(F.when(F.col("severity") == "WARNING", 1).otherwise(0))
        .cast("bigint")
        .alias("warning_count"),
        F.max(F.when(F.col("disposition") == "REJECT", 1).otherwise(0)).alias(
            "has_reject"
        ),
        F.max(
            F.when(F.col("disposition") == "QUARANTINE", 1).otherwise(0)
        ).alias("has_quarantine"),
    )
    evaluated = candidates.join(
        summaries,
        candidates.audit_source_record_id == summaries.source_record_id,
        "left",
    ).drop(summaries.source_record_id)
    evaluated = (
        evaluated.fillna(
            {
                "violation_count": 0,
                "warning_count": 0,
                "has_reject": 0,
                "has_quarantine": 0,
            }
        )
        .withColumn(
            "outcome",
            F.when(F.col("has_reject") == 1, F.lit("REJECTED"))
            .when(F.col("has_quarantine") == 1, F.lit("QUARANTINED"))
            .when(
                F.col("is_identical_duplicate_nonwinner"),
                F.lit("DEDUPLICATED"),
            )
            .when(F.col("current_record_hash").isNull(), F.lit("ACCEPTED_NEW"))
            .when(
                F.col("record_hash") == F.col("current_record_hash"),
                F.lit("UNCHANGED"),
            )
            .otherwise(F.lit("ACCEPTED_CORRECTION")),
        )
        .withColumn(
            "canonical_record_hash",
            F.when(
                F.col("outcome").isin(
                    "ACCEPTED_NEW",
                    "ACCEPTED_CORRECTION",
                    "UNCHANGED",
                ),
                F.col("record_hash"),
            ),
        )
        .withColumn(
            "deduplicated_to_source_record_id",
            F.when(
                F.col("outcome") == "DEDUPLICATED",
                F.col("deduplicated_to_source_record_id"),
            ),
        )
    )
    return evaluated


def dataset_failures(
    snapshot: DataFrame,
    *,
    dataset_name: str,
    specification: dict[str, Any],
) -> list[str]:
    """Evaluate high-value canonical snapshot rules."""
    failures: list[str] = []
    duplicate_count = (
        snapshot.groupBy(*specification["key_columns"])
        .count()
        .where(F.col("count") != 1)
        .count()
    )
    if duplicate_count:
        failures.append(f"{dataset_name}_BUSINESS_KEY_UNIQUE")

    fixture_snapshot = snapshot.where(
        F.col("source_id") == "PROJECT_GIT_FIXTURE"
    )
    if dataset_name == "DAILY_PRICES":
        if fixture_snapshot.count() != 60:
            failures.append("PRICE_EXPECTED_COVERAGE")
        date_counts = {
            row["price_date"]: row["count"]
            for row in fixture_snapshot.groupBy("price_date").count().collect()
        }
        if date_counts != {price_date: 15 for price_date in PRICE_DATES}:
            failures.append("PRICE_EXPECTED_DATE_GRID")
        instrument_count = fixture_snapshot.select("instrument_id").distinct().count()
        if instrument_count != 15:
            failures.append("PRICE_EXPECTED_INSTRUMENT_COVERAGE")
    else:
        if fixture_snapshot.count() != 2:
            failures.append("ACTION_EXPECTED_FIXTURE_COUNT")
        action_types = {
            row["action_type"]
            for row in fixture_snapshot.select("action_type").distinct().collect()
        }
        if action_types != {"CASH_DIVIDEND", "STOCK_SPLIT"}:
            failures.append("ACTION_EXPECTED_TYPE_COVERAGE")
    return sorted(set(failures))


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils = DBUtils(spark)

dbutils.widgets.text("source_batch_id", "", "Bronze source batch ID")
dbutils.widgets.text("code_version", "", "Git code version")
dbutils.widgets.text(
    "trigger_type", "MANUAL", "Silver processing trigger type"
)

source_batch_id = require_uuid(
    "source_batch_id",
    dbutils.widgets.get("source_batch_id").strip(),
)
code_version = require_code_version(
    dbutils.widgets.get("code_version").strip()
)
trigger_type = dbutils.widgets.get("trigger_type").strip().upper()
if trigger_type not in {"MANUAL", "SCHEDULED", "RECOVERY"}:
    raise ValueError("trigger_type must be MANUAL, SCHEDULED, or RECOVERY")

processing_run_id = str(uuid4())
processing_run_started_at_utc = datetime.now(UTC).replace(tzinfo=None)

batch_rows = (
    spark.table(BRONZE_BATCH_TABLE)
    .where(
        (F.col("batch_id") == source_batch_id)
        & F.col("dataset_name").isin(*DATASET_SPECS)
        & F.col("status").isin("SUCCEEDED", "SUCCEEDED_WITH_WARNINGS")
    )
    .select("dataset_name")
    .collect()
)
require_equal("eligible successful Bronze batch count", len(batch_rows), 1)
dataset_name = batch_rows[0]["dataset_name"]
specification = DATASET_SPECS[dataset_name]

previous_runs = (
    spark.table(SILVER_PROCESSING_RUN_TABLE)
    .where(F.col("source_batch_id") == source_batch_id)
    .orderBy(F.col("attempt_number").desc())
    .select("processing_run_id", "attempt_number")
    .limit(1)
    .collect()
)
if previous_runs:
    attempt_number = previous_runs[0]["attempt_number"] + 1
    reprocess_of_processing_run_id = previous_runs[0]["processing_run_id"]
else:
    attempt_number = 1
    reprocess_of_processing_run_id = None

bronze_source = spark.table(specification["bronze_table"]).where(
    F.col("batch_id") == source_batch_id
)
evaluated_count = bronze_source.count()
require_equal(
    "successful Bronze source row count",
    evaluated_count,
    specification["expected_source_count"],
)
input_record_set_sha256 = calculate_ordered_set_sha256(
    bronze_source,
    order_columns=["source_row_number"],
    value_columns=["source_row_number", "source_record_sha256"],
)

print(
    "processing_parameters=PASS "
    f"processing_run_id={processing_run_id} "
    f"source_batch_id={source_batch_id} "
    f"dataset_name={dataset_name} "
    f"attempt_number={attempt_number} "
    f"reprocess_of_processing_run_id={reprocess_of_processing_run_id} "
    f"evaluated_count={evaluated_count} "
    f"input_record_set_sha256={input_record_set_sha256}"
)


# COMMAND ----------

if dataset_name == "DAILY_PRICES":
    typed_candidates = typed_daily_price_candidates(bronze_source)
    record_rule_violations = daily_price_violations(typed_candidates)
else:
    typed_candidates = typed_action_candidates(bronze_source)
    record_rule_violations = action_violations(typed_candidates)

ranked_candidates = add_same_batch_evidence(
    typed_candidates,
    specification["key_columns"],
)
duplicate_rule_violations = same_batch_violations(
    ranked_candidates,
    dataset_name,
)
complete_rule_violations = freeze_small_dataframe(
    record_rule_violations.unionByName(duplicate_rule_violations),
    label="market record violations",
    max_rows=specification["expected_source_count"] * 20,
)
frozen_violation_count = complete_rule_violations.count()
compared_candidates = add_current_comparison(
    ranked_candidates,
    specification,
)
outcomes = freeze_small_dataframe(
    final_outcomes(
        candidates=compared_candidates,
        violations=complete_rule_violations,
    ),
    label="market final outcomes",
    max_rows=specification["expected_source_count"],
)
frozen_outcome_count = outcomes.count()
require_equal("frozen final outcome count", frozen_outcome_count, evaluated_count)

accepted_count = outcomes.where(
    F.col("outcome").isin("ACCEPTED_NEW", "ACCEPTED_CORRECTION")
).count()
quarantined_count = outcomes.where(
    F.col("outcome") == "QUARANTINED"
).count()
rejected_count = outcomes.where(F.col("outcome") == "REJECTED").count()
unchanged_count = outcomes.where(F.col("outcome") == "UNCHANGED").count()
deduplicated_count = outcomes.where(
    F.col("outcome") == "DEDUPLICATED"
).count()
warning_count = complete_rule_violations.where(
    F.col("severity") == "WARNING"
).count()
require_equal(
    "processing outcome reconciliation",
    accepted_count
    + quarantined_count
    + rejected_count
    + unchanged_count
    + deduplicated_count,
    evaluated_count,
)

print(
    "record_validation=PASS "
    f"accepted_count={accepted_count} "
    f"quarantined_count={quarantined_count} "
    f"rejected_count={rejected_count} "
    f"unchanged_count={unchanged_count} "
    f"deduplicated_count={deduplicated_count} "
    f"warning_count={warning_count}"
)
outcomes.select(
    F.col("audit_source_record_id").alias("source_record_id"),
    "business_key",
    "outcome",
    "warning_count",
    "violation_count",
).orderBy("source_row_number").show(60, truncate=False)


# COMMAND ----------

output_columns = specification["output_columns"]
key_columns = specification["key_columns"]
current_snapshot = spark.table(specification["silver_table"]).select(
    *output_columns
)
accepted_candidates = outcomes.where(
    F.col("outcome").isin("ACCEPTED_NEW", "ACCEPTED_CORRECTION")
).select(*output_columns)
accepted_keys = accepted_candidates.select(*key_columns)
prospective_snapshot = current_snapshot.join(
    accepted_keys,
    key_columns,
    "left_anti",
).unionByName(accepted_candidates)

canonical_before_count = current_snapshot.count()
canonical_before_sha256 = snapshot_sha256(
    current_snapshot,
    key_columns=key_columns,
)
prospective_count = prospective_snapshot.count()
prospective_sha256 = snapshot_sha256(
    prospective_snapshot,
    key_columns=key_columns,
)
failed_rule_ids = dataset_failures(
    prospective_snapshot,
    dataset_name=dataset_name,
    specification=specification,
)
publish_allowed = (
    rejected_count == 0
    and quarantined_count == 0
    and not failed_rule_ids
)
published = publish_allowed and accepted_count > 0
if publish_allowed:
    canonical_after_count = prospective_count
    canonical_after_sha256 = prospective_sha256
else:
    canonical_after_count = canonical_before_count
    canonical_after_sha256 = canonical_before_sha256

if not publish_allowed:
    run_status = "FAILED"
elif warning_count > 0:
    run_status = "SUCCEEDED_WITH_WARNINGS"
else:
    run_status = "SUCCEEDED"

print(
    "publication_gate=PASS "
    f"canonical_before_count={canonical_before_count} "
    f"prospective_count={prospective_count} "
    f"failed_rule_ids={failed_rule_ids} "
    f"publish_allowed={publish_allowed} "
    f"published={published} "
    f"canonical_after_count={canonical_after_count} "
    f"run_status={run_status} "
    f"canonical_before_sha256={canonical_before_sha256} "
    f"canonical_after_sha256={canonical_after_sha256}"
)


# COMMAND ----------

evaluated_at_utc = datetime.now(UTC).replace(tzinfo=None)
outcome_audit_records = outcomes.select(
    F.sha2(
        F.concat_ws(
            "|",
            F.lit(processing_run_id),
            F.col("audit_source_record_id"),
        ),
        256,
    ).alias("outcome_id"),
    F.lit(processing_run_id).alias("processing_run_id"),
    F.col("batch_id"),
    F.lit(dataset_name).alias("dataset_name"),
    F.col("audit_source_record_id").alias("source_record_id"),
    F.col("source_row_number"),
    F.col("source_record_sha256"),
    F.col("business_key"),
    F.col("outcome"),
    F.col("warning_count").cast("bigint"),
    F.col("violation_count").cast("bigint"),
    F.col("canonical_record_hash"),
    F.col("deduplicated_to_source_record_id"),
    F.lit(evaluated_at_utc).cast("timestamp").alias("evaluated_at_utc"),
    F.lit(specification["contract_version"]).alias(
        "dataset_contract_version"
    ),
    F.lit(OUTCOME_CONTRACT_VERSION).alias("contract_version"),
).select(*spark.table(SILVER_OUTCOME_TABLE).schema.fieldNames())

detected_at_utc = datetime.now(UTC).replace(tzinfo=None)
violation_audit_records = complete_rule_violations.select(
    F.sha2(
        F.concat_ws(
            "|",
            F.lit(processing_run_id),
            F.col("source_record_id"),
            F.col("rule_id"),
        ),
        256,
    ).alias("violation_id"),
    F.lit(processing_run_id).alias("processing_run_id"),
    F.col("batch_id"),
    F.lit(dataset_name).alias("dataset_name"),
    F.col("source_record_id"),
    F.col("source_row_number"),
    F.col("source_record_sha256"),
    F.col("rule_id"),
    F.lit(specification["contract_version"]).alias("rule_version"),
    F.col("severity"),
    F.col("disposition"),
    F.col("affected_field"),
    F.col("observed_value"),
    F.col("expected_condition"),
    F.col("message"),
    F.lit(detected_at_utc).cast("timestamp").alias("detected_at_utc"),
    F.lit("OPEN").alias("resolution_status"),
    F.lit(None).cast("string").alias("resolution_action"),
    F.lit(None).cast("string").alias("resolution_batch_id"),
    F.lit(None).cast("timestamp").alias("resolved_at_utc"),
    F.lit(None).cast("string").alias("resolution_note"),
    F.lit(VIOLATION_CONTRACT_VERSION).alias("contract_version"),
).select(*spark.table(SILVER_VIOLATION_TABLE).schema.fieldNames())

completed_at_utc = datetime.now(UTC).replace(tzinfo=None)
published_at_utc = completed_at_utc if published else None
error_code = None if publish_allowed else "VALIDATION_FAILED"
error_message = (
    None
    if publish_allowed
    else "Record or dataset validation prevented Silver publication."
)
processing_run_values = {
    "processing_run_id": processing_run_id,
    "source_batch_id": source_batch_id,
    "dataset_name": dataset_name,
    "attempt_number": attempt_number,
    "reprocess_of_processing_run_id": reprocess_of_processing_run_id,
    "trigger_type": trigger_type,
    "started_at_utc": processing_run_started_at_utc,
    "completed_at_utc": completed_at_utc,
    "status": run_status,
    "evaluated_count": evaluated_count,
    "accepted_count": accepted_count,
    "quarantined_count": quarantined_count,
    "rejected_count": rejected_count,
    "unchanged_count": unchanged_count,
    "deduplicated_count": deduplicated_count,
    "warning_count": warning_count,
    "canonical_before_count": canonical_before_count,
    "canonical_after_count": canonical_after_count,
    "input_record_set_sha256": input_record_set_sha256,
    "canonical_before_sha256": canonical_before_sha256,
    "canonical_after_sha256": canonical_after_sha256,
    "published": published,
    "published_at_utc": published_at_utc,
    "failed_rule_ids": failed_rule_ids,
    "error_code": error_code,
    "error_message": error_message,
    "dataset_contract_version": specification["contract_version"],
    "code_version": code_version,
    "contract_version": PROCESSING_RUN_CONTRACT_VERSION,
}
processing_run_audit = spark.createDataFrame(
    [processing_run_values],
    schema=spark.table(SILVER_PROCESSING_RUN_TABLE).schema,
)


# COMMAND ----------

if published:
    target = DeltaTable.forName(spark, specification["silver_table"])
    merge_condition = " AND ".join(
        f"target.{column} = source.{column}" for column in key_columns
    )
    (
        target.alias("target")
        .merge(accepted_candidates.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

if violation_audit_records.count() > 0:
    insert_only_audit_records(
        violation_audit_records,
        SILVER_VIOLATION_TABLE,
        "violation_id",
    )
insert_only_audit_records(
    outcome_audit_records,
    SILVER_OUTCOME_TABLE,
    "outcome_id",
)
insert_only_audit_records(
    processing_run_audit,
    SILVER_PROCESSING_RUN_TABLE,
    "processing_run_id",
)

persisted_snapshot = spark.table(specification["silver_table"]).select(
    *output_columns
)
require_equal(
    "persisted canonical count",
    persisted_snapshot.count(),
    canonical_after_count,
)
require_equal(
    "persisted canonical SHA-256",
    snapshot_sha256(persisted_snapshot, key_columns=key_columns),
    canonical_after_sha256,
)
require_equal(
    "persisted processing-run audit count",
    spark.table(SILVER_PROCESSING_RUN_TABLE)
    .where(F.col("processing_run_id") == processing_run_id)
    .count(),
    1,
)
require_equal(
    "persisted source-outcome count",
    spark.table(SILVER_OUTCOME_TABLE)
    .where(F.col("processing_run_id") == processing_run_id)
    .count(),
    evaluated_count,
)
require_equal(
    "persisted violation count",
    spark.table(SILVER_VIOLATION_TABLE)
    .where(F.col("processing_run_id") == processing_run_id)
    .count(),
    frozen_violation_count,
)

print(
    "silver_persistence=PASS "
    f"processing_run_id={processing_run_id} "
    f"dataset_name={dataset_name} "
    f"run_status={run_status} "
    f"published={published} "
    f"persisted_canonical_count={persisted_snapshot.count()} "
    f"persisted_outcome_count={evaluated_count} "
    f"persisted_violation_count={frozen_violation_count}"
)
