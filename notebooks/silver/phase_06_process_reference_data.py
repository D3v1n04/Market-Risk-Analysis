# Databricks notebook source
"""Validate and canonicalize one Phase 06 Bronze reference batch."""

import re
from datetime import UTC, datetime
from decimal import Decimal
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
    "workspace.devin_market_risk_dev."
    "silver_source_record_outcomes"
)
SILVER_VIOLATION_TABLE = (
    "workspace.devin_market_risk_dev."
    "silver_data_quality_violations"
)
SILVER_PORTFOLIO_TABLE = (
    "workspace.devin_market_risk_dev.silver_portfolios"
)

DATASET_SPECS = {
    "INSTRUMENTS": {
        "contract_version": "1.1.0",
        "bronze_table": (
            "workspace.devin_market_risk_dev.bronze_instruments"
        ),
        "silver_table": (
            "workspace.devin_market_risk_dev.silver_instruments"
        ),
        "expected_source_count": 15,
        "key_columns": ["instrument_id"],
        "version_column": "config_version",
        "output_columns": [
            "instrument_id",
            "display_symbol",
            "yfinance_symbol",
            "instrument_name",
            "asset_class",
            "security_type",
            "exchange_mic",
            "quote_currency",
            "issuer_country_code",
            "official_sector_name",
            "risk_cluster_id",
            "classification_source",
            "classification_as_of_date",
            "active_from",
            "active_to",
            "is_active",
            "config_version",
            "record_hash",
        ],
        "hash_fields": [
            "instrument_id",
            "display_symbol",
            "yfinance_symbol",
            "instrument_name",
            "asset_class",
            "security_type",
            "exchange_mic",
            "quote_currency",
            "issuer_country_code",
            "official_sector_name",
            "risk_cluster_id",
            "classification_source",
            "classification_as_of_date",
            "active_from",
            "active_to",
            "is_active",
            "config_version",
        ],
    },
    "TARGET_ALLOCATIONS": {
        "contract_version": "1.1.0",
        "bronze_table": (
            "workspace.devin_market_risk_dev."
            "bronze_target_allocations"
        ),
        "silver_table": (
            "workspace.devin_market_risk_dev."
            "silver_target_allocations"
        ),
        "expected_source_count": 30,
        "key_columns": [
            "portfolio_id",
            "instrument_id",
            "effective_from",
        ],
        "version_column": "allocation_version",
        "output_columns": [
            "portfolio_id",
            "instrument_id",
            "effective_from",
            "effective_to",
            "target_weight",
            "allocation_version",
            "record_hash",
        ],
        "hash_fields": [
            "portfolio_id",
            "instrument_id",
            "effective_from",
            "effective_to",
            "target_weight",
            "allocation_version",
        ],
    },
}

CODE_VERSION_PATTERN = re.compile(r"^[0-9a-f]{7,40}$")


def require_equal(label: str, actual: Any, expected: Any) -> None:
    """Raise a clear error when two values differ."""
    if actual != expected:
        raise ValueError(
            f"{label} mismatch: "
            f"expected {expected!r}, found {actual!r}"
        )


def require_uuid(label: str, value: str) -> str:
    """Require a canonical UUID string."""
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be a valid UUID") from exc

    canonical = str(parsed)
    if value.lower() != canonical:
        raise ValueError(
            f"{label} must use canonical UUID formatting"
        )
    return canonical


def require_code_version(value: str) -> str:
    """Require a lowercase abbreviated or complete Git SHA."""
    if CODE_VERSION_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "code_version must be a lowercase 7-to-40-character Git SHA"
        )
    return value


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
        F.coalesce(
            F.col(column_name).cast("string"),
            F.lit("<NULL>"),
        )
        for column_name in value_columns
    ]
    ordered_rows = frame.select(
        F.struct(
            *ordering_fields,
            F.concat_ws("|", *value_fields).alias("canonical_row"),
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


def insert_only_audit_records(
    frame: DataFrame,
    target_table: str,
    key_column: str,
) -> None:
    """Insert immutable audit rows without updating evidence."""
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
    """Build the contract-defined record-hash expression."""
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


def business_key_expression(key_columns: list[str]) -> Column:
    """Build a business key, leaving malformed keys null."""
    missing_key = None
    for column_name in key_columns:
        missing = F.col(column_name).isNull() | (
            F.trim(F.col(column_name).cast("string")) == ""
        )
        missing_key = missing if missing_key is None else missing_key | missing

    return F.when(
        missing_key,
        F.lit(None).cast("string"),
    ).otherwise(
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
    """Create a uniform record-level violation struct."""
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
    """Return a violation struct only when its rule fails."""
    return F.when(condition, violation)


def typed_instrument_candidates(bronze_source: DataFrame) -> DataFrame:
    """Convert source-aligned instrument values to Silver types."""
    lineage = [
        "batch_id",
        "source_row_number",
        "source_record_sha256",
    ]
    string_fields = [
        "instrument_id",
        "display_symbol",
        "yfinance_symbol",
        "instrument_name",
        "asset_class",
        "security_type",
        "exchange_mic",
        "quote_currency",
        "issuer_country_code",
        "official_sector_name",
        "risk_cluster_id",
        "classification_source",
        "config_version",
        "record_hash",
    ]
    raw_fields = DATASET_SPECS["INSTRUMENTS"]["output_columns"]

    candidates = bronze_source.select(
        *[F.col(name) for name in lineage],
        *[F.col(name).alias(f"raw_{name}") for name in raw_fields],
        *[F.col(name).alias(name) for name in string_fields],
        F.expr("TRY_CAST(classification_as_of_date AS DATE)").alias(
            "classification_as_of_date"
        ),
        F.expr("TRY_CAST(active_from AS DATE)").alias("active_from"),
        F.expr(
            "TRY_CAST(NULLIF(TRIM(active_to), '') AS DATE)"
        ).alias("active_to"),
        F.expr("TRY_CAST(is_active AS BOOLEAN)").alias("is_active"),
    )
    return (
        candidates.withColumn(
            "source_record_id",
            F.concat_ws(
                ":",
                F.col("batch_id"),
                F.col("source_row_number").cast("string"),
            ),
        )
        .withColumn(
            "business_key",
            business_key_expression(["instrument_id"]),
        )
        .withColumn(
            "computed_record_hash",
            canonical_hash_expression(
                DATASET_SPECS["INSTRUMENTS"]["hash_fields"]
            ),
        )
    )


def typed_allocation_candidates(bronze_source: DataFrame) -> DataFrame:
    """Convert source-aligned allocation values to Silver types."""
    raw_fields = DATASET_SPECS["TARGET_ALLOCATIONS"]["output_columns"]
    candidates = bronze_source.select(
        "batch_id",
        "source_row_number",
        "source_record_sha256",
        *[F.col(name).alias(f"raw_{name}") for name in raw_fields],
        F.col("portfolio_id"),
        F.col("instrument_id"),
        F.expr("TRY_CAST(effective_from AS DATE)").alias(
            "effective_from"
        ),
        F.expr(
            "TRY_CAST(NULLIF(TRIM(effective_to), '') AS DATE)"
        ).alias("effective_to"),
        F.expr("TRY_CAST(target_weight AS DECIMAL(12,10))").alias(
            "target_weight"
        ),
        F.col("allocation_version"),
        F.col("record_hash"),
    )
    candidates = (
        candidates.withColumn(
            "source_record_id",
            F.concat_ws(
                ":",
                F.col("batch_id"),
                F.col("source_row_number").cast("string"),
            ),
        )
        .withColumn(
            "business_key",
            business_key_expression(
                DATASET_SPECS["TARGET_ALLOCATIONS"]["key_columns"]
            ),
        )
        .withColumn(
            "computed_record_hash",
            canonical_hash_expression(
                DATASET_SPECS["TARGET_ALLOCATIONS"]["hash_fields"]
            ),
        )
    )

    active_portfolios = (
        spark.table(SILVER_PORTFOLIO_TABLE)
        .where(F.col("is_active") == F.lit(True))
        .select(
            F.col("portfolio_id").alias("valid_portfolio_id")
        )
    )
    active_instruments = (
        spark.table(
            DATASET_SPECS["INSTRUMENTS"]["silver_table"]
        )
        .where(F.col("is_active") == F.lit(True))
        .select(
            F.col("instrument_id").alias("valid_instrument_id")
        )
    )
    return (
        candidates.join(
            active_portfolios,
            candidates.portfolio_id
            == active_portfolios.valid_portfolio_id,
            "left",
        )
        .join(
            active_instruments,
            candidates.instrument_id
            == active_instruments.valid_instrument_id,
            "left",
        )
    )


def instrument_record_violations(candidates: DataFrame) -> DataFrame:
    """Evaluate instrument record rules."""
    required_fields = [
        field
        for field in DATASET_SPECS["INSTRUMENTS"]["output_columns"]
        if field != "active_to"
    ]
    missing_required = None
    for field in required_fields:
        condition = F.col(f"raw_{field}").isNull() | (
            F.trim(F.col(f"raw_{field}")) == ""
        )
        missing_required = (
            condition
            if missing_required is None
            else missing_required | condition
        )

    invalid_type = (
        (
            F.trim(F.col("raw_classification_as_of_date")) != ""
        )
        & F.col("classification_as_of_date").isNull()
    ) | (
        (F.trim(F.col("raw_active_from")) != "")
        & F.col("active_from").isNull()
    ) | (
        (F.trim(F.col("raw_active_to")) != "")
        & F.col("active_to").isNull()
    ) | (
        (F.trim(F.col("raw_is_active")) != "")
        & F.col("is_active").isNull()
    )

    allowed_values = (
        ~F.col("asset_class").isin("EQUITY")
        | ~F.col("security_type").isin("COMMON_STOCK", "ADR")
        | ~F.col("exchange_mic").isin("XNAS", "XNYS")
        | ~F.col("quote_currency").isin("USD")
        | ~F.col("issuer_country_code").isin("US", "TW")
        | ~F.col("official_sector_name").isin(
            "Communication Services",
            "Consumer Discretionary",
            "Consumer Staples",
            "Energy",
            "Financials",
            "Health Care",
            "Industrials",
            "Information Technology",
        )
        | ~F.col("risk_cluster_id").isin(
            "SYSTEMIC_CORE_TECHNOLOGY",
            "SEMICONDUCTOR_INFRASTRUCTURE",
            "CONSUMER_AND_LOGISTICS",
            "HEALTHCARE_AND_POLICY_SENSITIVITY",
            "FINANCIAL_FOUNDATIONS",
            "MACRO_AND_GEOPOLITICAL_SENSITIVITY",
        )
    )
    classification_missing = (
        (F.col("is_active") == F.lit(True))
        & (
            F.col("official_sector_name").isNull()
            | (F.trim(F.col("official_sector_name")) == "")
            | F.col("classification_source").isNull()
            | (F.trim(F.col("classification_source")) == "")
            | F.col("classification_as_of_date").isNull()
        )
    )

    observed_types = F.to_json(
        F.struct(
            "raw_classification_as_of_date",
            "raw_active_from",
            "raw_active_to",
            "raw_is_active",
        )
    )
    observed_allowed = F.to_json(
        F.struct(
            "asset_class",
            "security_type",
            "exchange_mic",
            "quote_currency",
            "issuer_country_code",
            "official_sector_name",
            "risk_cluster_id",
        )
    )
    violations = [
        conditional_violation(
            missing_required,
            violation_struct(
                rule_id="INSTRUMENT_REQUIRED_FIELDS",
                severity="ERROR",
                disposition="REJECT",
                affected_field=None,
                observed_value=F.lit(None),
                expected_condition="Every required instrument field is present.",
                message="One or more required instrument fields are missing.",
            ),
        ),
        conditional_violation(
            invalid_type,
            violation_struct(
                rule_id="INSTRUMENT_TYPES_CASTABLE",
                severity="ERROR",
                disposition="REJECT",
                affected_field=None,
                observed_value=observed_types,
                expected_condition="Dates and booleans cast without loss.",
                message="One or more instrument values cannot be safely typed.",
            ),
        ),
        conditional_violation(
            allowed_values,
            violation_struct(
                rule_id="INSTRUMENT_ALLOWED_VALUES",
                severity="ERROR",
                disposition="REJECT",
                affected_field=None,
                observed_value=observed_allowed,
                expected_condition="Categorical values match the contract.",
                message="One or more instrument categories are not allowed.",
            ),
        ),
        conditional_violation(
            ~F.col("instrument_id").rlike(
                r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$"
            ),
            violation_struct(
                rule_id="INSTRUMENT_ID_FORMAT",
                severity="ERROR",
                disposition="REJECT",
                affected_field="instrument_id",
                observed_value=F.col("instrument_id"),
                expected_condition="instrument_id uses uppercase snake case.",
                message="Instrument identifier format is invalid.",
            ),
        ),
        conditional_violation(
            ~F.col("config_version").rlike(
                r"^[0-9]+\.[0-9]+\.[0-9]+$"
            ),
            violation_struct(
                rule_id="INSTRUMENT_CONFIG_VERSION_VALID",
                severity="ERROR",
                disposition="REJECT",
                affected_field="config_version",
                observed_value=F.col("config_version"),
                expected_condition="config_version is numeric semantic versioning.",
                message="Instrument configuration version is invalid.",
            ),
        ),
        conditional_violation(
            classification_missing,
            violation_struct(
                rule_id="INSTRUMENT_CLASSIFICATION_REQUIRED",
                severity="ERROR",
                disposition="REJECT",
                affected_field=None,
                observed_value=F.lit(None),
                expected_condition="Active classifications are complete.",
                message="An active instrument lacks classification evidence.",
            ),
        ),
        conditional_violation(
            F.col("active_to").isNotNull()
            & (F.col("active_to") < F.col("active_from")),
            violation_struct(
                rule_id="INSTRUMENT_ACTIVE_DATE_RANGE",
                severity="ERROR",
                disposition="REJECT",
                affected_field="active_to",
                observed_value=F.col("raw_active_to"),
                expected_condition="active_to is null or not before active_from.",
                message="Instrument active date range is invalid.",
            ),
        ),
        conditional_violation(
            F.col("record_hash") != F.col("computed_record_hash"),
            violation_struct(
                rule_id="INSTRUMENT_RECORD_HASH_VALID",
                severity="ERROR",
                disposition="REJECT",
                affected_field="record_hash",
                observed_value=F.col("record_hash"),
                expected_condition="record_hash matches canonical SHA-256.",
                message="Instrument record hash does not match its values.",
            ),
        ),
    ]
    return explode_record_violations(candidates, violations)


def allocation_record_violations(candidates: DataFrame) -> DataFrame:
    """Evaluate target-allocation record rules."""
    required_fields = [
        field
        for field in DATASET_SPECS["TARGET_ALLOCATIONS"]["output_columns"]
        if field != "effective_to"
    ]
    missing_required = None
    for field in required_fields:
        condition = F.col(f"raw_{field}").isNull() | (
            F.trim(F.col(f"raw_{field}")) == ""
        )
        missing_required = (
            condition
            if missing_required is None
            else missing_required | condition
        )

    invalid_type = (
        (F.trim(F.col("raw_effective_from")) != "")
        & F.col("effective_from").isNull()
    ) | (
        (F.trim(F.col("raw_effective_to")) != "")
        & F.col("effective_to").isNull()
    ) | (
        (F.trim(F.col("raw_target_weight")) != "")
        & F.col("target_weight").isNull()
    )
    invalid_foreign_key = (
        F.col("valid_portfolio_id").isNull()
        | F.col("valid_instrument_id").isNull()
    )
    observed_types = F.to_json(
        F.struct(
            "raw_effective_from",
            "raw_effective_to",
            "raw_target_weight",
        )
    )
    observed_foreign_keys = F.to_json(
        F.struct("portfolio_id", "instrument_id")
    )
    violations = [
        conditional_violation(
            missing_required,
            violation_struct(
                rule_id="ALLOCATION_REQUIRED_FIELDS",
                severity="ERROR",
                disposition="REJECT",
                affected_field=None,
                observed_value=F.lit(None),
                expected_condition="Every required allocation field is present.",
                message="One or more required allocation fields are missing.",
            ),
        ),
        conditional_violation(
            invalid_type,
            violation_struct(
                rule_id="ALLOCATION_TYPES_CASTABLE",
                severity="ERROR",
                disposition="REJECT",
                affected_field=None,
                observed_value=observed_types,
                expected_condition="Dates and target_weight cast without loss.",
                message="One or more allocation values cannot be safely typed.",
            ),
        ),
        conditional_violation(
            ~F.col("allocation_version").rlike(
                r"^[0-9]+\.[0-9]+\.[0-9]+$"
            ),
            violation_struct(
                rule_id="ALLOCATION_VERSION_VALID",
                severity="ERROR",
                disposition="REJECT",
                affected_field="allocation_version",
                observed_value=F.col("allocation_version"),
                expected_condition="allocation_version is numeric semantic versioning.",
                message="Allocation version is invalid.",
            ),
        ),
        conditional_violation(
            invalid_foreign_key,
            violation_struct(
                rule_id="ALLOCATION_FOREIGN_KEYS_VALID",
                severity="ERROR",
                disposition="REJECT",
                affected_field=None,
                observed_value=observed_foreign_keys,
                expected_condition="Portfolio and instrument are active in Silver.",
                message="Allocation contains an unresolved foreign key.",
            ),
        ),
        conditional_violation(
            F.col("target_weight") == F.lit(Decimal("0")),
            violation_struct(
                rule_id="ALLOCATION_WEIGHT_NONZERO",
                severity="ERROR",
                disposition="REJECT",
                affected_field="target_weight",
                observed_value=F.col("raw_target_weight"),
                expected_condition="target_weight is nonzero.",
                message="Allocation target weight is zero.",
            ),
        ),
        conditional_violation(
            F.col("effective_to").isNotNull()
            & (F.col("effective_to") < F.col("effective_from")),
            violation_struct(
                rule_id="ALLOCATION_EFFECTIVE_DATE_RANGE",
                severity="ERROR",
                disposition="REJECT",
                affected_field="effective_to",
                observed_value=F.col("raw_effective_to"),
                expected_condition="effective_to is null or not before effective_from.",
                message="Allocation effective date range is invalid.",
            ),
        ),
        conditional_violation(
            F.col("record_hash") != F.col("computed_record_hash"),
            violation_struct(
                rule_id="ALLOCATION_RECORD_HASH_VALID",
                severity="ERROR",
                disposition="REJECT",
                affected_field="record_hash",
                observed_value=F.col("record_hash"),
                expected_condition="record_hash matches canonical SHA-256.",
                message="Allocation record hash does not match its values.",
            ),
        ),
    ]
    return explode_record_violations(candidates, violations)


def explode_record_violations(
    candidates: DataFrame,
    violations: list[Column],
) -> DataFrame:
    """Explode failed rule structs into one row per violation."""
    return (
        candidates.withColumn(
            "violations",
            F.filter(
                F.array(*violations),
                lambda violation: violation.isNotNull(),
            ),
        )
        .select(
            "source_record_id",
            "batch_id",
            "source_row_number",
            "source_record_sha256",
            "business_key",
            F.explode("violations").alias("violation"),
        )
        .select(
            "source_record_id",
            "batch_id",
            "source_row_number",
            "source_record_sha256",
            "business_key",
            "violation.*",
        )
    )


def add_same_batch_evidence(
    candidates: DataFrame,
) -> DataFrame:
    """Add deterministic duplicate and conflict evidence."""
    keyed = candidates.withColumn(
        "deduplication_key",
        F.coalesce(F.col("business_key"), F.col("source_record_id")),
    )
    order_window = Window.partitionBy("deduplication_key").orderBy(
        F.col("source_row_number").asc()
    )
    ranked = keyed.withColumn(
        "key_row_number",
        F.row_number().over(order_window),
    )
    key_summary = ranked.groupBy("deduplication_key").agg(
        F.count("*").alias("key_count"),
        F.countDistinct("record_hash").alias("key_hash_count"),
        F.max(
            F.when(
                F.col("key_row_number") == 1,
                F.col("source_record_id"),
            )
        ).alias("winning_source_record_id"),
    )
    return ranked.join(
        key_summary,
        on="deduplication_key",
        how="left",
    )


def same_batch_violations(
    candidates: DataFrame,
    dataset_name: str,
) -> DataFrame:
    """Emit duplicate warnings and conflict errors."""
    prefix = "INSTRUMENT" if dataset_name == "INSTRUMENTS" else "ALLOCATION"
    violations = [
        conditional_violation(
            (F.col("key_count") > 1)
            & (F.col("key_hash_count") == 1)
            & (F.col("key_row_number") > 1),
            violation_struct(
                rule_id=f"{prefix}_SAME_BATCH_IDENTICAL_DUPLICATE",
                severity="WARNING",
                disposition="WARN_AND_DEDUPLICATE",
                affected_field=None,
                observed_value=F.col("record_hash"),
                expected_condition=(
                    "The lowest source row wins an identical duplicate set."
                ),
                message="This identical nonwinning record was deduplicated.",
            ),
        ),
        conditional_violation(
            (F.col("key_count") > 1)
            & (F.col("key_hash_count") > 1),
            violation_struct(
                rule_id=f"{prefix}_SAME_BATCH_CONFLICT",
                severity="ERROR",
                disposition="REJECT",
                affected_field=None,
                observed_value=F.col("record_hash"),
                expected_condition=(
                    "One key cannot contain differing hashes in one batch."
                ),
                message="Conflicting same-batch records were rejected.",
            ),
        ),
    ]
    return explode_record_violations(candidates, violations)


def add_semantic_version_parts(
    frame: DataFrame,
    *,
    column_name: str,
    prefix: str,
) -> DataFrame:
    """Add numeric semantic-version components."""
    parts = F.split(F.col(column_name), r"\.")
    return (
        frame.withColumn(
            f"{prefix}_version_major",
            parts.getItem(0).cast("bigint"),
        )
        .withColumn(
            f"{prefix}_version_minor",
            parts.getItem(1).cast("bigint"),
        )
        .withColumn(
            f"{prefix}_version_patch",
            parts.getItem(2).cast("bigint"),
        )
    )


def canonical_comparisons(
    *,
    candidates: DataFrame,
    record_violations: DataFrame,
    specification: dict[str, Any],
) -> DataFrame:
    """Compare valid same-batch winners with current Silver."""
    error_counts = record_violations.groupBy("source_record_id").agg(
        F.sum(
            F.when(
                F.col("severity").isin("ERROR", "CRITICAL"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("record_error_count")
    )
    eligible = (
        candidates.join(error_counts, "source_record_id", "left")
        .withColumn(
            "record_error_count",
            F.coalesce(F.col("record_error_count"), F.lit(0)),
        )
        .where(
            (F.col("record_error_count") == 0)
            & (F.col("key_hash_count") == 1)
            & (F.col("key_row_number") == 1)
        )
    )

    key_columns = specification["key_columns"]
    version_column = specification["version_column"]
    current = spark.table(specification["silver_table"])
    current = current.select(
        *[F.col(column).alias(f"current_{column}") for column in key_columns],
        F.col(version_column).alias("current_version"),
        F.col("record_hash").alias("current_record_hash"),
    )
    join_condition = None
    for column in key_columns:
        condition = F.col(column) == F.col(f"current_{column}")
        join_condition = (
            condition
            if join_condition is None
            else join_condition & condition
        )

    comparison = eligible.join(current, join_condition, "left")
    comparison = add_semantic_version_parts(
        comparison,
        column_name=version_column,
        prefix="candidate",
    )
    comparison = add_semantic_version_parts(
        comparison,
        column_name="current_version",
        prefix="current",
    )
    higher_version = (
        F.col("candidate_version_major") > F.col("current_version_major")
    ) | (
        (F.col("candidate_version_major") == F.col("current_version_major"))
        & (F.col("candidate_version_minor") > F.col("current_version_minor"))
    ) | (
        (F.col("candidate_version_major") == F.col("current_version_major"))
        & (F.col("candidate_version_minor") == F.col("current_version_minor"))
        & (F.col("candidate_version_patch") > F.col("current_version_patch"))
    )

    return comparison.withColumn(
        "canonical_comparison_outcome",
        F.when(F.col("current_record_hash").isNull(), "ACCEPTED_NEW")
        .when(
            (F.col(version_column) == F.col("current_version"))
            & (F.col("record_hash") == F.col("current_record_hash")),
            "UNCHANGED",
        )
        .when(higher_version, "ACCEPTED_CORRECTION")
        .otherwise("REJECTED"),
    )


def invalid_correction_violations(
    comparisons: DataFrame,
    dataset_name: str,
) -> DataFrame:
    """Emit rejection evidence for invalid later versions."""
    prefix = "INSTRUMENT" if dataset_name == "INSTRUMENTS" else "ALLOCATION"
    version_column = DATASET_SPECS[dataset_name]["version_column"]
    return (
        comparisons.where(
            F.col("canonical_comparison_outcome") == "REJECTED"
        )
        .select(
            "source_record_id",
            "batch_id",
            "source_row_number",
            "source_record_sha256",
            "business_key",
            F.lit(f"{prefix}_INVALID_CORRECTION").alias("rule_id"),
            F.lit("ERROR").alias("severity"),
            F.lit("REJECT").alias("disposition"),
            F.lit(version_column).alias("affected_field"),
            F.col(version_column).cast("string").alias("observed_value"),
            F.lit(
                "Changed values require a higher semantic version."
            ).alias("expected_condition"),
            F.lit(
                "The candidate cannot replace the current canonical record."
            ).alias("message"),
        )
    )


def final_outcomes(
    *,
    candidates: DataFrame,
    comparisons: DataFrame,
    violations: DataFrame,
) -> DataFrame:
    """Assign exactly one final state to every source record."""
    summary = violations.groupBy("source_record_id").agg(
        F.count("*").alias("violation_count"),
        F.sum(
            F.when(F.col("severity") == "WARNING", 1).otherwise(0)
        ).alias("warning_count"),
        F.sum(
            F.when(
                F.col("severity").isin("ERROR", "CRITICAL"),
                1,
            ).otherwise(0)
        ).alias("error_count"),
    )
    comparison_states = comparisons.select(
        "source_record_id",
        "canonical_comparison_outcome",
    )
    result = (
        candidates.join(summary, "source_record_id", "left")
        .join(comparison_states, "source_record_id", "left")
        .withColumn(
            "violation_count",
            F.coalesce(F.col("violation_count"), F.lit(0)),
        )
        .withColumn(
            "warning_count",
            F.coalesce(F.col("warning_count"), F.lit(0)),
        )
        .withColumn(
            "error_count",
            F.coalesce(F.col("error_count"), F.lit(0)),
        )
        .withColumn(
            "outcome",
            F.when(F.col("error_count") > 0, "REJECTED")
            .when(
                (F.col("key_count") > 1)
                & (F.col("key_hash_count") == 1)
                & (F.col("key_row_number") > 1),
                "DEDUPLICATED",
            )
            .otherwise(F.col("canonical_comparison_outcome")),
        )
        .withColumn(
            "deduplicated_to_source_record_id",
            F.when(
                F.col("outcome") == "DEDUPLICATED",
                F.col("winning_source_record_id"),
            ).otherwise(F.lit(None).cast("string")),
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
            ).otherwise(F.lit(None).cast("string")),
        )
    )
    require_equal("final outcome count", result.count(), evaluated_count)
    require_equal(
        "distinct final source count",
        result.select("source_record_id").distinct().count(),
        evaluated_count,
    )
    require_equal(
        "null final outcome count",
        result.where(F.col("outcome").isNull()).count(),
        0,
    )
    return result


def instrument_dataset_failures(snapshot: DataFrame) -> list[str]:
    """Evaluate instrument snapshot-level rules."""
    failures: list[str] = []
    total_count = snapshot.count()
    active = snapshot.where(F.col("is_active") == F.lit(True))
    if active.count() != 15:
        failures.append("INSTRUMENT_ACTIVE_COUNT")
    if snapshot.select("instrument_id").distinct().count() != total_count:
        failures.append("INSTRUMENT_ID_UNIQUE")
    if (
        active.select("yfinance_symbol").distinct().count()
        != active.count()
    ):
        failures.append("INSTRUMENT_PROVIDER_SYMBOL_UNIQUE")
    return failures


def allocation_dataset_failures(snapshot: DataFrame) -> list[str]:
    """Evaluate allocation snapshot-level rules."""
    failures: list[str] = []
    key_columns = DATASET_SPECS["TARGET_ALLOCATIONS"]["key_columns"]
    if snapshot.select(*key_columns).distinct().count() != snapshot.count():
        failures.append("ALLOCATION_BUSINESS_KEY_UNIQUE")

    active_allocations = snapshot.where(F.col("effective_to").isNull())
    active_portfolios = (
        spark.table(SILVER_PORTFOLIO_TABLE)
        .where(F.col("is_active") == F.lit(True))
        .select("portfolio_id")
    )
    coverage = (
        active_portfolios.join(
            active_allocations.groupBy("portfolio_id").agg(
                F.countDistinct("instrument_id").alias("instrument_count")
            ),
            "portfolio_id",
            "left",
        )
        .where(
            F.coalesce(F.col("instrument_count"), F.lit(0)) != 15
        )
        .count()
    )
    if coverage > 0:
        failures.append("ALLOCATION_ACTIVE_COVERAGE")

    overlap_window = Window.partitionBy(
        "portfolio_id", "instrument_id"
    ).orderBy(F.col("effective_from").asc())
    overlap_count = (
        snapshot.withColumn(
            "next_effective_from",
            F.lead("effective_from").over(overlap_window),
        )
        .where(
            F.col("next_effective_from").isNotNull()
            & (
                F.col("effective_to").isNull()
                | (F.col("effective_to") >= F.col("next_effective_from"))
            )
        )
        .count()
    )
    if overlap_count > 0:
        failures.append("ALLOCATION_EFFECTIVE_RANGES_NONOVERLAPPING")

    metrics = {
        row["portfolio_id"]: row
        for row in active_allocations.groupBy("portfolio_id").agg(
            F.sum(
                F.when(F.col("target_weight") > 0, F.col("target_weight"))
                .otherwise(F.lit(Decimal("0")))
            ).alias("long_total"),
            F.sum(
                F.when(F.col("target_weight") < 0, F.col("target_weight"))
                .otherwise(F.lit(Decimal("0")))
            ).alias("short_total"),
            F.sum(F.abs(F.col("target_weight"))).alias("gross_total"),
            F.sum("target_weight").alias("net_total"),
        ).collect()
    }
    tolerance = Decimal("0.000001")
    core = metrics.get("CORE_15_LONG")
    core_valid = core is not None and all(
        abs(actual - expected) <= tolerance
        for actual, expected in [
            (core["long_total"], Decimal("1.0")),
            (core["short_total"], Decimal("0.0")),
            (core["gross_total"], Decimal("1.0")),
            (core["net_total"], Decimal("1.0")),
        ]
    )
    if not core_valid:
        failures.append("ALLOCATION_LONG_ONLY_TOTALS")

    long_short = metrics.get("LONG_SHORT_130_30")
    long_short_valid = long_short is not None and all(
        abs(actual - expected) <= tolerance
        for actual, expected in [
            (long_short["long_total"], Decimal("1.3")),
            (long_short["short_total"], Decimal("-0.3")),
            (long_short["gross_total"], Decimal("1.6")),
            (long_short["net_total"], Decimal("1.0")),
        ]
    )
    if not long_short_valid:
        failures.append("ALLOCATION_LONG_SHORT_TOTALS")
    return failures


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
    raise ValueError(
        "trigger_type must be MANUAL, SCHEDULED, or RECOVERY"
    )

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

print("processing_parameters=PASS")
print(f"processing_run_id={processing_run_id}")
print(f"source_batch_id={source_batch_id}")
print(f"dataset_name={dataset_name}")
print(f"attempt_number={attempt_number}")
print(f"reprocess_of_processing_run_id={reprocess_of_processing_run_id}")
print(f"evaluated_count={evaluated_count}")
print(f"input_record_set_sha256={input_record_set_sha256}")


# COMMAND ----------

if dataset_name == "INSTRUMENTS":
    typed_candidates = typed_instrument_candidates(bronze_source)
    record_rule_violations = instrument_record_violations(typed_candidates)
else:
    typed_candidates = typed_allocation_candidates(bronze_source)
    record_rule_violations = allocation_record_violations(typed_candidates)

ranked_candidates = add_same_batch_evidence(typed_candidates)
duplicate_rule_violations = same_batch_violations(
    ranked_candidates,
    dataset_name,
)
precomparison_violations = record_rule_violations.unionByName(
    duplicate_rule_violations
)
comparisons = canonical_comparisons(
    candidates=ranked_candidates,
    record_violations=precomparison_violations,
    specification=specification,
)
correction_rule_violations = invalid_correction_violations(
    comparisons,
    dataset_name,
)
complete_rule_violations = (
    precomparison_violations.unionByName(correction_rule_violations)
).cache()
frozen_violation_count = complete_rule_violations.count()
outcomes = final_outcomes(
    candidates=ranked_candidates,
    comparisons=comparisons,
    violations=complete_rule_violations,
).cache()
frozen_outcome_count = outcomes.count()
require_equal(
    "frozen final outcome count",
    frozen_outcome_count,
    evaluated_count,
)

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

print("record_validation=PASS")
print(f"accepted_count={accepted_count}")
print(f"quarantined_count={quarantined_count}")
print(f"rejected_count={rejected_count}")
print(f"unchanged_count={unchanged_count}")
print(f"deduplicated_count={deduplicated_count}")
print(f"warning_count={warning_count}")
outcomes.select(
    "source_record_id",
    "business_key",
    "outcome",
    "warning_count",
    "violation_count",
).orderBy("source_row_number").show(40, truncate=False)


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
prospective_snapshot = (
    current_snapshot.join(accepted_keys, key_columns, "left_anti")
    .unionByName(accepted_candidates)
)

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

if dataset_name == "INSTRUMENTS":
    failed_rule_ids = instrument_dataset_failures(prospective_snapshot)
else:
    failed_rule_ids = allocation_dataset_failures(prospective_snapshot)

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

print("publication_gate=PASS")
print(f"canonical_before_count={canonical_before_count}")
print(f"prospective_count={prospective_count}")
print(f"failed_rule_ids={failed_rule_ids}")
print(f"publish_allowed={publish_allowed}")
print(f"published={published}")
print(f"canonical_after_count={canonical_after_count}")
print(f"run_status={run_status}")
print(f"canonical_before_sha256={canonical_before_sha256}")
print(f"canonical_after_sha256={canonical_after_sha256}")


# COMMAND ----------

evaluated_at_utc = datetime.now(UTC).replace(tzinfo=None)
outcome_audit_records = outcomes.select(
    F.sha2(
        F.concat_ws(
            "|",
            F.lit(processing_run_id),
            F.col("source_record_id"),
        ),
        256,
    ).alias("outcome_id"),
    F.lit(processing_run_id).alias("processing_run_id"),
    F.col("batch_id"),
    F.lit(dataset_name).alias("dataset_name"),
    F.col("source_record_id"),
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
)
outcome_audit_records = outcome_audit_records.select(
    *spark.table(SILVER_OUTCOME_TABLE).schema.fieldNames()
)

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
)
violation_audit_records = violation_audit_records.select(
    *spark.table(SILVER_VIOLATION_TABLE).schema.fieldNames()
)

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
        f"target.{column} = source.{column}"
        for column in key_columns
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

print("silver_persistence=PASS")
print(f"processing_run_id={processing_run_id}")
print(f"dataset_name={dataset_name}")
print(f"run_status={run_status}")
print(f"published={published}")
print(f"persisted_canonical_count={persisted_snapshot.count()}")
print(f"persisted_outcome_count={evaluated_count}")
print(f"persisted_violation_count={frozen_violation_count}")

outcomes.unpersist()
complete_rule_violations.unpersist()
