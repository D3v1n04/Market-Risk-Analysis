# Databricks notebook source
"""Validate and canonicalize one Bronze portfolio batch into Silver."""

import re
from datetime import UTC, datetime
from uuid import UUID, uuid4

from delta.tables import DeltaTable
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PORTFOLIO_CONTRACT_VERSION = "1.1.0"
VIOLATION_CONTRACT_VERSION = "1.2.0"
OUTCOME_CONTRACT_VERSION = "1.0.0"
PROCESSING_RUN_CONTRACT_VERSION = "1.0.0"

BRONZE_PORTFOLIO_TABLE = (
    "workspace.devin_market_risk_dev.bronze_portfolios"
)
BRONZE_BATCH_TABLE = (
    "workspace.devin_market_risk_dev.bronze_ingestion_batches"
)

SILVER_PORTFOLIO_TABLE = (
    "workspace.devin_market_risk_dev.silver_portfolios"
)
SILVER_PROCESSING_RUN_TABLE = (
    "workspace.devin_market_risk_dev.silver_processing_runs"
)
SILVER_OUTCOME_TABLE = (
    "workspace.devin_market_risk_dev."
    "silver_portfolio_record_outcomes"
)
SILVER_VIOLATION_TABLE = (
    "workspace.devin_market_risk_dev."
    "silver_data_quality_violations"
)

CODE_VERSION_PATTERN = re.compile(r"^[0-9a-f]{7,40}$")


def require_uuid(label: str, value: str) -> str:
    """Require a nonempty canonical UUID string."""
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be a valid UUID") from exc

    canonical = str(parsed)

    if value.lower() != canonical:
        raise ValueError(f"{label} must use canonical UUID formatting")

    return canonical


def calculate_ordered_set_sha256(
    frame: DataFrame,
    *,
    order_columns: list[str],
    value_columns: list[str],
) -> str:
    """Hash a deterministically ordered set of DataFrame rows."""
    ordering_fields = [
        F.col(column_name).alias(f"order_{index}")
        for index, column_name in enumerate(order_columns)
    ]
    canonical_value_fields = [
        F.coalesce(
            F.col(column_name).cast("string"),
            F.lit("<NULL>"),
        )
        for column_name in value_columns
    ]

    ordered_rows = frame.select(
        F.struct(
            *ordering_fields,
            F.concat_ws(
                "|",
                *canonical_value_fields,
            ).alias("canonical_row"),
        ).alias("ordered_row")
    )

    digest_row = (
        ordered_rows
        .agg(
            F.sha2(
                F.concat_ws(
                    "\n",
                    F.transform(
                        F.array_sort(
                            F.collect_list("ordered_row")
                        ),
                        lambda ordered_row: ordered_row[
                            "canonical_row"
                        ],
                    ),
                ),
                256,
            ).alias("set_sha256")
        )
        .first()
    )

    return digest_row["set_sha256"]


def build_processing_run_audit_records(
    *,
    completed_at_utc: datetime,
    status: str,
    published_value: bool,
    published_at_utc: datetime | None,
    final_canonical_after_count: int,
    final_canonical_after_sha256: str | None,
    error_code: str | None,
    error_message: str | None,
) -> DataFrame:
    """Build the single terminal audit row for this processing run."""
    if published_value != (published_at_utc is not None):
        raise ValueError(
            "published and published_at_utc must be consistent"
        )

    if status == "FAILED" and (
        error_code is None or error_message is None
    ):
        raise ValueError(
            "A failed processing run requires failure diagnostics"
        )

    if status != "FAILED" and (
        error_code is not None or error_message is not None
    ):
        raise ValueError(
            "A successful processing run cannot contain failure diagnostics"
        )

    processing_run_record = {
        "processing_run_id": processing_run_id,
        "source_batch_id": source_batch_id,
        "dataset_name": "PORTFOLIOS",
        "attempt_number": attempt_number,
        "reprocess_of_processing_run_id": (
            reprocess_of_processing_run_id
        ),
        "trigger_type": trigger_type,
        "started_at_utc": processing_run_started_at_utc,
        "completed_at_utc": completed_at_utc,
        "status": status,
        "evaluated_count": evaluated_count,
        "accepted_count": accepted_count,
        "quarantined_count": quarantined_count,
        "rejected_count": rejected_count,
        "unchanged_count": unchanged_count,
        "deduplicated_count": deduplicated_count,
        "warning_count": warning_count,
        "canonical_before_count": canonical_before_count,
        "canonical_after_count": final_canonical_after_count,
        "input_record_set_sha256": input_record_set_sha256,
        "canonical_before_sha256": canonical_before_sha256,
        "canonical_after_sha256": final_canonical_after_sha256,
        "published": published_value,
        "published_at_utc": published_at_utc,
        "failed_rule_ids": list(failed_rule_ids),
        "error_code": error_code,
        "error_message": error_message,
        "dataset_contract_version": (
            PORTFOLIO_CONTRACT_VERSION
        ),
        "code_version": code_version,
        "contract_version": (
            PROCESSING_RUN_CONTRACT_VERSION
        ),
    }

    processing_run_schema = spark.table(
        SILVER_PROCESSING_RUN_TABLE
    ).schema

    return spark.createDataFrame(
        [processing_run_record],
        schema=processing_run_schema,
    )

def require_code_version(value: str) -> str:
    """Require a lowercase abbreviated or complete Git commit SHA."""
    if CODE_VERSION_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "code_version must be a lowercase 7-to-40-character Git SHA"
        )

    return value


def insert_only_audit_records(
    frame: DataFrame,
    target_table: str,
    key_column: str,
) -> None:
    """Insert immutable audit rows without updating existing evidence."""
    target_delta_table = DeltaTable.forName(
        spark,
        target_table,
    )

    (
        target_delta_table.alias("target")
        .merge(
            frame.alias("source"),
            (
                f"target.{key_column} = "
                f"source.{key_column}"
            ),
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils = DBUtils(spark)

dbutils.widgets.text(
    "source_batch_id",
    "",
    "Bronze source batch ID",
)
dbutils.widgets.text(
    "code_version",
    "",
    "Git code version",
)
dbutils.widgets.text(
    "trigger_type",
    "MANUAL",
    "Silver processing trigger type",
)

source_batch_id = require_uuid(
    "source_batch_id",
    dbutils.widgets.get("source_batch_id").strip(),
)
code_version = require_code_version(
    dbutils.widgets.get("code_version").strip(),
)

trigger_type = (
    dbutils.widgets.get("trigger_type")
    .strip()
    .upper()
)

if trigger_type not in {"MANUAL", "SCHEDULED", "RECOVERY"}:
    raise ValueError(
        "trigger_type must be MANUAL, SCHEDULED, or RECOVERY"
    )

processing_run_id = str(uuid4())
processing_run_started_at_utc = (
    datetime.now(UTC).replace(tzinfo=None)
)

previous_processing_run_rows = (
    spark.table(SILVER_PROCESSING_RUN_TABLE)
    .where(F.col("source_batch_id") == source_batch_id)
    .orderBy(F.col("attempt_number").desc())
    .select(
        "processing_run_id",
        "attempt_number",
    )
    .limit(1)
    .collect()
)

if previous_processing_run_rows:
    previous_processing_run = previous_processing_run_rows[0]
    attempt_number = (
        previous_processing_run["attempt_number"] + 1
    )
    reprocess_of_processing_run_id = (
        previous_processing_run["processing_run_id"]
    )
else:
    attempt_number = 1
    reprocess_of_processing_run_id = None

print(f"processing_run_id={processing_run_id}")
print(f"attempt_number={attempt_number}")
print(
    "reprocess_of_processing_run_id="
    f"{reprocess_of_processing_run_id}"
)
print(f"trigger_type={trigger_type}")

print(f"source_batch_id={source_batch_id}")
print(f"code_version={code_version}")


# COMMAND ----------

eligible_batch_count = (
    spark.table(BRONZE_BATCH_TABLE)
    .where(
        (F.col("batch_id") == source_batch_id)
        & (F.col("dataset_name") == "PORTFOLIOS")
        & F.col("status").isin(
            "SUCCEEDED",
            "SUCCEEDED_WITH_WARNINGS",
        )
    )
    .count()
)

if eligible_batch_count != 1:
    raise ValueError(
        "source_batch_id must identify exactly one successful "
        "PORTFOLIOS Bronze ingestion batch"
    )

bronze_source = (
    spark.table(BRONZE_PORTFOLIO_TABLE)
    .where(F.col("batch_id") == source_batch_id)
)

evaluated_count = bronze_source.count()

if evaluated_count == 0:
    raise ValueError(
        "The selected successful Bronze batch contains no portfolio rows"
    )

input_record_set_sha256 = calculate_ordered_set_sha256(
    bronze_source,
    order_columns=["source_row_number"],
    value_columns=[
        "source_row_number",
        "source_record_sha256",
    ]
)

print(
    "input_record_set_sha256="
    f"{input_record_set_sha256}"
)

bronze_source.createOrReplaceTempView(
    "phase_05_bronze_portfolio_source"
)

print("bronze_batch_validation=PASS")
print(f"evaluated_count={evaluated_count}")


# COMMAND ----------

typed_candidates = spark.sql(
    """
    SELECT
        portfolio_id,
        portfolio_name,
        strategy_code,
        base_currency,
        target_inception_date,
        actual_inception_date,
        initial_nav,
        target_long_ratio,
        target_short_ratio,
        target_gross_ratio,
        target_net_ratio,
        rebalance_policy,
        cash_policy,
        is_active,
        config_version,
        record_hash,
        batch_id,
        source_id,
        source_object_path,
        source_sha256,
        source_row_number,
        source_record_sha256,
        raw_record,
        ingested_at_utc,
        contract_version AS bronze_contract_version,
        CONCAT(
            batch_id,
            ':',
            CAST(source_row_number AS STRING)
        ) AS source_record_id,
        TRY_CAST(target_inception_date AS DATE)
            AS typed_target_inception_date,
        TRY_CAST(
            NULLIF(TRIM(actual_inception_date), '')
            AS DATE
        ) AS typed_actual_inception_date,
        TRY_CAST(initial_nav AS DECIMAL(18,2))
            AS typed_initial_nav,
        TRY_CAST(
            target_long_ratio AS DECIMAL(12,10)
        ) AS typed_target_long_ratio,
        TRY_CAST(
            target_short_ratio AS DECIMAL(12,10)
        ) AS typed_target_short_ratio,
        TRY_CAST(
            target_gross_ratio AS DECIMAL(12,10)
        ) AS typed_target_gross_ratio,
        TRY_CAST(
            target_net_ratio AS DECIMAL(12,10)
        ) AS typed_target_net_ratio,
        TRY_CAST(is_active AS BOOLEAN)
            AS typed_is_active
    FROM phase_05_bronze_portfolio_source
    """
)

typed_candidates.createOrReplaceTempView(
    "phase_05_typed_portfolio_candidates"
)

print("silver_type_conversion=PASS")

typed_candidates.select(
    "portfolio_id",
    "typed_target_inception_date",
    "typed_actual_inception_date",
    "typed_initial_nav",
    "typed_target_long_ratio",
    "typed_target_short_ratio",
    "typed_target_gross_ratio",
    "typed_target_net_ratio",
    "typed_is_active",
    "source_record_id",
).show(truncate=False)

# COMMAND ----------

structural_violations = spark.sql(
    f"""
    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_REQUIRED_FIELDS' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        CAST(NULL AS STRING) AS affected_field,
        CAST(NULL AS STRING) AS observed_value,
        'Every required field must be present and nonempty.'
            AS expected_condition,
        'One or more required portfolio fields are missing.'
            AS message
    FROM phase_05_typed_portfolio_candidates
    WHERE portfolio_id IS NULL
       OR TRIM(portfolio_id) = ''
       OR portfolio_name IS NULL
       OR TRIM(portfolio_name) = ''
       OR strategy_code IS NULL
       OR TRIM(strategy_code) = ''
       OR base_currency IS NULL
       OR TRIM(base_currency) = ''
       OR target_inception_date IS NULL
       OR TRIM(target_inception_date) = ''
       OR initial_nav IS NULL
       OR TRIM(initial_nav) = ''
       OR target_long_ratio IS NULL
       OR TRIM(target_long_ratio) = ''
       OR target_short_ratio IS NULL
       OR TRIM(target_short_ratio) = ''
       OR target_gross_ratio IS NULL
       OR TRIM(target_gross_ratio) = ''
       OR target_net_ratio IS NULL
       OR TRIM(target_net_ratio) = ''
       OR rebalance_policy IS NULL
       OR TRIM(rebalance_policy) = ''
       OR cash_policy IS NULL
       OR TRIM(cash_policy) = ''
       OR is_active IS NULL
       OR TRIM(is_active) = ''
       OR config_version IS NULL
       OR TRIM(config_version) = ''
       OR record_hash IS NULL
       OR TRIM(record_hash) = ''

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_TYPES_CASTABLE' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        CAST(NULL AS STRING) AS affected_field,
        TO_JSON(
            NAMED_STRUCT(
                'target_inception_date',
                target_inception_date,
                'actual_inception_date',
                actual_inception_date,
                'initial_nav',
                initial_nav,
                'target_long_ratio',
                target_long_ratio,
                'target_short_ratio',
                target_short_ratio,
                'target_gross_ratio',
                target_gross_ratio,
                'target_net_ratio',
                target_net_ratio,
                'is_active',
                is_active
            )
        ) AS observed_value,
        'Dates, decimals, and booleans must cast without loss.'
            AS expected_condition,
        'One or more portfolio values cannot be safely typed.'
            AS message
    FROM phase_05_typed_portfolio_candidates
    WHERE typed_target_inception_date IS NULL
       OR (
            actual_inception_date IS NOT NULL
            AND TRIM(actual_inception_date) <> ''
            AND typed_actual_inception_date IS NULL
       )
       OR typed_initial_nav IS NULL
       OR typed_target_long_ratio IS NULL
       OR typed_target_short_ratio IS NULL
       OR typed_target_gross_ratio IS NULL
       OR typed_target_net_ratio IS NULL
       OR typed_is_active IS NULL

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_ACTUAL_INCEPTION_DATE_MISSING' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'WARNING' AS severity,
        'WARN_AND_ACCEPT' AS disposition,
        'actual_inception_date' AS affected_field,
        actual_inception_date AS observed_value,
        'The verified actual inception date should be populated.'
            AS expected_condition,
        'Actual inception date is missing and remains null in Silver.'
            AS message
    FROM phase_05_typed_portfolio_candidates
    WHERE actual_inception_date IS NULL
       OR TRIM(actual_inception_date) = ''

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_ALLOWED_VALUES' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        CAST(NULL AS STRING) AS affected_field,
        TO_JSON(
            NAMED_STRUCT(
                'strategy_code',
                strategy_code,
                'base_currency',
                base_currency,
                'rebalance_policy',
                rebalance_policy,
                'cash_policy',
                cash_policy
            )
        ) AS observed_value,
        'Categorical values must match the portfolio contract.'
            AS expected_condition,
        'One or more categorical values are not allowed.'
            AS message
    FROM phase_05_typed_portfolio_candidates
    WHERE (
            strategy_code IS NOT NULL
            AND strategy_code NOT IN (
                'LONG_ONLY',
                'LONG_SHORT_130_30'
            )
       )
       OR (
            base_currency IS NOT NULL
            AND base_currency <> 'USD'
       )
       OR (
            rebalance_policy IS NOT NULL
            AND rebalance_policy <> 'BUY_AND_HOLD'
       )
       OR (
            cash_policy IS NOT NULL
            AND cash_policy <> 'RETAIN_DIVIDENDS_NO_INTEREST'
       )

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_ID_FORMAT' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        'portfolio_id' AS affected_field,
        portfolio_id AS observed_value,
        'portfolio_id must use uppercase snake case.'
            AS expected_condition,
        'Portfolio identifier has an invalid format.'
            AS message
    FROM phase_05_typed_portfolio_candidates
    WHERE portfolio_id IS NOT NULL
      AND TRIM(portfolio_id) <> ''
      AND NOT (
          portfolio_id RLIKE
          '^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$'
      )

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_CONFIG_VERSION_VALID' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        'config_version' AS affected_field,
        config_version AS observed_value,
        'config_version must use three numeric components.'
            AS expected_condition,
        'Portfolio configuration version is not valid semantic versioning.'
            AS message
    FROM phase_05_typed_portfolio_candidates
    WHERE config_version IS NOT NULL
      AND TRIM(config_version) <> ''
      AND NOT (
          config_version RLIKE '^[0-9]+[.][0-9]+[.][0-9]+$'
      )
    """
)

structural_violations.createOrReplaceTempView(
    "phase_05_portfolio_rule_violations"
)

structural_violation_count = structural_violations.count()
structural_warning_count = (
    structural_violations
    .where(F.col("severity") == "WARNING")
    .count()
)
structural_error_count = (
    structural_violations
    .where(F.col("severity").isin("ERROR", "CRITICAL"))
    .count()
)

print("structural_validation=PASS")
print(f"structural_violation_count={structural_violation_count}")
print(f"structural_warning_count={structural_warning_count}")
print(f"structural_error_count={structural_error_count}")

structural_violations.orderBy(
    "source_row_number",
    "rule_id",
).show(truncate=False)

# COMMAND ----------

business_candidates = spark.sql(
    """
    SELECT
        *,
        SHA2(
            CONCAT_WS(
                '|',
                COALESCE(
                    NULLIF(portfolio_id, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(portfolio_name, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(strategy_code, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(base_currency, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(target_inception_date, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(actual_inception_date, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(initial_nav, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(target_long_ratio, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(target_short_ratio, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(target_gross_ratio, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(target_net_ratio, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(rebalance_policy, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(cash_policy, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(is_active, ''),
                    '<NULL>'
                ),
                COALESCE(
                    NULLIF(config_version, ''),
                    '<NULL>'
                )
            ),
            256
        ) AS computed_record_hash
    FROM phase_05_typed_portfolio_candidates
    """
)

business_candidates.createOrReplaceTempView(
    "phase_05_business_portfolio_candidates"
)

# COMMAND ----------

business_violations = spark.sql(
    f"""
    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_INITIAL_NAV_POSITIVE' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        'initial_nav' AS affected_field,
        initial_nav AS observed_value,
        'initial_nav must be greater than zero.'
            AS expected_condition,
        'Portfolio initial NAV is not positive.'
            AS message
    FROM phase_05_business_portfolio_candidates
    WHERE typed_initial_nav IS NOT NULL
      AND typed_initial_nav <= 0

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_RATIO_RECONCILIATION' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        CAST(NULL AS STRING) AS affected_field,
        TO_JSON(
            NAMED_STRUCT(
                'long',
                target_long_ratio,
                'short',
                target_short_ratio,
                'gross',
                target_gross_ratio,
                'net',
                target_net_ratio
            )
        ) AS observed_value,
        'Gross must equal long plus short and net must equal long minus short.'
            AS expected_condition,
        'Portfolio exposure ratios do not reconcile.'
            AS message
    FROM phase_05_business_portfolio_candidates
    WHERE typed_target_long_ratio IS NOT NULL
      AND typed_target_short_ratio IS NOT NULL
      AND typed_target_gross_ratio IS NOT NULL
      AND typed_target_net_ratio IS NOT NULL
      AND (
          ABS(
              typed_target_gross_ratio
              - (
                  typed_target_long_ratio
                  + typed_target_short_ratio
              )
          ) > 0.000001
          OR
          ABS(
              typed_target_net_ratio
              - (
                  typed_target_long_ratio
                  - typed_target_short_ratio
              )
          ) > 0.000001
      )

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_STRATEGY_TARGETS' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        'strategy_code' AS affected_field,
        TO_JSON(
            NAMED_STRUCT(
                'strategy_code',
                strategy_code,
                'long',
                target_long_ratio,
                'short',
                target_short_ratio,
                'gross',
                target_gross_ratio,
                'net',
                target_net_ratio
            )
        ) AS observed_value,
        'Exposure ratios must match the declared strategy.'
            AS expected_condition,
        'Portfolio ratios do not match the strategy definition.'
            AS message
    FROM phase_05_business_portfolio_candidates
    WHERE typed_target_long_ratio IS NOT NULL
      AND typed_target_short_ratio IS NOT NULL
      AND typed_target_gross_ratio IS NOT NULL
      AND typed_target_net_ratio IS NOT NULL
      AND (
          (
              strategy_code = 'LONG_ONLY'
              AND NOT (
                  typed_target_long_ratio = 1.0000000000
                  AND typed_target_short_ratio = 0.0000000000
                  AND typed_target_gross_ratio = 1.0000000000
                  AND typed_target_net_ratio = 1.0000000000
              )
          )
          OR
          (
              strategy_code = 'LONG_SHORT_130_30'
              AND NOT (
                  typed_target_long_ratio = 1.3000000000
                  AND typed_target_short_ratio = 0.3000000000
                  AND typed_target_gross_ratio = 1.6000000000
                  AND typed_target_net_ratio = 1.0000000000
              )
          )
      )

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_INCEPTION_ORDER' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        'actual_inception_date' AS affected_field,
        actual_inception_date AS observed_value,
        'Actual inception date must be null or on or after the target date.'
            AS expected_condition,
        'Actual inception date occurs before the target date.'
            AS message
    FROM phase_05_business_portfolio_candidates
    WHERE typed_actual_inception_date IS NOT NULL
      AND typed_target_inception_date IS NOT NULL
      AND typed_actual_inception_date
          < typed_target_inception_date

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_RECORD_HASH_VALID' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        'record_hash' AS affected_field,
        record_hash AS observed_value,
        'record_hash must match the canonical SHA-256 calculation.'
            AS expected_condition,
        'Source record hash does not match the canonical values.'
            AS message
    FROM phase_05_business_portfolio_candidates
    WHERE record_hash IS NOT NULL
      AND TRIM(record_hash) <> ''
      AND (
          NOT (
              record_hash RLIKE '^[0-9a-f]{{64}}$'
          )
          OR record_hash <> computed_record_hash
      )
    """
)

business_violations.createOrReplaceTempView(
    "phase_05_business_rule_violations"
)

all_rule_violations = structural_violations.unionByName(
    business_violations
)

all_rule_violations.createOrReplaceTempView(
    "phase_05_all_portfolio_rule_violations"
)

business_violation_count = business_violations.count()
total_violation_count = all_rule_violations.count()

print("business_validation=PASS")
print(f"business_violation_count={business_violation_count}")
print(f"total_violation_count={total_violation_count}")

business_candidates.select(
    "portfolio_id",
    "record_hash",
    "computed_record_hash",
).show(truncate=False)

business_violations.orderBy(
    "source_row_number",
    "rule_id",
).show(truncate=False)

# COMMAND ----------

DEDUPLICATED_OUTCOME = "DEDUPLICATED"
REJECTED_OUTCOME = "REJECTED"

same_batch_key_stats = spark.sql(
    """
    SELECT
        portfolio_id,
        COUNT(*) AS key_record_count,
        COUNT(DISTINCT record_hash)
            AS distinct_record_hash_count
    FROM phase_05_business_portfolio_candidates
    WHERE portfolio_id IS NOT NULL
      AND TRIM(portfolio_id) <> ''
    GROUP BY portfolio_id
    """
)

same_batch_key_stats.createOrReplaceTempView(
    "phase_05_same_batch_key_stats"
)

same_batch_candidates = spark.sql(
    f"""
    WITH ranked_candidates AS (
        SELECT
            candidate.*,
            ROW_NUMBER() OVER (
                PARTITION BY portfolio_id, record_hash
                ORDER BY source_row_number
            ) AS duplicate_rank,
            FIRST_VALUE(source_record_id) OVER (
                PARTITION BY portfolio_id, record_hash
                ORDER BY source_row_number
            ) AS duplicate_winner_source_record_id
        FROM phase_05_business_portfolio_candidates AS candidate
    )
    SELECT
        ranked.*,
        stats.key_record_count,
        stats.distinct_record_hash_count,
        CASE
            WHEN stats.distinct_record_hash_count > 1
                THEN '{REJECTED_OUTCOME}'
            WHEN ranked.duplicate_rank > 1
                THEN '{DEDUPLICATED_OUTCOME}'
            ELSE NULL
        END AS same_batch_outcome,
        CASE
            WHEN stats.distinct_record_hash_count > 1
                THEN 'PORTFOLIO_SAME_BATCH_CONFLICT'
            WHEN ranked.duplicate_rank > 1
                THEN 'PORTFOLIO_SAME_BATCH_IDENTICAL_DUPLICATE'
            ELSE NULL
        END AS same_batch_rule_id,
        CASE
            WHEN stats.distinct_record_hash_count > 1
                THEN 'ERROR'
            WHEN ranked.duplicate_rank > 1
                THEN 'WARNING'
            ELSE NULL
        END AS same_batch_severity,
        CASE
            WHEN stats.distinct_record_hash_count > 1
                THEN 'REJECT'
            WHEN ranked.duplicate_rank > 1
                THEN 'WARN_AND_DEDUPLICATE'
            ELSE NULL
        END AS same_batch_disposition
    FROM ranked_candidates AS ranked
    LEFT JOIN phase_05_same_batch_key_stats AS stats
        ON ranked.portfolio_id = stats.portfolio_id
    """
)

same_batch_candidates.createOrReplaceTempView(
    "phase_05_same_batch_portfolio_candidates"
)

same_batch_conflict_count = (
    same_batch_candidates
    .where(F.col("same_batch_outcome") == REJECTED_OUTCOME)
    .count()
)

same_batch_deduplicated_count = (
    same_batch_candidates
    .where(F.col("same_batch_outcome") == DEDUPLICATED_OUTCOME)
    .count()
)

print("same_batch_classification=PASS")
print(f"same_batch_conflict_count={same_batch_conflict_count}")
print(
    "same_batch_deduplicated_count="
    f"{same_batch_deduplicated_count}"
)

same_batch_candidates.select(
    "portfolio_id",
    "record_hash",
    "source_row_number",
    "duplicate_rank",
    "distinct_record_hash_count",
    "same_batch_outcome",
    "same_batch_rule_id",
).orderBy(
    "portfolio_id",
    "source_row_number",
).show(truncate=False)

# COMMAND ----------

ACCEPTED_NEW_OUTCOME = "ACCEPTED_NEW"
ACCEPTED_CORRECTION_OUTCOME = "ACCEPTED_CORRECTION"
UNCHANGED_OUTCOME = "UNCHANGED"

current_silver_portfolios = (
    spark.table(SILVER_PORTFOLIO_TABLE)
    .select(
        F.col("portfolio_id").alias("current_portfolio_id"),
        F.col("config_version").alias("current_config_version"),
        F.col("record_hash").alias("current_record_hash"),
    )
    .withColumn(
        "current_version_major",
        F.expr(
            "TRY_CAST("
            "ELEMENT_AT(SPLIT(current_config_version, '[.]'), 1) "
            "AS BIGINT)"
        ),
    )
    .withColumn(
        "current_version_minor",
        F.expr(
            "TRY_CAST("
            "ELEMENT_AT(SPLIT(current_config_version, '[.]'), 2) "
            "AS BIGINT)"
        ),
    )
    .withColumn(
        "current_version_patch",
        F.expr(
            "TRY_CAST("
            "ELEMENT_AT(SPLIT(current_config_version, '[.]'), 3) "
            "AS BIGINT)"
        ),
    )
)

current_silver_portfolios.createOrReplaceTempView(
    "phase_05_current_silver_portfolios"
)

versioned_candidates = spark.sql(
    """
    SELECT
        candidate.*,
        TRY_CAST(
            ELEMENT_AT(SPLIT(config_version, '[.]'), 1)
            AS BIGINT
        ) AS candidate_version_major,
        TRY_CAST(
            ELEMENT_AT(SPLIT(config_version, '[.]'), 2)
            AS BIGINT
        ) AS candidate_version_minor,
        TRY_CAST(
            ELEMENT_AT(SPLIT(config_version, '[.]'), 3)
            AS BIGINT
        ) AS candidate_version_patch
    FROM phase_05_same_batch_portfolio_candidates AS candidate
    WHERE same_batch_outcome IS NULL
    """
)

versioned_candidates.createOrReplaceTempView(
    "phase_05_versioned_portfolio_candidates"
)

canonical_comparisons = spark.sql(
    f"""
    SELECT
        candidate.*,
        current.current_portfolio_id,
        current.current_config_version,
        current.current_record_hash,
        current.current_version_major,
        current.current_version_minor,
        current.current_version_patch,
        CASE
            WHEN current.current_portfolio_id IS NULL
                THEN '{ACCEPTED_NEW_OUTCOME}'
            WHEN candidate.config_version =
                    current.current_config_version
                 AND candidate.record_hash =
                    current.current_record_hash
                THEN '{UNCHANGED_OUTCOME}'
            WHEN (
                candidate_version_major > current_version_major
                OR (
                    candidate_version_major =
                        current_version_major
                    AND candidate_version_minor >
                        current_version_minor
                )
                OR (
                    candidate_version_major =
                        current_version_major
                    AND candidate_version_minor =
                        current_version_minor
                    AND candidate_version_patch >
                        current_version_patch
                )
            )
                THEN '{ACCEPTED_CORRECTION_OUTCOME}'
            ELSE '{REJECTED_OUTCOME}'
        END AS canonical_comparison_outcome,
        CASE
            WHEN current.current_portfolio_id IS NULL
                THEN NULL
            WHEN candidate.config_version =
                    current.current_config_version
                 AND candidate.record_hash =
                    current.current_record_hash
                THEN 'PORTFOLIO_LATER_BATCH_UNCHANGED'
            WHEN (
                candidate_version_major > current_version_major
                OR (
                    candidate_version_major =
                        current_version_major
                    AND candidate_version_minor >
                        current_version_minor
                )
                OR (
                    candidate_version_major =
                        current_version_major
                    AND candidate_version_minor =
                        current_version_minor
                    AND candidate_version_patch >
                        current_version_patch
                )
            )
                THEN 'PORTFOLIO_LATER_BATCH_CORRECTION'
            ELSE 'PORTFOLIO_INVALID_CORRECTION'
        END AS canonical_comparison_rule_id
    FROM phase_05_versioned_portfolio_candidates AS candidate
    LEFT JOIN phase_05_current_silver_portfolios AS current
        ON candidate.portfolio_id =
            current.current_portfolio_id
    """
)

canonical_comparisons.createOrReplaceTempView(
    "phase_05_canonical_portfolio_comparisons"
)

print("canonical_comparison=PASS")

canonical_comparisons.select(
    "portfolio_id",
    "config_version",
    "current_config_version",
    "candidate_version_major",
    "candidate_version_minor",
    "candidate_version_patch",
    "canonical_comparison_outcome",
    "canonical_comparison_rule_id",
).orderBy(
    "portfolio_id",
).show(truncate=False)

# COMMAND ----------

same_batch_violations = spark.sql(
    f"""
    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_SAME_BATCH_IDENTICAL_DUPLICATE'
            AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'WARNING' AS severity,
        'WARN_AND_DEDUPLICATE' AS disposition,
        CAST(NULL AS STRING) AS affected_field,
        record_hash AS observed_value,
        'Identical rows must keep the lowest source row number.'
            AS expected_condition,
        'This identical nonwinning row was deduplicated.'
            AS message
    FROM phase_05_same_batch_portfolio_candidates
    WHERE same_batch_outcome = 'DEDUPLICATED'

    UNION ALL

    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_SAME_BATCH_CONFLICT' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        CAST(NULL AS STRING) AS affected_field,
        record_hash AS observed_value,
        'One portfolio_id must not have differing hashes in one batch.'
            AS expected_condition,
        'Conflicting same-batch portfolio records were rejected.'
            AS message
    FROM phase_05_same_batch_portfolio_candidates
    WHERE same_batch_outcome = 'REJECTED'
    """
)

invalid_correction_violations = spark.sql(
    f"""
    SELECT
        source_record_id,
        batch_id,
        source_row_number,
        source_record_sha256,
        portfolio_id,
        'PORTFOLIO_INVALID_CORRECTION' AS rule_id,
        '{PORTFOLIO_CONTRACT_VERSION}' AS rule_version,
        'ERROR' AS severity,
        'REJECT' AS disposition,
        'config_version' AS affected_field,
        config_version AS observed_value,
        'Changed values require a higher semantic config_version.'
            AS expected_condition,
        'The candidate cannot replace the current canonical record.'
            AS message
    FROM phase_05_canonical_portfolio_comparisons
    WHERE canonical_comparison_rule_id =
        'PORTFOLIO_INVALID_CORRECTION'
    """
)

complete_rule_violations = (
    all_rule_violations
    .unionByName(same_batch_violations)
    .unionByName(invalid_correction_violations)
)

complete_rule_violations.createOrReplaceTempView(
    "phase_05_complete_portfolio_rule_violations"
)

violation_summary = spark.sql(
    """
    SELECT
        source_record_id,
        COUNT(*) AS violation_count,
        SUM(
            CASE
                WHEN severity = 'WARNING' THEN 1
                ELSE 0
            END
        ) AS warning_count,
        SUM(
            CASE
                WHEN severity IN ('ERROR', 'CRITICAL') THEN 1
                ELSE 0
            END
        ) AS violation_error_count
    FROM phase_05_complete_portfolio_rule_violations
    GROUP BY source_record_id
    """
)

violation_summary.createOrReplaceTempView(
    "phase_05_portfolio_violation_summary"
)

final_record_outcomes = spark.sql(
    """
    SELECT
        candidate.source_record_id,
        candidate.batch_id,
        candidate.source_row_number,
        candidate.source_record_sha256,
        candidate.portfolio_id,
        COALESCE(summary.violation_count, 0)
            AS violation_count,
        COALESCE(summary.warning_count, 0)
            AS warning_count,
        COALESCE(summary.violation_error_count, 0)
            AS violation_error_count,
        candidate.same_batch_outcome,
        comparison.canonical_comparison_outcome,
        CASE
            WHEN COALESCE(
                summary.violation_error_count,
                0
            ) > 0
                THEN 'REJECTED'
            WHEN candidate.same_batch_outcome = 'REJECTED'
                THEN 'REJECTED'
            WHEN candidate.same_batch_outcome = 'DEDUPLICATED'
                THEN 'DEDUPLICATED'
            ELSE comparison.canonical_comparison_outcome
        END AS final_outcome
    FROM phase_05_same_batch_portfolio_candidates AS candidate
    LEFT JOIN phase_05_canonical_portfolio_comparisons AS comparison
        ON candidate.source_record_id =
            comparison.source_record_id
    LEFT JOIN phase_05_portfolio_violation_summary AS summary
        ON candidate.source_record_id =
            summary.source_record_id
    """
)

final_record_outcomes.createOrReplaceTempView(
    "phase_05_final_portfolio_record_outcomes"
)

final_outcome_count = final_record_outcomes.count()
final_distinct_source_count = (
    final_record_outcomes
    .select("source_record_id")
    .distinct()
    .count()
)
null_final_outcome_count = (
    final_record_outcomes
    .where(F.col("final_outcome").isNull())
    .count()
)

if final_outcome_count != evaluated_count:
    raise ValueError(
        "Every evaluated Bronze row must produce one final outcome"
    )

if final_distinct_source_count != evaluated_count:
    raise ValueError(
        "Each source record must appear exactly once in final outcomes"
    )

if null_final_outcome_count != 0:
    raise ValueError("Every source record must have a final outcome")

print("final_outcome_reconciliation=PASS")
print(f"final_outcome_count={final_outcome_count}")
print(
    "final_distinct_source_count="
    f"{final_distinct_source_count}"
)
print(f"null_final_outcome_count={null_final_outcome_count}")

final_record_outcomes.orderBy(
    "source_row_number",
).show(truncate=False)

complete_rule_violations.orderBy(
    "source_row_number",
    "rule_id",
).show(truncate=False)

# COMMAND ----------

QUARANTINED_OUTCOME = "QUARANTINED"

accepted_count = (
    final_record_outcomes
    .where(
        F.col("final_outcome").isin(
            ACCEPTED_NEW_OUTCOME,
            ACCEPTED_CORRECTION_OUTCOME,
        )
    )
    .count()
)

quarantined_count = (
    final_record_outcomes
    .where(F.col("final_outcome") == QUARANTINED_OUTCOME)
    .count()
)

rejected_count = (
    final_record_outcomes
    .where(F.col("final_outcome") == REJECTED_OUTCOME)
    .count()
)

unchanged_count = (
    final_record_outcomes
    .where(F.col("final_outcome") == UNCHANGED_OUTCOME)
    .count()
)

deduplicated_count = (
    final_record_outcomes
    .where(F.col("final_outcome") == DEDUPLICATED_OUTCOME)
    .count()
)

warning_count = (
    complete_rule_violations
    .where(F.col("severity") == "WARNING")
    .count()
)

record_outcome_total = (
    accepted_count
    + quarantined_count
    + rejected_count
    + unchanged_count
    + deduplicated_count
)

if record_outcome_total != evaluated_count:
    raise ValueError(
        "Processing-run outcome counts do not reconcile "
        "to evaluated_count"
    )

print("processing_run_count_reconciliation=PASS")
print(f"evaluated_count={evaluated_count}")
print(f"accepted_count={accepted_count}")
print(f"quarantined_count={quarantined_count}")
print(f"rejected_count={rejected_count}")
print(f"unchanged_count={unchanged_count}")
print(f"deduplicated_count={deduplicated_count}")
print(f"warning_count={warning_count}")
print(f"record_outcome_total={record_outcome_total}")

# COMMAND ----------

current_canonical_snapshot = (
    spark.table(SILVER_PORTFOLIO_TABLE)
    .select(
        "portfolio_id",
        "portfolio_name",
        "strategy_code",
        "base_currency",
        "target_inception_date",
        "actual_inception_date",
        "initial_nav",
        "target_long_ratio",
        "target_short_ratio",
        "target_gross_ratio",
        "target_net_ratio",
        "rebalance_policy",
        "cash_policy",
        "is_active",
        "config_version",
        "record_hash",
    )
)

accepted_canonical_candidates = (
    canonical_comparisons.alias("comparison")
    .join(
        final_record_outcomes.alias("outcome"),
        F.col("comparison.source_record_id")
        == F.col("outcome.source_record_id"),
    )
    .where(
        F.col("outcome.final_outcome").isin(
            ACCEPTED_NEW_OUTCOME,
            ACCEPTED_CORRECTION_OUTCOME,
        )
    )
    .select(
        F.col("comparison.portfolio_id").alias("portfolio_id"),
        F.col("comparison.portfolio_name").alias("portfolio_name"),
        F.col("comparison.strategy_code").alias("strategy_code"),
        F.col("comparison.base_currency").alias("base_currency"),
        F.col("comparison.typed_target_inception_date").alias(
            "target_inception_date"
        ),
        F.col("comparison.typed_actual_inception_date").alias(
            "actual_inception_date"
        ),
        F.col("comparison.typed_initial_nav").alias("initial_nav"),
        F.col("comparison.typed_target_long_ratio").alias(
            "target_long_ratio"
        ),
        F.col("comparison.typed_target_short_ratio").alias(
            "target_short_ratio"
        ),
        F.col("comparison.typed_target_gross_ratio").alias(
            "target_gross_ratio"
        ),
        F.col("comparison.typed_target_net_ratio").alias(
            "target_net_ratio"
        ),
        F.col("comparison.rebalance_policy").alias(
            "rebalance_policy"
        ),
        F.col("comparison.cash_policy").alias("cash_policy"),
        F.col("comparison.typed_is_active").alias("is_active"),
        F.col("comparison.config_version").alias("config_version"),
        F.col("comparison.record_hash").alias("record_hash"),
    )
)

accepted_portfolio_ids = accepted_canonical_candidates.select(
    "portfolio_id"
)

retained_current_canonical = current_canonical_snapshot.join(
    accepted_portfolio_ids,
    on="portfolio_id",
    how="left_anti",
)

prospective_canonical_snapshot = (
    retained_current_canonical
    .unionByName(accepted_canonical_candidates)
)

canonical_before_count = current_canonical_snapshot.count()
prospective_canonical_count = (
    prospective_canonical_snapshot.count()
)
prospective_active_count = (
    prospective_canonical_snapshot
    .where(F.col("is_active") == F.lit(True))
    .count()
)
prospective_distinct_portfolio_count = (
    prospective_canonical_snapshot
    .select("portfolio_id")
    .distinct()
    .count()
)

failed_rule_ids: list[str] = []

if prospective_active_count != 2:
    failed_rule_ids.append("PORTFOLIO_ACTIVE_COUNT")

if (
    prospective_distinct_portfolio_count
    != prospective_canonical_count
):
    failed_rule_ids.append("PORTFOLIO_ID_UNIQUE")

publish_allowed = (
    rejected_count == 0
    and quarantined_count == 0
    and not failed_rule_ids
)

published = publish_allowed and accepted_count > 0

calculated_canonical_before_sha256 = (
    calculate_ordered_set_sha256(
        current_canonical_snapshot,
        order_columns=["portfolio_id"],
        value_columns=["portfolio_id", "record_hash"],
    )
)

canonical_before_sha256 = (
    calculated_canonical_before_sha256
    if canonical_before_count > 0
    else None
)

calculated_prospective_canonical_sha256 = (
    calculate_ordered_set_sha256(
        prospective_canonical_snapshot,
        order_columns=["portfolio_id"],
        value_columns=["portfolio_id", "record_hash"],
    )
)

prospective_canonical_sha256 = (
    calculated_prospective_canonical_sha256
    if prospective_canonical_count > 0
    else None
)

if not publish_allowed:
    canonical_after_count = canonical_before_count
    canonical_after_sha256 = canonical_before_sha256
else:
    canonical_after_count = prospective_canonical_count
    canonical_after_sha256 = prospective_canonical_sha256

if not publish_allowed:
    run_status = "FAILED"
elif warning_count > 0:
    run_status = "SUCCEEDED_WITH_WARNINGS"
else:
    run_status = "SUCCEEDED"

print("publication_gate_evaluation=PASS")
print(f"canonical_before_count={canonical_before_count}")
print(
    "prospective_canonical_count="
    f"{prospective_canonical_count}"
)
print(f"prospective_active_count={prospective_active_count}")
print(
    "prospective_distinct_portfolio_count="
    f"{prospective_distinct_portfolio_count}"
)
print(f"failed_rule_ids={failed_rule_ids}")
print(f"publish_allowed={publish_allowed}")
print(f"published={published}")
print(f"canonical_after_count={canonical_after_count}")
print(f"run_status={run_status}")

print(f"canonical_before_sha256={canonical_before_sha256}")
print(
    "prospective_canonical_sha256="
    f"{prospective_canonical_sha256}"
)
print(f"canonical_after_sha256={canonical_after_sha256}")

# COMMAND ----------

violation_detected_at_utc = (
    datetime.now(UTC).replace(tzinfo=None)
)
complete_violation_count = complete_rule_violations.count()

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
    F.col("batch_id").alias("batch_id"),
    F.lit("PORTFOLIOS").alias("dataset_name"),
    F.col("source_record_id").alias("source_record_id"),
    F.col("source_row_number").alias("source_row_number"),
    F.col("source_record_sha256").alias(
        "source_record_sha256"
    ),
    F.col("rule_id").alias("rule_id"),
    F.col("rule_version").alias("rule_version"),
    F.col("severity").alias("severity"),
    F.col("disposition").alias("disposition"),
    F.col("affected_field").alias("affected_field"),
    F.col("observed_value").alias("observed_value"),
    F.col("expected_condition").alias(
        "expected_condition"
    ),
    F.col("message").alias("message"),
    F.lit(violation_detected_at_utc)
    .cast("timestamp")
    .alias("detected_at_utc"),
    F.lit("OPEN").alias("resolution_status"),
    F.lit(None)
    .cast("string")
    .alias("resolution_action"),
    F.lit(None)
    .cast("string")
    .alias("resolution_batch_id"),
    F.lit(None)
    .cast("timestamp")
    .alias("resolved_at_utc"),
    F.lit(None)
    .cast("string")
    .alias("resolution_note"),
    F.lit(VIOLATION_CONTRACT_VERSION).alias("contract_version")
)

violation_audit_count = violation_audit_records.count()
distinct_violation_id_count = (
    violation_audit_records
    .select("violation_id")
    .distinct()
    .count()
)

if violation_audit_count != complete_violation_count:
    raise ValueError(
        "Every rule violation must produce one audit record"
    )

if distinct_violation_id_count != violation_audit_count:
    raise ValueError(
        "Violation identifiers must be unique within the run"
    )

print("violation_audit_reconciliation=PASS")
print(f"complete_violation_count={complete_violation_count}")
print(f"violation_audit_count={violation_audit_count}")
print(
    "distinct_violation_id_count="
    f"{distinct_violation_id_count}"
)

violation_audit_records.orderBy(
    "source_row_number",
    "rule_id",
).show(truncate=False)

# COMMAND ----------

record_outcome_evaluated_at_utc = (
    datetime.now(UTC).replace(tzinfo=None)
)

record_outcome_audit_records = (
    final_record_outcomes.alias("outcome")
    .join(
        same_batch_candidates.select(
            "source_record_id",
            "record_hash",
            "duplicate_winner_source_record_id",
        ).alias("candidate"),
        F.col("outcome.source_record_id")
        == F.col("candidate.source_record_id"),
    )
    .select(
        F.sha2(
            F.concat_ws(
                "|",
                F.lit(processing_run_id),
                F.col("outcome.source_record_id"),
            ),
            256,
        ).alias("outcome_id"),
        F.lit(processing_run_id).alias("processing_run_id"),
        F.col("outcome.batch_id").alias("batch_id"),
        F.lit("PORTFOLIOS").alias("dataset_name"),
        F.col("outcome.source_record_id").alias(
            "source_record_id"
        ),
        F.col("outcome.source_row_number").alias(
            "source_row_number"
        ),
        F.col("outcome.source_record_sha256").alias(
            "source_record_sha256"
        ),
        F.col("outcome.portfolio_id").alias("portfolio_id"),
        F.col("outcome.final_outcome").alias("outcome"),
        F.col("outcome.warning_count").alias("warning_count"),
        F.col("outcome.violation_count").alias(
            "violation_count"
        ),
        F.when(
            F.col("outcome.final_outcome").isin(
                ACCEPTED_NEW_OUTCOME,
                ACCEPTED_CORRECTION_OUTCOME,
                UNCHANGED_OUTCOME,
            ),
            F.col("candidate.record_hash"),
        )
        .otherwise(F.lit(None).cast("string"))
        .alias("canonical_record_hash"),
        F.when(
            F.col("outcome.final_outcome")
            == DEDUPLICATED_OUTCOME,
            F.col(
                "candidate.duplicate_winner_source_record_id"
            ),
        )
        .otherwise(F.lit(None).cast("string"))
        .alias("deduplicated_to_source_record_id"),
        F.lit(record_outcome_evaluated_at_utc)
        .cast("timestamp")
        .alias("evaluated_at_utc"),
        F.lit(PORTFOLIO_CONTRACT_VERSION).alias(
            "dataset_contract_version"
        ),
        F.lit(OUTCOME_CONTRACT_VERSION).alias(
            "contract_version"
        ),
    )
)

record_outcome_audit_count = (
    record_outcome_audit_records.count()
)
distinct_outcome_id_count = (
    record_outcome_audit_records
    .select("outcome_id")
    .distinct()
    .count()
)
missing_deduplication_link_count = (
    record_outcome_audit_records
    .where(
        (F.col("outcome") == DEDUPLICATED_OUTCOME)
        & F.col("deduplicated_to_source_record_id").isNull()
    )
    .count()
)

if record_outcome_audit_count != evaluated_count:
    raise ValueError(
        "Every evaluated record must produce one outcome audit row"
    )

if distinct_outcome_id_count != record_outcome_audit_count:
    raise ValueError(
        "Outcome identifiers must be unique within the run"
    )

if missing_deduplication_link_count != 0:
    raise ValueError(
        "Every deduplicated record must identify its winning row"
    )

print("record_outcome_audit_reconciliation=PASS")
print(
    "record_outcome_audit_count="
    f"{record_outcome_audit_count}"
)
print(
    "distinct_outcome_id_count="
    f"{distinct_outcome_id_count}"
)
print(
    "missing_deduplication_link_count="
    f"{missing_deduplication_link_count}"
)

record_outcome_audit_records.orderBy(
    "source_row_number",
).show(truncate=False)

# COMMAND ----------

canonical_publication_timestamp_utc = (
    datetime.now(UTC).replace(tzinfo=None)
)

canonical_publication_records = (
    canonical_comparisons.alias("candidate")
    .join(
        final_record_outcomes.alias("outcome"),
        F.col("candidate.source_record_id")
        == F.col("outcome.source_record_id"),
    )
    .where(
        F.col("outcome.final_outcome").isin(
            ACCEPTED_NEW_OUTCOME,
            ACCEPTED_CORRECTION_OUTCOME,
        )
    )
    .select(
        F.col("candidate.portfolio_id").alias("portfolio_id"),
        F.col("candidate.portfolio_name").alias(
            "portfolio_name"
        ),
        F.col("candidate.strategy_code").alias("strategy_code"),
        F.col("candidate.base_currency").alias("base_currency"),
        F.col("candidate.typed_target_inception_date").alias(
            "target_inception_date"
        ),
        F.col("candidate.typed_actual_inception_date").alias(
            "actual_inception_date"
        ),
        F.col("candidate.typed_initial_nav").alias("initial_nav"),
        F.col("candidate.typed_target_long_ratio").alias(
            "target_long_ratio"
        ),
        F.col("candidate.typed_target_short_ratio").alias(
            "target_short_ratio"
        ),
        F.col("candidate.typed_target_gross_ratio").alias(
            "target_gross_ratio"
        ),
        F.col("candidate.typed_target_net_ratio").alias(
            "target_net_ratio"
        ),
        F.col("candidate.rebalance_policy").alias(
            "rebalance_policy"
        ),
        F.col("candidate.cash_policy").alias("cash_policy"),
        F.col("candidate.typed_is_active").alias("is_active"),
        F.col("candidate.config_version").alias("config_version"),
        F.col("candidate.record_hash").alias("record_hash"),
        F.lit(processing_run_id).alias("processing_run_id"),
        F.col("candidate.batch_id").alias("source_batch_id"),
        F.col("candidate.source_record_id").alias(
            "source_record_id"
        ),
        F.col("candidate.source_row_number").alias(
            "source_row_number"
        ),
        F.col("candidate.source_record_sha256").alias(
            "source_record_sha256"
        ),
        F.lit(canonical_publication_timestamp_utc)
        .cast("timestamp")
        .alias("canonicalized_at_utc"),
        F.lit(PORTFOLIO_CONTRACT_VERSION).alias(
            "dataset_contract_version"
        ),
    )
)

canonical_publication_count = (
    canonical_publication_records.count()
)
distinct_publication_portfolio_count = (
    canonical_publication_records
    .select("portfolio_id")
    .distinct()
    .count()
)

if canonical_publication_count != accepted_count:
    raise ValueError(
        "Canonical publication rows must equal accepted_count"
    )

if (
    distinct_publication_portfolio_count
    != canonical_publication_count
):
    raise ValueError(
        "Canonical publication records must have unique keys"
    )

print("canonical_publication_reconciliation=PASS")
print(
    "canonical_publication_count="
    f"{canonical_publication_count}"
)
print(
    "distinct_publication_portfolio_count="
    f"{distinct_publication_portfolio_count}"
)

canonical_publication_records.orderBy(
    "portfolio_id",
).show(truncate=False)


# COMMAND ----------

insert_only_audit_records(
    violation_audit_records,
    SILVER_VIOLATION_TABLE,
    "violation_id",
)

insert_only_audit_records(
    record_outcome_audit_records,
    SILVER_OUTCOME_TABLE,
    "outcome_id",
)

if publish_allowed and accepted_count > 0:
    canonical_delta_table = DeltaTable.forName(
        spark,
        SILVER_PORTFOLIO_TABLE,
    )

    (
        canonical_delta_table.merge(
            canonical_publication_records.alias("source"),
            "target.portfolio_id = source.portfolio_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

canonical_after_verification = spark.table(
    SILVER_PORTFOLIO_TABLE
)
canonical_after_verification_count = (
    canonical_after_verification.count()
)

calculated_after_verification_sha256 = (
    calculate_ordered_set_sha256(
        canonical_after_verification,
        order_columns=["portfolio_id"],
        value_columns=["portfolio_id", "record_hash"],
    )
)

canonical_after_verification_sha256 = (
    calculated_after_verification_sha256
    if canonical_after_verification_count > 0
    else None
)

if canonical_after_verification_count != canonical_after_count:
    raise ValueError(
        "Published canonical count does not match its expected value"
    )

if (
    canonical_after_verification_sha256
    != canonical_after_sha256
):
    raise ValueError(
        "Published canonical hash does not match its expected value"
    )

published_at_utc = (
    canonical_publication_timestamp_utc
    if published
    else None
)
processing_run_completed_at_utc = (
    datetime.now(UTC).replace(tzinfo=None)
)

if run_status == "FAILED":
    processing_error_code = "QUALITY_GATE_FAILED"
    processing_error_message = (
        "Silver publication was blocked by record or "
        "dataset quality controls."
    )
else:
    processing_error_code = None
    processing_error_message = None

processing_run_audit_records = build_processing_run_audit_records(
    completed_at_utc=processing_run_completed_at_utc,
    status=run_status,
    published_value=published,
    published_at_utc=published_at_utc,
    final_canonical_after_count=(
        canonical_after_verification_count
    ),
    final_canonical_after_sha256=(
        canonical_after_verification_sha256
    ),
    error_code=processing_error_code,
    error_message=processing_error_message,
)

processing_run_audit_count = (
    processing_run_audit_records.count()
)

if processing_run_audit_count != 1:
    raise ValueError(
        "One processing attempt must produce one run audit row"
    )

insert_only_audit_records(
    processing_run_audit_records,
    SILVER_PROCESSING_RUN_TABLE,
    "processing_run_id",
)

print("silver_persistence=PASS")
print(f"processing_run_id={processing_run_id}")
print(f"run_status={run_status}")
print(f"published={published}")
print(
    "canonical_after_verification_count="
    f"{canonical_after_verification_count}"
)
print(
    "canonical_after_verification_sha256="
    f"{canonical_after_verification_sha256}"
)
