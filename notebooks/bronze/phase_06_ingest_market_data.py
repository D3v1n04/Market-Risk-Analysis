# Databricks notebook source
# MAGIC %run ./phase_06_validate_market_landing

# COMMAND ----------

"""Ingest validated Phase 06 market data into Bronze."""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from phase_06_validate_market_landing import (
        LANDING_ROOT,
        MARKET_SOURCES,
        calculate_bytes_sha256,
        load_and_validate_landing,
        require_equal,
        validate_common_records,
        validate_corporate_actions,
        validate_daily_prices,
    )

BRONZE_BATCH_TABLE = (
    "workspace.devin_market_risk_dev.bronze_ingestion_batches"
)
INGESTION_CONTRACT_VERSION = "2.0.0"

APPROVED_DATASETS = {
    "DAILY_PRICES": {
        "bronze_table": (
            "workspace.devin_market_risk_dev.bronze_daily_prices"
        ),
        "expected_record_count": 60,
    },
    "CORPORATE_ACTIONS": {
        "bronze_table": (
            "workspace.devin_market_risk_dev.bronze_corporate_actions"
        ),
        "expected_record_count": 2,
    },
}


def validate_approved_specifications() -> None:
    """Restrict runtime writes to the reviewed market datasets."""
    specifications_by_name = {
        specification["dataset_name"]: specification
        for specification in MARKET_SOURCES
    }
    require_equal(
        "approved market dataset names",
        set(specifications_by_name),
        set(APPROVED_DATASETS),
    )

    for dataset_name, approval in APPROVED_DATASETS.items():
        specification = specifications_by_name[dataset_name]
        require_equal(
            f"{dataset_name} approved Bronze table",
            specification["bronze_table"],
            approval["bronze_table"],
        )
        require_equal(
            f"{dataset_name} approved record count",
            specification["expected_record_count"],
            approval["expected_record_count"],
        )


def build_ingestion_candidates(
    *,
    spark: SparkSession,
    specification: dict[str, Any],
    manifest: dict[str, Any],
    source_rows: list[dict[str, str]],
    raw_records: list[str],
    batch_id: str,
    started_at_utc: datetime,
) -> DataFrame:
    """Create source-aligned Bronze candidates with fixed batch lineage."""
    bronze_schema = spark.table(specification["bronze_table"]).schema
    expected_columns = set(bronze_schema.fieldNames())
    candidate_records: list[dict[str, Any]] = []

    for source_row_number, (source_values, raw_record) in enumerate(
        zip(source_rows, raw_records, strict=True),
        start=1,
    ):
        lineage_values = {
            "batch_id": batch_id,
            "source_id": manifest["source_id"],
            "source_object_path": manifest["source_object_path"],
            "source_sha256": manifest["source_sha256"],
            "source_row_number": source_row_number,
            "source_record_sha256": calculate_bytes_sha256(
                raw_record.encode("utf-8")
            ),
            "raw_record": raw_record,
            "ingested_at_utc": started_at_utc,
            "contract_version": manifest["source_contract_version"],
        }
        candidate_record = source_values | lineage_values
        require_equal(
            "candidate record columns",
            set(candidate_record),
            expected_columns,
        )
        candidate_records.append(candidate_record)

    candidates = spark.createDataFrame(candidate_records, schema=bronze_schema)
    require_equal(
        "candidate row count",
        candidates.count(),
        specification["expected_record_count"],
    )
    require_equal(
        "candidate DataFrame columns",
        set(candidates.columns),
        expected_columns,
    )
    require_equal(
        "candidate batch lineage",
        {
            row["batch_id"]
            for row in candidates.select("batch_id").distinct().collect()
        },
        {batch_id},
    )
    return candidates


def build_ingestion_plan(
    *,
    spark: SparkSession,
    prepared_source: dict[str, Any],
) -> dict[str, Any]:
    """Choose a first-write or audited duplicate-skip outcome."""
    specification = prepared_source["specification"]
    manifest = prepared_source["manifest"]
    batch_id = prepared_source["batch_id"]
    source_row_count = len(prepared_source["source_rows"])

    successful_batches = (
        spark.table(BRONZE_BATCH_TABLE)
        .where(
            (F.col("dataset_name") == manifest["dataset_name"])
            & (F.col("source_id") == manifest["source_id"])
            & (F.col("source_sha256") == manifest["source_sha256"])
            & F.col("status").isin(
                "SUCCEEDED",
                "SUCCEEDED_WITH_WARNINGS",
            )
        )
        .select("batch_id")
        .distinct()
        .collect()
    )
    if len(successful_batches) > 1:
        raise ValueError(
            "More than one successful batch exists for identical "
            f"{manifest['dataset_name']} source content"
        )

    previous_successful_batch_id = (
        successful_batches[0]["batch_id"]
        if successful_batches
        else None
    )
    existing_source_rows = (
        spark.table(specification["bronze_table"])
        .where(
            (F.col("source_id") == manifest["source_id"])
            & (F.col("source_sha256") == manifest["source_sha256"])
        )
    )
    existing_source_count = existing_source_rows.count()
    existing_source_batch_ids = {
        row["batch_id"]
        for row in existing_source_rows.select("batch_id")
        .distinct()
        .collect()
    }

    if previous_successful_batch_id is None:
        require_equal(
            "unexpected pre-existing source rows",
            existing_source_count,
            0,
        )
        require_equal(
            "unexpected pre-existing source batch IDs",
            existing_source_batch_ids,
            set(),
        )
        batch_status = "SUCCEEDED"
        accepted_count = source_row_count
        deduplicated_count = 0
        duplicate_of_batch_id = None
        should_write_business_rows = True
    else:
        require_equal(
            "existing duplicate source row count",
            existing_source_count,
            source_row_count,
        )
        require_equal(
            "existing duplicate source batch IDs",
            existing_source_batch_ids,
            {previous_successful_batch_id},
        )
        batch_status = "SKIPPED_DUPLICATE"
        accepted_count = 0
        deduplicated_count = source_row_count
        duplicate_of_batch_id = previous_successful_batch_id
        should_write_business_rows = False

    require_equal(
        "candidate batch count reconciliation",
        source_row_count,
        accepted_count + deduplicated_count,
    )

    completed_at_utc = datetime.now(UTC).replace(tzinfo=None)
    batch_audit_values = {
        "batch_id": batch_id,
        "dataset_name": manifest["dataset_name"],
        "source_id": manifest["source_id"],
        "batch_type": "BACKFILL",
        "trigger_type": "MANUAL",
        "retry_of_batch_id": None,
        "attempt_number": 1,
        "requested_start_date": None,
        "requested_end_date": None,
        "started_at_utc": prepared_source["started_at_utc"],
        "completed_at_utc": completed_at_utc,
        "status": batch_status,
        "received_count": source_row_count,
        "accepted_count": accepted_count,
        "quarantined_count": 0,
        "rejected_count": 0,
        "unchanged_count": 0,
        "deduplicated_count": deduplicated_count,
        "warning_count": 0,
        "manifest_sha256": prepared_source["manifest_sha256"],
        "error_code": None,
        "error_message": None,
        "contract_version": INGESTION_CONTRACT_VERSION,
        "source_object_path": manifest["source_object_path"],
        "source_sha256": manifest["source_sha256"],
        "duplicate_of_batch_id": duplicate_of_batch_id,
    }
    batch_schema = spark.table(BRONZE_BATCH_TABLE).schema
    batch_audit_candidate = spark.createDataFrame(
        [batch_audit_values],
        schema=batch_schema,
    )
    require_equal(
        "candidate batch audit row count",
        batch_audit_candidate.count(),
        1,
    )
    require_equal(
        "candidate batch ID already persisted",
        spark.table(BRONZE_BATCH_TABLE)
        .where(F.col("batch_id") == batch_id)
        .count(),
        0,
    )

    return {
        **prepared_source,
        "batch_status": batch_status,
        "accepted_count": accepted_count,
        "deduplicated_count": deduplicated_count,
        "duplicate_of_batch_id": duplicate_of_batch_id,
        "previous_successful_batch_id": previous_successful_batch_id,
        "should_write_business_rows": should_write_business_rows,
        "batch_audit_candidate": batch_audit_candidate,
    }


def reconcile_persisted_batch(
    *,
    spark: SparkSession,
    plan: dict[str, Any],
) -> None:
    """Verify the exact persisted audit values for one attempt."""
    manifest = plan["manifest"]
    batch_id = plan["batch_id"]
    persisted_batch_rows = (
        spark.table(BRONZE_BATCH_TABLE)
        .where(F.col("batch_id") == batch_id)
        .collect()
    )
    require_equal(
        "persisted candidate batch audit count",
        len(persisted_batch_rows),
        1,
    )
    persisted_batch = persisted_batch_rows[0].asDict()
    expected_values = {
        "dataset_name": manifest["dataset_name"],
        "source_id": manifest["source_id"],
        "status": plan["batch_status"],
        "received_count": len(plan["source_rows"]),
        "accepted_count": plan["accepted_count"],
        "quarantined_count": 0,
        "rejected_count": 0,
        "unchanged_count": 0,
        "deduplicated_count": plan["deduplicated_count"],
        "warning_count": 0,
        "manifest_sha256": plan["manifest_sha256"],
        "contract_version": INGESTION_CONTRACT_VERSION,
        "source_object_path": manifest["source_object_path"],
        "source_sha256": manifest["source_sha256"],
        "duplicate_of_batch_id": plan["duplicate_of_batch_id"],
    }
    for field, expected_value in expected_values.items():
        require_equal(
            f"persisted batch {field}",
            persisted_batch[field],
            expected_value,
        )


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
validate_approved_specifications()

# Complete every landing, record, and schema preflight before planning writes.
prepared_sources: list[dict[str, Any]] = []

for specification in MARKET_SOURCES:
    started_at_utc = datetime.now(UTC).replace(tzinfo=None)
    batch_id = str(uuid4())
    manifest, source_columns, source_rows, raw_records = (
        load_and_validate_landing(specification)
    )
    validate_common_records(specification, source_rows)

    if specification["dataset_name"] == "DAILY_PRICES":
        validate_daily_prices(source_rows)
    elif specification["dataset_name"] == "CORPORATE_ACTIONS":
        validate_corporate_actions(source_rows)
    else:
        raise ValueError(
            f"Unsupported market dataset: {specification['dataset_name']}"
        )

    bronze_candidates = build_ingestion_candidates(
        spark=spark,
        specification=specification,
        manifest=manifest,
        source_rows=source_rows,
        raw_records=raw_records,
        batch_id=batch_id,
        started_at_utc=started_at_utc,
    )
    manifest_sha256 = calculate_bytes_sha256(
        (
            LANDING_ROOT
            / specification["dataset_slug"]
            / specification["source_sha256"]
            / "manifest.json"
        ).read_bytes()
    )
    prepared_sources.append(
        {
            "specification": specification,
            "manifest": manifest,
            "source_rows": source_rows,
            "manifest_sha256": manifest_sha256,
            "batch_id": batch_id,
            "started_at_utc": started_at_utc,
            "bronze_candidates": bronze_candidates,
        }
    )

    print(f"{specification['dataset_name']} preflight_validation=PASS")
    print(f"candidate_batch_id={batch_id}")
    print(f"candidate_row_count={len(source_rows)}")
    bronze_candidates.select(
        *specification["preview_columns"],
        "batch_id",
        "source_row_number",
        "source_record_sha256",
    ).show(5, truncate=False)


# COMMAND ----------

# Complete both idempotency decisions before performing either dataset write.
ingestion_plans = [
    build_ingestion_plan(
        spark=spark,
        prepared_source=prepared_source,
    )
    for prepared_source in prepared_sources
]

for plan in ingestion_plans:
    dataset_name = plan["manifest"]["dataset_name"]
    print(f"{dataset_name} batch_decision_validation=PASS")
    print(f"batch_status={plan['batch_status']}")
    print(
        "should_write_business_rows="
        f"{plan['should_write_business_rows']}"
    )
    print(
        "previous_successful_batch_id="
        f"{plan['previous_successful_batch_id']}"
    )


# COMMAND ----------

approved_business_tables = {
    approval["bronze_table"] for approval in APPROVED_DATASETS.values()
}

for plan in ingestion_plans:
    specification = plan["specification"]
    manifest = plan["manifest"]
    batch_id = plan["batch_id"]
    bronze_table = specification["bronze_table"]
    expected_record_count = specification["expected_record_count"]

    require_equal(
        "approved Bronze business table",
        bronze_table in approved_business_tables,
        True,
    )

    if plan["should_write_business_rows"]:
        (
            plan["bronze_candidates"].write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table)
        )

    (
        plan["batch_audit_candidate"].write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_BATCH_TABLE)
    )

    persisted_source_rows = (
        spark.table(bronze_table)
        .where(
            (F.col("source_id") == manifest["source_id"])
            & (F.col("source_sha256") == manifest["source_sha256"])
        )
    )
    persisted_source_count = persisted_source_rows.count()
    persisted_business_batch_ids = {
        row["batch_id"]
        for row in persisted_source_rows.select("batch_id")
        .distinct()
        .collect()
    }
    expected_business_batch_id = (
        batch_id
        if plan["should_write_business_rows"]
        else plan["previous_successful_batch_id"]
    )

    require_equal(
        "persisted source row count",
        persisted_source_count,
        expected_record_count,
    )
    require_equal(
        "persisted business batch lineage",
        persisted_business_batch_ids,
        {expected_business_batch_id},
    )
    reconcile_persisted_batch(spark=spark, plan=plan)

    print(f"{manifest['dataset_name']} bronze_persistence=PASS")
    print(f"persisted_batch_id={batch_id}")
    print(f"persisted_batch_status={plan['batch_status']}")
    print(f"persisted_source_count={persisted_source_count}")
    print(f"persisted_accepted_count={plan['accepted_count']}")
    print(
        "persisted_deduplicated_count="
        f"{plan['deduplicated_count']}"
    )
