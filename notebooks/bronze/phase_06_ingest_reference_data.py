# Databricks notebook source
"""Ingest validated Phase 06 reference fixtures into Bronze."""

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LANDING_ROOT = Path(
    "/Volumes/workspace/devin_market_risk_dev/bronze_landing"
)
BRONZE_BATCH_TABLE = (
    "workspace.devin_market_risk_dev.bronze_ingestion_batches"
)
INGESTION_CONTRACT_VERSION = "2.0.0"

REFERENCE_SOURCES = (
    {
        "dataset_name": "INSTRUMENTS",
        "dataset_slug": "instruments",
        "source_filename": "instruments.csv",
        "source_object_path": "data/fixtures/instruments.csv",
        "source_sha256": (
            "7eb9b32d1f4ef5689404b7f88c5f685a"
            "c55fa2c27ea930e8d5de35b638200b7a"
        ),
        "manifest_sha256": (
            "dd40645daf887cde5d0141e939f0ed53"
            "c60f642bc450af31fad0e44fc22c8f6c"
        ),
        "source_contract_version": "1.0.0",
        "expected_record_count": 15,
        "expected_column_count": 18,
        "bronze_table": (
            "workspace.devin_market_risk_dev."
            "bronze_instruments"
        ),
        "preview_columns": [
            "instrument_id",
            "display_symbol",
            "exchange_mic",
        ],
    },
    {
        "dataset_name": "TARGET_ALLOCATIONS",
        "dataset_slug": "target_allocations",
        "source_filename": "target_allocations.csv",
        "source_object_path": (
            "data/fixtures/target_allocations.csv"
        ),
        "source_sha256": (
            "6182be5e69859a138aa2b94c642943460"
            "aa1914439d340f68d63e4dffccb1e66"
        ),
        "manifest_sha256": (
            "6a71c37089a661a95aeea28ca27b77c6"
            "9ae745ea9fc14315e69d18b4828ba28a"
        ),
        "source_contract_version": "1.0.0",
        "expected_record_count": 30,
        "expected_column_count": 7,
        "bronze_table": (
            "workspace.devin_market_risk_dev."
            "bronze_target_allocations"
        ),
        "preview_columns": [
            "portfolio_id",
            "instrument_id",
            "target_weight",
        ],
    },
)


def calculate_bytes_sha256(payload: bytes) -> str:
    """Return the SHA-256 digest of bytes already in memory."""
    return hashlib.sha256(payload).hexdigest()


def require_equal(
    label: str,
    actual: Any,
    expected: Any,
) -> None:
    """Raise a clear error when actual evidence differs."""
    if actual != expected:
        raise ValueError(
            f"{label} mismatch: "
            f"expected {expected!r}, found {actual!r}"
        )


def load_and_validate_landing(
    specification: dict[str, Any],
) -> tuple[
    dict[str, Any],
    list[str],
    list[list[str]],
    list[str],
    str,
]:
    """Load one landed fixture and verify all pinned evidence."""
    landing_directory = (
        LANDING_ROOT
        / specification["dataset_slug"]
        / specification["source_sha256"]
    )
    source_path = (
        landing_directory
        / specification["source_filename"]
    )
    manifest_path = landing_directory / "manifest.json"

    source_payload = source_path.read_bytes()
    manifest_payload = manifest_path.read_bytes()

    try:
        source_text = source_payload.decode("utf-8")
        manifest = json.loads(
            manifest_payload.decode("utf-8")
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "Landed source or manifest is not valid UTF-8"
        ) from exc

    if not isinstance(manifest, dict):
        raise ValueError(
            "Manifest must contain one JSON object"
        )

    reader = csv.reader(StringIO(source_text))
    try:
        source_columns = next(reader)
    except StopIteration as exc:
        raise ValueError(
            "Landed CSV source is empty"
        ) from exc

    source_rows = list(reader)
    for physical_line, row in enumerate(
        source_rows,
        start=2,
    ):
        if len(row) != len(source_columns):
            raise ValueError(
                "CSV record width does not match the header "
                f"at physical line {physical_line}"
            )

    source_lines = source_text.splitlines()
    if len(source_lines) != len(source_rows) + 1:
        raise ValueError(
            "Multiline CSV records are not supported"
        )

    raw_records = source_lines[1:]
    source_sha256 = calculate_bytes_sha256(
        source_payload
    )
    manifest_sha256 = calculate_bytes_sha256(
        manifest_payload
    )

    require_equal(
        "source SHA-256",
        source_sha256,
        specification["source_sha256"],
    )
    require_equal(
        "manifest SHA-256",
        manifest_sha256,
        specification["manifest_sha256"],
    )
    require_equal(
        "manifest source SHA-256",
        manifest.get("source_sha256"),
        source_sha256,
    )
    require_equal(
        "source size",
        manifest.get("source_size_bytes"),
        len(source_payload),
    )
    require_equal(
        "source record count",
        manifest.get("source_record_count"),
        specification["expected_record_count"],
    )
    require_equal(
        "parsed source record count",
        len(source_rows),
        specification["expected_record_count"],
    )
    require_equal(
        "source column count",
        len(source_columns),
        specification["expected_column_count"],
    )
    require_equal(
        "manifest source columns",
        manifest.get("source_columns"),
        source_columns,
    )
    require_equal(
        "dataset name",
        manifest.get("dataset_name"),
        specification["dataset_name"],
    )
    require_equal(
        "source identifier",
        manifest.get("source_id"),
        "PROJECT_GIT_FIXTURE",
    )
    require_equal(
        "source object path",
        manifest.get("source_object_path"),
        specification["source_object_path"],
    )
    require_equal(
        "landed object path",
        manifest.get("landed_object_path"),
        source_path.as_posix(),
    )
    require_equal(
        "source contract version",
        manifest.get("source_contract_version"),
        specification["source_contract_version"],
    )

    return (
        manifest,
        source_columns,
        source_rows,
        raw_records,
        manifest_sha256,
    )


def build_bronze_candidates(
    *,
    spark: SparkSession,
    specification: dict[str, Any],
    manifest: dict[str, Any],
    source_columns: list[str],
    source_rows: list[list[str]],
    raw_records: list[str],
    batch_id: str,
    started_at_utc: datetime,
) -> DataFrame:
    """Create source-aligned Bronze candidates with lineage."""
    bronze_schema = spark.table(
        specification["bronze_table"]
    ).schema
    expected_columns = set(bronze_schema.fieldNames())
    candidate_records: list[dict[str, Any]] = []

    for source_row_number, (
        source_row,
        raw_record,
    ) in enumerate(
        zip(source_rows, raw_records, strict=True),
        start=1,
    ):
        source_values = dict(
            zip(
                source_columns,
                source_row,
                strict=True,
            )
        )
        lineage_values = {
            "batch_id": batch_id,
            "source_id": manifest["source_id"],
            "source_object_path": (
                manifest["source_object_path"]
            ),
            "source_sha256": manifest["source_sha256"],
            "source_row_number": source_row_number,
            "source_record_sha256": (
                calculate_bytes_sha256(
                    raw_record.encode("utf-8")
                )
            ),
            "raw_record": raw_record,
            "ingested_at_utc": started_at_utc,
            "contract_version": (
                manifest["source_contract_version"]
            ),
        }
        candidate_record = source_values | lineage_values
        require_equal(
            "candidate record columns",
            set(candidate_record),
            expected_columns,
        )
        candidate_records.append(candidate_record)

    candidates = spark.createDataFrame(
        candidate_records,
        schema=bronze_schema,
    )
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
    return candidates


def build_ingestion_plan(
    *,
    spark: SparkSession,
    prepared_source: dict[str, Any],
) -> dict[str, Any]:
    """Choose a first-write or duplicate-skip outcome."""
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
        "previous_successful_batch_id": (
            previous_successful_batch_id
        ),
        "should_write_business_rows": (
            should_write_business_rows
        ),
        "batch_audit_candidate": batch_audit_candidate,
    }


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Complete the landing and schema preflight for every dataset before writing.
prepared_sources: list[dict[str, Any]] = []

for specification in REFERENCE_SOURCES:
    started_at_utc = datetime.now(UTC).replace(tzinfo=None)
    batch_id = str(uuid4())
    (
        manifest,
        source_columns,
        source_rows,
        raw_records,
        manifest_sha256,
    ) = load_and_validate_landing(specification)
    bronze_candidates = build_bronze_candidates(
        spark=spark,
        specification=specification,
        manifest=manifest,
        source_columns=source_columns,
        source_rows=source_rows,
        raw_records=raw_records,
        batch_id=batch_id,
        started_at_utc=started_at_utc,
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

    print(
        f"{specification['dataset_name']} "
        "preflight_validation=PASS"
    )
    print(f"candidate_batch_id={batch_id}")
    print(f"candidate_row_count={len(source_rows)}")
    bronze_candidates.select(
        *specification["preview_columns"],
        "batch_id",
        "source_row_number",
        "source_record_sha256",
    ).show(5, truncate=False)


# COMMAND ----------

# Complete all idempotency decisions before performing any write.
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
    specification["bronze_table"]
    for specification in REFERENCE_SOURCES
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
    require_equal(
        "persisted candidate batch audit count",
        spark.table(BRONZE_BATCH_TABLE)
        .where(F.col("batch_id") == batch_id)
        .count(),
        1,
    )

    print(f"{manifest['dataset_name']} bronze_persistence=PASS")
    print(f"persisted_batch_id={batch_id}")
    print(f"persisted_batch_status={plan['batch_status']}")
    print(f"persisted_source_count={persisted_source_count}")
