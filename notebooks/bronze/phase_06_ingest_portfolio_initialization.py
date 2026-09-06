# Databricks notebook source
"""Ingest the governed Phase 06 portfolio-inception correction into Bronze."""

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_SHA256 = (
    "0c36be496eed0d124fd3649f38d926d5"
    "93f10e37fa61ff3fb101f68e2f21dfbd"
)

LANDING_DIRECTORY = (
    Path("/Volumes/workspace/devin_market_risk_dev/bronze_landing")
    / "portfolios"
    / SOURCE_SHA256
)
SOURCE_PATH = LANDING_DIRECTORY / "portfolios_initialization.csv"
MANIFEST_PATH = LANDING_DIRECTORY / "manifest.json"


def calculate_bytes_sha256(payload: bytes) -> str:
    """Return the SHA-256 digest of bytes already loaded in memory."""
    return hashlib.sha256(payload).hexdigest()


def require_equal(label: str, actual: Any, expected: Any) -> None:
    """Raise a clear error when landed evidence does not match the manifest."""
    if actual != expected:
        raise ValueError(
            f"{label} mismatch: expected {expected!r}, found {actual!r}"
        )


def load_and_validate_landing() -> tuple[
    dict[str, Any],
    list[str],
    list[list[str]],
    list[str],
    str,
]:
    """Load the landed files and verify their manifest evidence."""
    source_payload = SOURCE_PATH.read_bytes()
    manifest_payload = MANIFEST_PATH.read_bytes()

    try:
        source_text = source_payload.decode("utf-8")
        manifest = json.loads(manifest_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Landed source or manifest is not valid UTF-8") from exc

    if not isinstance(manifest, dict):
        raise ValueError("Manifest must contain one JSON object")

    reader = csv.reader(StringIO(source_text))

    try:
        source_columns = next(reader)
    except StopIteration as exc:
        raise ValueError("Landed CSV source is empty") from exc

    source_rows = list(reader)

    for physical_line, row in enumerate(source_rows, start=2):
        if len(row) != len(source_columns):
            raise ValueError(
                "Landed CSV record width does not match the header at "
                f"physical line {physical_line}"
            )

    source_lines = source_text.splitlines()
    if len(source_lines) != len(source_rows) + 1:
        raise ValueError(
            "Multiline CSV records are not supported by this fixture ingestion"
        )

    raw_records = source_lines[1:]
    actual_source_sha256 = calculate_bytes_sha256(source_payload)
    manifest_sha256 = calculate_bytes_sha256(manifest_payload)

    require_equal(
        "source SHA-256",
        actual_source_sha256,
        SOURCE_SHA256,
    )
    require_equal(
        "manifest source SHA-256",
        manifest.get("source_sha256"),
        actual_source_sha256,
    )
    require_equal(
        "source size",
        manifest.get("source_size_bytes"),
        len(source_payload),
    )
    require_equal(
        "source record count",
        manifest.get("source_record_count"),
        len(source_rows),
    )
    require_equal(
        "source columns",
        manifest.get("source_columns"),
        source_columns,
    )
    require_equal(
        "landed object path",
        manifest.get("landed_object_path"),
        SOURCE_PATH.as_posix(),
    )
    require_equal("dataset name", manifest.get("dataset_name"), "PORTFOLIOS")
    require_equal(
        "source identifier",
        manifest.get("source_id"),
        "PROJECT_GIT_FIXTURE",
    )
    require_equal(
        "source contract version",
        manifest.get("source_contract_version"),
        "1.1.0",
    )

    return (
        manifest,
        source_columns,
        source_rows,
        raw_records,
        manifest_sha256,
    )


(
    manifest,
    source_columns,
    source_rows,
    raw_records,
    manifest_sha256,
) = load_and_validate_landing()

print("landing_validation=PASS")
print(f"dataset_name={manifest['dataset_name']}")
print(f"source_column_count={len(source_columns)}")
print(f"source_record_count={len(source_rows)}")
print(f"raw_record_count={len(raw_records)}")
print(f"source_sha256={manifest['source_sha256']}")
print(f"manifest_sha256={manifest_sha256}")


# COMMAND ----------

BRONZE_PORTFOLIO_TABLE = (
    "workspace.devin_market_risk_dev.bronze_portfolios"
)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

candidate_batch_id = str(uuid4())
ingested_at_utc = datetime.now(UTC).replace(tzinfo=None)

portfolio_schema = spark.table(BRONZE_PORTFOLIO_TABLE).schema

bronze_records: list[dict[str, Any]] = []

for source_row_number, (source_row, raw_record) in enumerate(
    zip(source_rows, raw_records, strict=True),
    start=1,
):
    source_values = dict(
        zip(source_columns, source_row, strict=True)
    )

    lineage_values = {
        "batch_id": candidate_batch_id,
        "source_id": manifest["source_id"],
        "source_object_path": manifest["source_object_path"],
        "source_sha256": manifest["source_sha256"],
        "source_row_number": source_row_number,
        "source_record_sha256": calculate_bytes_sha256(
            raw_record.encode("utf-8")
        ),
        "raw_record": raw_record,
        "ingested_at_utc": ingested_at_utc,
        "contract_version": manifest["source_contract_version"],
    }

    bronze_records.append(source_values | lineage_values)

bronze_portfolio_candidates = spark.createDataFrame(
    bronze_records,
    schema=portfolio_schema,
)

candidate_row_count = bronze_portfolio_candidates.count()

require_equal(
    "candidate Bronze row count",
    candidate_row_count,
    len(source_rows),
)

print("dataframe_validation=PASS")
print(f"candidate_batch_id={candidate_batch_id}")
print(f"candidate_row_count={candidate_row_count}")

bronze_portfolio_candidates.select(
    "portfolio_id",
    "portfolio_name",
    "actual_inception_date",
    "batch_id",
    "source_row_number",
    "source_record_sha256",
).show(truncate=False)


# COMMAND ----------

BRONZE_BATCH_TABLE = (
    "workspace.devin_market_risk_dev.bronze_ingestion_batches"
)

successful_batch_rows = (
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
    .orderBy(F.col("completed_at_utc").asc())
    .select("batch_id")
    .limit(1)
    .collect()
)

previous_successful_batch_id = (
    successful_batch_rows[0]["batch_id"]
    if successful_batch_rows
    else None
)

if previous_successful_batch_id is None:
    batch_status = "SUCCEEDED"
    accepted_count = len(source_rows)
    deduplicated_count = 0
    duplicate_of_batch_id = None
    should_write_portfolios = True
else:
    batch_status = "SKIPPED_DUPLICATE"
    accepted_count = 0
    deduplicated_count = len(source_rows)
    duplicate_of_batch_id = previous_successful_batch_id
    should_write_portfolios = False

completed_at_utc = datetime.now(UTC).replace(tzinfo=None)

batch_audit_values = {
    "batch_id": candidate_batch_id,
    "dataset_name": manifest["dataset_name"],
    "source_id": manifest["source_id"],
    "batch_type": "INCREMENTAL",
    "trigger_type": "MANUAL",
    "retry_of_batch_id": None,
    "attempt_number": 1,
    "requested_start_date": None,
    "requested_end_date": None,
    "started_at_utc": ingested_at_utc,
    "completed_at_utc": completed_at_utc,
    "status": batch_status,
    "received_count": len(source_rows),
    "accepted_count": accepted_count,
    "quarantined_count": 0,
    "rejected_count": 0,
    "unchanged_count": 0,
    "deduplicated_count": deduplicated_count,
    "warning_count": 0,
    "manifest_sha256": manifest_sha256,
    "error_code": None,
    "error_message": None,
    "contract_version": "1.1.0",
    "source_object_path": manifest["source_object_path"],
    "source_sha256": manifest["source_sha256"],
    "duplicate_of_batch_id": duplicate_of_batch_id,
}

batch_audit_schema = spark.table(BRONZE_BATCH_TABLE).schema

batch_audit_candidate = spark.createDataFrame(
    [batch_audit_values],
    schema=batch_audit_schema,
)

require_equal(
    "candidate batch audit row count",
    batch_audit_candidate.count(),
    1,
)

print("batch_decision_validation=PASS")
print(f"batch_status={batch_status}")
print(f"should_write_portfolios={should_write_portfolios}")
print(f"previous_successful_batch_id={previous_successful_batch_id}")

batch_audit_candidate.select(
    "batch_id",
    "status",
    "received_count",
    "accepted_count",
    "deduplicated_count",
    "duplicate_of_batch_id",
).show(truncate=False)


# COMMAND ----------

existing_candidate_batch_count = (
    spark.table(BRONZE_BATCH_TABLE)
    .where(F.col("batch_id") == candidate_batch_id)
    .count()
)

require_equal(
    "existing candidate batch ID count",
    existing_candidate_batch_count,
    0,
)

existing_portfolio_source_count = (
    spark.table(BRONZE_PORTFOLIO_TABLE)
    .where(F.col("source_sha256") == manifest["source_sha256"])
    .count()
)

if should_write_portfolios:
    require_equal(
        "pre-write portfolio source count",
        existing_portfolio_source_count,
        0,
    )
else:
    require_equal(
        "existing duplicate portfolio source count",
        existing_portfolio_source_count,
        len(source_rows),
    )

if should_write_portfolios:
    (
        bronze_portfolio_candidates.write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_PORTFOLIO_TABLE)
    )

persisted_batch_audit = batch_audit_candidate.withColumn(
    "completed_at_utc",
    F.current_timestamp(),
)

(
    persisted_batch_audit.write
    .format("delta")
    .mode("append")
    .saveAsTable(BRONZE_BATCH_TABLE)
)

persisted_portfolio_source_count = (
    spark.table(BRONZE_PORTFOLIO_TABLE)
    .where(F.col("source_sha256") == manifest["source_sha256"])
    .count()
)

persisted_candidate_batch_count = (
    spark.table(BRONZE_BATCH_TABLE)
    .where(F.col("batch_id") == candidate_batch_id)
    .count()
)

require_equal(
    "persisted portfolio source count",
    persisted_portfolio_source_count,
    len(source_rows),
)
require_equal(
    "persisted candidate batch count",
    persisted_candidate_batch_count,
    1,
)

print("bronze_persistence=PASS")
print(f"persisted_batch_id={candidate_batch_id}")
print(f"persisted_batch_status={batch_status}")
print(
    "persisted_portfolio_source_count="
    f"{persisted_portfolio_source_count}"
)
