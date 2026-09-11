# Databricks notebook source
# MAGIC %run ./phase_12_validate_yahoo_landing

# COMMAND ----------

"""Ingest one validated Yahoo Finance snapshot into Bronze."""

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_BATCH_TABLE = (
    "workspace.devin_market_risk_dev.bronze_ingestion_batches"
)
BRONZE_TABLES = {
    "DAILY_PRICES": "workspace.devin_market_risk_dev.bronze_daily_prices",
    "CORPORATE_ACTIONS": (
        "workspace.devin_market_risk_dev.bronze_corporate_actions"
    ),
}
REQUEST_START_DATE = "2020-01-01"
REQUEST_END_DATE = "2026-01-01"
CONTRACT_VERSION = "1.1.0"
SOURCE_ID = "YAHOO_FINANCE"
VOLUME_ROOT = Path(
    "/Volumes/workspace/devin_market_risk_dev/bronze_landing/yahoo_finance"
)
EXPECTED_FILES = {
    "DAILY_PRICES": {
        "folder": "daily_prices",
        "filename": "daily_prices.csv",
    },
    "CORPORATE_ACTIONS": {
        "folder": "corporate_actions",
        "filename": "corporate_actions.csv",
    },
}

# Defined by the preceding %run notebook at Databricks runtime.
validated: dict[str, dict[str, object]]


def require_equal(label: str, actual: object, expected: object) -> None:
    if actual != expected:
        raise ValueError(f"{label}: expected {expected!r}; found {actual!r}")


def source_package_paths(
    dataset_name: str,
    source_sha256: str,
) -> tuple[Path, Path]:
    specification = EXPECTED_FILES[dataset_name]
    package_root = (
        VOLUME_ROOT / specification["folder"] / source_sha256
    )
    return package_root / specification["filename"], package_root / "manifest.json"


def build_candidates(
    *,
    spark: SparkSession,
    dataset_name: str,
    result: dict[str, object],
    batch_id: str,
    ingested_at_utc: datetime,
) -> tuple[object, dict[str, object]]:
    """Build schema-aligned Bronze candidates only after read-only validation."""
    source_sha256 = str(result["source_sha256"])
    data_path, manifest_path = source_package_paths(
        dataset_name,
        source_sha256,
    )
    manifest_payload = manifest_path.read_bytes()
    manifest = json.loads(manifest_payload.decode("utf-8"))
    source_lines = data_path.read_text(encoding="utf-8").splitlines()
    reader = csv.DictReader(source_lines)
    source_rows = list(reader)
    raw_records = source_lines[1:]

    require_equal("candidate raw record count", len(raw_records), len(source_rows))
    require_equal(
        "candidate source SHA-256",
        hashlib.sha256(data_path.read_bytes()).hexdigest(),
        source_sha256,
    )
    require_equal(
        "candidate manifest SHA-256",
        manifest["source_sha256"],
        source_sha256,
    )

    bronze_table = BRONZE_TABLES[dataset_name]
    bronze_schema = spark.table(bronze_table).schema
    records: list[dict[str, object]] = []

    for source_row_number, (source_values, raw_record) in enumerate(
        zip(source_rows, raw_records, strict=True),
        start=1,
    ):
        lineage_values = {
            "batch_id": batch_id,
            "source_id": SOURCE_ID,
            "source_object_path": manifest["source_object_path"],
            "source_sha256": source_sha256,
            "source_row_number": source_row_number,
            "source_record_sha256": hashlib.sha256(
                raw_record.encode("utf-8")
            ).hexdigest(),
            "raw_record": raw_record,
            "ingested_at_utc": ingested_at_utc,
            "contract_version": CONTRACT_VERSION,
        }
        records.append(source_values | lineage_values)

    candidates = spark.createDataFrame(records, schema=bronze_schema)
    require_equal(
        "candidate row count",
        candidates.count(),
        int(result["record_count"]),
    )
    return candidates, {
        "manifest": manifest,
        "manifest_sha256": hashlib.sha256(manifest_payload).hexdigest(),
        "source_rows": source_rows,
        "bronze_table": bronze_table,
    }


def build_plan(
    *,
    spark: SparkSession,
    dataset_name: str,
    batch_id: str,
    started_at_utc: datetime,
    candidates: object,
    prepared: dict[str, object],
) -> dict[str, object]:
    """Decide a first write or audited duplicate skip before persistence."""
    manifest = prepared["manifest"]
    source_sha256 = manifest["source_sha256"]
    source_rows = prepared["source_rows"]
    bronze_table = str(prepared["bronze_table"])

    successful_batches = (
        spark.table(BRONZE_BATCH_TABLE)
        .where(
            (F.col("dataset_name") == dataset_name)
            & (F.col("source_id") == SOURCE_ID)
            & (F.col("source_sha256") == source_sha256)
            & F.col("status").isin("SUCCEEDED", "SUCCEEDED_WITH_WARNINGS")
        )
        .select("batch_id")
        .distinct()
        .collect()
    )
    if len(successful_batches) > 1:
        raise ValueError("Multiple successful batches exist for one source")

    previous_batch_id = (
        successful_batches[0]["batch_id"] if successful_batches else None
    )
    existing_count = (
        spark.table(bronze_table)
        .where(
            (F.col("source_id") == SOURCE_ID)
            & (F.col("source_sha256") == source_sha256)
        )
        .count()
    )
    if previous_batch_id is None:
        require_equal("pre-write source row count", existing_count, 0)
        status, accepted_count, deduplicated_count = (
            "SUCCEEDED",
            len(source_rows),
            0,
        )
        should_write = True
    else:
        require_equal(
            "existing duplicate source row count",
            existing_count,
            len(source_rows),
        )
        status, accepted_count, deduplicated_count = (
            "SKIPPED_DUPLICATE",
            0,
            len(source_rows),
        )
        should_write = False

    completed_at_utc = datetime.now(UTC).replace(tzinfo=None)
    audit_values = {
        "batch_id": batch_id,
        "dataset_name": dataset_name,
        "source_id": SOURCE_ID,
        "batch_type": "BACKFILL",
        "trigger_type": "MANUAL",
        "retry_of_batch_id": None,
        "attempt_number": 1,
        "requested_start_date": REQUEST_START_DATE,
        "requested_end_date": REQUEST_END_DATE,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "status": status,
        "received_count": len(source_rows),
        "accepted_count": accepted_count,
        "quarantined_count": 0,
        "rejected_count": 0,
        "unchanged_count": 0,
        "deduplicated_count": deduplicated_count,
        "warning_count": 0,
        "manifest_sha256": prepared["manifest_sha256"],
        "error_code": None,
        "error_message": None,
        "contract_version": CONTRACT_VERSION,
        "source_object_path": manifest["source_object_path"],
        "source_sha256": source_sha256,
        "duplicate_of_batch_id": previous_batch_id,
    }
    audit = spark.createDataFrame(
        [audit_values],
        schema=spark.table(BRONZE_BATCH_TABLE).schema,
    )
    return {
        "dataset_name": dataset_name,
        "batch_id": batch_id,
        "source_sha256": source_sha256,
        "source_row_count": len(source_rows),
        "bronze_table": bronze_table,
        "candidates": candidates,
        "audit": audit,
        "status": status,
        "should_write": should_write,
        "previous_batch_id": previous_batch_id,
    }


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils = DBUtils(spark)

plans: list[dict[str, object]] = []
for dataset_name, result in validated.items():
    batch_id = str(uuid4())
    started_at_utc = datetime.now(UTC).replace(tzinfo=None)
    candidates, prepared = build_candidates(
        spark=spark,
        dataset_name=dataset_name,
        result=result,
        batch_id=batch_id,
        ingested_at_utc=started_at_utc,
    )
    plans.append(
        build_plan(
            spark=spark,
            dataset_name=dataset_name,
            batch_id=batch_id,
            started_at_utc=started_at_utc,
            candidates=candidates,
            prepared=prepared,
        )
    )

# COMMAND ----------

for plan in plans:
    if plan["should_write"]:
        (
            plan["candidates"].write.format("delta").mode("append").saveAsTable(
                plan["bronze_table"]
            )
        )
    plan["audit"].write.format("delta").mode("append").saveAsTable(
        BRONZE_BATCH_TABLE
    )

    persisted_count = (
        spark.table(plan["bronze_table"])
        .where(
            (F.col("source_id") == SOURCE_ID)
            & (F.col("source_sha256") == plan["source_sha256"])
        )
        .count()
    )
    require_equal(
        "persisted source row count",
        persisted_count,
        plan["source_row_count"],
    )
    dbutils.jobs.taskValues.set(
        key=f"{str(plan['dataset_name']).lower()}_batch_id",
        value=plan["batch_id"]
        if plan["should_write"]
        else plan["previous_batch_id"],
    )
    print(f"{plan['dataset_name']} bronze_persistence=PASS")
    print(f"batch_status={plan['status']}")
    print(f"persisted_source_count={persisted_count}")
