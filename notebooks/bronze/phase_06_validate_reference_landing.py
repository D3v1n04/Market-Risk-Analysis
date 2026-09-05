# Databricks notebook source
"""Validate Phase 06 reference fixtures before Bronze ingestion."""

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession

LANDING_ROOT = Path(
    "/Volumes/workspace/devin_market_risk_dev/bronze_landing"
)

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
    """Raise a clear error when evidence differs."""
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
]:
    """Validate one landed CSV and its manifest."""
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

    print(
        f"{specification['dataset_name']} "
        "landing_validation=PASS"
    )
    print(f"source_sha256={source_sha256}")
    print(f"manifest_sha256={manifest_sha256}")
    print(f"source_record_count={len(source_rows)}")

    return (
        manifest,
        source_columns,
        source_rows,
        raw_records,
    )


def build_bronze_candidates(
    *,
    spark: SparkSession,
    specification: dict[str, Any],
    manifest: dict[str, Any],
    source_columns: list[str],
    source_rows: list[list[str]],
    raw_records: list[str],
) -> DataFrame:
    """Create a write-free Bronze candidate DataFrame."""
    candidate_batch_id = str(uuid4())
    validation_time = datetime.now(UTC).replace(
        tzinfo=None
    )
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
            "batch_id": candidate_batch_id,
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
            "ingested_at_utc": validation_time,
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

    print(
        f"{specification['dataset_name']} "
        "candidate_validation=PASS"
    )
    print(f"candidate_batch_id={candidate_batch_id}")
    print(
        "candidate_row_count="
        f"{candidates.count()}"
    )

    return candidates


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

for specification in REFERENCE_SOURCES:
    (
        manifest,
        source_columns,
        source_rows,
        raw_records,
    ) = load_and_validate_landing(specification)

    candidates = build_bronze_candidates(
        spark=spark,
        specification=specification,
        manifest=manifest,
        source_columns=source_columns,
        source_rows=source_rows,
        raw_records=raw_records,
    )

    candidates.select(
        *specification["preview_columns"],
        "batch_id",
        "source_row_number",
        "source_record_sha256",
    ).show(5, truncate=False)
