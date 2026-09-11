# Databricks notebook source
"""Read-only validation for one approved Yahoo Finance Bronze landing package."""

import csv
import hashlib
import json
from pathlib import Path

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

VOLUME_ROOT = Path(
    "/Volumes/workspace/devin_market_risk_dev/bronze_landing/yahoo_finance"
)
SOURCE_ID = "YAHOO_FINANCE"
EXPECTED_FILES = {
    "DAILY_PRICES": {
        "folder": "daily_prices",
        "filename": "daily_prices.csv",
        "expected_columns": [
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
            "record_hash",
        ],
    },
    "CORPORATE_ACTIONS": {
        "folder": "corporate_actions",
        "filename": "corporate_actions.csv",
        "expected_columns": [
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
            "record_hash",
        ],
    },
}


def require_equal(label: str, actual: object, expected: object) -> None:
    if actual != expected:
        raise ValueError(f"{label}: expected {expected!r}; found {actual!r}")


def validate_dataset(
    *,
    dataset_name: str,
    expected_sha256: str,
) -> dict[str, object]:
    """Validate one immutable, approved Volume package without writing tables."""
    specification = EXPECTED_FILES[dataset_name]
    package_root = (
        VOLUME_ROOT / specification["folder"] / expected_sha256
    )
    data_path = package_root / specification["filename"]
    manifest_path = package_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    require_equal("dataset name", manifest["dataset_name"], dataset_name)
    require_equal("source ID", manifest["source_id"], SOURCE_ID)
    require_equal(
        "publication status",
        manifest["publication_status"],
        "APPROVED",
    )
    require_equal("manifest source SHA-256", manifest["source_sha256"], expected_sha256)
    require_equal(
        "landed source SHA-256",
        hashlib.sha256(data_path.read_bytes()).hexdigest(),
        expected_sha256,
    )
    with data_path.open(newline="", encoding="utf-8") as source_file:
        reader = csv.DictReader(source_file)
        require_equal(
            "source columns",
            reader.fieldnames,
            specification["expected_columns"],
        )
        record_count = sum(1 for _ in reader)
    require_equal(
        "manifest record count",
        manifest["record_count"],
        record_count,
    )
    return {
        "dataset_name": dataset_name,
        "source_sha256": expected_sha256,
        "snapshot_sha256": manifest["snapshot_sha256"],
        "record_count": record_count,
    }


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
dbutils.widgets.text(
    "daily_prices_sha256",
    "7f860c0a3d8191b9b2840e5c0d784954b8b9102af51d76b24828a5c0118d7c27",
)
dbutils.widgets.text(
    "corporate_actions_sha256",
    "e6cc097f3cfe44ef325e968cf9b9c0d4869476d946fd67720854cb89d7315a83",
)

validated = {
    "DAILY_PRICES": validate_dataset(
        dataset_name="DAILY_PRICES",
        expected_sha256=dbutils.widgets.get("daily_prices_sha256"),
    ),
    "CORPORATE_ACTIONS": validate_dataset(
        dataset_name="CORPORATE_ACTIONS",
        expected_sha256=dbutils.widgets.get("corporate_actions_sha256"),
    ),
}
require_equal(
    "shared snapshot SHA-256",
    validated["DAILY_PRICES"]["snapshot_sha256"],
    validated["CORPORATE_ACTIONS"]["snapshot_sha256"],
)
for dataset_name, result in validated.items():
    dbutils.jobs.taskValues.set(
        key=f"{dataset_name.lower()}_source_sha256",
        value=result["source_sha256"],
    )
    print(f"{dataset_name} landing_validation=PASS")
    print(f"record_count={result['record_count']}")
