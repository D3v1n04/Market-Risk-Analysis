# Databricks notebook source
"""Read-only reconciliation for the approved Yahoo Finance Bronze snapshot."""

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_ID = "YAHOO_FINANCE"
BRONZE_BATCH_TABLE = (
    "workspace.devin_market_risk_dev.bronze_ingestion_batches"
)
DATASETS = {
    "DAILY_PRICES": {
        "table": "workspace.devin_market_risk_dev.bronze_daily_prices",
        "source_sha256": (
            "7f860c0a3d8191b9b2840e5c0d784954b8b9102af51d76b24828a5c0118d7c27"
        ),
        "expected_count": 22620,
        "expected_instrument_count": 15,
        "expected_dates_per_instrument": 1508,
    },
    "CORPORATE_ACTIONS": {
        "table": (
            "workspace.devin_market_risk_dev.bronze_corporate_actions"
        ),
        "source_sha256": (
            "e6cc097f3cfe44ef325e968cf9b9c0d4869476d946fd67720854cb89d7315a83"
        ),
        "expected_count": 280,
        "expected_action_counts": {
            "CASH_DIVIDEND": 271,
            "STOCK_SPLIT": 9,
        },
    },
}


def require_equal(label: str, actual: object, expected: object) -> None:
    if actual != expected:
        raise ValueError(f"{label}: expected {expected!r}; found {actual!r}")


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
dbutils = DBUtils(spark)

for dataset_name, specification in DATASETS.items():
    source_sha256 = specification["source_sha256"]
    source_rows = (
        spark.table(specification["table"])
        .where(
            (F.col("source_id") == SOURCE_ID)
            & (F.col("source_sha256") == source_sha256)
        )
    )
    source_count = source_rows.count()
    require_equal(
        f"{dataset_name} source row count",
        source_count,
        specification["expected_count"],
    )

    successful_batches = (
        spark.table(BRONZE_BATCH_TABLE)
        .where(
            (F.col("dataset_name") == dataset_name)
            & (F.col("source_id") == SOURCE_ID)
            & (F.col("source_sha256") == source_sha256)
            & (F.col("status") == "SUCCEEDED")
        )
        .select("batch_id", "accepted_count", "deduplicated_count")
        .collect()
    )
    require_equal(
        f"{dataset_name} successful batch count",
        len(successful_batches),
        1,
    )
    batch = successful_batches[0]
    require_equal(
        f"{dataset_name} accepted count",
        batch["accepted_count"],
        source_count,
    )
    require_equal(
        f"{dataset_name} deduplicated count",
        batch["deduplicated_count"],
        0,
    )
    require_equal(
        f"{dataset_name} business batch IDs",
        {
            row["batch_id"]
            for row in source_rows.select("batch_id").distinct().collect()
        },
        {batch["batch_id"]},
    )

    if dataset_name == "DAILY_PRICES":
        require_equal(
            "daily-price instrument count",
            source_rows.select("instrument_id").distinct().count(),
            specification["expected_instrument_count"],
        )
        date_counts = {
            row["instrument_id"]: row["date_count"]
            for row in source_rows.groupBy("instrument_id")
            .agg(F.countDistinct("price_date").alias("date_count"))
            .collect()
        }
        require_equal(
            "daily-price dates per instrument",
            set(date_counts.values()),
            {specification["expected_dates_per_instrument"]},
        )
    else:
        action_counts = {
            row["action_type"]: row["count"]
            for row in source_rows.groupBy("action_type")
            .count()
            .collect()
        }
        require_equal(
            "corporate-action counts by type",
            action_counts,
            specification["expected_action_counts"],
        )

    dbutils.jobs.taskValues.set(
        key=f"{dataset_name.lower()}_bronze_batch_id",
        value=batch["batch_id"],
    )
    print(f"{dataset_name} bronze_reconciliation=PASS")
    print(f"batch_id={batch['batch_id']}")
    print(f"source_row_count={source_count}")
