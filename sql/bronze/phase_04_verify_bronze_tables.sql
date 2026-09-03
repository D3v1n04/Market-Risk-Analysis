-- Phase 04 Bronze Delta table verification.

SHOW TABLES IN workspace.devin_market_risk_dev;

DESCRIBE DETAIL workspace.devin_market_risk_dev.bronze_portfolios;

DESCRIBE DETAIL workspace.devin_market_risk_dev.bronze_ingestion_batches;

DESCRIBE HISTORY workspace.devin_market_risk_dev.bronze_portfolios;

DESCRIBE HISTORY workspace.devin_market_risk_dev.bronze_ingestion_batches;

SELECT
    (
        SELECT COUNT(*)
        FROM workspace.devin_market_risk_dev.bronze_portfolios
    ) AS portfolio_row_count,
    (
        SELECT COUNT(*)
        FROM workspace.devin_market_risk_dev.bronze_ingestion_batches
    ) AS batch_row_count;


-- Verify batch outcome reconciliation and duplicate lineage.

SELECT
    batch_id,
    dataset_name,
    source_id,
    status,
    received_count,
    accepted_count,
    quarantined_count,
    rejected_count,
    unchanged_count,
    deduplicated_count,
    warning_count,
    received_count = (
        accepted_count
        + quarantined_count
        + rejected_count
        + unchanged_count
        + deduplicated_count
    ) AS counts_reconcile,
    requested_start_date,
    requested_end_date,
    duplicate_of_batch_id,
    source_sha256,
    manifest_sha256,
    completed_at_utc >= started_at_utc AS completion_order_valid,
    started_at_utc,
    completed_at_utc
FROM workspace.devin_market_risk_dev.bronze_ingestion_batches
ORDER BY started_at_utc;


-- Verify source-row lineage and preservation.

SELECT
    portfolio_id,
    portfolio_name,
    CASE
        WHEN actual_inception_date IS NULL THEN 'NULL'
        WHEN actual_inception_date = '' THEN 'EMPTY_STRING'
        ELSE actual_inception_date
    END AS actual_inception_date_state,
    batch_id,
    source_object_path,
    source_row_number,
    source_sha256,
    source_record_sha256,
    LENGTH(raw_record) AS raw_record_length,
    raw_record,
    ingested_at_utc,
    contract_version
FROM workspace.devin_market_risk_dev.bronze_portfolios
ORDER BY source_row_number;
