-- Phase 04 managed Bronze Delta tables.
-- Bronze preserves source-aligned values and adds technical lineage.
-- These small tables are intentionally not partitioned.

CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.bronze_portfolios (
    portfolio_id STRING
        COMMENT 'Source portfolio identifier.',
    portfolio_name STRING
        COMMENT 'Source portfolio name.',
    strategy_code STRING
        COMMENT 'Source strategy code.',
    base_currency STRING
        COMMENT 'Source base currency.',
    target_inception_date STRING
        COMMENT 'Unmodified source target inception date.',
    actual_inception_date STRING
        COMMENT 'Unmodified source actual inception date, including blank values.',
    initial_nav STRING
        COMMENT 'Unmodified source initial NAV.',
    target_long_ratio STRING
        COMMENT 'Unmodified source target long ratio.',
    target_short_ratio STRING
        COMMENT 'Unmodified source target short ratio.',
    target_gross_ratio STRING
        COMMENT 'Unmodified source target gross ratio.',
    target_net_ratio STRING
        COMMENT 'Unmodified source target net ratio.',
    rebalance_policy STRING
        COMMENT 'Source rebalance policy.',
    cash_policy STRING
        COMMENT 'Source cash policy.',
    is_active STRING
        COMMENT 'Unmodified source active indicator.',
    config_version STRING
        COMMENT 'Source portfolio configuration version.',
    record_hash STRING
        COMMENT 'Source-provided canonical record hash.',
    batch_id STRING NOT NULL
        COMMENT 'Ingestion batch that accepted this source record.',
    source_id STRING NOT NULL
        COMMENT 'Identifier for the source system or fixture.',
    source_object_path STRING NOT NULL
        COMMENT 'Repository-relative path or external source object identifier.',
    source_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the complete source file.',
    source_row_number BIGINT NOT NULL
        COMMENT 'One-based data-row position in the source file.',
    source_record_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the exact raw source record.',
    raw_record STRING NOT NULL
        COMMENT 'Exact source record retained for technical traceability.',
    ingested_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when Databricks ingested the record.',
    contract_version STRING NOT NULL
        COMMENT 'Portfolio data-contract version applied during ingestion.'
)
USING DELTA
COMMENT 'Immutable source-aligned Bronze portfolio records with ingestion lineage';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.bronze_ingestion_batches (
    batch_id STRING NOT NULL
        COMMENT 'Unique UUID for one ingestion attempt.',
    dataset_name STRING NOT NULL
        COMMENT 'Dataset processed by the attempt.',
    source_id STRING NOT NULL
        COMMENT 'Source used by the attempt.',
    batch_type STRING NOT NULL
        COMMENT 'BACKFILL, INCREMENTAL, RETRY, or REPROCESS.',
    trigger_type STRING NOT NULL
        COMMENT 'MANUAL, SCHEDULED, or RECOVERY.',
    retry_of_batch_id STRING
        COMMENT 'Prior batch in a retry chain.',
    attempt_number BIGINT NOT NULL
        COMMENT 'Attempt number within a retry chain.',
    requested_start_date DATE
        COMMENT 'Inclusive source request start when a request window exists.',
    requested_end_date DATE
        COMMENT 'Exclusive source request end when a request window exists.',
    started_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when the attempt started.',
    completed_at_utc TIMESTAMP
        COMMENT 'UTC timestamp when a terminal attempt completed.',
    status STRING NOT NULL
        COMMENT 'Current or terminal ingestion status.',
    received_count BIGINT NOT NULL
        COMMENT 'Number of source records received.',
    accepted_count BIGINT NOT NULL
        COMMENT 'Number of records accepted into Bronze.',
    quarantined_count BIGINT NOT NULL
        COMMENT 'Number of records placed in quarantine.',
    rejected_count BIGINT NOT NULL
        COMMENT 'Number of rejected records.',
    unchanged_count BIGINT NOT NULL
        COMMENT 'Number of valid records already represented without change.',
    deduplicated_count BIGINT NOT NULL
        COMMENT 'Number of records skipped as duplicates.',
    warning_count BIGINT NOT NULL
        COMMENT 'Warning violations counted separately from row outcomes.',
    manifest_sha256 STRING
        COMMENT 'SHA-256 digest of the ingestion manifest.',
    error_code STRING
        COMMENT 'Sanitized machine-readable failure code.',
    error_message STRING
        COMMENT 'Sanitized failure message containing no secrets.',
    contract_version STRING NOT NULL
        COMMENT 'Ingestion-batch contract version applied.',
    source_object_path STRING
        COMMENT 'Repository-relative path or external source object identifier.',
    source_sha256 STRING
        COMMENT 'SHA-256 digest of the exact source object.',
    duplicate_of_batch_id STRING
        COMMENT 'Earlier successful batch containing identical source content.'
)
USING DELTA
COMMENT 'One auditable record for every dataset ingestion attempt';
