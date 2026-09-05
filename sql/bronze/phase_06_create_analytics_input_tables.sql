-- Phase 06 source-input Bronze Delta tables.
-- Bronze preserves source-aligned values without business-value correction.
-- Technical ingestion lineage is required for every accepted Bronze row.
-- These small development tables are intentionally not partitioned.

CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.bronze_instruments (
    instrument_id STRING,
    display_symbol STRING,
    yfinance_symbol STRING,
    instrument_name STRING,
    asset_class STRING,
    security_type STRING,
    exchange_mic STRING,
    quote_currency STRING,
    issuer_country_code STRING,
    official_sector_name STRING,
    risk_cluster_id STRING,
    classification_source STRING,
    classification_as_of_date STRING,
    active_from STRING,
    active_to STRING,
    is_active STRING,
    config_version STRING,
    record_hash STRING,
    batch_id STRING NOT NULL
        COMMENT 'Ingestion batch that accepted this record.',
    source_id STRING NOT NULL
        COMMENT 'Actual origin of the supplied record.',
    source_object_path STRING NOT NULL
        COMMENT 'Repository-relative source fixture path.',
    source_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the complete source object.',
    source_row_number BIGINT NOT NULL
        COMMENT 'Deterministic one-based source record position.',
    source_record_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the exact raw record.',
    raw_record STRING NOT NULL
        COMMENT 'Exact supplied record retained for traceability.',
    ingested_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when Databricks ingested the record.',
    contract_version STRING NOT NULL
        COMMENT 'Instrument contract version applied during ingestion.'
)
USING DELTA
COMMENT 'Immutable source-aligned Bronze instrument records with ingestion lineage';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.bronze_target_allocations (
    portfolio_id STRING,
    instrument_id STRING,
    effective_from STRING,
    effective_to STRING,
    target_weight STRING,
    allocation_version STRING,
    record_hash STRING,
    batch_id STRING NOT NULL
        COMMENT 'Ingestion batch that accepted this record.',
    source_id STRING NOT NULL
        COMMENT 'Actual origin of the supplied record.',
    source_object_path STRING NOT NULL
        COMMENT 'Repository-relative source fixture path.',
    source_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the complete source object.',
    source_row_number BIGINT NOT NULL
        COMMENT 'Deterministic one-based source record position.',
    source_record_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the exact raw record.',
    raw_record STRING NOT NULL
        COMMENT 'Exact supplied record retained for traceability.',
    ingested_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when Databricks ingested the record.',
    contract_version STRING NOT NULL
        COMMENT 'Target-allocation contract version applied during ingestion.'
)
USING DELTA
COMMENT 'Immutable source-aligned Bronze target-allocation records with ingestion lineage';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.bronze_daily_prices (
    instrument_id STRING,
    price_date STRING,
    source_id STRING NOT NULL
        COMMENT 'Actual provider or controlled-fixture origin.',
    source_symbol STRING,
    open_price STRING,
    high_price STRING,
    low_price STRING,
    close_price STRING,
    adjusted_close_price STRING,
    volume STRING,
    quote_currency STRING,
    source_updated_at_utc STRING,
    source_record_id STRING,
    record_hash STRING,
    batch_id STRING NOT NULL
        COMMENT 'Ingestion batch that accepted this record.',
    source_object_path STRING NOT NULL
        COMMENT 'Source object or repository-relative fixture path.',
    source_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the complete source object.',
    source_row_number BIGINT NOT NULL
        COMMENT 'Deterministic one-based source record position.',
    source_record_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the exact raw record.',
    raw_record STRING NOT NULL
        COMMENT 'Exact supplied record retained for traceability.',
    ingested_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when Databricks ingested the record.',
    contract_version STRING NOT NULL
        COMMENT 'Daily-price contract version applied during ingestion.'
)
USING DELTA
COMMENT 'Immutable source-aligned Bronze daily-price observations with ingestion lineage';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.bronze_corporate_actions (
    instrument_id STRING,
    effective_date STRING,
    action_type STRING,
    source_id STRING NOT NULL
        COMMENT 'Actual provider or controlled-fixture origin.',
    source_symbol STRING,
    dividend_amount_per_share STRING,
    dividend_currency STRING,
    split_ratio STRING,
    source_action_id STRING,
    source_updated_at_utc STRING,
    source_record_id STRING,
    record_hash STRING,
    batch_id STRING NOT NULL
        COMMENT 'Ingestion batch that accepted this record.',
    source_object_path STRING NOT NULL
        COMMENT 'Source object or repository-relative fixture path.',
    source_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the complete source object.',
    source_row_number BIGINT NOT NULL
        COMMENT 'Deterministic one-based source record position.',
    source_record_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the exact raw record.',
    raw_record STRING NOT NULL
        COMMENT 'Exact supplied record retained for traceability.',
    ingested_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when Databricks ingested the record.',
    contract_version STRING NOT NULL
        COMMENT 'Corporate-action contract version applied during ingestion.'
)
USING DELTA
COMMENT 'Immutable source-aligned Bronze corporate-action records with ingestion lineage';
