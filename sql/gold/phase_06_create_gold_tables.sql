-- Phase 06 Gold analytics foundation Delta tables.
-- The MVP tables are intentionally unpartitioned because their volume is small.
-- This DDL creates empty structures and performs no data mutation.


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.gold_analytics_runs (
    analytics_run_id STRING NOT NULL
        COMMENT 'Unique UUID for one Gold analytics attempt.',
    output_dataset_name STRING NOT NULL
        COMMENT 'Gold dataset calculated by the run.',
    portfolio_id STRING NOT NULL
        COMMENT 'Portfolio calculated independently by this run.',
    valuation_date DATE NOT NULL
        COMMENT 'Close-of-business date represented by the output partition.',
    attempt_number BIGINT NOT NULL,
    reprocess_of_analytics_run_id STRING,
    trigger_type STRING NOT NULL,
    started_at_utc TIMESTAMP NOT NULL,
    completed_at_utc TIMESTAMP,
    status STRING NOT NULL,
    input_dataset_names ARRAY<STRING> NOT NULL,
    input_record_count BIGINT NOT NULL,
    input_manifest_sha256 STRING NOT NULL,
    expected_output_count BIGINT NOT NULL,
    calculated_output_count BIGINT NOT NULL,
    canonical_before_count BIGINT NOT NULL,
    canonical_after_count BIGINT NOT NULL,
    canonical_before_sha256 STRING,
    canonical_after_sha256 STRING,
    published BOOLEAN NOT NULL,
    published_at_utc TIMESTAMP,
    warning_count BIGINT NOT NULL,
    failed_rule_ids ARRAY<STRING> NOT NULL,
    error_code STRING,
    error_message STRING,
    output_contract_version STRING NOT NULL,
    calculation_version STRING NOT NULL,
    code_version STRING NOT NULL,
    contract_version STRING NOT NULL
)
USING DELTA
COMMENT 'Immutable audit evidence for Gold analytics calculations';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.gold_position_market_values (
    portfolio_id STRING NOT NULL,
    instrument_id STRING NOT NULL,
    valuation_date DATE NOT NULL,
    signed_quantity DECIMAL(38,16) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    quote_currency STRING NOT NULL,
    base_currency STRING NOT NULL,
    position_side STRING NOT NULL,
    signed_market_value DECIMAL(38,16) NOT NULL,
    absolute_market_value DECIMAL(38,16) NOT NULL,
    input_position_record_sha256 STRING NOT NULL,
    input_price_record_sha256 STRING NOT NULL,
    analytics_run_id STRING NOT NULL,
    calculation_version STRING NOT NULL,
    calculated_at_utc TIMESTAMP NOT NULL,
    contract_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Close-of-business USD valuation for each trusted portfolio position';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.gold_portfolio_daily_metrics (
    portfolio_id STRING NOT NULL,
    valuation_date DATE NOT NULL,
    base_currency STRING NOT NULL,
    position_count BIGINT NOT NULL,
    long_position_count BIGINT NOT NULL,
    short_position_count BIGINT NOT NULL,
    long_market_value DECIMAL(38,16) NOT NULL,
    short_market_value DECIMAL(38,16) NOT NULL,
    gross_market_value DECIMAL(38,16) NOT NULL,
    net_security_market_value DECIMAL(38,16) NOT NULL,
    closing_cash_balance DECIMAL(38,16) NOT NULL,
    closing_nav DECIMAL(38,16) NOT NULL,
    baseline_nav DECIMAL(38,16) NOT NULL,
    baseline_source STRING NOT NULL,
    prior_valuation_date DATE,
    daily_pnl DECIMAL(38,16) NOT NULL,
    daily_return DECIMAL(38,18) NOT NULL,
    long_exposure_ratio DECIMAL(38,18) NOT NULL,
    short_exposure_ratio DECIMAL(38,18) NOT NULL,
    gross_exposure_ratio DECIMAL(38,18) NOT NULL,
    net_exposure_ratio DECIMAL(38,18) NOT NULL,
    input_market_value_partition_sha256 STRING NOT NULL,
    input_cash_balance_record_sha256 STRING NOT NULL,
    input_portfolio_record_sha256 STRING NOT NULL,
    input_prior_metric_record_sha256 STRING,
    analytics_run_id STRING NOT NULL,
    calculation_version STRING NOT NULL,
    calculated_at_utc TIMESTAMP NOT NULL,
    contract_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Audited close-of-business USD portfolio performance and exposure metrics';
