-- Phase 06 trusted analytical-input Silver Delta tables.
-- Silver stores typed, validated, canonical records.
-- Source-record outcomes preserve record-level processing evidence.
-- These small development tables are intentionally not partitioned.

CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_instruments (
    instrument_id STRING NOT NULL,
    display_symbol STRING NOT NULL,
    yfinance_symbol STRING NOT NULL,
    instrument_name STRING NOT NULL,
    asset_class STRING NOT NULL,
    security_type STRING NOT NULL,
    exchange_mic STRING NOT NULL,
    quote_currency STRING NOT NULL,
    issuer_country_code STRING NOT NULL,
    official_sector_name STRING NOT NULL,
    risk_cluster_id STRING NOT NULL,
    classification_source STRING NOT NULL,
    classification_as_of_date DATE NOT NULL,
    active_from DATE NOT NULL,
    active_to DATE,
    is_active BOOLEAN NOT NULL,
    config_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Typed and validated canonical instrument reference records';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_target_allocations (
    portfolio_id STRING NOT NULL,
    instrument_id STRING NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    target_weight DECIMAL(12,10) NOT NULL,
    allocation_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Typed and validated canonical target allocations';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_stress_scenarios (
    scenario_id STRING NOT NULL,
    scenario_name STRING NOT NULL,
    scenario_description STRING NOT NULL,
    scenario_type STRING NOT NULL,
    is_active BOOLEAN NOT NULL,
    scenario_version STRING NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Typed and validated canonical deterministic hypothetical stress scenarios';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_stress_scenario_shocks (
    scenario_id STRING NOT NULL,
    instrument_id STRING NOT NULL,
    shock_ratio DECIMAL(12,10) NOT NULL,
    shock_rationale STRING NOT NULL,
    scenario_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Typed and validated canonical deterministic hypothetical instrument shocks';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_daily_prices (
    instrument_id STRING NOT NULL,
    price_date DATE NOT NULL,
    source_id STRING NOT NULL,
    source_symbol STRING NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    adjusted_close_price DECIMAL(20,8) NOT NULL,
    volume BIGINT,
    quote_currency STRING NOT NULL,
    source_updated_at_utc TIMESTAMP,
    source_record_id STRING NOT NULL,
    batch_id STRING NOT NULL,
    ingested_at_utc TIMESTAMP NOT NULL,
    contract_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Typed and validated canonical end-of-day prices';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_corporate_actions (
    instrument_id STRING NOT NULL,
    effective_date DATE NOT NULL,
    action_type STRING NOT NULL,
    source_id STRING NOT NULL,
    source_symbol STRING NOT NULL,
    dividend_amount_per_share DECIMAL(20,8),
    dividend_currency STRING,
    split_ratio DECIMAL(20,10),
    source_action_id STRING,
    source_updated_at_utc TIMESTAMP,
    source_record_id STRING NOT NULL,
    batch_id STRING NOT NULL,
    ingested_at_utc TIMESTAMP NOT NULL,
    contract_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Typed and validated canonical dividend and split events';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_trading_calendar (
    exchange_mic STRING NOT NULL,
    calendar_date DATE NOT NULL,
    is_trading_day BOOLEAN NOT NULL,
    market_open_utc TIMESTAMP,
    market_close_utc TIMESTAMP,
    is_early_close BOOLEAN NOT NULL,
    holiday_name STRING,
    exchange_timezone STRING NOT NULL,
    calendar_source STRING NOT NULL,
    calendar_version STRING NOT NULL,
    generated_at_utc TIMESTAMP NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Versioned and validated exchange trading calendar';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_source_record_outcomes (
    outcome_id STRING NOT NULL,
    processing_run_id STRING NOT NULL,
    batch_id STRING NOT NULL,
    dataset_name STRING NOT NULL,
    source_record_id STRING NOT NULL,
    source_row_number BIGINT NOT NULL,
    source_record_sha256 STRING NOT NULL,
    business_key STRING,
    outcome STRING NOT NULL,
    warning_count BIGINT NOT NULL,
    violation_count BIGINT NOT NULL,
    canonical_record_hash STRING,
    deduplicated_to_source_record_id STRING,
    evaluated_at_utc TIMESTAMP NOT NULL,
    dataset_contract_version STRING NOT NULL,
    contract_version STRING NOT NULL
)
USING DELTA
COMMENT 'Immutable record-level outcomes for non-portfolio Bronze-to-Silver processing';
