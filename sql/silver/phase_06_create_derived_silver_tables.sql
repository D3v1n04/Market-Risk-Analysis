-- Phase 06 trusted derived Silver Delta tables.
-- These tables store derivation audit evidence, positions, and cash balances.
-- The MVP tables are intentionally unpartitioned because their volume is small.
-- This DDL creates empty structures and performs no data mutation.


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_derivation_runs (
    derivation_run_id STRING NOT NULL
        COMMENT 'Unique UUID for one trusted-input derivation attempt.',
    output_dataset_name STRING NOT NULL
        COMMENT 'POSITIONS or CASH_BALANCES.',
    portfolio_id STRING NOT NULL
        COMMENT 'Portfolio processed independently by this run.',
    business_date DATE NOT NULL
        COMMENT 'Portfolio business date represented by the output partition.',
    attempt_number BIGINT NOT NULL
        COMMENT 'Attempt number for the same dataset, portfolio, and date.',
    reprocess_of_derivation_run_id STRING
        COMMENT 'Previous derivation run retried by this attempt.',
    trigger_type STRING NOT NULL
        COMMENT 'MANUAL, SCHEDULED, or RECOVERY.',
    started_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when derivation started.',
    completed_at_utc TIMESTAMP
        COMMENT 'UTC timestamp when terminal processing completed.',
    status STRING NOT NULL
        COMMENT 'Current or terminal derivation status.',
    input_dataset_names ARRAY<STRING> NOT NULL
        COMMENT 'Sorted unique trusted datasets represented in the manifest.',
    input_record_count BIGINT NOT NULL
        COMMENT 'Total trusted records represented by the input manifest.',
    input_manifest_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the ordered dataset input manifest.',
    expected_output_count BIGINT NOT NULL
        COMMENT 'Required complete output count for the target partition.',
    derived_output_count BIGINT NOT NULL
        COMMENT 'Output candidates derived before atomic publication.',
    canonical_before_count BIGINT NOT NULL
        COMMENT 'Canonical target records visible before this run.',
    canonical_after_count BIGINT NOT NULL
        COMMENT 'Canonical target records visible after this run.',
    canonical_before_sha256 STRING
        COMMENT 'Digest of the prior canonical target partition.',
    canonical_after_sha256 STRING
        COMMENT 'Digest of the canonical target partition after the run.',
    published BOOLEAN NOT NULL
        COMMENT 'Whether this run atomically changed the target partition.',
    published_at_utc TIMESTAMP
        COMMENT 'UTC timestamp of successful atomic publication.',
    warning_count BIGINT NOT NULL
        COMMENT 'Number of warning conditions recorded by the run.',
    failed_rule_ids ARRAY<STRING> NOT NULL
        COMMENT 'Derivation rules that failed.',
    error_code STRING
        COMMENT 'Machine-readable technical failure code.',
    error_message STRING
        COMMENT 'Sanitized technical failure message.',
    output_contract_version STRING NOT NULL
        COMMENT 'Contract version governing the derived output.',
    calculation_version STRING NOT NULL
        COMMENT 'Version of the approved derivation formulas.',
    code_version STRING NOT NULL
        COMMENT 'Git commit SHA identifying the derivation code.',
    contract_version STRING NOT NULL
        COMMENT 'Derivation-run contract version.'
)
USING DELTA
COMMENT 'Immutable audit evidence for trusted Silver derivation attempts';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_positions (
    portfolio_id STRING NOT NULL
        COMMENT 'Stable portfolio identifier.',
    instrument_id STRING NOT NULL
        COMMENT 'Stable instrument identifier.',
    position_date DATE NOT NULL
        COMMENT 'Closing portfolio business date represented by the position.',
    signed_quantity DECIMAL(38,16) NOT NULL
        COMMENT 'Signed shares; positive is long and negative is short.',
    position_basis STRING NOT NULL
        COMMENT 'Initial, ordinary carry-forward, or split-adjusted basis.',
    allocation_effective_from DATE NOT NULL
        COMMENT 'Effective date of the allocation governing this position.',
    prior_position_date DATE
        COMMENT 'Previous complete portfolio business date after inception.',
    input_allocation_record_sha256 STRING NOT NULL
        COMMENT 'Digest of the trusted governing allocation record.',
    input_price_record_sha256 STRING
        COMMENT 'Digest of the trusted inception price record.',
    input_prior_position_record_sha256 STRING
        COMMENT 'Digest of the prior canonical position after inception.',
    input_corporate_action_set_sha256 STRING NOT NULL
        COMMENT 'Digest of applicable split actions, including an empty set.',
    derivation_run_id STRING NOT NULL
        COMMENT 'Trusted-input derivation run that produced this position.',
    calculation_version STRING NOT NULL
        COMMENT 'Version of the approved position calculation.',
    derived_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when the position was derived.',
    contract_version STRING NOT NULL
        COMMENT 'Position contract version.',
    record_hash STRING NOT NULL
        COMMENT 'SHA-256 digest of the stable derived business fields.'
)
USING DELTA
COMMENT 'Trusted daily signed portfolio position quantities';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_cash_balances (
    portfolio_id STRING NOT NULL
        COMMENT 'Stable portfolio identifier.',
    cash_date DATE NOT NULL
        COMMENT 'Approved portfolio business date represented by the balance.',
    prior_cash_date DATE
        COMMENT 'Previous complete portfolio business date after inception.',
    position_input_date DATE NOT NULL
        COMMENT 'Position date used for initialization or dividends.',
    base_currency STRING NOT NULL
        COMMENT 'Portfolio reporting currency.',
    opening_cash_balance DECIMAL(38,16) NOT NULL
        COMMENT 'Inception residual cash or prior complete closing cash.',
    dividend_cash_flow DECIMAL(38,16) NOT NULL
        COMMENT 'Cash received by longs or owed by shorts.',
    closing_cash_balance DECIMAL(38,16) NOT NULL
        COMMENT 'Opening cash plus dividend cash flow.',
    corporate_action_count BIGINT NOT NULL
        COMMENT 'Validated dividend events included in the calculation.',
    input_position_set_sha256 STRING NOT NULL
        COMMENT 'Digest of the ordered position inputs.',
    input_corporate_action_set_sha256 STRING NOT NULL
        COMMENT 'Digest of applicable dividend actions, including an empty set.',
    derivation_run_id STRING NOT NULL
        COMMENT 'Trusted-input derivation run that produced this balance.',
    calculation_version STRING NOT NULL
        COMMENT 'Version of the approved cash calculation.',
    derived_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when the cash balance was derived.',
    contract_version STRING NOT NULL
        COMMENT 'Cash-balance contract version.',
    record_hash STRING NOT NULL
        COMMENT 'SHA-256 digest of the stable derived business fields.'
)
USING DELTA
COMMENT 'Trusted reconciled daily portfolio cash balances';
