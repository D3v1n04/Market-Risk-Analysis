-- Phase 05 managed Silver Delta tables.
-- Silver validates and canonicalizes immutable Bronze input.
-- These small MVP tables are intentionally not partitioned.
-- Execute only after local contract and DDL checks pass.


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_portfolios (
    portfolio_id STRING NOT NULL
        COMMENT 'Canonical portfolio business identifier.',
    portfolio_name STRING NOT NULL
        COMMENT 'Canonical human-readable portfolio name.',
    strategy_code STRING NOT NULL
        COMMENT 'Validated portfolio strategy code.',
    base_currency STRING NOT NULL
        COMMENT 'Validated portfolio base currency.',
    target_inception_date DATE NOT NULL
        COMMENT 'Validated requested first portfolio date.',
    actual_inception_date DATE
        COMMENT 'Verified common inception date; null until initialization succeeds.',
    initial_nav DECIMAL(18,2) NOT NULL
        COMMENT 'Validated initial portfolio NAV in USD.',
    target_long_ratio DECIMAL(12,10) NOT NULL
        COMMENT 'Validated target long exposure ratio.',
    target_short_ratio DECIMAL(12,10) NOT NULL
        COMMENT 'Validated absolute target short exposure ratio.',
    target_gross_ratio DECIMAL(12,10) NOT NULL
        COMMENT 'Validated target gross exposure ratio.',
    target_net_ratio DECIMAL(12,10) NOT NULL
        COMMENT 'Validated target net exposure ratio.',
    rebalance_policy STRING NOT NULL
        COMMENT 'Validated portfolio rebalance policy.',
    cash_policy STRING NOT NULL
        COMMENT 'Validated portfolio cash policy.',
    is_active BOOLEAN NOT NULL
        COMMENT 'Validated active portfolio indicator.',
    config_version STRING NOT NULL
        COMMENT 'Validated semantic portfolio configuration version.',
    record_hash STRING NOT NULL
        COMMENT 'Validated canonical SHA-256 business-record hash.',
    processing_run_id STRING NOT NULL
        COMMENT 'Silver run that accepted this canonical version.',
    source_batch_id STRING NOT NULL
        COMMENT 'Bronze ingestion batch that supplied this canonical version.',
    source_record_id STRING NOT NULL
        COMMENT 'Deterministic batch and source-row identifier.',
    source_row_number BIGINT NOT NULL
        COMMENT 'One-based source row number in the Bronze batch.',
    source_record_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the evaluated raw Bronze record.',
    canonicalized_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when this version became canonical.',
    dataset_contract_version STRING NOT NULL
        COMMENT 'Portfolio contract version used for validation.'
)
USING DELTA
COMMENT 'Current trusted canonical portfolio records';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_processing_runs (
    processing_run_id STRING NOT NULL
        COMMENT 'Unique UUID for one Silver processing attempt.',
    source_batch_id STRING NOT NULL
        COMMENT 'Bronze ingestion batch evaluated by this run.',
    dataset_name STRING NOT NULL
        COMMENT 'Dataset evaluated by this run.',
    attempt_number BIGINT NOT NULL
        COMMENT 'Attempt number for this Bronze batch.',
    reprocess_of_processing_run_id STRING
        COMMENT 'Prior Silver run when this is a reprocessing attempt.',
    trigger_type STRING NOT NULL
        COMMENT 'MANUAL, SCHEDULED, or RECOVERY.',
    started_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when processing started.',
    completed_at_utc TIMESTAMP
        COMMENT 'UTC timestamp when terminal processing completed.',
    status STRING NOT NULL
        COMMENT 'Current or terminal processing status.',
    evaluated_count BIGINT NOT NULL
        COMMENT 'Number of Bronze records evaluated.',
    accepted_count BIGINT NOT NULL
        COMMENT 'Number of new or corrected canonical candidates.',
    quarantined_count BIGINT NOT NULL
        COMMENT 'Number of records withheld pending resolution.',
    rejected_count BIGINT NOT NULL
        COMMENT 'Number of records rejected by validation.',
    unchanged_count BIGINT NOT NULL
        COMMENT 'Number of records already represented canonically.',
    deduplicated_count BIGINT NOT NULL
        COMMENT 'Number of nonwinning identical duplicates.',
    warning_count BIGINT NOT NULL
        COMMENT 'Warning violations counted separately from row outcomes.',
    canonical_before_count BIGINT NOT NULL
        COMMENT 'Canonical record count before the run.',
    canonical_after_count BIGINT NOT NULL
        COMMENT 'Canonical record count visible after the run.',
    input_record_set_sha256 STRING NOT NULL
        COMMENT 'Digest of the ordered Bronze input records.',
    canonical_before_sha256 STRING
        COMMENT 'Digest of the canonical snapshot before the run.',
    canonical_after_sha256 STRING
        COMMENT 'Digest of the canonical snapshot visible after the run.',
    published BOOLEAN NOT NULL
        COMMENT 'Whether the run changed the canonical snapshot.',
    published_at_utc TIMESTAMP
        COMMENT 'UTC timestamp of successful atomic publication.',
    failed_rule_ids ARRAY<STRING> NOT NULL
        COMMENT 'Dataset-level rules that failed.',
    error_code STRING
        COMMENT 'Machine-readable technical failure code.',
    error_message STRING
        COMMENT 'Sanitized technical failure message.',
    dataset_contract_version STRING NOT NULL
        COMMENT 'Dataset contract version used for validation.',
    code_version STRING NOT NULL
        COMMENT 'Git commit SHA identifying the transformation code.',
    contract_version STRING NOT NULL
        COMMENT 'Processing-run contract version.'
)
USING DELTA
COMMENT 'Audit evidence for every Silver processing attempt';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_portfolio_record_outcomes (
    outcome_id STRING NOT NULL
        COMMENT 'Deterministic processing-run and source-record outcome identifier.',
    processing_run_id STRING NOT NULL
        COMMENT 'Silver processing run that evaluated the record.',
    batch_id STRING NOT NULL
        COMMENT 'Bronze ingestion batch that supplied the record.',
    dataset_name STRING NOT NULL
        COMMENT 'Dataset evaluated by the processing run.',
    source_record_id STRING NOT NULL
        COMMENT 'Deterministic batch and source-row identifier.',
    source_row_number BIGINT NOT NULL
        COMMENT 'One-based row number within the Bronze source.',
    source_record_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the evaluated raw Bronze record.',
    portfolio_id STRING
        COMMENT 'Portfolio identifier when structurally available.',
    outcome STRING NOT NULL
        COMMENT 'Single final state assigned to the evaluated record.',
    warning_count BIGINT NOT NULL
        COMMENT 'Number of warning violations attached to this record.',
    violation_count BIGINT NOT NULL
        COMMENT 'Total violations attached to this record.',
    canonical_record_hash STRING
        COMMENT 'Canonical business-record hash for accepted or unchanged records.',
    deduplicated_to_source_record_id STRING
        COMMENT 'Winning source record when this row is an identical duplicate.',
    evaluated_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when the final outcome was assigned.',
    dataset_contract_version STRING NOT NULL
        COMMENT 'Portfolio contract version used for validation.',
    contract_version STRING NOT NULL
        COMMENT 'Portfolio-record-outcome contract version.'
)
USING DELTA
COMMENT 'One final outcome for every evaluated Bronze portfolio row';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.silver_data_quality_violations (
    violation_id STRING NOT NULL
        COMMENT 'Deterministic processing-run, source-record, and rule identifier.',
    processing_run_id STRING NOT NULL
        COMMENT 'Silver processing run that evaluated the record.',
    batch_id STRING NOT NULL
        COMMENT 'Bronze ingestion batch that supplied the record.',
    dataset_name STRING NOT NULL
        COMMENT 'Dataset whose quality rule was evaluated.',
    source_record_id STRING NOT NULL
        COMMENT 'Deterministic batch and source-row identifier.',
    source_row_number BIGINT NOT NULL
        COMMENT 'One-based row number within the Bronze source.',
    source_record_sha256 STRING NOT NULL
        COMMENT 'SHA-256 digest of the evaluated raw Bronze record.',
    rule_id STRING NOT NULL
        COMMENT 'Stable machine-readable quality-rule identifier.',
    rule_version STRING NOT NULL
        COMMENT 'Dataset contract version containing the evaluated rule.',
    severity STRING NOT NULL
        COMMENT 'WARNING, ERROR, or CRITICAL.',
    disposition STRING NOT NULL
        COMMENT 'Action required by the violated rule.',
    affected_field STRING
        COMMENT 'Specific affected field when applicable.',
    observed_value STRING
        COMMENT 'Sanitized observed source representation.',
    expected_condition STRING NOT NULL
        COMMENT 'Plain-language condition the source record failed.',
    message STRING NOT NULL
        COMMENT 'Sanitized explanation and analytical impact.',
    detected_at_utc TIMESTAMP NOT NULL
        COMMENT 'UTC timestamp when the violation was detected.',
    resolution_status STRING NOT NULL
        COMMENT 'OPEN or RESOLVED.',
    resolution_action STRING
        COMMENT 'Approved action used to resolve the violation.',
    resolution_batch_id STRING
        COMMENT 'Later ingestion batch associated with resolution.',
    resolved_at_utc TIMESTAMP
        COMMENT 'UTC timestamp when the violation was resolved.',
    resolution_note STRING
        COMMENT 'Sanitized explanation of the resolution.',
    contract_version STRING NOT NULL
        COMMENT 'Data-quality-violation contract version.'
)
USING DELTA
COMMENT 'Immutable warning and error evidence from Silver validation';
