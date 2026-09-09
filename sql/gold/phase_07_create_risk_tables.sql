-- Phase 07 Gold risk-contract foundation Delta tables.
-- The tables are append-only audit and risk-result structures; this DDL creates
-- empty structures only and performs no data mutation.


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.gold_risk_runs (
    risk_run_id STRING NOT NULL
        COMMENT 'Unique immutable UUID for one complete portfolio risk bundle attempt.',
    portfolio_id STRING NOT NULL,
    as_of_date DATE NOT NULL,
    base_currency STRING NOT NULL,
    attempt_number BIGINT NOT NULL,
    reprocess_of_risk_run_id STRING,
    trigger_type STRING NOT NULL,
    started_at_utc TIMESTAMP NOT NULL,
    completed_at_utc TIMESTAMP,
    status STRING NOT NULL,
    methodology STRING NOT NULL,
    holding_period_days BIGINT NOT NULL,
    return_price_field STRING NOT NULL,
    return_formula STRING NOT NULL,
    price_history_start_date DATE NOT NULL,
    price_history_end_date DATE NOT NULL,
    trading_session_count BIGINT NOT NULL,
    return_observation_count BIGINT NOT NULL,
    minimum_return_observation_count BIGINT NOT NULL,
    confidence_levels ARRAY<DECIMAL(3,2)> NOT NULL,
    static_exposure_source_date DATE NOT NULL,
    static_exposure_valuation_date DATE NOT NULL,
    static_exposure_policy STRING NOT NULL,
    input_dataset_names ARRAY<STRING> NOT NULL,
    input_record_count BIGINT NOT NULL,
    input_manifest_sha256 STRING NOT NULL,
    input_static_exposure_sha256 STRING NOT NULL,
    input_price_history_sha256 STRING NOT NULL,
    input_stress_scenario_set_sha256 STRING NOT NULL,
    input_stress_shock_set_sha256 STRING NOT NULL,
    expected_historical_pnl_scenario_count BIGINT NOT NULL,
    calculated_historical_pnl_scenario_count BIGINT NOT NULL,
    expected_var_measure_count BIGINT NOT NULL,
    calculated_var_measure_count BIGINT NOT NULL,
    expected_stress_result_count BIGINT NOT NULL,
    calculated_stress_result_count BIGINT NOT NULL,
    published BOOLEAN NOT NULL,
    published_at_utc TIMESTAMP,
    warning_count BIGINT NOT NULL,
    failed_rule_ids ARRAY<STRING> NOT NULL,
    error_code STRING,
    error_message STRING,
    historical_pnl_scenarios_contract_version STRING NOT NULL,
    var_measures_contract_version STRING NOT NULL,
    stress_results_contract_version STRING NOT NULL,
    calculation_version STRING NOT NULL,
    code_version STRING NOT NULL,
    contract_version STRING NOT NULL
)
USING DELTA
COMMENT 'Immutable audit evidence for complete portfolio historical-risk and stress-risk bundles';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.gold_historical_pnl_scenarios (
    risk_run_id STRING NOT NULL,
    portfolio_id STRING NOT NULL,
    prior_price_date DATE NOT NULL,
    scenario_date DATE NOT NULL,
    return_observation_number BIGINT NOT NULL,
    base_currency STRING NOT NULL,
    simulated_portfolio_pnl DECIMAL(38,16) NOT NULL,
    loss_amount DECIMAL(38,16) NOT NULL,
    input_static_exposure_sha256 STRING NOT NULL,
    input_prior_price_partition_sha256 STRING NOT NULL,
    input_scenario_price_partition_sha256 STRING NOT NULL,
    calculation_version STRING NOT NULL,
    calculated_at_utc TIMESTAMP NOT NULL,
    contract_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Auditable one-day historical simulated portfolio PnL and signed losses';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.gold_var_measures (
    risk_run_id STRING NOT NULL,
    portfolio_id STRING NOT NULL,
    as_of_date DATE NOT NULL,
    base_currency STRING NOT NULL,
    confidence_level DECIMAL(3,2) NOT NULL,
    observation_count BIGINT NOT NULL,
    quantile_rank BIGINT NOT NULL,
    var_amount DECIMAL(38,16) NOT NULL,
    input_historical_pnl_scenario_set_sha256 STRING NOT NULL,
    input_static_exposure_sha256 STRING NOT NULL,
    calculation_version STRING NOT NULL,
    calculated_at_utc TIMESTAMP NOT NULL,
    contract_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Discrete empirical one-day historical Value at Risk measures in USD';


CREATE TABLE IF NOT EXISTS workspace.devin_market_risk_dev.gold_stress_results (
    risk_run_id STRING NOT NULL,
    portfolio_id STRING NOT NULL,
    as_of_date DATE NOT NULL,
    base_currency STRING NOT NULL,
    scenario_id STRING NOT NULL,
    scenario_version STRING NOT NULL,
    shock_count BIGINT NOT NULL,
    stress_pnl DECIMAL(38,16) NOT NULL,
    stressed_nav DECIMAL(38,16) NOT NULL,
    input_static_exposure_sha256 STRING NOT NULL,
    input_stress_scenario_record_sha256 STRING NOT NULL,
    input_shock_set_sha256 STRING NOT NULL,
    calculation_version STRING NOT NULL,
    calculated_at_utc TIMESTAMP NOT NULL,
    contract_version STRING NOT NULL,
    record_hash STRING NOT NULL
)
USING DELTA
COMMENT 'Deterministic hypothetical stress results for approved scenario shock sets';
