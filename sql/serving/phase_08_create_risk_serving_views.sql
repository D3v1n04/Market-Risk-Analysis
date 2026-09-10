-- Phase 08 controlled risk-serving layer.
-- Gold risk-result tables are append-only. These views intentionally select one
-- complete, published risk run for each portfolio and as-of date.

CREATE OR REPLACE VIEW workspace.devin_market_risk_dev.vw_latest_published_risk_runs
COMMENT 'One deterministic complete published risk run per portfolio and as-of date.'
AS
WITH eligible_complete_published_runs AS (
    SELECT
        runs.risk_run_id,
        runs.portfolio_id,
        runs.as_of_date,
        runs.base_currency,
        runs.attempt_number,
        runs.reprocess_of_risk_run_id,
        runs.trigger_type,
        runs.started_at_utc,
        runs.completed_at_utc,
        runs.status,
        runs.methodology,
        runs.holding_period_days,
        runs.return_price_field,
        runs.return_formula,
        runs.price_history_start_date,
        runs.price_history_end_date,
        runs.trading_session_count,
        runs.return_observation_count,
        runs.minimum_return_observation_count,
        runs.confidence_levels,
        runs.static_exposure_source_date,
        runs.static_exposure_valuation_date,
        runs.static_exposure_policy,
        runs.published,
        runs.published_at_utc,
        runs.warning_count,
        runs.failed_rule_ids,
        runs.calculation_version,
        runs.code_version,
        runs.contract_version
    FROM workspace.devin_market_risk_dev.gold_risk_runs AS runs
    WHERE runs.status = 'SUCCEEDED'
      AND runs.published = TRUE
      AND runs.completed_at_utc IS NOT NULL
      AND runs.published_at_utc IS NOT NULL
      AND runs.expected_historical_pnl_scenario_count = 251
      AND runs.calculated_historical_pnl_scenario_count = 251
      AND runs.expected_var_measure_count = 2
      AND runs.calculated_var_measure_count = 2
      AND runs.expected_stress_result_count = 3
      AND runs.calculated_stress_result_count = 3
),
ranked_eligible_runs AS (
    SELECT
        eligible.*,
        ROW_NUMBER() OVER (
            PARTITION BY eligible.portfolio_id, eligible.as_of_date
            ORDER BY
                eligible.published_at_utc DESC,
                eligible.completed_at_utc DESC,
                eligible.attempt_number DESC,
                eligible.risk_run_id DESC
        ) AS published_run_rank
    FROM eligible_complete_published_runs AS eligible
)
SELECT
    ranked.risk_run_id,
    ranked.portfolio_id,
    ranked.as_of_date,
    ranked.base_currency,
    ranked.attempt_number,
    ranked.reprocess_of_risk_run_id,
    ranked.trigger_type,
    ranked.started_at_utc,
    ranked.completed_at_utc,
    ranked.status,
    ranked.methodology,
    ranked.holding_period_days,
    ranked.return_price_field,
    ranked.return_formula,
    ranked.price_history_start_date,
    ranked.price_history_end_date,
    ranked.trading_session_count,
    ranked.return_observation_count,
    ranked.minimum_return_observation_count,
    ranked.confidence_levels,
    ranked.static_exposure_source_date,
    ranked.static_exposure_valuation_date,
    ranked.static_exposure_policy,
    ranked.published,
    ranked.published_at_utc,
    ranked.warning_count,
    ranked.failed_rule_ids,
    ranked.calculation_version,
    ranked.code_version,
    ranked.contract_version
FROM ranked_eligible_runs AS ranked
WHERE ranked.published_run_rank = 1;


CREATE OR REPLACE VIEW workspace.devin_market_risk_dev.vw_latest_published_var
COMMENT 'Historical Value at Risk measures for the selected complete published risk run.'
AS
SELECT
    selected.portfolio_id,
    selected.as_of_date,
    selected.base_currency,
    selected.risk_run_id,
    selected.attempt_number,
    selected.methodology,
    selected.holding_period_days,
    selected.return_observation_count,
    measures.confidence_level,
    measures.observation_count,
    measures.quantile_rank,
    measures.var_amount AS var_amount_usd,
    measures.calculation_version,
    measures.calculated_at_utc
FROM workspace.devin_market_risk_dev.vw_latest_published_risk_runs AS selected
INNER JOIN workspace.devin_market_risk_dev.gold_var_measures AS measures
    ON selected.risk_run_id = measures.risk_run_id;


CREATE OR REPLACE VIEW workspace.devin_market_risk_dev.vw_latest_published_stress_results
COMMENT 'Deterministic stress-test results for the selected complete published risk run.'
AS
SELECT
    selected.portfolio_id,
    selected.as_of_date,
    selected.base_currency,
    selected.risk_run_id,
    selected.attempt_number,
    selected.methodology,
    stress.scenario_id,
    stress.scenario_version,
    stress.shock_count,
    stress.stress_pnl AS stress_pnl_usd,
    stress.stressed_nav AS stressed_nav_usd,
    stress.calculation_version,
    stress.calculated_at_utc
FROM workspace.devin_market_risk_dev.vw_latest_published_risk_runs AS selected
INNER JOIN workspace.devin_market_risk_dev.gold_stress_results AS stress
    ON selected.risk_run_id = stress.risk_run_id;
