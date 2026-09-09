-- Phase 08 read-only serving-layer QA pack.
-- Run in Databricks SQL after deploying the Phase 08 serving views.
-- No query below mutates Bronze, Silver, Gold, or serving data.


-- Confirm that all five consumer views exist.
SELECT
    table_name,
    table_type
FROM workspace.information_schema.tables
WHERE table_schema = 'devin_market_risk_dev'
  AND table_name IN (
      'vw_portfolio_daily_analytics',
      'vw_position_exposure_detail',
      'vw_latest_published_risk_runs',
      'vw_latest_published_var',
      'vw_latest_published_stress_results'
  )
ORDER BY table_name;


-- Daily analytics: one row per portfolio and valuation date.
-- Expected: served_row_count = distinct_business_key_count = 8.
SELECT
    COUNT(*) AS served_row_count,
    COUNT(DISTINCT portfolio_id, valuation_date) AS distinct_business_key_count,
    COUNT(*) - COUNT(DISTINCT portfolio_id, valuation_date) AS duplicate_business_key_count,
    MIN(valuation_date) AS first_valuation_date,
    MAX(valuation_date) AS last_valuation_date,
    MIN(base_currency) AS minimum_currency,
    MAX(base_currency) AS maximum_currency
FROM workspace.devin_market_risk_dev.vw_portfolio_daily_analytics;


-- Daily analytics: serving view must preserve canonical Gold row count.
-- Expected: both rows report 8.
SELECT
    'canonical_gold_metrics' AS source_name,
    COUNT(*) AS row_count
FROM workspace.devin_market_risk_dev.gold_portfolio_daily_metrics

UNION ALL

SELECT
    'served_daily_analytics',
    COUNT(*)
FROM workspace.devin_market_risk_dev.vw_portfolio_daily_analytics;


-- Position exposure: one row per portfolio, instrument, and valuation date.
-- Expected: served_row_count = distinct_business_key_count = 120.
SELECT
    COUNT(*) AS served_row_count,
    COUNT(DISTINCT portfolio_id, instrument_id, valuation_date) AS distinct_business_key_count,
    COUNT(*) - COUNT(DISTINCT portfolio_id, instrument_id, valuation_date)
        AS duplicate_business_key_count,
    MIN(base_currency) AS minimum_currency,
    MAX(base_currency) AS maximum_currency
FROM workspace.devin_market_risk_dev.vw_position_exposure_detail;


-- Risk-run selection: exactly one intentionally selected run per
-- portfolio and as-of date. This is the no-double-count control.
-- Expected: selected_row_count = distinct_business_key_count = 2.
SELECT
    COUNT(*) AS selected_row_count,
    COUNT(DISTINCT portfolio_id, as_of_date) AS distinct_business_key_count,
    COUNT(*) - COUNT(DISTINCT portfolio_id, as_of_date)
        AS duplicate_business_key_count
FROM workspace.devin_market_risk_dev.vw_latest_published_risk_runs;


-- Complete selected risk bundles.
-- Expected: two rows; each has two VaR measures and three stress results.
WITH selected_runs AS (
    SELECT
        portfolio_id,
        as_of_date,
        risk_run_id,
        attempt_number
    FROM workspace.devin_market_risk_dev.vw_latest_published_risk_runs
),
var_counts AS (
    SELECT
        risk_run_id,
        COUNT(*) AS var_measure_count
    FROM workspace.devin_market_risk_dev.vw_latest_published_var
    GROUP BY risk_run_id
),
stress_counts AS (
    SELECT
        risk_run_id,
        COUNT(*) AS stress_result_count
    FROM workspace.devin_market_risk_dev.vw_latest_published_stress_results
    GROUP BY risk_run_id
)
SELECT
    selected_runs.portfolio_id,
    selected_runs.as_of_date,
    selected_runs.risk_run_id,
    selected_runs.attempt_number,
    COALESCE(var_counts.var_measure_count, 0) AS var_measure_count,
    COALESCE(stress_counts.stress_result_count, 0) AS stress_result_count
FROM selected_runs
LEFT JOIN var_counts
    ON selected_runs.risk_run_id = var_counts.risk_run_id
LEFT JOIN stress_counts
    ON selected_runs.risk_run_id = stress_counts.risk_run_id
ORDER BY selected_runs.portfolio_id;


-- Consumer-ready VaR values.
SELECT
    portfolio_id,
    as_of_date,
    confidence_level,
    var_amount_usd,
    risk_run_id
FROM workspace.devin_market_risk_dev.vw_latest_published_var
ORDER BY portfolio_id, confidence_level;


-- Consumer-ready stress-test results.
SELECT
    portfolio_id,
    as_of_date,
    scenario_id,
    scenario_version,
    shock_count,
    stress_pnl_usd,
    stressed_nav_usd,
    risk_run_id
FROM workspace.devin_market_risk_dev.vw_latest_published_stress_results
ORDER BY portfolio_id, scenario_id;
