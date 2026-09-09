-- Read-only Phase 07 published-risk reconciliation.
-- Selected historical-scenario dollar contributions are reported per instrument
-- and confidence level. They are not normalized or aggregated across scenarios.

WITH published_risk_runs AS (
    SELECT
        risk_run_id,
        portfolio_id,
        as_of_date,
        status,
        published,
        expected_historical_pnl_scenario_count,
        calculated_historical_pnl_scenario_count,
        expected_var_measure_count,
        calculated_var_measure_count,
        expected_stress_result_count,
        calculated_stress_result_count
    FROM workspace.devin_market_risk_dev.gold_risk_runs
    WHERE status = 'SUCCEEDED' AND published = true
)
SELECT
    risk_run_id,
    portfolio_id,
    as_of_date,
    status,
    published,
    expected_historical_pnl_scenario_count,
    calculated_historical_pnl_scenario_count,
    expected_var_measure_count,
    calculated_var_measure_count,
    expected_stress_result_count,
    calculated_stress_result_count,
    expected_historical_pnl_scenario_count = 251
        AND calculated_historical_pnl_scenario_count = 251
        AND expected_var_measure_count = 2
        AND calculated_var_measure_count = 2
        AND expected_stress_result_count = 3
        AND calculated_stress_result_count = 3
        AS published_result_counts_valid
FROM published_risk_runs
ORDER BY portfolio_id, as_of_date, risk_run_id;


WITH published_successful_runs AS (
    SELECT
        risk_run_id,
        portfolio_id,
        as_of_date,
        static_exposure_source_date,
        static_exposure_valuation_date,
        expected_historical_pnl_scenario_count = 251
            AND calculated_historical_pnl_scenario_count = 251
            AND expected_var_measure_count = 2
            AND calculated_var_measure_count = 2
            AND expected_stress_result_count = 3
            AND calculated_stress_result_count = 3
            AS published_result_counts_valid
    FROM workspace.devin_market_risk_dev.gold_risk_runs
    WHERE status = 'SUCCEEDED' AND published = true
),
expected_var_levels AS (
    SELECT CAST(0.95 AS DECIMAL(3,2)) AS confidence_level, 239 AS quantile_rank
    UNION ALL
    SELECT CAST(0.99 AS DECIMAL(3,2)) AS confidence_level, 249 AS quantile_rank
),
ranked_historical_losses AS (
    SELECT
        historical.risk_run_id,
        historical.portfolio_id,
        historical.prior_price_date,
        historical.scenario_date,
        historical.loss_amount,
        ROW_NUMBER() OVER (
            PARTITION BY historical.risk_run_id
            ORDER BY historical.loss_amount ASC, historical.scenario_date ASC
        ) AS deterministic_loss_rank
    FROM workspace.devin_market_risk_dev.gold_historical_pnl_scenarios AS historical
    INNER JOIN published_successful_runs AS run
        ON historical.risk_run_id = run.risk_run_id
),
var_measure_validations AS (
    SELECT
        measures.risk_run_id,
        COUNT(*) = 2
            AND COUNT(DISTINCT measures.confidence_level) = 2
            AND SUM(
                CASE
                    WHEN measures.confidence_level = CAST(0.95 AS DECIMAL(3,2))
                        AND measures.quantile_rank = 239 THEN 1
                    WHEN measures.confidence_level = CAST(0.99 AS DECIMAL(3,2))
                        AND measures.quantile_rank = 249 THEN 1
                    ELSE 0
                END
            ) = 2 AS var_measure_mapping_valid
    FROM workspace.devin_market_risk_dev.gold_var_measures AS measures
    INNER JOIN published_successful_runs AS run
        ON measures.risk_run_id = run.risk_run_id
    GROUP BY measures.risk_run_id
),
selected_var_scenarios AS (
    SELECT
        run.risk_run_id,
        run.portfolio_id,
        expected.confidence_level,
        expected.quantile_rank,
        measures.var_amount,
        losses.prior_price_date,
        losses.scenario_date,
        losses.loss_amount AS selected_historical_loss_amount,
        COALESCE(validation.var_measure_mapping_valid, false)
            AS var_measure_mapping_valid,
        measures.confidence_level = expected.confidence_level
            AND measures.quantile_rank = expected.quantile_rank
            AND losses.deterministic_loss_rank = expected.quantile_rank
            AS approved_confidence_rank_mapping,
        run.published_result_counts_valid
    FROM published_successful_runs AS run
    CROSS JOIN expected_var_levels AS expected
    LEFT JOIN workspace.devin_market_risk_dev.gold_var_measures AS measures
        ON measures.risk_run_id = run.risk_run_id
        AND measures.confidence_level = expected.confidence_level
    LEFT JOIN ranked_historical_losses AS losses
        ON losses.risk_run_id = run.risk_run_id
        AND measures.quantile_rank = expected.quantile_rank
        AND losses.deterministic_loss_rank = expected.quantile_rank
    LEFT JOIN var_measure_validations AS validation
        ON run.risk_run_id = validation.risk_run_id
),
static_exposure AS (
    SELECT
        run.risk_run_id,
        run.portfolio_id,
        position.instrument_id,
        CAST(position.signed_quantity AS DECIMAL(38,16)) AS signed_quantity,
        CAST(price.close_price AS DECIMAL(20,8)) AS december_30_close_price,
        CAST(
            position.signed_quantity * price.close_price AS DECIMAL(38,16)
        ) AS signed_market_value
    FROM published_successful_runs AS run
    INNER JOIN workspace.devin_market_risk_dev.silver_positions AS position
        ON position.portfolio_id = run.portfolio_id
        AND position.position_date = run.static_exposure_source_date
    INNER JOIN workspace.devin_market_risk_dev.silver_daily_prices AS price
        ON price.instrument_id = position.instrument_id
        AND price.price_date = run.static_exposure_valuation_date
),
selected_scenario_returns AS (
    SELECT
        selected.risk_run_id,
        selected.portfolio_id,
        selected.confidence_level,
        selected.quantile_rank,
        selected.var_amount,
        selected.prior_price_date,
        selected.scenario_date,
        selected.selected_historical_loss_amount,
        selected.var_measure_mapping_valid,
        selected.approved_confidence_rank_mapping,
        selected.published_result_counts_valid,
        scenario_price.instrument_id,
        CAST(
            CAST(scenario_price.adjusted_close_price AS DECIMAL(38,16))
                / CAST(prior_price.adjusted_close_price AS DECIMAL(38,16))
                - CAST(1 AS DECIMAL(38,16))
            AS DECIMAL(38,16)
        ) AS historical_return
    FROM selected_var_scenarios AS selected
    INNER JOIN workspace.devin_market_risk_dev.silver_daily_prices AS prior_price
        ON prior_price.price_date = selected.prior_price_date
    INNER JOIN workspace.devin_market_risk_dev.silver_daily_prices AS scenario_price
        ON scenario_price.instrument_id = prior_price.instrument_id
        AND scenario_price.price_date = selected.scenario_date
),
component_contributions AS (
    SELECT
        returns.risk_run_id,
        returns.portfolio_id,
        returns.confidence_level,
        returns.quantile_rank,
        returns.prior_price_date,
        returns.scenario_date,
        returns.instrument_id,
        exposure.signed_quantity,
        exposure.december_30_close_price,
        exposure.signed_market_value,
        returns.historical_return,
        CAST(
            exposure.signed_market_value * returns.historical_return AS DECIMAL(38,16)
        ) AS component_pnl,
        CAST(
            -(exposure.signed_market_value * returns.historical_return)
            AS DECIMAL(38,16)
        ) AS component_loss,
        returns.selected_historical_loss_amount,
        returns.var_amount,
        returns.var_measure_mapping_valid,
        returns.approved_confidence_rank_mapping,
        returns.published_result_counts_valid,
        'SELECTED_HISTORICAL_SCENARIO_CONTRIBUTION' AS contribution_basis
    FROM selected_scenario_returns AS returns
    INNER JOIN static_exposure AS exposure
        ON returns.risk_run_id = exposure.risk_run_id
        AND returns.instrument_id = exposure.instrument_id
),
component_loss_reconciliations AS (
    SELECT
        risk_run_id,
        confidence_level,
        quantile_rank,
        COUNT(*) AS component_instrument_count,
        COUNT(DISTINCT instrument_id) AS distinct_component_instrument_count,
        COUNT(*) = 15 AND COUNT(DISTINCT instrument_id) = 15
            AS component_cardinality_valid,
        CAST(SUM(component_loss) AS DECIMAL(38,16)) AS component_loss_total,
        MAX(var_amount) AS var_amount,
        MAX(selected_historical_loss_amount) AS selected_historical_loss_amount
    FROM component_contributions
    GROUP BY risk_run_id, confidence_level, quantile_rank
)
SELECT
    selected.risk_run_id,
    selected.portfolio_id,
    selected.confidence_level,
    selected.quantile_rank,
    component.prior_price_date,
    component.scenario_date,
    component.instrument_id,
    component.signed_quantity,
    component.december_30_close_price,
    component.signed_market_value,
    component.historical_return,
    component.component_pnl,
    component.component_loss,
    component.contribution_basis,
    reconciliation.component_instrument_count,
    reconciliation.distinct_component_instrument_count,
    reconciliation.component_cardinality_valid,
    reconciliation.component_loss_total,
    selected.selected_historical_loss_amount,
    selected.var_amount,
    selected.var_measure_mapping_valid,
    selected.approved_confidence_rank_mapping,
    selected.published_result_counts_valid,
    selected.selected_historical_loss_amount = selected.var_amount
        AS selected_ranked_loss_reconciles_to_var_amount,
    reconciliation.component_loss_total = selected.var_amount
        AS component_loss_reconciles_to_var_amount,
    COALESCE(reconciliation.component_cardinality_valid, false)
        AND selected.var_measure_mapping_valid
        AND selected.approved_confidence_rank_mapping
        AND selected.published_result_counts_valid
        AND selected.selected_historical_loss_amount = selected.var_amount
        AND reconciliation.component_loss_total = selected.var_amount
        AS reconciles
FROM selected_var_scenarios AS selected
LEFT JOIN component_contributions AS component
    ON selected.risk_run_id = component.risk_run_id
    AND selected.confidence_level = component.confidence_level
    AND selected.quantile_rank = component.quantile_rank
LEFT JOIN component_loss_reconciliations AS reconciliation
    ON selected.risk_run_id = reconciliation.risk_run_id
    AND selected.confidence_level = reconciliation.confidence_level
    AND selected.quantile_rank = reconciliation.quantile_rank
ORDER BY
    selected.portfolio_id,
    selected.risk_run_id,
    selected.confidence_level,
    component.instrument_id;


WITH published_successful_runs AS (
    SELECT
        risk_run_id,
        portfolio_id,
        static_exposure_source_date,
        static_exposure_valuation_date,
        expected_stress_result_count = 3
            AND calculated_stress_result_count = 3
            AS published_stress_result_count_valid
    FROM workspace.devin_market_risk_dev.gold_risk_runs
    WHERE status = 'SUCCEEDED' AND published = true
),
static_exposure AS (
    SELECT
        run.risk_run_id,
        run.portfolio_id,
        position.instrument_id,
        CAST(
            position.signed_quantity * price.close_price AS DECIMAL(38,16)
        ) AS signed_market_value
    FROM published_successful_runs AS run
    INNER JOIN workspace.devin_market_risk_dev.silver_positions AS position
        ON position.portfolio_id = run.portfolio_id
        AND position.position_date = run.static_exposure_source_date
    INNER JOIN workspace.devin_market_risk_dev.silver_daily_prices AS price
        ON price.instrument_id = position.instrument_id
        AND price.price_date = run.static_exposure_valuation_date
),
static_nav AS (
    SELECT
        exposure.risk_run_id,
        CAST(
            SUM(exposure.signed_market_value) + cash.closing_cash_balance
            AS DECIMAL(38,16)
        ) AS static_nav
    FROM static_exposure AS exposure
    INNER JOIN published_successful_runs AS run
        ON exposure.risk_run_id = run.risk_run_id
    INNER JOIN workspace.devin_market_risk_dev.silver_cash_balances AS cash
        ON cash.portfolio_id = run.portfolio_id
        AND cash.cash_date = run.static_exposure_source_date
    GROUP BY exposure.risk_run_id, cash.closing_cash_balance
),
stress_components AS (
    SELECT
        result.risk_run_id,
        result.scenario_id,
        result.scenario_version,
        exposure.instrument_id,
        exposure.signed_market_value,
        shock.shock_ratio,
        CAST(
            exposure.signed_market_value * shock.shock_ratio AS DECIMAL(38,16)
        ) AS component_stress_pnl,
        scenario.scenario_type,
        scenario.is_active,
        scenario.record_hash = result.input_stress_scenario_record_sha256
            AS scenario_lineage_matches
    FROM workspace.devin_market_risk_dev.gold_stress_results AS result
    INNER JOIN published_successful_runs AS run
        ON result.risk_run_id = run.risk_run_id
    INNER JOIN static_exposure AS exposure
        ON result.risk_run_id = exposure.risk_run_id
    INNER JOIN workspace.devin_market_risk_dev.silver_stress_scenarios AS scenario
        ON result.scenario_id = scenario.scenario_id
        AND result.scenario_version = scenario.scenario_version
    INNER JOIN workspace.devin_market_risk_dev.silver_stress_scenario_shocks AS shock
        ON result.scenario_id = shock.scenario_id
        AND result.scenario_version = shock.scenario_version
        AND exposure.instrument_id = shock.instrument_id
),
stress_reconciliations AS (
    SELECT
        component.risk_run_id,
        component.scenario_id,
        component.scenario_version,
        COUNT(*) AS recalculated_shock_count,
        COUNT(DISTINCT component.instrument_id) AS distinct_shock_instrument_count,
        COUNT(*) = 15 AND COUNT(DISTINCT component.instrument_id) = 15
            AS stress_cardinality_valid,
        CAST(SUM(component.component_stress_pnl) AS DECIMAL(38,16))
            AS recalculated_stress_pnl,
        MIN(
            CASE
                WHEN component.scenario_type = 'DETERMINISTIC_HYPOTHETICAL'
                    THEN 1
                ELSE 0
            END
        ) = 1 AS has_deterministic_hypothetical_scenario,
        MIN(CASE WHEN component.is_active THEN 1 ELSE 0 END) = 1
            AS has_active_scenario,
        MIN(CASE WHEN component.scenario_lineage_matches THEN 1 ELSE 0 END) = 1
            AS scenario_lineage_matches
    FROM stress_components AS component
    GROUP BY component.risk_run_id, component.scenario_id, component.scenario_version
)
SELECT
    result.risk_run_id,
    result.portfolio_id,
    result.scenario_id,
    result.scenario_version,
    result.shock_count AS recorded_shock_count,
    reconciliation.recalculated_shock_count,
    reconciliation.distinct_shock_instrument_count,
    reconciliation.stress_cardinality_valid,
    result.stress_pnl,
    reconciliation.recalculated_stress_pnl,
    result.stressed_nav,
    static_nav.static_nav,
    reconciliation.has_deterministic_hypothetical_scenario,
    reconciliation.has_active_scenario,
    reconciliation.scenario_lineage_matches,
    result.shock_count = reconciliation.recalculated_shock_count
        AND reconciliation.recalculated_shock_count = 15
        AND reconciliation.distinct_shock_instrument_count = 15
        AS stress_shock_count_reconciles,
    result.stress_pnl = reconciliation.recalculated_stress_pnl
        AS stress_pnl_reconciles,
    result.stressed_nav = CAST(
        static_nav.static_nav + reconciliation.recalculated_stress_pnl
        AS DECIMAL(38,16)
    ) AS stressed_nav_reconciles,
    COALESCE(reconciliation.stress_cardinality_valid, false)
        AND result.shock_count = reconciliation.recalculated_shock_count
        AND reconciliation.recalculated_shock_count = 15
        AND reconciliation.distinct_shock_instrument_count = 15
        AND result.stress_pnl = reconciliation.recalculated_stress_pnl
        AND result.stressed_nav = CAST(
            static_nav.static_nav + reconciliation.recalculated_stress_pnl
            AS DECIMAL(38,16)
        )
        AND reconciliation.has_deterministic_hypothetical_scenario
        AND reconciliation.has_active_scenario
        AND reconciliation.scenario_lineage_matches
        AND run.published_stress_result_count_valid
        AS reconciles
FROM workspace.devin_market_risk_dev.gold_stress_results AS result
INNER JOIN published_successful_runs AS run
    ON result.risk_run_id = run.risk_run_id
LEFT JOIN stress_reconciliations AS reconciliation
    ON result.risk_run_id = reconciliation.risk_run_id
    AND result.scenario_id = reconciliation.scenario_id
    AND result.scenario_version = reconciliation.scenario_version
LEFT JOIN static_nav
    ON result.risk_run_id = static_nav.risk_run_id
ORDER BY result.portfolio_id, result.risk_run_id, result.scenario_id;
