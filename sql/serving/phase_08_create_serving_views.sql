-- Phase 08 SQL serving layer.
-- Consumer views preserve the governed Gold grain and do not recalculate metrics.

CREATE OR REPLACE VIEW workspace.devin_market_risk_dev.vw_portfolio_daily_analytics
COMMENT 'Business-friendly daily portfolio NAV, exposure, PnL, and return analytics sourced from canonical Gold metrics.'
AS
SELECT
    metrics.portfolio_id,
    metrics.valuation_date,
    metrics.base_currency,
    metrics.position_count,
    metrics.long_position_count,
    metrics.short_position_count,
    metrics.long_market_value AS long_exposure_usd,
    metrics.short_market_value AS short_exposure_magnitude_usd,
    metrics.gross_market_value AS gross_exposure_usd,
    metrics.net_security_market_value AS net_security_market_value_usd,
    metrics.closing_cash_balance AS closing_cash_balance_usd,
    metrics.closing_nav AS closing_nav_usd,
    metrics.baseline_nav AS baseline_nav_usd,
    metrics.baseline_source,
    metrics.prior_valuation_date,
    metrics.daily_pnl AS daily_pnl_usd,
    metrics.daily_return AS daily_return_ratio,
    metrics.long_exposure_ratio,
    metrics.short_exposure_ratio,
    metrics.gross_exposure_ratio,
    metrics.net_exposure_ratio,
    metrics.analytics_run_id,
    metrics.calculation_version,
    metrics.calculated_at_utc
FROM workspace.devin_market_risk_dev.gold_portfolio_daily_metrics AS metrics;


CREATE OR REPLACE VIEW workspace.devin_market_risk_dev.vw_position_exposure_detail
COMMENT 'Business-friendly position-level market values and exposures sourced from canonical Gold valuations.'
AS
SELECT
    positions.portfolio_id,
    positions.instrument_id,
    positions.valuation_date,
    positions.position_side,
    positions.signed_quantity,
    positions.close_price AS close_price_usd_per_share,
    positions.quote_currency,
    positions.base_currency,
    positions.signed_market_value AS signed_market_value_usd,
    positions.absolute_market_value AS absolute_market_value_usd,
    positions.analytics_run_id,
    positions.calculation_version,
    positions.calculated_at_utc
FROM workspace.devin_market_risk_dev.gold_position_market_values AS positions;
