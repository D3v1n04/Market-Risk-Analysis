-- Phase 04 Bronze Delta table verification.

SHOW TABLES IN workspace.devin_market_risk_dev;

DESCRIBE DETAIL workspace.devin_market_risk_dev.bronze_portfolios;

DESCRIBE DETAIL workspace.devin_market_risk_dev.bronze_ingestion_batches;

DESCRIBE HISTORY workspace.devin_market_risk_dev.bronze_portfolios;

DESCRIBE HISTORY workspace.devin_market_risk_dev.bronze_ingestion_batches;

SELECT
    (
        SELECT COUNT(*)
        FROM workspace.devin_market_risk_dev.bronze_portfolios
    ) AS portfolio_row_count,
    (
        SELECT COUNT(*)
        FROM workspace.devin_market_risk_dev.bronze_ingestion_batches
    ) AS batch_row_count;
