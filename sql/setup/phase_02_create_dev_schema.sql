-- Phase 02 persistent development namespace setup.
-- Retention decision: keep this schema for later project development.
-- This statement creates no tables and ingests no data.

CREATE SCHEMA IF NOT EXISTS workspace.devin_market_risk_dev
COMMENT 'Learner-owned development schema for the Market Risk Analysis project.';

DESCRIBE SCHEMA EXTENDED workspace.devin_market_risk_dev;

-- Phase 02 expects this retained development schema to contain no tables.
SHOW TABLES IN workspace.devin_market_risk_dev;
