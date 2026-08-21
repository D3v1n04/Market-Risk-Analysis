-- Phase 02 read-only Databricks SQL connectivity smoke test.
-- Run each statement independently in Databricks SQL Editor or DBeaver.
-- Expected project context: catalog workspace, schema default.
-- This file must not create, modify, or delete persistent objects.

SELECT
    1 AS connectivity_ok;

SELECT
    current_user() AS authenticated_identity;

SELECT
    current_catalog() AS current_catalog,
    current_schema() AS current_schema;

SHOW CATALOGS;

SHOW SCHEMAS IN workspace;

SHOW TABLES IN workspace.default;
