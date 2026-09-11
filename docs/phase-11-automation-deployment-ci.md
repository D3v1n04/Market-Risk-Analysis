# Phase 11 — Automation, Deployment, and CI

## Objective

Turn the existing governed notebooks into safe, manual development workflows with
credential-free GitHub CI, explicit Databricks bundle deployment, auditable task
dependencies, and consumer-facing serving validation.

## Completed implementation

- Added credential-free GitHub Actions CI for push and pull-request validation.
  CI uses locked `uv` installation, Ruff, pytest, environment checks, offline bundle
  configuration checks, and generated-file/secret hygiene checks.
- Added Databricks bundle resource inclusion through `resources/*.yml`.
- Added two development-only jobs with `max_concurrent_runs: 1`, 30-minute task
  timeouts, and zero configured retries:
  - `Market Risk Initialization`
  - `Market Risk Daily Risk`
- Added Bronze task values for effective batch IDs. On first ingestion they expose
  the new batch; on an identical rerun they expose the earlier successful batch.
- Added a read-only serving-output validation notebook as the final Daily Risk task.
  It validates one portfolio/date/risk-date scope and raises an error if the
  consumer-facing serving views are missing, duplicated, incomplete, or inconsistent.

## Workflow behavior

Initialization runs the controlled portfolio and reference-data chain:

```text
Bronze portfolios
→ Silver portfolios
→ Bronze reference data
→ Silver instruments
→ Silver target allocations
→ Silver stress scenarios
→ Silver stress scenario shocks

Bronze market data
→ trading calendar
→ Silver daily prices
→ Silver corporate actions
→ positions
→ cash balances
→ Gold position market values
→ Gold daily metrics
→ risk measures
→ serving-output validation
