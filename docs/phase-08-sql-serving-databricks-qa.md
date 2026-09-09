# Phase 08 — SQL Serving and Databricks QA

## Objective

Publish stable consumer-serving views over approved Gold outputs and provide a
version-controlled, read-only Databricks SQL QA pack. Consumers must receive
business-friendly portfolio analytics and exactly one complete published risk bundle
per portfolio and as-of date, without recalculating governed metrics or
double-counting append-only risk reruns.

## Scope

Phase 08 creates consumer views only. It does not alter Bronze, Silver, Gold
calculation logic, contracts, historical PnL scenarios, VaR results, stress results,
or risk-run audit records.

Databricks SQL Editor is the validated execution and QA client. DBeaver remains an
optional exploration client and is not required for the Phase 08 acceptance gate.

## Serving views

| View | Consumer grain | Governed source | Purpose |
| --- | --- | --- | --- |
| `vw_portfolio_daily_analytics` | One row per portfolio and valuation date | `gold_portfolio_daily_metrics` | Daily NAV, cash, exposures, P&L, and return analytics |
| `vw_position_exposure_detail` | One row per portfolio, instrument, and valuation date | `gold_position_market_values` | Signed quantities and signed/absolute position exposure |
| `vw_latest_published_risk_runs` | One row per portfolio and as-of date | `gold_risk_runs` | Deterministically selects one complete published risk bundle |
| `vw_latest_published_var` | One row per selected risk run and confidence level | `gold_var_measures` | Consumer-ready 95% and 99% historical VaR |
| `vw_latest_published_stress_results` | One row per selected risk run and scenario | `gold_stress_results` | Consumer-ready deterministic stress P&L and stressed NAV |

## Risk-run selection policy

Phase 07 risk results are append-only audit evidence. A portfolio can therefore
have several successful historical calculations for the same as-of date. Those
attempts are standalone calculations, not additive observations.

The risk-run serving view accepts only runs that are:

- `status = 'SUCCEEDED'`
- `published = TRUE`
- complete and timestamped
- reconciled to 251 historical PnL scenarios, two VaR measures, and three stress
  results

It then assigns `ROW_NUMBER()` by portfolio and as-of date, ordered by newest
published timestamp, newest completion timestamp, highest attempt number, and
risk-run ID as a deterministic final tie-breaker. Only rank one is served.

This prevents analysts from summing independent reruns and creating false risk
totals.

## QA pack

`sql/qa/phase_08_serving_qa.sql` is read-only and validates:

1. the existence of all five serving views;
2. daily-analytics grain, duplicate absence, date range, and USD-only currency;
3. row-count preservation from canonical Gold daily metrics;
4. position-exposure grain, duplicate absence, and USD-only currency;
5. one selected risk run per portfolio and as-of date;
6. complete selected risk bundles with two VaR measures and three stress results;
7. consumer-ready VaR output; and
8. consumer-ready stress output.

## Live Databricks validation

| Check | Result | Evidence |
| --- | --- | --- |
| Serving objects | Pass | Five expected objects exist as views |
| Daily analytics grain | Pass | 8 rows, 8 distinct portfolio/date keys, 0 duplicates, USD only |
| Daily Gold reconciliation | Pass | Canonical Gold metrics: 8 rows; served daily analytics: 8 rows |
| Position-exposure grain | Pass | 120 rows, 120 distinct portfolio/instrument/date keys, 0 duplicates, USD only |
| Risk-run selection grain | Pass | 2 rows, 2 distinct portfolio/as-of-date keys, 0 duplicates |
| Selected risk bundles | Pass | Each selected run has 2 VaR measures and 3 stress results |
| Rerun control | Pass | `CORE_15_LONG` serves attempt 3; `LONG_SHORT_130_30` serves attempt 1 |
| Consumer risk output | Pass | 4 VaR rows and 6 stress-result rows |

## Local verification

- `pytest -q tests/test_phase_08_serving_sql.py` — 8 passed.
- `git diff --check` — passed.
- Serving SQL tests protect canonical-source usage, consumer grain, controlled
  risk-run selection, no governed-data mutation, read-only QA coverage, and the
  alias regression found during live deployment.

## Limitations and next phase

The serving views expose the approved deterministic Phase 06 and 07 data only.
They do not add live market history, multi-currency support, formal backtesting,
expected shortfall, Monte Carlo VaR, or new risk calculations.

Phase 09 will connect Power BI to these serving views, define a semantic model, and
reconcile report measures to Databricks SQL.
