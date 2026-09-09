# Phase 08 Handoff — SQL Serving and Databricks QA

## Status

- **Phase:** 08 — SQL Serving and Databricks QA
- **Status:** Complete pending Git checkpoint
- **Date:** 2026-09-09
- **Next phase:** 09 — Power BI Semantic Model

## Objective

Publish stable consumer-serving views over governed Gold outputs and prove through
read-only Databricks SQL QA that analysts can query trusted daily analytics,
position exposure, VaR, and stress results without recalculating metrics, mixing
grains, or double-counting append-only risk reruns.

## Completed work

- Added a daily portfolio analytics view at one row per portfolio and valuation date.
- Added a position-exposure view at one row per portfolio, instrument, and valuation
  date.
- Added a controlled published-risk-run view that selects one complete successful
  published risk run per portfolio and as-of date.
- Added consumer VaR and stress-result views that use only the controlled selected
  risk runs.
- Added a version-controlled read-only SQL QA pack for Databricks SQL.
- Added automated SQL tests for source/grain preservation, controlled risk selection,
  no governed-data mutation, read-only QA coverage, and alias regression protection.
- Deployed all five consumer views successfully in Databricks SQL.

## Validation evidence

| Check | Command or method | Result | What it proves |
| --- | --- | --- | --- |
| Local serving and QA tests | `pytest -q tests/test_phase_08_serving_sql.py` | Pass — 8 tests | View structure, consumer grain, mutation boundary, QA coverage, and alias safeguards are protected |
| Whitespace check | `git diff --check` | Pass | No whitespace errors in the intentional local changes |
| Serving-object inventory | Databricks SQL QA pack | Pass — 5 views | All expected persistent consumer views exist |
| Daily analytics grain | Databricks SQL QA pack | Pass — 8 rows, 8 keys, 0 duplicates, USD only | One row per portfolio and valuation date is preserved |
| Daily Gold reconciliation | Databricks SQL QA pack | Pass — canonical Gold 8 rows, serving view 8 rows | The daily view preserves canonical Gold output without recalculation |
| Position-exposure grain | Databricks SQL QA pack | Pass — 120 rows, 120 keys, 0 duplicates, USD only | One row per portfolio, instrument, and valuation date is preserved |
| Risk-run selection | Databricks SQL QA pack | Pass — 2 rows, 2 keys, 0 duplicates | Exactly one risk run is served for each portfolio and as-of date |
| Rerun control | Databricks SQL QA pack | Pass — CORE_15_LONG attempt 3; LONG_SHORT_130_30 attempt 1 | Independent append-only attempts are not shown together or summed |
| Complete bundles | Databricks SQL QA pack | Pass — each selected run has 2 VaR and 3 stress results | Consumer risk output is complete for the selected risk run |
| Consumer output preview | Databricks SQL QA pack | Pass — 4 VaR and 6 stress rows | Analysts can query the controlled risk results directly |

## Decisions and reasoning

| Decision | Reason | Rejected alternative or tradeoff |
| --- | --- | --- |
| Serve Phase 06 daily and position Gold facts separately | They have different grains and business purposes | Joining them would multiply rows and create misleading analytics |
| Preserve canonical Phase 06 facts without latest-run filtering | Gold fact partitions are already canonical outputs | Applying Phase 07 append-only rerun logic would be incorrect |
| Select one complete published Phase 07 run per portfolio/as-of date | Risk attempts are standalone bundles and must not be added together | Serving every successful rerun would double-count risk results |
| Use deterministic newest-published ordering with explicit tie-breakers | Selection is reproducible and explainable | Unordered selection or aggregation could choose an arbitrary attempt |
| Use Databricks SQL Editor as the validated QA client | It runs directly in the governed platform and executed the live acceptance checks | DBeaver remains optional exploration tooling, not a required gate |
| Keep QA SQL read-only and version controlled | Analysts and future phases can rerun evidence without altering governed data | Ad hoc local query history is not a durable control |

## Concepts the learner can explain

- A serving view is a business-friendly query layer over governed outputs; it should
  not silently recalculate metrics.
- Grain is the meaning of one row. Joining different grains without an explicit
  aggregation plan can duplicate or distort data.
- Phase 06 Gold facts are canonical published partitions, while Phase 07 risk
  outputs are append-only audit results.
- A successful published risk run is a complete standalone calculation. Several
  reruns are not additive and must not be summed.
- QA verifies that consumers receive trusted, complete, duplicate-free data with
  clear lineage and scope.

## Knowledge check

The learner correctly explained that separate risk attempts are standalone
calculations and that summing them would create incorrect data. The learner also
identified that QA is an audit of the pipeline and consumer-serving layer to ensure
trusted, reliable, clean, valid data without duplicates or mismatches.

## Open questions or blockers

- None for Phase 08 completion.
- Separate least-privilege consumer access was not independently demonstrated because
  validation used the existing administrator identity.
- Phase 09 must define and validate Power BI model relationships and measures without
  reintroducing cross-grain joins or risk-rerun double counting.

## Git checkpoint

- **Branch:** `phase-08-sql-serving-dbeaver-qa`
- **Commit:** Not committed
- **Working tree:** Intentional Phase 08 serving SQL, QA SQL, tests, roadmap,
  progress, phase record, and handoff changes are pending review.
- **Ignored/generated artifacts checked:** No credentials, tokens, or generated
  platform state are intended for commit.

## Files and platform objects changed

### Repository

- `sql/serving/phase_08_create_serving_views.sql` — daily analytics and position
  exposure serving views
- `sql/serving/phase_08_create_risk_serving_views.sql` — controlled risk-run, VaR,
  and stress serving views
- `sql/qa/phase_08_serving_qa.sql` — read-only Databricks QA pack
- `tests/test_phase_08_serving_sql.py` — serving, QA, and alias-regression tests
- `docs/phase-08-sql-serving-databricks-qa.md` — implementation and validation
  record
- `docs/project-roadmap.md` and `docs/progress.md` — Phase 08 plan and evidence
- `docs/handoffs/phase-08-handoff.md` — this handoff

### Databricks

- `workspace.devin_market_risk_dev.vw_portfolio_daily_analytics`
- `workspace.devin_market_risk_dev.vw_position_exposure_detail`
- `workspace.devin_market_risk_dev.vw_latest_published_risk_runs`
- `workspace.devin_market_risk_dev.vw_latest_published_var`
- `workspace.devin_market_risk_dev.vw_latest_published_stress_results`

All five are persistent views. No Bronze, Silver, or Gold governed table rows were
mutated by Phase 08 serving deployment or QA.

## Next-phase readiness

- Serving views and their consumer grains are explicit and live in Databricks.
- The QA pack provides repeatable evidence for serving-view counts, keys, currency,
  canonical-source preservation, complete risk bundles, and rerun selection.
- Phase 09 can use these views as its approved Power BI sources.
- Phase 09 must verify the Phase 08 Git checkpoint and re-run the QA pack before
  creating a semantic model.

## Suggested next-chat opening

> We are starting Phase 09 — Power BI Semantic Model. Read `README.md`,
> `docs/project-roadmap.md`, `docs/progress.md`, and
> `docs/handoffs/phase-08-handoff.md` first. Verify the Phase 08 Git checkpoint and
> live Databricks serving-view QA, then build only the validated Power BI semantic
> model scope.
