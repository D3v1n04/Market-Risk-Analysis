# Phase 11 Handoff — Automation, Deployment, and CI

## Status

- **Phase:** 11 — Automation, Deployment, and CI
- **Status:** Complete
- **Date:** 2026-09-11
- **Next phase:** 12 — End-to-End Validation and Portfolio Handoff

## Objective

Add safe, manual development automation around the governed lakehouse: repository CI,
Databricks job definitions, controlled deployment, runtime validation, and rerun
evidence without introducing schedules, production resources, or unattended
credentials.

## Completed work

- Added credential-free GitHub Actions CI for push and pull-request quality gates.
- Added development-only Databricks bundle resources for Initialization and Daily
  Risk jobs.
- Added effective Bronze batch-ID task values for safe downstream Silver processing.
- Added a read-only serving-output validation notebook and wired it as the final
  Daily Risk task.
- Deployed the reviewed bundle to the learner-owned Databricks development workspace.
- Ran Initialization successfully.
- Ran Daily Risk successfully for `CORE_15_LONG`, `2016-01-07`, and `2016-12-30`.
- Ran the identical Daily Risk request again to prove safe rerun behavior.

## Validation evidence

| Check | Result | What it proves |
| --- | --- | --- |
| Local quality gate | Pass — 245 tests, Ruff, and 11/11 environment checks | Repository behavior and quality safeguards remain valid |
| GitHub Actions CI | Pass | Push/PR repository quality is independently enforced without Databricks credentials |
| Bundle validation | Pass — `databricks bundle validate --target dev` | Development resource definitions are valid |
| Bundle deployment | Pass — 2 jobs created | Reviewed bundle is deployed only to the development workspace |
| Initialization run `393419250031124` | Pass — 7/7 tasks | Portfolio and reference initialization chain is executable |
| Daily Risk run `1091019722211887` | Pass — 10/10 tasks | Full Bronze → Silver → Gold → risk → serving chain is executable |
| Daily Risk rerun `142939735918543` | Pass — 10/10 tasks | Identical request is safe to repeat |
| Serving evidence | Pass — 1 daily row, 15 positions, 1 selected risk run, 2 VaR, 3 stress results | Consumer-facing views remain complete and non-duplicated |

## Rerun evidence

- Bronze repeated sources were recorded as `SKIPPED_DUPLICATE`; duplicate business rows
  were not inserted.
- Silver repeated market batches succeeded with zero accepted rows, full unchanged
  counts, and `published = false`.
- `published = false` on a successful Silver rerun means there was nothing new or
  corrected to publish—not that the task was still running.
- Risk attempts remain immutable. The rerun created attempt 5, while the serving
  layer continued to return exactly one selected complete risk run.

## Decisions and boundaries

| Decision | Reason |
| --- | --- |
| CI is credential-free and local-only | A green CI run proves repository quality, not remote execution or business correctness |
| Deployment remains manual via OAuth | Avoids unattended credentials and keeps the learning MVP reviewable |
| Jobs are development-only and unscheduled | Prevents accidental repeated execution while operation is still being learned |
| `max_concurrent_runs: 1` | Prevents overlapping workflow execution |
| Final task validates serving views | A successful notebook chain alone is insufficient if consumer-facing results are incomplete or duplicated |
| Power BI remains manual | No Power BI Service publishing or automated refresh was approved |

## Concepts the learner can explain

- Bronze records a duplicate attempt so operations remain auditable even when no
  duplicate business data is written.
- A successful unchanged Silver run has `published = false` because no new canonical
  snapshot should be emitted.
- Immutable risk-attempt history supports debugging and replay, while the serving
  view selects one complete published attempt to prevent double counting.
- GitHub CI validates version-controlled repository quality; manual bundle deployment
  and job execution control the actual Databricks workspace runtime.

## Git and platform state

- **Branch:** `phase-11-automation-deployment-ci`
- **Implementation commit:** `d848fef` — `feat: validate serving outputs in market risk workflow`
- **Deployed code version:** `d848fef6a7e7a836066fd61139a7db59b74a75b6`
- **Development workspace:** `dbc-e87c98ff-1b61.cloud.databricks.com`
- **Initialization job ID:** `264824630592706`
- **Daily Risk job ID:** `29942438961628`

## Files and platform objects changed

### Repository

- `.github/workflows/ci.yml` — credential-free GitHub Actions quality gate.
- `databricks.yml` — resource inclusion and development target.
- `resources/phase_11_initialization_job.yml` — Initialization job definition.
- `resources/phase_11_market_risk_job.yml` — Daily Risk job definition.
- `notebooks/qa/phase_11_validate_serving_outputs.py` — read-only serving validator.
- Phase 11 tests and documentation — workflow safeguards and handoff evidence.

### Databricks

- `[dev devinklee3] Market Risk Initialization` — persistent bundle-managed job.
- `[dev devinklee3] Market Risk Daily Risk` — persistent bundle-managed job.
- No schedule, production target, Power BI automation, or unattended credential was
  introduced.

## Next-phase readiness

- Development bundle deployment and manual job operation are proven.
- Initialization and Daily Risk reruns are proven safe for the controlled inputs.
- Serving views still enforce one selected complete risk run for each portfolio/date.
- Phase 12 should begin with a fresh Git/CI/bundle/resource inventory and define its
  final portfolio-handoff scope before changing code or data.
