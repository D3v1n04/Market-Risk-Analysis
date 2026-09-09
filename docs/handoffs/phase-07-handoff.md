# Phase 07 Handoff — Risk Measures and Validation

## Status

- **Phase:** 07 — Risk Measures and Validation
- **Status:** Complete
- **Date:** 2026-09-09
- **Next phase:** 08 — SQL Serving and DBeaver QA

## Objective

Implement and validate an audited USD-only one-day historical-risk bundle over the
deterministic 2016 market history, with explainable VaR, deterministic hypothetical
stress results, immutable publication evidence, and independent reconciliation.

## Completed work

- Added contracts and Gold Delta definitions for risk runs, historical P&L scenarios,
  VaR measures, and stress results.
- Completed the governed stress-configuration Bronze-to-Silver path for three active
  deterministic hypothetical scenarios and 15 shocks per scenario.
- Implemented the audited Gold risk calculation using trusted Silver inputs and the
  approved published Gold outputs.
- Added read-only Gold validation SQL for selected historical-scenario contributions,
  exact VaR loss reconciliation, and deterministic stress-result reconciliation.
- Preserved USD-only policy, signed short exposure, immutable run lineage, append-only
  replay behavior, exact Decimal calculations, and no forecast interpretation.

## Validation evidence

| Check | Command or method | Result | What it proves |
| --- | --- | --- | --- |
| Deterministic market history | Live fixture and Gold input reconciliation | Pass — 252 sessions, 251 adjacent returns, 15 instruments | The approved 2016 price spine and complete instrument coverage are present |
| Historical VaR | Live Gold calculation | Pass — one-day USD VaR at 95% and 99% | Signed losses and discrete empirical measures use the approved static exposure and history |
| Static exposure | Live Gold calculation | Pass — January 7 quantities/cash repriced at December 30 | Buy-and-hold/no-rebalance/no-later-corporate-actions policy is applied |
| Stress scenarios | Live Gold calculation | Pass — 3 deterministic hypothetical scenarios × 15 shocks | Approved shock coverage and hypothetical-only semantics are preserved |
| Published portfolios | Live risk runs | Pass — `CORE_15_LONG` and `LONG_SHORT_130_30` completed successful published runs | Both representative long-only and long-short portfolios have successful bundles |
| Independent reconciliation | Read-only validation SQL and live checks | Pass — final VaR, contribution, and stress checks, including exact stress precision alignment | Stored results reconcile independently without tolerance masking |
| Append-only replay | CORE live attempts 1/2/3 | Pass — identical VaR and stress results and the same input manifest | Replays retain immutable audit attempts without changing calculated content |
| Local quality gate | 230 pytest tests, Ruff, `market-risk-check` | Pass — 230 tests, Ruff passed, 11/11 checks | Repository and environment checks pass at completion |
| Runtime warning review | Live Spark execution | Bounded expected warning | The global window warning is limited to the 251-row VaR distribution |

## Decisions and reasoning

| Decision | Reason | Rejected alternative or tradeoff |
| --- | --- | --- |
| Use one-day historical simulation | Transparent and auditable on the approved fixture | Monte Carlo and parametric methods are deferred |
| Keep loss signed as negative P&L | Gains remain negative loss amounts and shorts retain direction | Clipping or flooring would change the approved contract |
| Use January 7 static exposure and December 30 repricing | Makes the buy-and-hold risk question explicit | Later rebalances and corporate actions are outside this scenario policy |
| Treat stress scenarios as deterministic hypotheses | Shock assumptions are explainable and reproducible | They are not forecasts or predictions |
| Publish logically through a successful risk-run audit | Delta is atomic per table, so consumers require the matching successful published run | Cross-table physical atomicity is not claimed |

## Concepts the learner can explain

- Historical VaR selects an observed loss from a sorted one-day loss distribution; it
  does not promise a maximum loss or forecast a future outcome.
- Signed market value preserves long and short direction; component loss is exactly
  negative component P&L, so a gain has a negative loss amount.
- Static exposure is established from January 7 quantities and cash, then repriced at
  December 30 under buy-and-hold assumptions.
- A deterministic stress result is signed exposure multiplied by each approved shock,
  summed across 15 instruments, and added to static NAV.
- Immutable risk-run attempts and input manifests make successful reruns auditable;
  identical CORE attempts preserve the same calculated result content.

## Knowledge check

The supplied completion evidence covers implementation, live publication, replay
safety, independent reconciliation, and runtime warning review. No separate verbatim
explain-back transcript is included in this concise handoff.

## Open questions or blockers

- None for Phase 07 completion.
- Future work may add live history, Monte Carlo or parametric VaR, expected shortfall,
  formal backtesting, and consumer serving under separately governed scope.

## Git checkpoint

- **Branch:** `phase-07-risk-measures-validation`
- **Validated checkpoint:** `87e1f23` — `fix: align Phase 07 risk validation precision`
- **Working tree:** This documentation update is intentionally pending commit; no
  commit or push is performed by this handoff update.
- **Ignored/generated artifacts checked:** No generated manifests or platform assets
  were changed by this documentation update.

## Files and platform objects changed

### Repository

- `contracts/risk_runs.yml`, `contracts/historical_pnl_scenarios.yml`,
  `contracts/var_measures.yml`, `contracts/stress_results.yml` — Phase 07 contracts
- `sql/gold/phase_07_create_risk_tables.sql` — Gold risk table definitions
- `sql/gold/phase_07_validate_risk_results.sql` — read-only contribution validation
- `notebooks/gold/phase_07_calculate_risk_measures.py` — audited risk calculation
- `notebooks/bronze/phase_06_*reference*` and `notebooks/silver/phase_06_*reference*` —
  governed stress-configuration path
- `tests/` — Phase 07 contract, SQL, notebook, and validation checks
- `README.md` and `docs/progress.md` — completion status and evidence

### Databricks

- Published risk bundles for `CORE_15_LONG` and `LONG_SHORT_130_30` in the four
  Phase 07 Gold outputs; detailed run IDs and hashes remain in governed audit tables.

## Next-phase readiness

- Phase 07 contracts, Gold tables, published runs, contribution validation, and local
  quality evidence are complete.
- Phase 08 should verify the completion commit, current platform status, and published
  Gold counts before creating serving views or DBeaver QA scripts.
- Preserve the rule that consumers use only successful published risk runs and that
  Gold calculations read governed Silver or explicitly published Gold inputs.

## Suggested next-chat opening

> We are starting Phase 08 — SQL Serving and DBeaver QA. Read `README.md`,
> `docs/project-roadmap.md`, `docs/progress.md`, and
> `docs/handoffs/phase-07-handoff.md` first. Verify the Phase 07 published Gold
> evidence and Git status, then build only the governed serving and reconciliation
> scope.
