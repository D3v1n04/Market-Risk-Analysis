# Phase 03 Handoff — Risk Requirements and Data Contracts

## Status

- **Phase:** 03 — Risk Requirements and Data Contracts
- **Status:** In progress — implementation and knowledge check complete; awaiting final documentation and Git checkpoint
- **Date:** 2026-08-27
- **Next phase:** 04 — Bronze Ingestion

## Objective

Define analytical scope, risk conventions, source strategy, dataset grains, keys, schemas, quality rules, deterministic reference data, and acceptance examples before creating lakehouse tables. Every planned logical dataset must be machine-readable and testable without licensed-data ambiguity or premature Bronze/Silver implementation.

## Completed work

- Defined dashboard questions, metric conventions, freshness expectations, risk assumptions, and MVP non-goals.
- Selected 15 US-listed instruments and defined two USD portfolios: 100% long-only and 130/30.
- Created 30 target-allocation rows: 15 per portfolio.
- Defined 11 logical dataset contracts: `instruments`, `portfolios`, `target_allocations`, `positions`, `daily_prices`, `corporate_actions`, `trading_calendar`, `ingestion_batches`, `data_quality_violations`, `stress_scenarios`, and `stress_scenario_shocks`.
- Declared grain, keys, fields, types, nullability, references, allowed values, ranges, fixture policy, and stable quality rules.
- Defined Yahoo Finance as the planned MVP price/action source while keeping deterministic fixtures authoritative for learning and tests.
- Defined exchange-calendar behavior for Nasdaq (`XNAS`) and NYSE (`XNYS`), including closures, early closes, UTC timestamps, and daylight-saving behavior.
- Defined immutable batch audits, count reconciliation, warning semantics, retry linkage, secret-safe diagnostics, and violation resolution through reprocessing.
- Created three hypothetical stress scenarios with exactly 45 deterministic instrument shocks.
- Created 28 deterministic passing/failing examples covering prices, duplicates, corrections, corporate actions, calendars, positions, batches, retries, and violation resolution.
- Added development-only PyYAML and automated contract validation.
- Aligned human documentation with `SUCCEEDED_WITH_WARNINGS`.
- Completed the Phase 03 explain-back.
- Did not create Databricks tables, retrieve live data, ingest Bronze records, or implement Silver transformations.

## Validation evidence

| Check | Command or method | Result | What it proves |
| --- | --- | --- | --- |
| YAML contracts | Automated parsing and structural assertions | 11 represented | Every planned logical dataset has required metadata |
| Instrument fixture | Independent contract test | 15 unique rows | Approved universe is complete |
| Portfolio fixture | Independent contract test | 2 unique rows | Both portfolio definitions are present |
| Allocation fixture | Independent totals/references | 30 rows; 15 each | Allocations are complete and reconcile |
| Long-only exposure | Decimal recomputation | 100% long, 0% short | Portfolio matches its mandate |
| 130/30 exposure | Decimal recomputation | 130% long, 30% short, 160% gross, 100% net | Signed allocations match the mandate |
| Stress scenarios | Coverage/reference tests | 3 scenarios, 45 shocks | Every scenario covers every instrument |
| Fixture integrity | SHA-256 recomputation | Pass | Stored hashes match canonical values |
| Symbol mapping | Explicit assertion | `BRK_B_US` → `BRK.B` → `BRK-B` | Internal, display, and provider symbols remain distinct |
| Edge examples | Rule/disposition tests | 28 unique cases | Expected behavior is deterministic |
| Contract tests | `uv run pytest tests/test_contracts.py` | 8/8 passed | Contracts and fixtures are internally consistent |
| Quality gate | `make check` | Ruff; pytest 12/12; environment 11/11 | Repository is healthy on Ubuntu |
| Explain-back | Guided knowledge check | Pass | Learner can explain core concepts and limitations |
| Phase boundary | Repository/platform review | Pass | No Bronze, live, licensed, or table work began |

## Decisions and reasoning

| Decision | Reason | Tradeoff rejected |
| --- | --- | --- |
| Define questions first | Keeps metrics tied to analytical needs | Calculating impressive but irrelevant metrics |
| Declare exact grain | Prevents ambiguous joins and duplicates | Letting implementation imply row meaning |
| Use stable internal IDs | Provider/display symbols can change | Using ticker text as every key |
| Use 15 instruments | Useful variation while explainable | Adding volume before understanding |
| Use long-only and 130/30 portfolios | Exercises signed and short exposure | One portfolio would miss long-short behavior |
| Use deterministic fixtures first | Reproducible, free, and licensing-safe | Changing live inputs too early |
| Preserve immutable Bronze evidence | Supports audit and reprocessing | Silently correcting source data |
| Make Silver canonical | Separates evidence from validated data | Cleaning Bronze in place |
| Include every calendar date | Recognizes closures and early closes | Weekday-only logic |
| Separate warnings from row outcomes | Prevents double-counting | Adding warnings to received counts |
| Use `SUCCEEDED_WITH_WARNINGS` | Separates usable warned output from partial data | Treating all warnings as incomplete |
| New batch for every retry | Preserves recovery history | Overwriting failed attempts |
| Resolve quarantine by reprocessing | Preserves immutable evidence | Manual Silver edits |
| Use varied deterministic shocks | Transparent and repeatable sensitivity | Unexplained randomness |
| Keep uniform -10% baseline | Simple independent reconciliation | Removing the simplest sanity check |
| Label stress as hypothetical | Avoids false forecasting claims | Presenting assumptions as predictions |
| Recompute hashes in tests | Detects accidental content changes | Checking format only |
| Keep PyYAML development-only | It supports tests, not runtime ingestion | Overstating runtime dependencies |

## Concepts the learner can explain

- Grain is exactly what one row represents.
- Primary keys identify rows; foreign keys connect datasets.
- Bronze preserves source evidence; Silver performs canonical validation.
- Trading calendars distinguish expected closures from missing prices.
- Warnings are diagnostic and do not create extra received records.
- `SUCCEEDED`, `SUCCEEDED_WITH_WARNINGS`, `PARTIAL`, and `FAILED` represent different completeness outcomes.
- Gross exposure measures absolute activity; net exposure measures direction.
- Negative quantity represents a short position and is prohibited in long-only portfolios.
- Deterministic stress shocks are explainable assumptions, not predictions.

## Glossary

| Term | Meaning |
| --- | --- |
| Grain | Exact real-world meaning of one row |
| Data contract | Agreement for schema, meaning, keys, and validation |
| Primary key | Fields uniquely identifying a row |
| Business key | Domain fields identifying the same observation |
| Foreign key | Field referencing another dataset |
| Reference data | Stable descriptive data |
| Observation data | Time-varying facts |
| Canonical data | Approved standardized downstream representation |
| Bronze | Immutable source-aligned layer with provenance |
| Silver | Typed, validated, deduplicated canonical layer |
| Batch | One processing attempt for a dataset/window |
| Idempotent | Safe to repeat without unexplained additional state |
| Provenance | Where, when, and how a record was obtained |
| Quarantine | Temporary exclusion pending resolvable context |
| Rejection | Terminal exclusion for invalid data |
| Warning | Investigation signal that may still allow use |
| Record hash | SHA-256 digest of canonical record content |
| Trading calendar | Exchange-aware expected dates/session times |
| Corporate action | Split or dividend affecting valuation |
| Long/short position | Positive owned exposure / negative borrowed exposure |
| Gross/net exposure | Total absolute activity / overall direction |
| Stress scenario | Hypothetical shocks measuring sensitivity |
| Deterministic fixture | Fixed reproducible test data |

## Knowledge check

The learner completed questions on grain, keys, Bronze preservation, batch statuses, warning counts, trading calendars, exposure, short positions, deterministic shocks, and scenario limitations.

Corrections reinforced that grain is one-row meaning; Bronze preserves audit evidence; missing weekend prices concern exchange closures rather than settlement; and stress scenarios show conditional portfolio sensitivity rather than forecasts.

The learner then correctly explained expected Saturday versus Monday price behavior and the conditional purpose of stress scenarios.

**Assessment:** passed after explain-back and correction.

## Open questions or blockers

- None.
- Phase 03 remains in progress only until this handoff, final status update, quality gate, and clean synchronized checkpoint are verified.
- Phase 04 must not begin before the completion checkpoint.

## Git checkpoint

- **Branch:** `phase-03-risk-requirements-contracts`
- **Latest validated implementation:** `3588ac3` — `docs: align ingestion batch status semantics`
- **Progress record:** `d067e8e` — `docs: record Phase 03 progress`
- **Working tree:** clean and synchronized after the learner's last validation; this handoff still requires pull/validation
- **Ignored/generated artifacts checked:** yes; credentials, Databricks state, environments, caches, and unapproved generated data remain outside Git

## Files and platform objects changed

### Repository

- `docs/phase-03-risk-requirements.md` — scope and metric conventions
- `docs/decisions/phase-03-source-strategy.md` — source/licensing strategy
- `docs/contracts/phase-03-data-contracts.md` — human-readable contracts
- `contracts/*.yml` — 11 machine-readable contracts
- `data/fixtures/instruments.csv` — 15 instruments
- `data/fixtures/portfolios.csv` — two portfolios
- `data/fixtures/target_allocations.csv` — 30 allocations
- `data/fixtures/stress_scenarios.csv` — three scenarios
- `data/fixtures/stress_scenario_shocks.csv` — 45 shocks
- `data/fixtures/contract_examples.yml` — 28 edge cases
- `tests/test_contracts.py` — contract/fixture validation
- `pyproject.toml` and `uv.lock` — development-only PyYAML
- `docs/progress.md` — Phase 03 evidence
- `docs/handoffs/phase-03-handoff.md` — this handoff

### Databricks, DBeaver, or Power BI

- No new Databricks table, view, workflow, or data file.
- No DBeaver or Power BI object.
- `workspace.devin_market_risk_dev` remains intentionally empty.

## Next-phase readiness

- All 11 logical datasets have approved contracts.
- Deterministic fixtures and edge cases define expected behavior.
- Contract tests protect keys, references, hashes, exposure totals, scenarios, and rules.
- Batch audit and violation behavior exist before ingestion.
- Phase 04 must verify the final Phase 03 commit and clean branch first.
- Phase 04 should ingest approved deterministic inputs, preserve source values/provenance, attach batch IDs, and prove idempotent reruns.
- Phase 04 must not implement Silver cleaning or claim live-data ingestion.

## Suggested next-chat opening

> We are starting Phase 04 — Bronze Ingestion. Read `README.md`, `docs/project-roadmap.md`, `docs/progress.md`, and `docs/handoffs/phase-03-handoff.md` first. Verify the final Phase 03 Git checkpoint and clean synchronized branch. Then teach and implement only Bronze ingestion for approved deterministic fixtures: preserve source values, add provenance and batch identifiers, create Delta tables in the approved Databricks schema, and prove idempotent reruns. Do not begin Silver cleaning or live-data ingestion.
