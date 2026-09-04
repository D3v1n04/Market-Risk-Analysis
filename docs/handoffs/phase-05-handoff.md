# Phase 05 Handoff — Silver Quality and Canonical Data

## Status

- **Phase:** 05 — Silver Quality and Canonical Data
- **Status:** Complete
- **Date:** 2026-09-04
- **Next phase:** 06 — Gold Analytics Foundation

## Objective

Implement a contract-aligned Silver processing path for the controlled Bronze
portfolio dataset. Convert preserved source strings into documented types, evaluate
structural and business rules without modifying Bronze, resolve duplicate and
canonical versions deterministically, publish only safe canonical records, and
retain record-level and processing-run audit evidence for every persisted attempt.

## Completed work

- Upgraded the portfolio, ingestion-batch, and data-quality-violation contracts for
  Silver validation and lineage.
- Added contracts for processing runs and per-record processing outcomes.
- Created four small, unpartitioned Silver Delta tables for canonical portfolios,
  processing runs, record outcomes, and data-quality violations.
- Implemented a Git-backed Databricks Python notebook that reads Bronze without
  updating, deleting, or appending to Bronze tables.
- Added validated notebook parameters for the Bronze batch, Git code version, and
  processing trigger type.
- Used `TRY_CAST` for dates, decimals, and booleans so malformed values become
  observable rule failures instead of aborting evaluation immediately.
- Implemented structural rules for required values, castability, allowed values,
  identifiers, configuration versions, and missing actual inception dates.
- Implemented business rules for positive NAV, ratio reconciliation, strategy
  targets, inception-date ordering, and source record hashes.
- Implemented deterministic same-batch duplicate and conflict handling using
  source-row ordering rather than arbitrary Spark row order.
- Compared semantic configuration-version components against the current canonical
  portfolio rather than comparing version strings lexicographically.
- Assigned exactly one final outcome to every evaluated Bronze record while
  counting warning violations separately from record outcomes.
- Added dataset-level publication gates for active-portfolio count and portfolio-key
  uniqueness.
- Calculated deterministic SHA-256 fingerprints for the ordered input set and the
  canonical snapshots before and after processing.
- Built insert-only violation, outcome, and processing-run audit records with
  deterministic identifiers and reprocessing lineage.
- Used controlled Delta merges for audit deduplication and canonical upserts.
- Discovered and repaired two Spark/Delta runtime aliasing defects that local static
  tests could not execute.
- Preserved the partially committed audit evidence from the failed persistence
  attempt and added its missing `FAILED` processing-run record.
- Published two canonical portfolio rows through a linked recovery attempt.
- Reprocessed the same Bronze batch and proved canonical idempotency with unchanged
  counts and an unchanged canonical SHA-256 fingerprint.
- Completed the Phase 05 explain-back knowledge check.

## Validation evidence

| Check | Command or method | Result | What it proves |
| --- | --- | --- | --- |
| Contract tests | `uv run pytest tests/test_contracts.py -v` | 23 passed | The Phase 05 contracts remain parseable, unique, deterministic, and internally consistent |
| Silver DDL tests | `uv run pytest tests/test_silver_sql.py -v` | 5 passed | The four Delta schemas, types, nullability, and non-destructive DDL match their contracts |
| Silver notebook tests | `uv run pytest tests/test_silver_notebook.py -v` | 16 passed | The source contains the required validation, outcome, hash, publication, audit, and controlled-write safeguards |
| Complete local gate | `make check` | Pass: 61 tests and 11/11 environment checks | The complete local static and environment quality gate passes |
| Bronze immutability | Notebook source checks and Databricks execution | Pass | Silver reads the selected Bronze batch without writing to either Bronze table |
| Initial persisted attempt | Databricks run and SQL reconciliation | Audited failure | Two violation rows and two outcome rows committed before an unresolved Delta target alias stopped canonical publication |
| Failed-attempt recovery | Targeted Databricks recovery cell and SQL verification | Pass | The partial attempt received one linked `FAILED` processing-run row and left canonical count at zero |
| Recovery publication | Databricks run `f21e9261-e1dd-43cf-95b5-759d8a439659` | `SUCCEEDED_WITH_WARNINGS` | Attempt 2 evaluated 2, accepted 2, recorded 2 warnings, and published 2 canonical portfolios |
| Canonical reconciliation | Databricks SQL scalar reconciliation | Pass | Canonical count, distinct portfolio count, and active portfolio count all equal 2 |
| Outcome reconciliation | Databricks SQL scalar reconciliation | Pass | The successful recovery has 2 outcomes for 2 distinct Bronze source records |
| Violation reconciliation | Databricks SQL scalar reconciliation | Pass | The recovery has 2 warnings and zero error or critical violations |
| Canonical typing | Databricks SQL record inspection | Pass | Dates, decimal NAV and ratios, booleans, USD currency, lineage, and contract versions are represented as designed |
| Idempotency rerun | Databricks run `71bf477d-40f4-492d-a45e-a5517c2cd7ba` | `SUCCEEDED_WITH_WARNINGS` | Attempt 3 accepted zero, classified both records unchanged, and did not publish |
| Canonical fingerprint | Before/after processing-run hashes | Match | The idempotency rerun left the complete canonical record set unchanged |
| Formatting check | `git diff --check` | Pass | The implementation and regression fixes contain no whitespace errors |

The selected successful Bronze batch is:

```text
01b3f6f7-0175-4793-97c4-e46446813954
```

The audited failed Silver processing run is:

```text
e4906a8a-e1e0-415d-abd2-712e5e5deb87
```

The successful recovery processing run is:

```text
f21e9261-e1dd-43cf-95b5-759d8a439659
```

The successful idempotency processing run is:

```text
71bf477d-40f4-492d-a45e-a5517c2cd7ba
```

The canonical before/after SHA-256 on the idempotency run is:

```text
c828f4b29b8b04ead42c38e7f99110b420fd2b0a186cce60bc820913fff25ebc
```

## Decisions and reasoning

| Decision | Reason | Rejected alternative or tradeoff |
| --- | --- | --- |
| Preserve Bronze as immutable Silver input | Bronze remains the reproducible source evidence | Correcting Bronze in place would destroy the original representation |
| Use `TRY_CAST` for source conversion | Every record and rule can be evaluated while invalid conversions remain observable | Ordinary casts could terminate evaluation at the first malformed value |
| Store a missing actual inception date as `NULL` plus a warning | The date is genuinely unknown but the portfolio is still usable | A fabricated sentinel date would look like real business data |
| Separate warning violations from final row outcomes | Warnings should not double-count or automatically reject usable records | Treating every warning as rejection would overstate failed records |
| Use deterministic row ordering for same-batch duplicates | Repeated execution must select the same winning record | Arbitrary Spark row order would make results unstable |
| Reject conflicting same-batch business keys | Conflicting versions cannot be chosen safely without an explicit correction rule | Silently selecting one conflict would hide a source-quality problem |
| Compare semantic-version components numerically | `1.10.0` must correctly compare above `1.2.0` | Lexicographic string comparison produces incorrect version ordering |
| Gate publication on record and dataset quality | Canonical Silver should change only when the prospective snapshot is safe | Publishing partial or structurally invalid snapshots would weaken downstream trust |
| Hash ordered input and canonical record sets | Counts alone cannot prove that record contents stayed unchanged | Row counts can match while values differ |
| Use insert-only audit merges | Rerunning a persistence cell cannot duplicate the same audit identifiers | Unconditional appends would create duplicate audit evidence |
| Preserve and reconcile the failed persistence attempt | The partial writes are operational evidence and explain the recovery lineage | Deleting the rows would erase what actually occurred |
| Treat Delta writes as atomic per table | The runtime failure demonstrated that separate table merges can commit independently | The current Free Edition design does not provide one cross-table transaction |
| Keep the small Silver tables unpartitioned | The controlled dataset is too small to benefit from partitioning | Partitioning would add avoidable files and metadata overhead |

## Concepts the learner can explain

- Bronze preserves the source representation and lineage; Silver validates, types,
  deduplicates, and publishes trusted canonical records.
- `TRY_CAST` allows evaluation to continue by returning `NULL` for invalid
  conversions, after which explicit quality rules record the failure.
- A warning identifies a nonfatal concern and is counted separately from the one
  final disposition assigned to each record.
- A canonical business key identifies the current trusted record, while source and
  processing identifiers preserve the history that produced it.
- Explicit DataFrame and SQL aliases are required when joined inputs expose the same
  column names.
- A Delta merge condition can reference `target.column` only when the Delta target
  has been explicitly aliased as `target`.
- Delta transaction atomicity applies to each table operation, not automatically to
  a sequence of writes across several tables.
- A failed processing attempt should remain visible and linked to its recovery so an
  investigator can reconstruct what happened.
- Idempotency does not mean that no audit row is created. It means that repeating the
  same input does not create or alter canonical business records unnecessarily.
- Matching canonical before/after counts and hashes prove both the number and content
  of canonical records remained unchanged.

## Knowledge check

The learner passed the Phase 05 explain-back.

The learner correctly explained:

- Bronze and Silver have different preservation and trust responsibilities.
- `TRY_CAST` permits complete evaluation and observable violations instead of
  terminating at the first invalid conversion.
- Warnings are nonfatal observations rather than automatic record failures.
- Matching Bronze-derived candidates and Silver records produce unchanged outcomes
  rather than new canonical versions.
- Failed attempts must remain auditable so later investigation can connect the
  committed evidence, failure, code correction, and recovery.
- `candidate.same_batch_outcome` resolves the duplicate joined-column reference.
- `.alias("target")` makes `target.portfolio_id` valid in the Delta merge condition.

The difference between per-table Delta atomicity and a multi-table transaction was
clarified using the real failed persistence attempt and its recovery.

## Open questions or blockers

- None.

The absence of one cross-table transaction remains a documented design tradeoff.
Insert-only audit identifiers, explicit failed-run recovery, publication gates, and
post-write reconciliation make the controlled workflow observable and recoverable.

## Git checkpoint

- **Branch:** `phase-05-silver-quality-canonical-data`
- **Validated implementation checkpoint:** `0d09a6a` —
  `fix: alias Silver canonical merge target`
- **Runtime regression checkpoints:** `95e5e65` and `0d09a6a`
- **Working tree:** Phase 05 completion documentation is intentionally pending its
  final commit
- **Ignored/generated artifacts checked:** Yes; no Databricks credentials, local
  authentication state, or generated runtime data is tracked

## Files and platform objects changed

### Repository

- `contracts/data_quality_violations.yml` — Silver violation fields, resolution
  metadata, and rule evidence
- `contracts/ingestion_batches.yml` — source-batch compatibility and lineage updates
- `contracts/portfolio_record_outcomes.yml` — one final Silver outcome per evaluated
  Bronze record
- `contracts/portfolios.yml` — canonical portfolio types, nullability, hashes, and
  quality rules
- `contracts/processing_runs.yml` — Silver attempt identity, counts, fingerprints,
  publication state, failure evidence, and reprocessing lineage
- `sql/silver/phase_05_create_silver_tables.sql` — four contract-aligned Silver Delta
  table definitions
- `notebooks/silver/phase_05_process_portfolios.py` — portfolio validation,
  canonicalization, publication, auditing, and recovery-aware processing
- `tests/test_contracts.py` — expanded contract verification
- `tests/test_silver_sql.py` — Silver DDL safety and contract-alignment tests
- `tests/test_silver_notebook.py` — notebook structure, rules, lineage, outcome,
  hashing, publication, and controlled-write regression tests
- `README.md` — Phase 05 milestone and Phase 06 boundary
- `docs/progress.md` — Phase 05 completion evidence
- `docs/handoffs/phase-04-handoff.md` — corrected successful Bronze batch identifier
- `docs/handoffs/phase-05-handoff.md` — Phase 05 handoff

### Databricks

- `workspace.devin_market_risk_dev.silver_portfolios` — persistent canonical Delta
  table containing 2 current portfolio records
- `workspace.devin_market_risk_dev.silver_processing_runs` — persistent Delta audit
  table containing 3 processing attempts: one failure, one recovery publication, and
  one unchanged idempotency run
- `workspace.devin_market_risk_dev.silver_portfolio_record_outcomes` — persistent
  record-outcome Delta table containing 6 rows across the 3 processing attempts
- `workspace.devin_market_risk_dev.silver_data_quality_violations` — persistent
  violation Delta table containing 6 missing-inception-date warning rows across the
  3 processing attempts
- `phase_05_process_portfolios` — imported Databricks notebook corresponding to Git
  implementation checkpoint `0d09a6a`

No DBeaver, Power BI, Gold, or live market-data objects were created during this
phase. Bronze tables were read but not modified.

## Next-phase readiness

- Two trusted, typed, and uniquely keyed canonical portfolios are available in
  Silver.
- Silver record outcomes, warnings, processing counts, publication state, source
  lineage, and recovery lineage are auditable.
- The failed attempt, successful recovery, and unchanged rerun demonstrate the
  workflow's observable recovery and idempotency behavior.
- Canonical counts and fingerprints reconcile before and after processing.
- Local automated checks and live Databricks verification pass.
- The learner can explain the Phase 05 validation, canonicalization, audit, aliasing,
  failure-recovery, and idempotency concepts.
- Phase 06 must verify the final Phase 05 documentation commit, synchronized branch,
  and current Silver counts before creating Gold analytics.
- Phase 06 must keep metric grain, units, currency, sign conventions, and as-of dates
  explicit and must not duplicate shared business logic in downstream tools.

## Suggested next-chat opening

> We are starting Phase 06 — Gold Analytics Foundation. Read `README.md`,
> `docs/project-roadmap.md`, `docs/progress.md`, and
> `docs/handoffs/phase-05-handoff.md` first. Verify the stated Git and Databricks
> status, then teach me how Gold differs from Silver and implement only the Phase 06
> scope. Keep metric grain, units, currency, sign conventions, and as-of dates
> explicit; reconcile calculations independently; and require an explain-back before
> Phase 06 completion.
