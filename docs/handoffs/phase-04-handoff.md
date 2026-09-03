# Phase 04 Handoff — Bronze Ingestion

## Status

- **Phase:** 04 — Bronze Ingestion
- **Status:** Complete
- **Date:** 2026-09-02
- **Next phase:** 05 — Silver Quality and Canonical Data

## Objective

Implement a deterministic and auditable Bronze ingestion path for the approved
portfolio fixture. Preserve source values without premature cleaning, attach
file-level and record-level lineage, record every ingestion attempt, and prevent an
identical source rerun from duplicating portfolio records.

## Completed work

- Extended the ingestion-batch contract for fixture ingestion and duplicate lineage.
- Created the managed Unity Catalog Volume
  `workspace.devin_market_risk_dev.bronze_landing`.
- Created the unpartitioned Delta tables `bronze_portfolios` and
  `bronze_ingestion_batches`.
- Built a deterministic Python manifest generator that inspects the CSV, calculates
  its SHA-256 digest, and records source metadata.
- Landed the approved portfolio CSV and its manifest in a content-addressed Volume
  directory.
- Implemented a Git-backed Databricks Python notebook that validates the landed CSV
  and manifest before constructing a Spark DataFrame.
- Preserved all portfolio source fields as strings and added batch, file, row,
  record-hash, raw-record, timestamp, and contract-version lineage.
- Implemented idempotent duplicate handling with `SKIPPED_DUPLICATE` audit records.
- Added SQL verification for table details, history, row counts, batch-count
  reconciliation, duplicate lineage, and source-row lineage.
- Added automated tests for the manifest builder, Bronze DDL, verification SQL, and
  Databricks notebook structure.
- Ran a successful first ingestion and an identical duplicate rerun.
- Completed the Phase 04 explain-back knowledge check.

## Validation evidence

| Check | Command or method | Result | What it proves |
| --- | --- | --- | --- |
| Python quality | `uv run ruff check .` | Pass | Tracked Python and notebook source satisfies the configured lint rules |
| Manifest tests | `uv run pytest tests/test_manifest.py -v` | 4 passed | Hashing, source metadata, deterministic JSON, and malformed-row validation behave as asserted |
| Notebook tests | `uv run pytest tests/test_bronze_notebook.py -v` | 4 passed | The notebook validates before writing and contains the expected lineage and idempotency controls |
| Bronze SQL tests | `uv run pytest tests/test_bronze_sql.py -v` | 5 passed | DDL, managed-Volume syntax, contracts, and verification queries match the tested design |
| Complete local gate | `make check` | Pass: 26 tests and 11/11 environment checks | The complete local quality gate passes |
| Managed Volume inspection | `SHOW VOLUMES` and `DESCRIBE VOLUME` | Pass | `bronze_landing` exists as a managed Unity Catalog Volume |
| Manifest generation | Manifest CLI and `sha256sum` | Pass | The 768-byte fixture contains 2 records and has the expected deterministic hashes |
| Landing inspection | `databricks fs ls --long` | Pass | The 768-byte CSV and 978-byte manifest exist in the expected content-addressed directory |
| Initial notebook run | Databricks notebook output and SQL verification | Pass | 2 portfolio records and 1 `SUCCEEDED` audit record were persisted |
| Duplicate notebook run | Databricks notebook output and SQL verification | Pass | Portfolio count stayed at 2 while audit count became 2; the second attempt was `SKIPPED_DUPLICATE` |
| Count reconciliation | Version-controlled verification SQL | Pass | Both audit records satisfy received-count reconciliation |
| Source preservation | Version-controlled verification SQL | Pass | Row numbers, raw records, hashes, batch IDs, and empty-string source values were preserved |
| Formatting check | `git diff --check` | Pass | The Phase 04 changes contain no whitespace errors |

The approved portfolio source SHA-256 is:

```text
21a3f3aa997b0038b0518cafd85e1bda1612c42857da5f776ddcd8ef8ff6bd78
```

The deterministic manifest SHA-256 is:

```text
993b4e80bd66324d09a188c82a669a05f5f6d9605673c00bbafa16385b6c5498
```

The successful batch was:

```text
d01b3f67-0175-4793-97c4-e46446813954
```

The duplicate attempt was:

```text
c2593f95-9677-41cc-bda6-153f3442c4ea
```

## Decisions and reasoning

| Decision | Reason | Rejected alternative or tradeoff |
| --- | --- | --- |
| Use a managed Unity Catalog Volume | Provides governed landing storage without external cloud credentials | An external Volume would require additional storage configuration and credentials |
| Use content-addressed landing directories | The source hash makes changed and identical files observable | Filename-only paths could overwrite or confuse different versions |
| Preserve source fields as strings | Bronze should retain the source representation for later validation | Casting in Bronze could convert, reject, or lose original values |
| Store raw records and lineage | Supports investigation and connection back to the exact source row | Keeping only parsed values would weaken traceability |
| Keep both Bronze tables unpartitioned | The current tables are too small to benefit from partitioning | Partitioning would add unnecessary files and metadata overhead |
| Skip duplicate portfolio writes but audit the attempt | Prevents duplicated business records while preserving operational history | Silently ignoring reruns would lose evidence that the attempt occurred |
| Use the deterministic portfolio fixture first | Provides reproducible proof without licensed or unstable dependencies | Live market retrieval would expand Phase 04 beyond its controlled scope |
| Write the portfolio and audit tables separately | Each Delta table receives its own atomic transaction | The two appends are not one cross-table transaction, so validation and duplicate guards reduce but do not eliminate interruption risk |

## Concepts the learner can explain

- The CSV contains the source portfolio records.
- The manifest describes and fingerprints the landed source file.
- The ingestion-batch table records the metadata, status, counts, and lineage of
  each processing attempt.
- A batch ID distinguishes one ingestion attempt from every other attempt.
- `source_sha256` fingerprints the complete CSV file.
- `manifest_sha256` fingerprints the complete manifest JSON file.
- `source_record_sha256` fingerprints one individual raw CSV data record.
- Bronze preserves source representations; Silver performs later casting,
  validation, and standardization.
- Spark transformations are lazily planned, while actions such as `count()` and
  `show()` execute work. A Delta write is what persists records to a table.
- An identical rerun must not duplicate portfolio records, but it should create a
  new audit record describing the skipped attempt.
- A mismatch between the source and manifest should stop ingestion before the data
  is trusted or written.

## Knowledge check

The learner passed the Phase 04 explain-back.

The learner correctly explained:

- The different responsibilities of the portfolio CSV, manifest, and batch-audit
  table.
- Why every ingestion attempt receives a unique batch ID.
- Why file, manifest, and record hashes are needed.
- Why Bronze source values remain strings.
- Why an identical rerun creates another audit row without adding portfolio rows.
- Why corrupted or mismatched source evidence must stop ingestion.

Two points were clarified during the check:

- `source_record_sha256` represents one raw CSV data record, not the entire file.
- Spark actions such as `count()` execute a computation but do not persist a new
  Delta-table record; persistence happens through the table write.

## Open questions or blockers

- None.

The known cross-table transaction limitation is documented as a design tradeoff,
not a blocker for the controlled Phase 04 milestone.

## Git checkpoint

- **Branch:** `phase-04-bronze-ingestion`
- **Validated implementation checkpoint:** `bf08e36` —
  `test: expand Bronze ingestion verification`
- **Working tree:** Phase 04 completion documentation is intentionally pending its
  final commit
- **Ignored/generated artifacts checked:** Yes; local generated manifests and data
  remain ignored, and no credentials are tracked

## Files and platform objects changed

### Repository

- `README.md` — current milestone, repository map, and phase boundary
- `data/README.md` — tracked-fixture and generated-data behavior
- `contracts/ingestion_batches.yml` — ingestion-attempt and duplicate lineage fields
- `sql/bronze/phase_04_create_bronze_volume.sql` — managed landing Volume DDL
- `sql/bronze/phase_04_create_bronze_tables.sql` — Bronze Delta table DDL
- `sql/bronze/phase_04_verify_bronze_tables.sql` — persistence and lineage checks
- `src/market_risk_analysis/ingestion/__init__.py` — ingestion package
- `src/market_risk_analysis/ingestion/manifest.py` — deterministic manifest builder
- `notebooks/bronze/phase_04_ingest_portfolios.py` — Databricks Bronze ingestion
- `tests/test_manifest.py` — manifest behavior tests
- `tests/test_bronze_notebook.py` — notebook structure and safety tests
- `tests/test_bronze_sql.py` — Bronze DDL and verification tests
- `docs/progress.md` — Phase 04 completion evidence
- `docs/handoffs/phase-04-handoff.md` — Phase 04 handoff

### Databricks

- `workspace.devin_market_risk_dev.bronze_landing` — persistent managed Volume
- `workspace.devin_market_risk_dev.bronze_portfolios` — persistent Bronze Delta
  table containing 2 source-aligned portfolio records
- `workspace.devin_market_risk_dev.bronze_ingestion_batches` — persistent Bronze
  Delta audit table containing 2 ingestion-attempt records
- `/Volumes/workspace/devin_market_risk_dev/bronze_landing/portfolios/21a3f3aa997b0038b0518cafd85e1bda1612c42857da5f776ddcd8ef8ff6bd78/portfolios.csv`
  — landed portfolio source
- `/Volumes/workspace/devin_market_risk_dev/bronze_landing/portfolios/21a3f3aa997b0038b0518cafd85e1bda1612c42857da5f776ddcd8ef8ff6bd78/manifest.json`
  — landed deterministic manifest

No DBeaver or Power BI objects were created during this phase.

## Next-phase readiness

- The approved portfolio source is preserved in governed landing storage.
- Bronze portfolio values and technical lineage are available as immutable Silver
  inputs.
- Successful and duplicate ingestion attempts are auditable.
- Local automated checks and Databricks verification queries pass.
- The learner can explain the Phase 04 architecture and safety controls.
- Phase 05 must verify the final Phase 04 commit and clean synchronized branch
  before changing code or data.
- Phase 05 should add casting, validation, standardization, and canonical
  deduplication without modifying Bronze records.

## Suggested next-chat opening

> We are starting Phase 05 — Silver Quality and Canonical Data. Read `README.md`,
> `docs/project-roadmap.md`, `docs/progress.md`, and
> `docs/handoffs/phase-04-handoff.md` first. Verify the stated Git and Databricks
> status, then teach me how Silver differs from Bronze and implement only the
> Phase 05 scope. Preserve Bronze as immutable input, explain each transformation,
> use deterministic tests and reconciliation checks, and require an explain-back
> before Phase 05 completion.
