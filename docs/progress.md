# Market Risk Analysis — Progress Tracker

## Current position

- **Current phase:** Phase 08 — SQL Serving and Databricks QA
- **Status:** Complete
- **Next phase:** Phase 09 — Power BI Semantic Model
- **Last updated:** 2026-09-09

Phase 06 started from the completed Phase 05 checkpoint at `944fc12`. The Git and
Databricks baselines were verified: two Bronze tables, four Silver tables, two
canonical portfolios, three Silver processing runs, and no Gold tables.

Phase 6A completed the governed deterministic instrument, allocation, calendar,
price, corporate-action, position, and cash dependencies. Phase 6B published and
reconciled 120 instrument market-value rows and 8 portfolio-daily rows at checkpoint
`632adc0`. Runtime validation, independent reconciliation, and the learner
explain-back passed. The [Phase 06 handoff](handoffs/phase-06-handoff.md) records
the completed evidence. The [Phase 07 handoff](handoffs/phase-07-handoff.md)
records the completed risk measures, live runs, and independent validation.

Phase 07 completed the approved deterministic risk foundation: 252 2016 trading
sessions, 251 adjacent adjusted-close returns, 15 instruments, USD-only one-day
historical VaR at 95% and 99%, static January 7 exposure repriced on December 30,
and three deterministic hypothetical scenarios with 15 shocks each. CORE_15_LONG
and LONG_SHORT_130_30 both completed successful published live runs. Final live
reconciliation passed, including exact stress Decimal precision alignment. CORE
reruns attempts 1/2/3 preserved identical VaR and stress results and the same input
manifest, proving append-only replay safety.

Phase 08 published five consumer-serving views and validated them directly in
Databricks SQL. Daily analytics served 8 unique portfolio/date rows and position
exposure served 120 unique portfolio/instrument/date rows, both USD-only and without
duplicates. The append-only Phase 07 risk outputs are controlled through one
deterministically selected complete published run per portfolio and as-of date:
CORE_15_LONG attempt 3 and LONG_SHORT_130_30 attempt 1. The selected runs each
serve exactly two VaR measures and three stress results. The version-controlled
read-only QA pack passed all object, grain, source-reconciliation, currency,
completeness, and rerun-control checks.

## Status definitions

| Status | Meaning |
| --- | --- |
| Not started | No phase implementation has begun |
| Ready to start | Prerequisites and scope are clear |
| In progress | Work or learning exercises are underway |
| Blocked | A named issue prevents useful progress |
| Awaiting knowledge check | Implementation passes but explain-back is unfinished |
| Complete | Exit evidence, knowledge check, Git checkpoint, and handoff are complete |

## Phase status

| Phase | Title | Status | Git checkpoint |
| --- | --- | --- | --- |
| 01 | Workstation and Repository Foundation | Complete | `bb80972` — foundation |
| 02 | Databricks and SQL Connectivity | Complete | `7207544` — validation record |
| 03 | Risk Requirements and Data Contracts | Complete | `9cec282` — completed handoff |
| 04 | Bronze Ingestion | Complete | `bf08e36` — ingestion verification |
| 05 | Silver Quality and Canonical Data | Complete | `0d09a6a` — canonical merge target alias fix |
| 06 | Gold Analytics Foundation | Complete | `632adc0` — audited Gold portfolio daily metrics v2 |
| 07 | Risk Measures and Validation | Complete | `87e1f23` — risk validation precision alignment |
| 08 | SQL Serving and Databricks QA | Complete | Pending commit |
| 09 | Power BI Semantic Model | Not started | — |
| 10 | Market Risk Dashboard | Not started | — |
| 11 | Automation, Deployment, and CI | Not started | — |
| 12 | End-to-End Validation and Portfolio Handoff | Not started | — |

## Confirmed decisions

| Decision | Reason |
| --- | --- |
| Project name is Market Risk Analysis | Clear human-facing description |
| Repository folder is `Market-Risk-Analysis` | Readable repository naming |
| Python import is `market_risk_analysis` | Python identifiers cannot contain hyphens |
| Windows hosts desktop tools | Required for Power BI Desktop and convenient for DBeaver |
| Windows 11 Home 25H2 is the verified host | `Win32_OperatingSystem` and registry version/build evidence reconciled to build 26200.9168 |
| Ubuntu 24.04 hosts development tools | Provides a consistent Linux command-line environment |
| Ubuntu 24.04 is the default WSL 2 distribution | Verified from the learner's machine with `wsl.exe --status` and `wsl.exe --list --verbose` |
| Docker Desktop uses the WSL 2 environment | Available for isolated services and integration tests without installing a second engine in Ubuntu |
| Docker is used selectively | Direct Ubuntu development remains easier to learn; containers are added when they improve reproducibility |
| VS Code is the main editor | One interface for WSL, Python, Git, and Databricks work |
| Databricks Free Edition is the lakehouse platform | Available learning environment with known resource limits |
| DBeaver Community Edition is the SQL client | Free SQL exploration and reconciliation tool |
| One separate chat per phase | Keeps questions, implementation, and handoff scoped and traceable |
| Synthetic deterministic data comes first | Reproducible, safe, and free from licensing ambiguity |
| Gold data feeds Power BI | Shared business logic should be governed and tested upstream |
| GitHub will track project progress from Phase 01 | Supports a visible portfolio history and off-machine copy of reviewed commits |
| Keep the GitHub repository private during the foundation-only stage | Publish the portfolio after it contains a substantive, reviewed analytics milestone rather than an initial scaffold alone |
| Use GitHub's account-linked `noreply` address for this repository | Preserves commit attribution without publishing the learner's personal email |
| Prefer OAuth U2M for user authentication | Uses browser authorization and short-lived credentials without manually handling a PAT |
| Keep Databricks authentication outside Git | CLI and DBeaver authentication state is local and machine-specific |
| Use Databricks OSS JDBC driver 3.4.2 | Supported JDBC 3.x path provides OAuth and Unity Catalog connectivity |
| Disable DBeaver OAuth token caching | Minimizes persistent credential material on the Windows host |
| Keep important SQL and DDL in Git | Enables review, reuse, validation, and reproducibility across SQL clients |
| Use `workspace.devin_market_risk_dev` | Provides an explicit learner-owned development namespace |
| Retain the development schema with zero tables | The governed namespace is useful later without starting ingestion |
| Stop the SQL warehouse when idle | Respects Free Edition compute constraints |
| Define risk questions before implementation | Keeps metrics tied to explicit analytical needs |
| Use 15 US-listed instruments and two portfolios | Provides useful diversification and long-short behavior while remaining explainable |
| Use deterministic synthetic fixtures first | Makes tests reproducible and avoids licensed-data ambiguity |
| Preserve immutable Bronze evidence | Supports audit, correction history, and safe reprocessing |
| Treat warnings separately from row outcomes | Prevents warning violations from double-counting received records |
| Use exchange-aware trading calendars | Distinguishes expected closures from missing-price failures |
| Use varied deterministic stress shocks | Produces transparent portfolio sensitivity without unexplained randomness |
| Label stress scenarios as hypothetical | Prevents assumptions from being mistaken for forecasts or investment advice |
| Use a managed Unity Catalog Volume for Bronze landing | Provides governed storage without requiring external cloud credentials |
| Use content-addressed source paths | SHA-256 separates changed files and makes identical source bytes observable |
| Preserve portfolio source fields as strings in Bronze | Prevents premature cleaning, casting, or loss of source representation |
| Keep the small Bronze tables unpartitioned | Partition overhead would exceed the benefit for these small datasets |
| Record identical reruns as `SKIPPED_DUPLICATE` | Prevents duplicate portfolio rows while preserving every ingestion attempt |
| Use the portfolio fixture as the controlled Phase 04 proof | Demonstrates the ingestion pattern without claiming live-data ingestion |
| Treat portfolio and audit writes as separate Delta transactions | Delta guarantees atomicity per table, not across both tables; validation and duplicate guards reduce risk |
| Preserve Bronze as immutable Silver input | Retains reproducible source representations and lineage while Silver adds trust |
| Use `TRY_CAST` for Silver conversions | Invalid values remain observable to quality rules without stopping evaluation immediately |
| Separate warning violations from final record outcomes | Nonfatal concerns remain visible without double-counting or rejecting usable records |
| Preserve and link failed Silver attempts to recovery | Per-table Delta writes can commit independently, so partial evidence must remain auditable |
| Qualify joined duplicate columns and alias Delta merge targets | Spark and Delta resolve column references only when their intended relation is explicit |
| Hash ordered canonical snapshots for idempotency proof | Matching counts alone cannot prove that record content is unchanged |

## Phase 07 completion

- Phase 07 contracts, Gold DDL, governed stress Bronze-to-Silver configuration, Gold
  risk calculation, and read-only contribution-validation SQL are complete.
- The expected Spark global-window warning is bounded to the 251-row VaR distribution.
- Limitations remain explicit: deterministic synthetic 2016 data only; no forecasts,
  Monte Carlo VaR, parametric VaR, expected shortfall, formal backtesting, or live
  2016–2026 history.

## Evidence log

Add evidence here only after it is produced on the learner's environment.

| Date | Phase | Check | Result | Notes |
| --- | --- | --- | --- | --- |
| 2026-08-18 | 01 | `wsl.exe --status` and `wsl.exe --list --verbose` | Pass | `Ubuntu-24.04` is the default, running WSL version 2; Docker Desktop's internal WSL distribution is also present |
| 2026-08-19 | 01 | Windows version reconciliation | Pass | Windows 11 Home 25H2, OS build 26200.9168; CIM caption resolved a stale registry product label |
| 2026-08-19 | 01 | Repository location | Pass | `/home/devin/MarketAnalytics/Market-Risk-Analysis`, inside Ubuntu's Linux filesystem rather than `/mnt/c` |
| 2026-08-19 | 01 | Git, Python, uv, and Make versions | Pass | Git 2.43.0, Python 3.12.3, uv 0.12.5, GNU Make 4.3 |
| 2026-08-19 | 01 | VS Code WSL and Python integration | Pass | WSL-connected window, `.venv/bin/python` selected, Python and Ruff extensions installed on the WSL side |
| 2026-08-19 | 01 | `make setup` | Pass | Created `.venv`, installed the local package, pytest 9.1.1, and Ruff 0.16.2; repeated setup was idempotent |
| 2026-08-19 | 01 | Configuration override exercise | Pass | Diagnostic changed only `environment=local` to `environment=practice`; both runs reported 8/8 checks passed |
| 2026-08-19 | 01 | `make check` | Pass | Ruff passed, pytest reported 4 passed, and the environment diagnostic reported 8/8 passed |
| 2026-08-19 | 01 | Intentional failing-test exercise | Pass | Altered override assertion produced 1 failed and 3 passed; restoring `"test"` returned 4 passed and a full quality-gate pass |
| 2026-08-19 | 01 | `docker version` | Pass | Ubuntu client reached Docker Desktop 4.86.0 server; Engine 29.7.2 on linux/amd64 |
| 2026-08-19 | 01 | Disposable Docker container | Pass | `hello-world` ran as `market-risk-phase01-check`; `--rm` left no container with that name |
| 2026-08-19 | 01 | Ignore and secret-hygiene rules | Pass | `.venv`, `.cache`, `.env`, and generated data are ignored; `.env.example` and `data/README.md` are explicitly trackable |
| 2026-08-19 | 01 | Explain-back knowledge check | Pass | Learner explained Git state, lockfile versus environment, repository structure, secrets, tests versus linting, and the limits of `make check` |
| 2026-08-19 | 01 | Git author privacy | Pass | Repository-local author name and account-linked GitHub `noreply` email configured; no address recorded here |
| 2026-08-19 | 01 | First Git checkpoint and GitHub publication | Pass | `bb80972` (`chore: establish Phase 01 project foundation`) is on local `main` and `origin/main`; `origin` is `https://github.com/D3v1n04/Market-Risk-Analysis.git`; repository remains private by learner decision; the working tree was clean before this completion-record update |
| 2026-08-21 | 02 | Databricks Free Edition inventory | Pass | Available catalogs were `samples`, `system`, and `workspace`; `workspace` contained `default` and `information_schema`; the single SQL warehouse was serverless and 2X-Small |
| 2026-08-21 | 02 | Databricks CLI installation | Pass | Databricks CLI 1.13.0 installed at `/usr/local/bin/databricks` inside Ubuntu |
| 2026-08-21 | 02 | CLI OAuth authentication | Pass | Profile `market-risk-dev` was valid, current-user lookup matched, and no PAT was created |
| 2026-08-21 | 02 | Authentication file permissions | Pass | `~/.databrickscfg` and the OAuth token cache were outside the repository with mode `600` |
| 2026-08-21 | 02 | VS Code Databricks integration | Pass | Verified extension 2.14.0 was enabled in WSL, used the correct OAuth profile, and displayed the expected Unity Catalog objects |
| 2026-08-21 | 02 | Bundle validation | Pass | `databricks bundle validate --profile market-risk-dev` returned `Validation OK!` without deploying resources |
| 2026-08-21 | 02 | Version-controlled SQL smoke test | Pass | Identity, `workspace.default`, three catalogs, two schemas, and zero default tables matched expectations |
| 2026-08-21 | 02 | DBeaver JDBC and OAuth connection | Pass | DBeaver Community 26.1.5 connected through Databricks JDBC 3.4.2 without a PAT |
| 2026-08-21 | 02 | Independent DBeaver SQL validation | Pass | Identity matched; catalog/schema and metadata queries returned the expected results |
| 2026-08-21 | 02 | Independent Ubuntu SQL validation | Pass | Statement Execution API returned `SUCCEEDED` with `workspace` and `default` |
| 2026-08-21 | 02 | Learner-owned development schema | Pass | `workspace.devin_market_risk_dev` was created from Git-backed DDL, ownership/comment were validated, and the schema was deliberately retained with zero tables |
| 2026-08-21 | 02 | `make check` | Pass | Ruff passed, pytest reported 4 passed, and the environment diagnostic reported 11/11 passed |
| 2026-08-21 | 02 | Secret and resource hygiene | Pass | Generated `.databricks/` state remained ignored, authentication stayed outside Git, no credential was documented, and the SQL warehouse finished in `STOPPED` state |
| 2026-08-27 | 03 | Risk requirements and boundaries | Pass | Dashboard questions, metric conventions, freshness behavior, risk assumptions, and MVP non-goals are explicit |
| 2026-08-27 | 03 | Logical data contracts | Pass | 11 YAML contracts declare dataset grain, keys, fields, nullability, references, valid ranges, and stable quality rules |
| 2026-08-27 | 03 | Deterministic reference fixtures | Pass | 15 instruments, 2 portfolios, and 30 allocation records reproduce the approved 100% long-only and 130/30 exposures |
| 2026-08-27 | 03 | Stress-test fixtures | Pass | 3 hypothetical scenarios contain exactly 45 deterministic instrument shocks with complete coverage |
| 2026-08-27 | 03 | Edge-case examples | Pass | 28 deterministic passing and failing cases cover prices, corporate actions, calendars, positions, batches, retries, and violation resolution |
| 2026-08-27 | 03 | Contract automation | Pass | 8 contract tests validate YAML structure, references, exposure totals, hashes, scenario coverage, rule IDs, and deterministic examples |
| 2026-08-27 | 03 | `make check` | Pass | Ruff passed, pytest reported 12 passed, and the environment diagnostic reported 11/11 passed |
| 2026-08-27 | 03 | Explain-back knowledge check | Pass | Learner explained grain, keys, Bronze preservation, batch statuses, warning counts, trading calendars, exposure, shorts, and stress-test limitations |
| 2026-08-27 | 03 | Phase boundary | Pass | No Databricks table, Bronze ingestion, live-data retrieval, or licensed dataset was introduced |
| 2026-09-02 | 04 | Managed landing and Bronze DDL | Pass | Created the managed `bronze_landing` Volume and two unpartitioned Bronze Delta tables |
| 2026-09-02 | 04 | Deterministic source manifest | Pass | Recorded the 768-byte portfolio fixture, 2 source records, 16 columns, source SHA-256, and manifest SHA-256 |
| 2026-09-02 | 04 | Initial Bronze ingestion | Pass | Persisted 2 portfolio records and 1 `SUCCEEDED` batch-audit record |
| 2026-09-02 | 04 | Identical-source rerun | Pass | Portfolio count remained 2 while the audit count became 2; the new attempt was `SKIPPED_DUPLICATE` and linked to the successful batch |
| 2026-09-02 | 04 | Source preservation and lineage | Pass | Verified source row numbers, file and record hashes, raw records, batch linkage, and preservation of empty strings |
| 2026-09-02 | 04 | Malformed CSV validation | Pass | Automated testing confirmed that a record-width mismatch stops processing with a clear error |
| 2026-09-02 | 04 | `make check` | Pass | Ruff passed, pytest reported 26 passed, and the environment diagnostic reported 11/11 passed |
| 2026-09-02 | 04 | Explain-back knowledge check | Pass | Learner explained manifests, batch auditing, source and record hashes, Bronze preservation, Spark execution, duplicate reruns, and corrupted-source rejection |
| 2026-09-02 | 04 | Phase boundary | Pass | No Silver transformation, live market-data retrieval, Gold analytics, Power BI work, or FastAPI development was introduced |
| 2026-09-04 | 05 | Local quality gate | Pass | Ruff passed; pytest reported 61 passed, including 5 Silver SQL and 16 Silver notebook tests; environment diagnostic reported 11/11 passed |
| 2026-09-04 | 05 | Static-test boundary | Confirmed | Local Silver tests inspect contracts and source safeguards; live Databricks execution exposed joined-column and Delta-target alias errors that static tests did not execute |
| 2026-09-04 | 05 | Failed-attempt audit recovery | Pass | Run `e4906a8a-e1e0-415d-abd2-712e5e5deb87` was recorded as `FAILED`; its 2 outcomes and 2 warnings remained auditable while canonical count stayed 0 |
| 2026-09-04 | 05 | Recovery publication | Pass | Linked run `f21e9261-e1dd-43cf-95b5-759d8a439659` evaluated and accepted 2 records, recorded 2 warnings, and published 2 canonical portfolios |
| 2026-09-04 | 05 | Idempotency verification | Pass | Run `71bf477d-40f4-492d-a45e-a5517c2cd7ba` classified both records unchanged, accepted 0, published nothing, and left canonical count at 2 |
| 2026-09-04 | 05 | Canonical fingerprint | Pass | Before and after SHA-256 matched: `c828f4b29b8b04ead42c38e7f99110b420fd2b0a186cce60bc820913fff25ebc` |
| 2026-09-04 | 05 | Final Silver reconciliation | Pass | 2 active unique canonical portfolios, 3 processing runs, 6 record outcomes, and 6 missing-inception-date warnings were persisted; Bronze was not modified |
| 2026-09-04 | 05 | Explain-back knowledge check | Pass | Learner explained Bronze versus Silver, `TRY_CAST`, warnings versus outcomes, unchanged reruns, per-table Delta atomicity, recovery lineage, and explicit aliases |

Phase 06 evidence below was recorded on 2026-09-07 from the supplied completion
evidence; this date does not assert the execution date of each runtime attempt.

| Date recorded | Phase | Check | Result | Notes |
| --- | --- | --- | --- | --- |
| 2026-09-07 | 06 | Local quality gate | Pass | Ruff passed, 198 pytest tests passed, and 11/11 environment checks passed at validated checkpoint `632adc0` |
| 2026-09-07 | 06 | Trusted input enablement | Pass | Seven deterministic dependencies completed; both USD portfolios have governed inception `2016-01-04` |
| 2026-09-07 | 06 | Instrument reconciliation | Pass | 120 rows and distinct keys, 2 portfolios, 15 instruments per portfolio-date, 4 dates; zero formula, absolute-value, or sign failures |
| 2026-09-07 | 06 | Portfolio reconciliation | Pass | 8 rows and distinct keys; zero gross, net, NAV, P&L, baseline, return, or exposure-ratio failures |
| 2026-09-07 | 06 | Market-value audit | Pass | 9 succeeded attempts, 8 published partitions, 1 unchanged nonpublished reprocess, 1 linked reprocess |
| 2026-09-07 | 06 | Portfolio-daily audit | Pass | 10 succeeded attempts, 8 published partitions, 2 unchanged nonpublished reprocesses, 2 linked reprocesses |
| 2026-09-07 | 06 | Independent economic checks | Pass | Cumulative P&L equals ending minus initial NAV exactly: USD 8,149.999997 long-only and USD 39,440 long-short; WMT dividend signs and NVDA split invariance reconcile |
| 2026-09-07 | 06 | Explain-back | Pass | Learner explained grains, signs, gross/net, NAV/P&L/return baselines, dividends, cash effects, audit-only reruns, SHA-256 evidence, and split invariance |
| 2026-09-09 | 07 | Deterministic history and exposure | Pass | 252 approved 2016 sessions, 251 adjacent returns, and 15 instruments; static January 7 quantities/cash repriced at December 30 |
| 2026-09-09 | 07 | Risk methodology | Pass | USD-only one-day historical VaR at 95%/99% and three deterministic hypothetical 15-shock scenarios |
| 2026-09-09 | 07 | Live published runs | Pass | CORE_15_LONG and LONG_SHORT_130_30 completed successful published risk runs |
| 2026-09-09 | 07 | Independent reconciliation | Pass | Final live contribution, VaR, and stress reconciliation passed, including exact stress precision alignment |
| 2026-09-09 | 07 | Append-only replay safety | Pass | CORE attempts 1/2/3 retained identical VaR and stress results and the same input manifest |
| 2026-09-09 | 07 | Latest local quality gate | Pass | 230 pytest tests passed, Ruff passed, and `market-risk-check` reported 11/11 |
| 2026-09-09 | 07 | Runtime warning review | Noted | Expected Spark global-window warning is bounded to the 251-row VaR distribution |

| 2026-09-09 | 08 | Local serving and QA tests | Pass | `pytest -q tests/test_phase_08_serving_sql.py` reported 8 passed; `git diff --check` passed |
| 2026-09-09 | 08 | Serving object deployment | Pass | Five expected consumer views were created successfully in `workspace.devin_market_risk_dev` |
| 2026-09-09 | 08 | Daily and position serving grain | Pass | Daily: 8 rows/8 keys/0 duplicates; position: 120 rows/120 keys/0 duplicates; USD only |
| 2026-09-09 | 08 | Gold and serving reconciliation | Pass | Canonical Gold daily metrics and served daily analytics both returned 8 rows |
| 2026-09-09 | 08 | Risk rerun control | Pass | Two selected runs/keys/0 duplicates; CORE attempt 3 and LONG_SHORT attempt 1 only |
| 2026-09-09 | 08 | Complete risk bundles | Pass | Each selected run served exactly 2 VaR measures and 3 stress results |

## Handoffs

Completed phase handoffs belong in `docs/handoffs/` and use
`docs/phase-handoff-template.md`.

- `docs/handoffs/phase-02-handoff.md` — Phase 02 validation, decisions,
  glossary, knowledge check, and next-phase readiness
- `docs/handoffs/phase-03-handoff.md` — Phase 03 requirements, contracts,
  evidence, glossary, knowledge check, and Phase 04 readiness
- `docs/handoffs/phase-04-handoff.md` — Phase 04 landing, Bronze ingestion,
  lineage, idempotency, validation evidence, knowledge check, and Phase 05 readiness
- `docs/handoffs/phase-05-handoff.md` — Phase 05 validation, canonicalization,
  runtime recovery, idempotency evidence, knowledge check, and Phase 06 readiness
- `docs/handoffs/phase-06-handoff.md` — Phase 06 governed inputs, Gold metrics,
  runtime and independent reconciliation, explain-back, and Phase 07 readiness
- `docs/handoffs/phase-07-handoff.md` — Phase 07 risk measures, live runs, replay
  safety, and independent reconciliation evidence
- `docs/handoffs/phase-08-handoff.md` — Phase 08 serving views, Databricks SQL
  QA, rerun-selection control, and Phase 09 readiness

## New-chat kickoff prompt

Copy and adapt this at the beginning of a phase chat:

> We are working on Phase NN — TITLE of the Market Risk Analysis project. Teach me
> while we build: explain new concepts simply, let me ask as many questions as I
> need, give me small exercises, help me implement and test the real milestone, and
> require an explain-back before completion. Use `README.md`,
> `docs/project-roadmap.md`, `docs/progress.md`, the previous phase handoff, and the
> current Git status as the source of truth. Stay within this phase unless a change
> is required for its acceptance criteria. At the end, update the progress tracker,
> create the phase handoff, provide the knowledge check and glossary, and give me a
> Git commit checklist. Do not make the commit for me unless I explicitly ask.

For Phase 01 there is no previous handoff; use
`docs/part-01-environment-and-repository-setup.md` as the detailed guide.
