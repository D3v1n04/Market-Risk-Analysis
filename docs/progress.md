# Market Risk Analysis — Progress Tracker

## Current position

- **Current phase:** Phase 04 — Bronze Ingestion
- **Status:** Complete
- **Next gate:** Verify the final Phase 04 completion commit and clean synchronized
  branch, then start Phase 05 in a separate chat
- **Last updated:** 2026-09-02

Phase 04 landed the approved deterministic portfolio fixture in a managed Unity
Catalog Volume, created source-aligned Bronze Delta tables, added record-level
lineage and immutable batch-attempt auditing, and proved safe identical reruns. The
learner passed the Phase 04 explain-back. Silver cleaning and live-data retrieval
have not begun.

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
| 05 | Silver Quality and Canonical Data | Not started | — |
| 06 | Gold Analytics Foundation | Not started | — |
| 07 | Risk Measures and Validation | Not started | — |
| 08 | SQL Serving and DBeaver QA | Not started | — |
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

## Open confirmations for Phase 05

- Verify the final Phase 04 documentation commit and clean synchronized branch.
- Treat Bronze as immutable input to Silver.
- Implement Silver casting, validation, standardization, and deduplication only.
- Do not begin Gold analytics or live market-data ingestion.

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

## Handoffs

Completed phase handoffs belong in `docs/handoffs/` and use
`docs/phase-handoff-template.md`.

- `docs/handoffs/phase-02-handoff.md` — Phase 02 validation, decisions,
  glossary, knowledge check, and next-phase readiness
- `docs/handoffs/phase-03-handoff.md` — Phase 03 requirements, contracts,
  evidence, glossary, knowledge check, and Phase 04 readiness
- `docs/handoffs/phase-04-handoff.md` — Phase 04 landing, Bronze ingestion,
  lineage, idempotency, validation evidence, knowledge check, and Phase 05 readiness

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
