# Market Risk Analysis — Progress Tracker

## Current position

- **Current phase:** Phase 01 — Workstation and Repository Foundation
- **Status:** In progress
- **Next gate:** Review and create the first Git commit, publish it to the chosen
  GitHub remote, then record a clean working tree
- **Last updated:** 2026-08-19

The learner's Windows/Ubuntu workstation, repository scaffold, local quality gate,
Docker integration, intentional failing-test exercise, and explain-back gate have
been validated. Phase 01 remains open only for the reviewed Git checkpoint, GitHub
publication, and final clean-working-tree evidence.

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
| 01 | Workstation and Repository Foundation | In progress | Awaiting first commit |
| 02 | Databricks and SQL Connectivity | Not started | — |
| 03 | Risk Requirements and Data Contracts | Not started | — |
| 04 | Bronze Ingestion | Not started | — |
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
| Use GitHub's account-linked `noreply` address for this repository | Preserves commit attribution without publishing the learner's personal email |

## Open confirmations for Phase 01

- Choose public or private visibility for the initial GitHub repository.
- Review the staged files, create the first intentional commit, publish `main`, and
  record the remote URL, commit hash, and clean working-tree evidence.

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

## Handoffs

Completed phase handoffs belong in `docs/handoffs/` and use
`docs/phase-handoff-template.md`.

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
