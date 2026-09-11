# Market Risk Analysis

A learning-first market-risk analytics lakehouse that preserves source history,
improves data quality through governed layers, and produces explainable portfolio
risk analytics.

## Current milestone

Phases 01 through 11 are complete.

Phase 11 added credential-free GitHub Actions CI, development-only Databricks bundle
jobs, explicit workflow dependencies, safe Bronze batch-ID handoffs, and read-only
serving-output validation. The reviewed bundle is deployed to the learner-owned
development workspace; Initialization and Daily Risk have both completed successful
manual runs and a successful Daily Risk rerun.

Identical Bronze sources create auditable `SKIPPED_DUPLICATE` attempts without
duplicating business rows. Silver records successful unchanged processing without
republishing an identical canonical snapshot. Immutable risk attempts remain
auditable, while the serving views expose exactly one complete selected risk run per
portfolio and as-of date to prevent double counting.

Current intentional boundaries: development only, manual deployment and job runs,
no schedule, no production target, no unattended credentials, and no Power BI
automation.

## Quick start

Prerequisites: Git, Python 3.12, and
[`uv`](https://docs.astral.sh/uv/) on your command path.

```bash
git clone <repository-url>
cd Market-Risk-Analysis
make setup
make check

```

Expected final environment diagnostic:

```text
11/11 checks passed
```

Useful commands:

| Command | Purpose |
| --- | --- |
| `make setup` | Create `.venv`, resolve dependencies, and install the package |
| `make format` | Apply consistent Python formatting |
| `make lint` | Find common code defects and style problems |
| `make test` | Run behavior checks in `tests/` |
| `make env` | Inspect local prerequisites and project paths |
| `make check` | Run the complete local quality gate |

## Repository map

```text
.
├── .github/workflows/             # Credential-free GitHub Actions CI
├── contracts/                    # Machine-readable data contracts
├── data/                         # Tracked fixtures and ignored generated data
├── docs/                         # Learning guides, decisions, and handoffs
├── notebooks/bronze/             # Git-backed Databricks Bronze ingestion
├── notebooks/silver/             # Git-backed Databricks Silver processing
├── notebooks/gold/               # Audited Gold market values and daily metrics
├── notebooks/qa/                  # Read-only serving-output validation
├── resources/                     # Databricks bundle job definitions
├── sql/bronze/                   # Bronze object definitions and verification
├── sql/silver/                   # Silver object definitions
├── sql/gold/                     # Gold object definitions
├── src/market_risk_analysis/     # Installable Python application code
├── tests/                        # Automated behavior and structure checks
├── Makefile                      # Short developer commands
├── pyproject.toml                # Python project and tool configuration
└── uv.lock                       # Resolved development dependencies
```

## Phase 05 Silver milestone

The persistent Silver Databricks objects are:

- `workspace.devin_market_risk_dev.silver_portfolios`
- `workspace.devin_market_risk_dev.silver_processing_runs`
- `workspace.devin_market_risk_dev.silver_portfolio_record_outcomes`
- `workspace.devin_market_risk_dev.silver_data_quality_violations`

The controlled Bronze portfolio batch produced two canonical Silver portfolio rows
and two nonfatal missing-inception-date warnings. The successful recovery run
published the two records with status `SUCCEEDED_WITH_WARNINGS`. An identical
reprocessing attempt classified both records as unchanged, published nothing, and
left the canonical SHA-256 fingerprint unchanged.

## Project plan and learning path

- [Project roadmap](docs/project-roadmap.md) defines the phases and exit gates.
- [Progress tracker](docs/progress.md) records the current phase and evidence.
- [Phase 05 handoff](docs/handoffs/phase-05-handoff.md) records the Silver
  implementation, validation, recovery, idempotency, and Phase 06 readiness.
- [Phase 06 handoff](docs/handoffs/phase-06-handoff.md) records the completed Gold
  analytics, reconciliation, explain-back, and Phase 07 readiness.
- [Phase 07 handoff](docs/handoffs/phase-07-handoff.md) records the completed risk
  measures, stress validation, live runs, and reconciliation evidence.
- [Phase 06 guide](docs/phase-06-gold-analytics-foundation.md) defines the governed
  dependencies, metric conventions, and completion evidence.
- [Phase handoff template](docs/phase-handoff-template.md) defines the handoff
  structure.
- [Phase 10 handoff](docs/handoffs/phase-10-handoff.md) records the completed
  dashboard implementation and validation.
- [Phase 11 handoff](docs/handoffs/phase-11-handoff.md) records CI, deployment,
  manual job-run, rerun-safety, and serving-validation evidence.

## Current phase boundary

Phase 11 is complete. The next phase is Phase 12 — End-to-End Validation and
Portfolio Handoff.

Future work must begin with a fresh Git, CI, bundle, deployed-resource, and serving
inventory. Scheduling, production deployment, Power BI Service publishing, automated
refresh, and live-data retrieval remain separate decisions requiring explicit scope
and approval.
