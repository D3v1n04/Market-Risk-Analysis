# Market Risk Analysis

A learning-first market-risk analytics lakehouse that preserves source history,
improves data quality through governed layers, and produces explainable portfolio
risk analytics.

## Current milestone

Phases 01 through 05 are complete.

Phase 05 implemented contract-aligned Silver processing for the controlled Bronze
portfolio dataset. The project now casts preserved source strings into documented
types, evaluates structural and business-quality rules, resolves duplicate and
canonical versions deterministically, records outcomes and violations, gates
publication, and maintains processing-run audit lineage.

Two typed and uniquely keyed portfolio records are canonical in Silver. A failed
persistence attempt was preserved and linked to a successful recovery, and an
identical reprocessing attempt accepted zero new records, classified both records as
unchanged, and left the canonical count and SHA-256 fingerprint unchanged.

The project does not retrieve live market data or calculate Gold analytics yet.

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
├── contracts/                    # Machine-readable data contracts
├── data/                         # Tracked fixtures and ignored generated data
├── docs/                         # Learning guides, decisions, and handoffs
├── notebooks/bronze/             # Git-backed Databricks Bronze ingestion
├── notebooks/silver/             # Git-backed Databricks Silver processing
├── sql/bronze/                   # Bronze object definitions and verification
├── sql/silver/                   # Silver object definitions
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
- [Phase handoff template](docs/phase-handoff-template.md) defines the handoff
  structure.

## Current phase boundary

The next phase is Phase 06 — Gold Analytics Foundation. Gold will join trusted data
at explicit grains and calculate documented market values, returns, P&L, and
portfolio exposure measures.

Live market-data retrieval, Gold risk calculations, Power BI work, and FastAPI
development have not started.
