# Market Risk Analysis

A learning-first market-risk analytics lakehouse that preserves source history,
improves data quality through governed layers, and produces explainable portfolio
risk analytics.

## Current milestone

Phases 01 through 04 are complete.

Phase 04 implemented deterministic Bronze ingestion for the approved portfolio
fixture. The project now includes a managed Unity Catalog landing Volume,
source and manifest fingerprints, source-aligned Bronze Delta tables, record-level
lineage, immutable batch-attempt auditing, and safe duplicate reruns.

The project does not retrieve live market data or perform Silver cleaning yet.
Dates, numbers, booleans, blank values, and other source representations remain
unchanged strings in Bronze.

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
├── notebooks/bronze/             # Git-backed Databricks ingestion notebook
├── sql/                          # Version-controlled Databricks SQL
├── src/market_risk_analysis/     # Installable Python application code
├── tests/                        # Automated behavior and structure checks
├── Makefile                      # Short developer commands
├── pyproject.toml                # Python project and tool configuration
└── uv.lock                       # Resolved development dependencies
```

## Phase 04 Bronze milestone

The persistent Databricks objects are:

- `workspace.devin_market_risk_dev.bronze_landing`
- `workspace.devin_market_risk_dev.bronze_portfolios`
- `workspace.devin_market_risk_dev.bronze_ingestion_batches`

The controlled portfolio fixture produced two Bronze records and one successful
audit record. An identical rerun preserved the two portfolio records and added a
`SKIPPED_DUPLICATE` audit record linked to the original batch.

## Project plan and learning path

- [Project roadmap](docs/project-roadmap.md) defines the phases and exit gates.
- [Progress tracker](docs/progress.md) records the current phase and evidence.
- [Phase 04 handoff](docs/handoffs/phase-04-handoff.md) records the Bronze
  implementation and Phase 05 readiness.
- [Phase handoff template](docs/phase-handoff-template.md) defines the handoff
  structure.

## Current phase boundary

The next phase is Phase 05 — Silver Quality and Canonical Data. Silver will cast,
validate, standardize, and deduplicate source values while Bronze remains immutable.

Live market-data retrieval, Gold risk calculations, Power BI work, and FastAPI
development have not started.
