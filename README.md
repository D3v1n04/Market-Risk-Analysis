# Market Risk Analysis

A learning-first market-risk analytics lakehouse that preserves source history,
improves data quality through governed layers, and produces explainable portfolio
risk analytics.

## Current milestone

Phases 01 through 06 are complete.

Phase 06 enabled governed deterministic instruments, target allocations, daily
prices, corporate actions, a trading calendar, daily positions, and daily cash
balances. Gold now publishes 120 instrument market-value rows and 8 portfolio-daily
metric rows for two USD portfolios across `2016-01-04` through `2016-01-07`.

Market values, exposures, NAV, P&L, and returns passed runtime validation and
independent reconciliation. Dividend signs, stock-split invariance, and unchanged
reruns reconcile; identical reruns retain immutable audit attempts without
republishing unchanged canonical rows. The learner passed the explain-back.

The validated implementation checkpoint is `632adc0`. The local quality gate passed
Ruff, 198 pytest tests, and 11/11 environment checks. Live market-data retrieval
remains outside the completed scope.

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
├── notebooks/gold/               # Audited Gold market values and daily metrics
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
- [Phase 06 guide](docs/phase-06-gold-analytics-foundation.md) defines the governed
  dependencies, metric conventions, and completion evidence.
- [Phase handoff template](docs/phase-handoff-template.md) defines the handoff
  structure.

## Current phase boundary

The next phase is Phase 07 — Risk Measures and Validation; it has not started.
Gold may consume governed Silver and previously published Gold when dependencies
and lineage are explicit. Gold never reads Bronze directly.

Multi-currency FX, VaR, production stress calculations, Power BI, scheduling, live
retrieval, and StockTracker changes remain outside Phase 06.
