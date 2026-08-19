# Market Risk Analysis

A learning-first project that will ingest market data, preserve its history, improve
its quality through lakehouse layers, and produce explainable risk analytics.

## Part 01 milestone

This repository currently provides a reproducible Python environment, an installable
package, automated tests, code-quality checks, safe local configuration, and an
environment diagnostic. It deliberately does **not** implement market data ingestion
or risk calculations yet.

## Quick start

Prerequisites: Git, Python 3.12, and
[`uv`](https://docs.astral.sh/uv/) on your command path.

```bash
git clone <repository-url>
cd Market-Risk-Analysis
make setup
make check
```

Expected final diagnostic line:

```text
8/8 checks passed
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

The Make targets keep uv's disposable download cache under `.cache/uv`. This makes
the same commands work in developer machines and restricted build environments.

## Repository map

```text
.
├── data/                         # Explanation only; generated data is ignored
├── docs/                         # Learning guides and decisions
├── src/market_risk_analysis/     # Installable application code
├── tests/                        # Automated behavior checks
├── .env.example                  # Safe configuration names, never secrets
├── .gitignore                    # Files Git must not track
├── Makefile                      # Short, memorable developer commands
├── pyproject.toml                # Python project and tool configuration
└── uv.lock                       # Exact resolved development dependencies
```

## Project plan and learning path

- [Project roadmap](docs/project-roadmap.md) defines the 12 phases, tools,
  deliverables, learning goals, and exit gates.
- [Progress tracker](docs/progress.md) records the current phase, confirmed
  decisions, evidence, and the prompt used to start a separate phase chat.
- [Phase handoff template](docs/phase-handoff-template.md) keeps project state
  transferable between phase chats.
- [Part 01: Environment & Repository Setup](docs/part-01-environment-and-repository-setup.md)
  is the detailed guide for the current phase.

Do not move to Phase 02 until Phase 01 has been run on the learner's Ubuntu machine,
its explain-back gate is complete, the first Git commit exists, and the progress
tracker and handoff contain the evidence.
