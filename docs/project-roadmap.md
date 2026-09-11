# Market Risk Analysis — Project Roadmap

## Purpose

This is a learning-first build of a market-risk analytics lakehouse. The project
must produce a working system, but every phase also has to leave the learner able
to explain what was built, why it was built that way, and what evidence shows that
it works.

The roadmap is intentionally divided into separate phases so that each phase can
have its own ChatGPT conversation, exercises, Git checkpoint, and handoff.

## Confirmed toolchain

| Tool | Role |
| --- | --- |
| Windows | Hosts desktop applications and the browser |
| Ubuntu 24.04 on WSL 2 | Linux development and command-line environment |
| Docker Desktop with WSL 2 | Runs selected isolated services and integration-test environments when a container adds real value |
| VS Code | Main editor, terminal, Git interface, and Databricks development interface |
| Git | Version history and phase checkpoints |
| Python, `uv`, Ruff, and pytest | Application code, dependency management, linting, and automated tests |
| Databricks Free Edition | Lakehouse storage, compute, Unity Catalog objects, SQL warehouse, and workflows within Free Edition limits |
| DBeaver Community Edition | SQL exploration and manual reconciliation through the Databricks JDBC driver |
| Power BI Desktop | Semantic model, DAX measures, reports, and dashboards |

No password, personal access token, connection string containing credentials, or
other secret may be committed to Git.

## Target architecture

```text
Source files or APIs
        |
        v
Bronze Delta tables  -- preserved source observations and ingestion metadata
        |
        v
Silver Delta tables  -- typed, validated, deduplicated, canonical data
        |
        v
Gold tables/views    -- explainable market-risk measures and aggregates
        |
        v
Databricks SQL warehouse
        |                         |
        v                         v
DBeaver validation          Power BI model and dashboard
```

Version-controlled code begins in VS Code. Databricks performs governed data
processing. DBeaver inspects and reconciles the results. Power BI communicates the
approved Gold-layer results.

## How every phase will be taught

Each phase follows the same learning loop:

1. **Orient:** explain the goal and new vocabulary in plain language.
2. **Predict:** answer a small question before seeing the implementation.
3. **Practice:** complete a small isolated exercise.
4. **Build:** implement the real phase milestone in the repository or platform.
5. **Test:** run automated and manual checks and interpret failures.
6. **Explain back:** answer a knowledge check in original words.
7. **Commit:** inspect and commit the intentional changes.
8. **Handoff:** update progress and write the next-chat handoff.

Questions are expected throughout a phase. A phase does not need to finish in one
session, and asking questions never counts as falling behind.

## Milestone map

| Milestone | Phases | Result |
| --- | --- | --- |
| Foundation | 01–02 | Reproducible local development connected securely to Databricks |
| Data product | 03–05 | Contracted, ingested, and validated lakehouse data |
| Risk engine | 06–07 | Reconciled analytics and explainable risk measures |
| Consumption | 08–10 | Governed SQL access and a validated Power BI experience |
| Operation | 11 | Credential-free CI and manually operated development workflows |
| Real-data rebuild | 12–14 | Governed real history, rebuilt analytics, and refreshed consumer handoff |

## Phase summary

| Phase | Title | Primary tools | Exit result |
| --- | --- | --- | --- |
| 01 | Workstation and Repository Foundation | Ubuntu, Docker, VS Code, Git, Python, `uv` | The learner's machine can run the quality gate and has a first clean commit |
| 02 | Databricks and SQL Connectivity | Databricks, VS Code, CLI, DBeaver | Both development and SQL clients connect securely without committed secrets |
| 03 | Risk Requirements and Data Contracts | Python, SQL, documentation | Market-risk scope, source grain, keys, schemas, and acceptance rules are explicit |
| 04 | Bronze Ingestion | Databricks, Delta Lake, Python | Raw inputs are ingested repeatably with provenance and ingestion metadata |
| 05 | Silver Quality and Canonical Data | Databricks, SQL, pytest | Typed, deduplicated data and rejected records are supported by quality evidence |
| 06 | Gold Analytics Foundation | Python, SQL, Databricks | Market values, returns, exposures, and P&L reconcile to known examples |
| 07 | Risk Measures and Validation | Python, SQL, Databricks | Historical VaR and stress results are explainable, tested, and reconciled |
| 08 | SQL Serving and Databricks QA | SQL warehouse, Databricks SQL | Stable Gold views and a repeatable read-only SQL reconciliation pack are available |
| 09 | Power BI Semantic Model | Power BI, DAX, Databricks connector | A validated star model and core measures match Databricks results |
| 10 | Market Risk Dashboard | Power BI | An understandable, interactive dashboard answers defined risk questions |
| 11 | Automation, Deployment, and CI | Databricks Workflows, bundles, GitHub | Credential-free CI and manual development workflows are safely repeatable |
| 12 | Real Historical Data Foundation | Python, yfinance, Databricks, Delta Lake | Real 2020–2025 prices and corporate actions are source-preserved, quality-controlled, and canonical in Silver |
| 13 | Real Portfolio Analytics and Risk Rebuild | Python, SQL, Databricks | Positions, Gold analytics, VaR, and stress results are rebuilt and reconciled from the approved real Silver source |
| 14 | Real Serving, Power BI, and Final Handoff | Databricks SQL, Power BI Desktop, documentation | Serving views and the manual dashboard refresh use real outputs; evidence and limitations are presentation-ready |

## Detailed phase plans

### Phase 01 — Workstation and Repository Foundation

**Learn:** Windows versus WSL, Linux paths, shell commands, container, image,
repository, working tree, commit, Python package, virtual environment, manifest,
lockfile, test, linter, and secret hygiene.

**Build:**

- Record that Ubuntu 24.04 is the default WSL 2 distribution.
- Install VS Code on Windows and open the repository through the WSL extension.
- Keep the repository in the Ubuntu filesystem, not under `/mnt/c`.
- Install Git, Python 3.12, `uv`, and required VS Code extensions in the correct
  environment.
- Confirm that Docker Desktop uses its WSL 2 backend and has integration enabled
  for `Ubuntu-24.04`.
- Verify Docker from Ubuntu with a harmless disposable container. Do not install a
  second Docker Engine inside Ubuntu alongside Docker Desktop.
- Run the existing repository diagnostic, linter, and tests.
- Configure Git identity and make the first intentional commit.

**Evidence and exit gate:** `make check` passes on the learner's Ubuntu machine;
Docker is reachable from Ubuntu and can run then remove a disposable test container;
the intentional failing-test exercise has been completed; no generated environment
or secret is staged; the learner can answer the Part 01 explain-back questions; and
`git status` is clean after the commit.

### Phase 02 — Databricks and SQL Connectivity

**Learn:** local versus remote compute, workspace, catalog, schema, table, SQL
warehouse, JDBC, CLI, authentication, OAuth, and personal access token.

**Build:**

- Tour Databricks Free Edition and record its relevant limits.
- Identify the available catalog, learner-owned schema, and SQL warehouse.
- Install and authenticate the Databricks CLI inside Ubuntu.
- Configure the verified Databricks VS Code extension in the WSL project.
- Run a harmless version-controlled Python or SQL connectivity check.
- Configure DBeaver Community Edition with the Databricks JDBC driver.
- Use the safest supported authentication available; if a PAT is required for the
  Community Edition path, keep it only in local credential storage and never Git.

**Evidence and exit gate:** VS Code/CLI and DBeaver can independently execute an
identity/catalog query; a small governed development object can be created and
removed or retained intentionally; no secret appears in `git diff`, logs, screenshots,
or committed configuration; and the learner can trace which computer performs each
step of a query.

### Phase 03 — Risk Requirements and Data Contracts

**Learn:** business requirement, analytical question, grain, primary key, data
contract, reference data, observation data, position, instrument, price, return,
exposure, P&L, confidence level, and time horizon.

**Build:**

- Write the first dashboard and risk questions before calculating anything.
- Choose an intentionally small first instrument scope.
- Define contracts for portfolios, positions, instruments, prices, FX rates, and
  a trading calendar.
- Define units, currencies, timestamps, null rules, keys, uniqueness, and valid
  ranges.
- Create deterministic synthetic learning data before considering licensed or
  live market data.
- Document expected failure cases and acceptance examples.

**Evidence and exit gate:** every field has a type and meaning; every dataset has
a declared grain and key; example records pass or fail for stated reasons; risk
scope and non-goals are explicit; and no licensed dataset is accidentally committed.

### Phase 04 — Bronze Ingestion

**Learn:** batch ingestion, idempotency, provenance, schema-on-read, schema
evolution, Delta table, transaction, partitioning, and audit metadata.

**Build:**

- Land the deterministic input datasets in an approved Databricks location.
- Create Bronze Delta tables without silently correcting source values.
- Add source name, source file, ingestion timestamp, batch identifier, and raw
  record traceability.
- Make rerunning the same batch safe and observable.
- Store ingestion code and table definitions in Git.

**Evidence and exit gate:** source and Bronze counts reconcile; duplicate reruns do
not create unexplained extra records; malformed inputs fail clearly; lineage back to
the input is demonstrable; and automated ingestion tests pass.

### Phase 05 — Silver Quality and Canonical Data

**Learn:** type casting, canonical model, deduplication, business key, referential
integrity, quarantine, data-quality dimension, and expectation.

**Build:**

- Convert raw fields into documented types, units, currencies, and timestamps.
- Deduplicate according to the contracts rather than arbitrary row order.
- Validate prices, quantities, currencies, dates, and instrument references.
- Separate valid records from rejected records with reason codes.
- Produce canonical Silver tables for downstream analytics.

**Evidence and exit gate:** quality counts reconcile as accepted plus rejected;
duplicate and invalid test fixtures behave as expected; keys and relationships pass;
and a rejected record can be traced back to Bronze.

### Phase 06 — Gold Analytics Foundation

**Learn:** market value, long and short exposure, net and gross exposure, return,
daily P&L, aggregation, FX conversion, as-of date, and reconciliation.

**Build:**

- Join positions to instruments, prices, currencies, and dates at an explicit grain.
- Calculate local and base-currency market values.
- Calculate simple returns and daily P&L using documented conventions.
- Aggregate gross, net, long, and short exposure by portfolio and useful dimensions.
- Publish business-named Gold tables or views.

**Evidence and exit gate:** hand-calculated fixtures match code; totals reconcile
across instrument and portfolio levels; missing-price behavior is explicit; Python
and SQL checks agree; and every metric states its grain, unit, currency, and as-of
date.

### Phase 07 — Risk Measures and Validation

**Learn:** loss distribution, quantile, historical simulation, Value at Risk,
confidence level, holding period, diversification, scenario, stress test, limitation,
and model risk.

**Build:**

- Implement one-day historical VaR at agreed confidence levels.
- Keep signs and units explicit so a loss is never mistaken for a gain.
- Add simple historical or hypothetical stress scenarios.
- Compare portfolio risk with useful segment or instrument contributions where the
  chosen method supports an honest interpretation.
- Document assumptions and limitations.
- Treat parametric VaR, Monte Carlo VaR, and formal backtesting as later additions
  unless the first method is already well understood and validated.

**Evidence and exit gate:** small known distributions produce expected quantiles;
edge cases and insufficient history fail safely; results reconcile through two
independent checks; assumptions are visible beside outputs; and the learner can
explain what VaR does and does not claim.

### Phase 08 — SQL Serving and Databricks QA

**Learn:** serving layer, view, consumer grain, SQL warehouse, governed result,
read-only consumer, rerun selection, reconciliation query, and query execution
order.

**Build:**

- Publish stable, business-friendly views over canonical Phase 06 Gold analytics.
- Publish controlled risk views that select exactly one complete, successful, and
  published risk run per portfolio and as-of date.
- Preserve the distinct consumer grains of daily analytics, position exposure, VaR,
  and stress results rather than joining unrelated grains.
- Create a version-controlled read-only Databricks SQL QA pack for object
  inventory, row counts, business-key uniqueness, canonical-source reconciliation,
  currency, risk-bundle completeness, and rerun control.
- Store all serving and QA SQL in Git.
- Use Databricks SQL Editor as the validated serving deployment and QA client;
  DBeaver is optional exploration tooling.

**Evidence and exit gate:** the five serving views exist and compile in Databricks;
daily and position consumer grains have zero duplicate business keys; daily serving
rows reconcile to canonical Gold; exactly one complete published risk run is served
per portfolio and as-of date; every selected run has two VaR measures and three
stress results; the read-only QA pack passes; local serving SQL tests pass; and the
learner can explain why standalone reruns must not be summed.

### Phase 09 — Power BI Semantic Model

**Learn:** connector, Import mode, DirectQuery, semantic model, fact, dimension,
star schema, relationship, filter context, measure, calculated column, Power Query,
and DAX.

**Build:**

- Connect Power BI Desktop to the Databricks SQL warehouse.
- Begin with Import mode unless a measured requirement justifies DirectQuery.
- Build a small star model from approved Gold objects.
- Create a proper date dimension and unambiguous relationships.
- Implement explicit DAX measures for core exposure, P&L, and VaR outputs.
- Keep transformations in Databricks when they define shared business meaning.

**Evidence and exit gate:** Power BI row counts and totals match the SQL QA pack;
relationships have intentional cardinality and filter direction; measures respond
correctly to filters; and refresh succeeds without embedding secrets in project files.

### Phase 10 — Market Risk Dashboard

**Learn:** analytical narrative, visual encoding, drill-down, slicer, tooltip,
conditional formatting, accessibility, performance, and misleading visualization.

**Build:**

- Create an executive overview with as-of date and refresh status.
- Create exposure and concentration analysis.
- Create P&L and risk-driver analysis.
- Create VaR and stress views with assumptions and limitations visible.
- Add portfolio, date, currency, asset-class, and other justified navigation.
- Use clear units, sign conventions, titles, and empty-state behavior.

**Evidence and exit gate:** every visual answers a named business question; selected
values reconcile to Databricks; filters do not create misleading totals; performance
is acceptable on the learning dataset; and another person can interpret the report
without verbal rescue.

### Phase 11 — Automation, Deployment, and CI

**Learn:** job, task, dependency, schedule, retry, parameter, environment, deployment,
continuous integration, artifact, secret scope, and rollback.

**Build:**

- Convert manual Databricks steps into a dependency-aware workflow.
- Define development deployment configuration using current Databricks bundle tooling.
- Parameterize dates, catalog/schema targets, and other environment-specific values.
- Add failure handling, observable logs, and safe reruns.
- Use Docker for an integration-test or CI environment only where it makes the
  result more reproducible; do not attempt to containerize the managed Databricks
  platform.
- Add a GitHub remote if not already configured and enable appropriate CI checks.
- Keep deployment credentials outside the repository.

**Evidence and exit gate:** a fresh deployment validates; a scheduled or manually
triggered run completes in order; an intentional failure is diagnosable; local and CI
quality gates agree; and rerunning does not corrupt results.

### Phase 12 — End-to-End Validation and Portfolio Handoff

**Learn:** acceptance test, runbook, data lineage, operational ownership, recovery,
cost awareness, technical demonstration, and retrospective.

**Build:**

- Run one controlled batch from source through Bronze, Silver, Gold, SQL, and Power BI.
- Reconcile record counts and key financial totals at every boundary.
- Review secrets, permissions, ignored files, Free Edition constraints, and costs.
- Finish architecture, data dictionary, testing, troubleshooting, and operating guides.
- Prepare a concise technical demonstration and an honest limitations section.
- Record future improvements without disguising them as completed work.

**Evidence and exit gate:** another person can follow the runbook; the end-to-end run
has retained evidence; Git is clean and tagged appropriately; the dashboard matches
the governed outputs; and the learner can explain the architecture and major choices.

## Optional extensions after the core project

These are not required for the first complete system:

- Licensed or live market-data ingestion
- Additional asset classes and pricing conventions
- Parametric and Monte Carlo VaR
- VaR backtesting and exception analysis
- Advanced stress-testing and scenario libraries
- Streaming ingestion
- Power BI Service publication, refresh, sharing, and row-level security
- Multiple Databricks environments and production-grade identity automation

They should be added only with a new contract, tests, and a clear reason.

## Separate-chat protocol

Use one main chat for each phase. Questions, troubleshooting, and exercises for that
phase stay in its chat. At the end of the phase:

1. Update `docs/progress.md`.
2. Create `docs/handoffs/phase-NN-handoff.md` from the handoff template.
3. Run the phase quality checks.
4. Inspect and create the phase Git commit.
5. Start the next chat with the kickoff prompt in `docs/progress.md` and attach or
   reference the repository files.

The repository and its handoff documents—not chat memory—are the authoritative record
of project state.
