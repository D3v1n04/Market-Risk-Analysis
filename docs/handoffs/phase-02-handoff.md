# Phase 02 Handoff — Databricks and SQL Connectivity

## Status

- **Phase:** 02 — Databricks and SQL Connectivity
- **Status:** In progress — awaiting final documentation and Git checkpoint
- **Date:** 2026-08-21
- **Next phase:** 03 — Risk Requirements and Data Contracts

## Objective

Establish secure, reproducible connectivity from Ubuntu, VS Code, and DBeaver to
Databricks Free Edition. Validate identity, catalog, schema, and SQL warehouse
routing independently while keeping credentials outside Git. Create and
intentionally retain one governed learner-owned development schema without
starting ingestion or creating tables.

## Completed work

- Toured the Databricks Free Edition workspace and SQL interface.
- Distinguished local tools from remote Databricks compute.
- Identified the `samples`, `system`, and `workspace` catalogs.
- Identified the `default` and `information_schema` schemas in `workspace`.
- Verified the Serverless Starter Warehouse is serverless, 2X-Small, and configured
  to stop after 10 minutes of inactivity.
- Installed Databricks CLI 1.13.0 at `/usr/local/bin/databricks` in Ubuntu.
- Authenticated CLI profile `market-risk-dev` using OAuth user-to-machine login.
- Verified CLI authentication files remain outside the repository with mode `600`.
- Installed and configured the verified Databricks VS Code extension 2.14.0 in the
  WSL environment.
- Added a version-controlled Databricks development target in `databricks.yml`.
- Ignored extension-generated `.databricks/` state.
- Validated the bundle configuration without deploying resources.
- Added and remotely validated a read-only SQL connectivity smoke test.
- Updated DBeaver Community Edition to 26.1.5.
- Installed the Databricks OSS JDBC driver 3.4.2 in DBeaver.
- Configured DBeaver connection `market-risk-databricks-dev` with OAuth U2M.
- Disabled DBeaver OAuth token caching and did not create a PAT.
- Executed identity and catalog validation independently through DBeaver.
- Executed a catalog/schema query from Ubuntu through the SQL Statement Execution
  API using the OAuth CLI profile.
- Created and intentionally retained `workspace.devin_market_risk_dev`.
- Verified the retained schema owner and comment.
- Verified both `workspace.default` and the retained development schema contained
  zero tables.
- Stopped the SQL warehouse after validation.
- Did not ingest data, create Bronze objects, or begin Phase 03 work.

## Validation evidence

| Check | Command or method | Result | What it proves |
| --- | --- | --- | --- |
| Repository preflight | Git branch, status, hashes, and commit inspection | Pass | Phase 02 began from synchronized `main` at Phase 01 completion commit `41f0588` |
| CLI installation | `databricks version` | Databricks CLI 1.13.0 | The supported CLI is available inside Ubuntu |
| CLI OAuth profile | `databricks auth profiles` | `market-risk-dev` valid | Ubuntu can authenticate to the intended workspace without a PAT |
| CLI identity | `databricks current-user me --profile market-risk-dev` | Pass; identity matched | The OAuth profile resolves to the expected Databricks user |
| Local auth permissions | `stat` on CLI authentication files | Both mode `600` | Only the Ubuntu user can read or write the local authentication files |
| VS Code extension | WSL extension inspection | Verified extension 2.14.0 | Databricks tooling is installed and enabled in the WSL environment |
| Bundle validation | `databricks bundle validate --profile market-risk-dev` | `Validation OK!` | Tracked workspace metadata is valid; no deployment was performed |
| Browser SQL context | Version-controlled smoke-test statements | Pass | Browser SQL returned the expected identity and `workspace.default` context |
| DBeaver connection | Connection Test | Connected; SparkSQL 3.3.3; JDBC 3.4.2 | DBeaver reached the SQL warehouse through JDBC and OAuth |
| DBeaver identity | `current_user()` | Pass; identity matched | DBeaver authenticated as the intended user |
| DBeaver context | `current_catalog()` and `current_schema()` | `workspace`, `default` | DBeaver used the expected Unity Catalog namespace |
| DBeaver catalogs | `SHOW CATALOGS` | 3 rows | `samples`, `system`, and `workspace` were accessible |
| DBeaver schemas | `SHOW SCHEMAS IN workspace` | 2 rows | `default` and `information_schema` were accessible |
| Default tables | `SHOW TABLES IN workspace.default` | Success; 0 rows | The default schema was accessible and empty |
| CLI SQL execution | Statement Execution API through `databricks api post` | `SUCCEEDED`; `workspace`, `default` | Ubuntu independently submitted SQL to the remote warehouse |
| Development schema DDL | Git-backed `CREATE SCHEMA IF NOT EXISTS` through DBeaver | Pass | The learner-owned persistent namespace was created safely |
| Development schema metadata | `DESCRIBE SCHEMA EXTENDED` | Name, catalog, owner, and comment matched | The governed object has the intended ownership and purpose |
| Development schema tables | `SHOW TABLES IN workspace.devin_market_risk_dev` | Success; 0 rows | Phase 02 created no tables or ingested data |
| Local quality gate | `make check` | Ruff passed, 4 tests passed, 11/11 readiness checks passed | The local repository remains healthy and requires both Phase 02 SQL assets |
| Resource cleanup | CLI warehouse stop and list | `STOPPED` | Remote compute was stopped while persistent metadata remained |

## Decisions and reasoning

| Decision | Reason | Rejected alternative or tradeoff |
| --- | --- | --- |
| Prefer OAuth U2M | Short-lived credentials and browser authorization reduce manual secret handling | A PAT was unnecessary for the supported JDBC 3.x path |
| Keep CLI authentication outside Git | Authentication state is machine-specific and sensitive | Tracked credentials would expose access |
| Disable DBeaver token caching | Minimizes persistent OAuth credential material | Reauthorization may occasionally be required |
| Use the Databricks OSS JDBC 3.x driver | Current supported driver with OAuth and Unity Catalog support | The legacy Simba driver is not needed |
| Track `databricks.yml` | Workspace target metadata is reproducible and reviewable | Extension-only local configuration would be difficult to maintain |
| Ignore `.databricks/` | It contains generated, machine-local extension state | Tracking it could expose local configuration and create noise |
| Keep SQL in Git | SQL can be reviewed, tested, and reused across clients | DBeaver history and browser queries alone are not durable project sources |
| Use fully qualified object names | Makes catalog and schema targets explicit | Session defaults can point to an unintended namespace |
| Retain `workspace.devin_market_risk_dev` | Provides an isolated learner-owned namespace for later phases | Dropping it would require unnecessary recreation |
| Stop compute after validation | Respects Free Edition resource limits | Leaving the warehouse running provides no benefit while idle |
| Do not start ingestion | Phase 02 is connectivity and governance only | Tables and Bronze ingestion belong to later phases |

## Concepts the learner can explain

- Local tools submit requests; the Databricks SQL warehouse performs remote compute.
- A SQL warehouse provides compute and does not store tables.
- Unity Catalog governs catalogs, schemas, tables, identities, and permissions.
- A schema is a logical namespace for objects, not a compute resource.
- JDBC connects and translates between DBeaver and Databricks.
- The server hostname identifies the workspace endpoint; the HTTP path routes to a
  specific SQL warehouse.
- OAuth authenticates a user with short-lived credentials; a PAT is a manually
  handled bearer credential and was not required.
- `IF NOT EXISTS` makes supported DDL safe to rerun without an already-exists error.
- Local automated checks and remote SQL validation prove different things.
- Analyst work explores and validates data; analytics-engineering work makes SQL,
  configuration, testing, security, and documentation reproducible.

## Glossary

| Term | Meaning in this project |
| --- | --- |
| Workspace | The Databricks environment containing tools, identities, and governed resources |
| Compute | Processing capacity that runs code or SQL; it is not data storage |
| SQL warehouse | SQL-optimized remote compute used by DBeaver and SQL clients |
| Serverless | Databricks manages the underlying compute infrastructure |
| Catalog | Top-level Unity Catalog namespace |
| Schema | Governed container for tables, views, and other objects inside a catalog |
| Table | Rows and columns with governed metadata and persistent data |
| Unity Catalog | Databricks governance layer for namespaces, ownership, and permissions |
| JDBC | Java standard and driver used by DBeaver to communicate with Databricks |
| Server hostname | Network address of the Databricks workspace endpoint |
| HTTP path | Routing identifier for a specific SQL warehouse |
| OAuth U2M | Browser-based user authorization using short-lived tokens |
| PAT | Personal access token; a manually handled bearer credential not used here |
| CLI profile | Named local Databricks host and authentication configuration |
| DDL | SQL that creates or changes persistent objects, such as `CREATE SCHEMA` |
| Idempotent | Safe to repeat without creating unintended additional state |
| Metadata query | Query that describes accessible catalogs, schemas, tables, or ownership |
| Smoke test | Small check proving a critical connection path works |
| Local validation | Checks performed on the repository without contacting Databricks |
| Remote validation | Evidence produced by executing against the Databricks platform |

## Knowledge check

The learner completed explain-back checkpoints covering warehouse versus storage,
catalog/schema/table hierarchy, local versus remote execution, JDBC routing, OAuth
versus PAT, idempotent DDL, Git-backed SQL, local versus remote validation, Free
Edition compute constraints, and analyst versus analytics-engineering workflows.

Initial answers were refined where needed:

- The HTTP path routes to a SQL warehouse rather than a file or webpage.
- OAuth proves rather than hides identity and avoids manual long-lived PAT handling.
- A schema organizes governed objects; the warehouse supplies compute.
- `make check` proves local checks, while CLI and DBeaver provide remote evidence.
- Stopped compute does not remove schemas, tables, data, or settings.

Assessment: knowledge check passed after explain-back and correction.

## Open questions or blockers

- None.
- Phase 03 must not begin until this handoff and progress update are reviewed,
  committed, pushed, and followed by a clean status check.

## Git checkpoint

- **Branch:** `phase-02-databricks-connectivity`
- **Implementation commits:**
  - `21f64ef` — `chore: configure Databricks workspace tooling`
  - `29fb249` — `test: add Databricks SQL connectivity smoke check`
  - `632a079` — `feat: add Databricks development schema setup`
- **Working tree:** implementation clean and synchronized before final documentation
- **Ignored/generated artifacts checked:** yes; `.databricks/` is ignored and CLI
  authentication remains outside the repository

## Files and platform objects changed

### Repository

- `.gitignore` — excludes generated `.databricks/` state
- `databricks.yml` — version-controlled development workspace target
- `sql/connectivity/phase_02_connectivity_check.sql` — read-only connectivity checks
- `sql/setup/phase_02_create_dev_schema.sql` — retained schema setup and validation
- `src/market_risk_analysis/environment.py` — requires Phase 02 SQL assets
- `docs/progress.md` — Phase 02 status and evidence
- `docs/handoffs/phase-02-handoff.md` — this handoff, glossary, and knowledge check

### Databricks, DBeaver, or Power BI

- `Serverless Starter Warehouse` — existing persistent warehouse configuration;
  stopped after validation
- `workspace.devin_market_risk_dev` — persistent learner-owned schema; deliberately
  retained with zero tables
- `market-risk-dev` — local Ubuntu OAuth CLI profile; outside Git
- `market-risk-databricks-dev` — local DBeaver OAuth/JDBC connection; outside Git
- Databricks VS Code development target — local extension connection backed by
  tracked non-secret workspace metadata
- No Power BI objects were created in Phase 02

## Next-phase readiness

- Secure CLI, VS Code, and DBeaver connectivity is validated.
- The learner-owned development schema is available and empty.
- Version-controlled SQL assets and local checks are present.
- No Phase 03 contracts, datasets, tables, or ingestion artifacts exist.
- Phase 03 must define analytical requirements and data contracts before creating
  data objects.

## Suggested next-chat opening

> We are starting Phase 03 — Risk Requirements and Data Contracts. Read
> `README.md`, `docs/project-roadmap.md`, `docs/progress.md`, and
> `docs/handoffs/phase-02-handoff.md` first. Verify the stated Git and platform
> status, then teach and implement only Phase 03. Do not begin Bronze ingestion.