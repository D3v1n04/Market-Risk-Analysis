# Phase 12 — Real Historical Data Foundation

## Purpose

Phase 12 replaces neither the project’s governance nor its deterministic learning
baseline. It introduces a separate, auditable real-data source for the existing
15-instrument universe so later phases can rebuild the portfolio analysis from
real 2020–2025 market history.

## Source boundary

Two sources remain deliberately separate:

| Source | Purpose | Git policy |
| --- | --- | --- |
| `PROJECT_GIT_FIXTURE` | Permanent controlled baseline for regression, failure, calculation, and audit tests | Small deterministic fixtures remain tracked |
| `YAHOO_FINANCE` | Primary real historical source for later portfolio analysis | Full provider-derived snapshots remain ignored/private; code, manifests, hashes, summaries, and tests are tracked |

The sources must never be silently mixed or relabeled. The synthetic 2016 history
remains valid evidence for the completed Phases 01–11 scope.

## Approved real-data window

- Existing universe: exactly the approved 15 instruments; no expansion in this phase.
- Period: 2020-01-01 through 2025-12-31.
- Yahoo request: inclusive `start="2020-01-01"`, exclusive
  `end="2026-01-01"`, and `interval="1d"`.
- Expected scale: approximately 15 × 1,500 sessions, or about 22,500 price
  observations. This is an estimate, never an acceptance count.

The stable internal identifier remains the business key. Provider tickers are
attributes; for example, `BRK_B_US` maps to project symbol `BRK.B` and Yahoo
ticker `BRK-B`.

## Source characterization

The real source is described accurately as:

> Real historical market observations retrieved through a pinned `yfinance`
> client from Yahoo Finance’s publicly accessible market-data interface.

It is not a direct exchange feed, an official Yahoo Finance API integration, an
institutional market-data feed, or a production service with an SLA. It is
appropriate for this educational portfolio project, subject to provider availability
and later corrections.

## Phase 12 objective

Build a reproducible local extraction and governed ingestion path that:

1. records the exact request, client version, instrument mapping, retrieval time,
   source file hashes, schema, and row-count evidence;
2. preserves source-aligned private snapshots before Bronze ingestion;
3. lands isolated real-data Bronze records with immutable batch audit evidence;
4. validates and canonicalizes valid real prices and corporate actions in Silver;
5. proves safe retry and identical-rerun behavior.

## Coverage and failure policy

A price is expected for every active instrument on every validated common trading
session. A missing expected price is never forward-filled, invented, or silently
accepted as complete. Usable delivered records are preserved; the relevant batch is
`PARTIAL`, and complete downstream publication is blocked for that date.

Corporate actions are event data. A successful request may validly yield zero actions
for an instrument; this differs from a missing expected daily price. A failed or
unverifiable action request is still a failure, not evidence of zero events.

Real counts are reconciled against the validated returned session grid and documented
missingness. The deterministic fixture retains its fixed count checks.

## Execution architecture

```text
Yahoo Finance
  → pinned local yfinance extractor in Ubuntu/WSL
  → ignored local raw snapshots and deterministic manifests
  → private Unity Catalog Volume landing path
  → real-data Bronze tables and batch audit
  → real-data Silver canonical tables and quality evidence
```

VS Code authors and reviews code. Ubuntu/WSL performs the external retrieval.
Databricks ingests and processes governed landed files. Git stores no full
provider-derived dataset unless redistribution rights are separately verified.

## Explicit exclusions

Phase 12 does not rebuild positions, cash, Gold analytics, VaR, stress results,
serving views, or Power BI. Those belong to Phases 13 and 14. It introduces no
schedule, production target, unattended authentication, or Power BI automation.

## Planned evidence

- A pinned dependency and lockfile before the first live request.
- A machine-readable real-source contract and source-profile tests.
- Private source files with SHA-256 manifests and safe, non-secret diagnostics.
- Per-dataset Bronze and Silver counts, outcomes, violations, and canonical hashes.
- A duplicate rerun that produces auditable attempts without duplicate business rows.
- Documentation of provider limitations and the manual operating boundary.
