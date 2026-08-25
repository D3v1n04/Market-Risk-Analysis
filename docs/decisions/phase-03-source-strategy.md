# Phase 03 Decision Record — Source and Landing Strategy

## Status

- **Decision:** Accepted for the MVP design
- **Decision date:** 2026-08-25
- **Implementation status:** Not started; Phase 04 must prove the source contract
- **Scope:** End-of-day market observations, corporate actions, reference data,
  trading calendars, portfolio inputs, and deterministic test data

## Decision summary

The MVP will use a hybrid source strategy:

- Yahoo Finance through the open-source `yfinance` package supplies real end-of-day
  price history and corporate-action observations.
- Deterministic project-owned fixtures supply portfolios, allocations, stress
  scenarios, and intentional data-quality failures.
- A small reviewed security-master fixture supplies stable internal identities,
  provider symbols, exchange identifiers, currencies, sectors, and custom risk
  clusters.
- A pinned exchange-calendar package supplies expected US trading sessions, holidays,
  early closes, and open/close timestamps after its required calendars are verified.
- StockTracker remains independent and is not an upstream dependency.

Fetching will occur outside Databricks. Structured batch files will land in a Unity
Catalog volume before Phase 04 loads them into Bronze. The initial safe transfer may
use the Databricks UI or CLI; automation is not assumed until it is proven.

## Why this decision fits the MVP

The project needs recognizable real history for credible risk analysis, but it also
needs reproducible portfolios and failure cases that a market-data provider will not
reliably produce on demand. The hybrid design provides both without making the MVP
dependent on a paid feed, a secret API key, or StockTracker's application schema.

Fifteen instruments with approximately ten years of daily history produce about
37,800 canonical price rows. This volume is analytically useful and operationally
small; provider behavior, licensing, completeness, and correction handling are more
important risks than storage scale.

## Source responsibilities

| Data domain | MVP source | Authority and limitations |
| --- | --- | --- |
| Daily OHLCV | Yahoo Finance through `yfinance` | Real educational/personal-use observations; no production SLA |
| Adjusted history | Yahoo Finance through `yfinance` | Used for return and risk calculations only after reconciliation |
| Dividends and splits | Yahoo Finance through `yfinance` | Must be requested explicitly and validated against price behavior |
| Instrument identity | Project-owned reviewed fixture | Internal IDs are authoritative for this project; provider symbols are mappings |
| Official sector classification | Reviewed source recorded per instrument | Classification source and as-of date are mandatory |
| Custom risk clusters | Project-owned configuration | Analytical grouping defined in the requirements document |
| Portfolios and allocations | Project-owned deterministic fixtures | Exact inputs defined by the approved portfolio requirements |
| Trading sessions | Pinned `exchange_calendars` package | Package version and required exchange calendars must be verified |
| Quality failures | Project-owned deterministic fixtures | Designed passing, warning, quarantine, and rejection cases |
| Scale experiments | Deterministic generator | Generated locally, not committed; fixed seed and parameters recorded |
| Macroeconomic series | None in MVP | FRED or another source may be evaluated as a later extension |

## Provider evaluation

| Candidate | Strengths | Material limitations | MVP decision |
| --- | --- | --- | --- |
| `yfinance` / Yahoo Finance | No API key; supports the 15 symbols; long daily history; adjusted data and actions available; simple Python integration | Unofficial wrapper; personal-use limitation; no SLA; provider behavior can change | Selected with explicit caveats and replacement gate |
| Alpha Vantage | Documented keyed API; raw and adjusted daily endpoints; formal provider | Standard free usage is limited; full daily history and daily adjusted endpoint require premium access unless a separately verified educational arrangement applies | Rejected for the zero-cost reproducible MVP |
| StockTracker PostgreSQL | Existing project and familiar schema | Couples analytics to an application database; historical universe differs; current schema does not own the new risk contracts | Rejected as an MVP dependency |
| FRED | Authoritative macroeconomic series | Does not supply the required equity OHLCV and corporate actions | Deferred optional enrichment |
| Synthetic-only | Fully reproducible, licensing-safe, and controllable | Cannot demonstrate real historical market behavior | Used only for portfolios, failures, and scale tests |

The selected source is replaceable. Dataset contracts use internal IDs and
source-specific mappings so a later provider change does not redefine portfolio or
risk semantics.

## Licensing and redistribution boundary

The `yfinance` documentation states that the package is not affiliated with or
endorsed by Yahoo, is intended for research and educational purposes, and that the
Yahoo Finance API is intended for personal use. The project therefore applies these
rules:

- Use downloaded observations only for this personal educational portfolio.
- Do not commit real downloaded market history to Git.
- Do not publish, sell, redistribute, or expose a bulk raw-data download.
- Commit only small, clearly synthetic fixtures created by this project.
- Store source name, package version, retrieval time, and provider symbol with each
  batch so downstream users can identify provenance.
- Re-review provider terms before making the project commercial, multi-user, or
  publicly data-serving.

References:

- [`yfinance` documentation and legal disclaimer](https://ranaroussi.github.io/yfinance/)
- [Yahoo terms linked by `yfinance`](https://legal.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.html)

## Market-data extraction contract

Phase 03 does not call the provider. Phase 04 must implement and prove a pinned
`yfinance` version using explicit arguments. The initial smoke-test configuration is:

```python
yfinance.download(
    tickers=provider_symbols,
    start=requested_start_date,
    end=exclusive_end_date,
    interval="1d",
    auto_adjust=False,
    actions=True,
    prepost=False,
    repair=False,
    keepna=True,
    rounding=False,
    threads=False,
    progress=False,
    multi_level_index=True,
)
```

This is a contract proposal, not executable Phase 03 code. Phase 04 may revise an
argument only with recorded evidence and a contract update.

The arguments are explicit because current `yfinance.download` documentation lists
`auto_adjust=True` and `actions=False` as defaults. Depending on defaults would risk
losing raw closes or corporate-action observations after a library change.

Additional extraction rules:

- Use explicit `start` and `end`; do not use an ambiguous relative period.
- Treat `start` as inclusive and `end` as exclusive, matching the documented API.
- Convert the latest completed expected session into the correct exclusive end date.
- Do not request intraday or pre/post-market observations.
- Do not silently repair values with provider-library heuristics.
- Preserve provider-returned nulls in the landing batch for Bronze and quality review.
- Begin single-threaded for deterministic diagnostics; concurrency is unnecessary for
  15 symbols and may be reconsidered only after correctness is proven.
- Preserve raw provider field names and values in the landing representation while
  adding separate technical metadata.

Reference: [`yfinance.download` API](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html)

## Phase 04 provider proof gate

The source remains approved only if a controlled smoke test proves all of the
following for every active provider symbol:

1. The symbol resolves to the intended internal instrument.
2. Daily raw open, high, low, close, and volume are available.
3. Adjusted close or an equivalent adjusted series is available and explainable.
4. Dividends and splits are returned with distinguishable fields.
5. Daily dates can be normalized to the expected exchange session without ambiguity.
6. Ten-year coverage reaches the required common inception date or identifies a
   documented later fallback.
7. Known split examples reconcile quantity, raw price, adjusted history, and market
   value without double counting.
8. Repeating the same request can be classified as unchanged, while a changed valid
   observation can be retained as a correction.

If these checks fail materially, ingestion must stop. The project must either revise
the provider adapter with evidence or reopen the provider decision; it must not
silently weaken the data contract.

## History and recurring-window policy

### Initial backfill

- Requested start: 2016-01-01, allowing the first valid common trading session to be
  selected according to the portfolio inception rule.
- Requested end: day after the latest completed expected trading session because the
  provider's `end` parameter is exclusive.
- Scope: all 15 active provider symbols.
- Expected canonical magnitude: approximately 37,800 daily price records before
  actual coverage differences and corporate actions are considered.

### Recurring batch

- Run after the latest expected US trading session, approximately 19:05
  `America/New_York`.
- Target freshness deadline: 20:00 `America/New_York`.
- Request the prior ten expected trading sessions plus the latest completed session.
- Do not treat weekends or exchange holidays as missing-price failures.
- Add only one current valid Silver observation per business key; retain every
  delivered Bronze record and batch.
- A retry receives a new `batch_id` and links to the failed or partial prior attempt.

The overlap intentionally detects late observations, corrections, dividend
adjustments, stock splits, and previously missing dates. An identical overlap record
is normal `UNCHANGED` behavior, not a new canonical observation.

## Trading-calendar decision

Use the open-source `exchange_calendars` package as the proposed calendar source. It
is designed to define and query security-exchange calendars and is distributed under
the Apache-2.0 license.

Phase 04 must pin a version and prove:

- Every instrument's `exchange_mic` maps to a supported calendar.
- Expected sessions, holidays, early closes, open times, close times, and timezone
  behavior match reviewed examples.
- The required NYSE and Nasdaq mappings are tested rather than assumed.
- Generated calendar rows are reproducible from the pinned version.
- A package upgrade is treated as a reference-data change and reviewed before it
  changes expected-session logic.

If the required calendars cannot be verified, use a small reviewed project-owned
calendar fixture for the MVP rather than substituting Monday-through-Friday logic.

Reference: [`exchange_calendars` project](https://github.com/gerrymanoim/exchange_calendars)

## Landing and Databricks boundary

Databricks Free Edition restricts outbound internet access, provides serverless-only
compute, and limits the workspace to one SQL warehouse with a maximum 2X-Small size.
The design therefore does not depend on calling Yahoo Finance from Databricks.

Planned flow:

```text
Yahoo Finance -> pinned Python fetcher in WSL -> immutable batch files
              -> Unity Catalog landing volume -> Bronze ingestion
```

The planned governed landing location is:

```text
dbfs:/Volumes/workspace/devin_market_risk_dev/market_data_landing/
```

The volume is a planned Phase 04 object and is not created in Phase 03.

Landing requirements:

- One immutable directory per `batch_id`.
- Separate price and corporate-action records when their grains differ.
- Parquet for structured batch records and JSON for a small batch manifest.
- Manifest includes source, dataset, request window, package version, retrieval
  timestamps, symbol list, file hashes, row counts, and batch relationship metadata.
- UTC technical timestamps; source business dates remain separate.
- No credentials or secret-bearing URLs in files, manifests, logs, or Git.
- File upload may initially use the Databricks UI or `databricks fs cp`.
- CLI volume paths include the required `dbfs:/Volumes/...` scheme.
- Bronze ingestion begins only after the file and manifest hashes are verified.

Databricks recommends Unity Catalog volumes for governed non-tabular files and
documents both browser upload and CLI file operations. The exact automated transfer
and authentication route remains a Phase 04 proof requirement.

References:

- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Work with files in Unity Catalog volumes](https://docs.databricks.com/aws/en/volumes/volume-files)
- [Databricks CLI file-system commands](https://docs.databricks.com/aws/en/dev-tools/cli/reference/fs-commands)

## Security and secret rules

- `yfinance` requires no project API key.
- Databricks OAuth state remains outside the repository as established in Phase 02.
- Do not add a provider key merely to make an optional fallback look implemented.
- If a future provider requires a secret, store only its environment-variable name in
  Git and place the value in an approved local or platform secret store.
- Never log authorization headers, OAuth tokens, cookies, or complete secret-bearing
  URLs.
- The Git ignore policy continues to exclude generated data directories and local
  environment files.

## Reliability and recovery rules

- Classify network, provider, parsing, schema, completeness, and quality failures
  separately.
- A provider response with missing expected instruments is `PARTIAL`, not successful.
- A batch is not complete until manifest counts reconcile with delivered records.
- Retry with bounded backoff and a new linked batch; do not overwrite the failed
  attempt.
- Never replace a prior valid Silver record with an invalid correction.
- Preserve enough raw evidence to explain which values were delivered and why they
  were accepted, warned, quarantined, rejected, or unchanged.
- Free Edition and `yfinance` provide no production SLA; scheduled completion is a
  project freshness target, not an availability guarantee.

## Consequences and tradeoffs

### Benefits

- Zero provider cost and no market-data API secret for the educational MVP.
- Real history supports recognizable and explainable risk analysis.
- Deterministic fixtures make failures, short positions, and scale tests repeatable.
- Internal IDs and source mappings reduce future provider-migration cost.
- External fetching avoids relying on restricted Databricks Free Edition egress.
- Immutable batches and overlap retrieval support correction and audit behavior.

### Costs and limitations

- The selected provider is unofficial and cannot support production guarantees.
- Provider schema or behavior can change without notice.
- Personal-use restrictions limit redistribution and commercial reuse.
- A separate transfer step is required before Databricks ingestion.
- Calendar and classification packages require version and provenance maintenance.
- Raw and adjusted series need explicit reconciliation to avoid corporate-action
  double counting.

## Phase 03 acceptance checks for this decision

- Source responsibilities and non-goals are explicit.
- The provider selection is supported by current primary documentation.
- Licensing and redistribution boundaries are visible.
- Freshness, backfill, overlap, retry, and correction behavior are defined.
- Calendar provenance and upgrade behavior are defined.
- Free Edition network and compute limitations affect the architecture explicitly.
- The landing path and fallback transfer method are documented without claiming they
  have already been implemented.
- Phase 04 has a stop/go provider proof gate.
- No live API was called and no real market data, credentials, Databricks volumes, or
  Bronze tables were created during this decision.
