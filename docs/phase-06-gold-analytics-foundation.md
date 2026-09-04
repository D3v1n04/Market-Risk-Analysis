# Phase 06 — Gold Analytics Foundation

## Status

Phase 06 is in progress.

The phase is divided into two explicit checkpoints:

- Phase 6A — Trusted Analytics Input Enablement
- Phase 6B — Gold Metric Implementation

Gold calculations must not begin until the Phase 6A dependency gate passes.

## Purpose

Build explainable and reconciled Gold analytics from trusted Silver data.

Phase 6A will create the missing deterministic analytical inputs at their correct
medallion-layer responsibilities. Phase 6B will calculate market value, returns,
daily P&L, long exposure, short exposure, gross exposure, net exposure, and
approved exposure ratios.

## Verified starting baseline

- Git branch: `phase-06-gold-analytics-foundation`
- Starting commit: `944fc12`
- Phase 05 ancestry: verified
- Local working tree was clean before Phase 06 documentation changes
- Databricks catalog: `workspace`
- Databricks schema: `devin_market_risk_dev`
- Bronze tables: 2
- Silver tables: 4
- Gold tables: 0
- Canonical Silver portfolios: 2
- Silver processing runs: 3
- Silver record outcomes: 6
- Silver quality violations: 6
- The documented failed Silver run is explained and auditably recovered
- The unchanged Silver rerun did not republish canonical portfolio records

## Approved Phase 6A datasets

| Dataset | Classification | Bronze responsibility | Silver responsibility |
| --- | --- | --- | --- |
| Portfolios | Git-managed configuration | Existing `bronze_portfolios` | Existing `silver_portfolios` |
| Instruments | Git-managed reference | Preserve fixture values and lineage | Publish typed and validated canonical instruments |
| Target allocations | Git-managed configuration | Preserve fixture values and lineage | Publish typed and validated canonical allocations |
| Daily prices | Sourced observation | Preserve deterministic source observations and lineage | Publish typed and validated canonical prices |
| Corporate actions | Sourced observation | Preserve deterministic source events and lineage | Publish typed and validated canonical actions |
| Trading calendar | Generated versioned reference | Not required | Generate and validate a versioned canonical calendar |
| Positions | Derived daily observation | Not appropriate | Derive signed, split-adjusted daily quantities |
| Cash balances | Derived daily portfolio state | Not appropriate | Derive reconcilable opening, movement, and closing cash |

## Layer rule

Bronze stores records that were supplied by a source or controlled fixture without
silently correcting their business values.

Silver validates and canonicalizes supplied records. Silver may also contain
deterministic derived business facts, such as positions and cash balances, when
their inputs, formulas, dates, versions, and lineage are explicit.

Gold consumes trusted Silver data and calculates business-ready analytical
measures. Gold must not bypass Silver to compensate for missing dependencies.

## Approved allocation source

`data/fixtures/target_allocations.csv` is the authoritative machine-readable
instrument allocation for both portfolios.

For `LONG_SHORT_130_30`, the tested fixture uses WMT, UNH, and TSLA as the three
short instruments. Its weights reconcile to:

- Long exposure ratio: `1.300000`
- Short exposure magnitude ratio: `0.300000`
- Gross exposure ratio: `1.600000`
- Net exposure ratio: `1.000000`

Target allocations describe intended portfolio construction. They are not actual
positions or current exposure.

## Currency scope

The current MVP is USD-only:

- Instrument quote currency: USD
- Portfolio base currency: USD
- Local market value equals base-currency market value
- No FX-rate table or pretend currency conversion will be implemented

Multi-currency valuation remains outside the current MVP.

## Inception dependency

Both canonical portfolios currently have a null `actual_inception_date`.

The date may be populated only after validated deterministic prices establish the
earliest common trading session with complete price coverage for all 15 active
instruments. Any portfolio correction must pass through the existing governed
Bronze-to-Silver process with a higher valid configuration version.

## Cash and NAV dependency

Current NAV cannot be assumed to equal initial NAV.

The approved relationship is:

```text
current NAV = signed position market value + closing cash balance

```

The cash-balance design must reconcile opening cash, approved daily cash movements,
and closing cash. Unsupported fees, interest, taxes, and external cash flows remain
excluded from the MVP.

## Audit requirements

Phase 6A must retain:

- Source and fixture lineage
- Stable business keys
- Dataset and calculation versions
- Processing-run evidence
- Record-level outcomes
- Data-quality violations
- Deterministic hashes where required
- Idempotent rerun behavior
- Correction and failure behavior

The existing Bronze and Silver portfolio records must remain governed upstream
evidence.

## Phase boundaries

Phase 06 includes deterministic dependency enablement and Gold analytics.

Phase 06 excludes:

- Live-market retrieval without separate approval
- Value at Risk
- Production stress calculations
- Power BI development
- Scheduling and deployment automation
- Multi-currency conversion
- Modifications to StockTracker
