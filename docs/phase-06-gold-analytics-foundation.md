# Phase 06 — Gold Analytics Foundation

## Status

Phase 06 is complete at validated implementation checkpoint `632adc0`.
Runtime validation, independent reconciliation, and the learner explain-back passed.
Phase 07 — Risk Measures and Validation is next and has not started.

The phase is divided into two explicit checkpoints:

- Phase 6A — Trusted Analytics Input Enablement: complete
- Phase 6B — Gold Metric Implementation: complete

The Phase 6A dependency gate passed before Phase 6B Gold calculations.

## Purpose

Build explainable and reconciled Gold analytics from trusted Silver data.

Phase 6A created the missing deterministic analytical inputs at their correct
medallion-layer responsibilities. Phase 6B calculated market value, returns,
daily P&L, long exposure, short exposure, gross exposure, net exposure, and
approved exposure ratios.

## Verified starting baseline

This is the historical baseline at `944fc12`, not the completed platform state.

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

Gold consumes governed Silver and previously published Gold when dependencies and
lineage are explicit, and calculates business-ready analytical measures. Gold never
reads Bronze directly or bypasses Silver to compensate for missing dependencies.

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

Both canonical portfolios started with a null `actual_inception_date`. Validated
deterministic prices established `2016-01-04` as the earliest common trading session
with complete coverage for all 15 active instruments. The date was established
through the governed Bronze-to-Silver correction path with a higher valid
configuration version. Gold consumed the governed date; it did not invent it.

## Cash and NAV dependency

Current NAV cannot be assumed to equal initial NAV.

The approved relationship is:

```text
current NAV = signed position market value + closing cash balance

```

The cash-balance design reconciles opening cash, approved daily cash movements,
and closing cash. Unsupported fees, interest, taxes, and external cash flows remain
excluded from the MVP.

## Audit requirements

Phase 6A retained:

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

## Completed Gold metrics

Both portfolios use USD and the approved close-of-business valuation dates
`2016-01-04` through `2016-01-07`. Monetary metrics are USD; returns and exposure
ratios are dimensionless. Local and base-currency market values are equal.

| Metric or convention | Definition |
| --- | --- |
| Instrument-market-value grain | One portfolio × one instrument × one valuation date |
| Portfolio-daily grain | One portfolio × one valuation date |
| `signed_market_value` | `signed_quantity × close_price` |
| `long_market_value` | Sum of positive signed instrument market values |
| `short_market_value` | Positive magnitude of short market value |
| `gross_market_value` | `long_market_value + short_market_value` |
| `net_security_market_value` | `long_market_value - short_market_value` |
| `closing_nav` | `net_security_market_value + closing_cash_balance` |
| `daily_pnl` | `closing_nav - baseline_nav` |
| `daily_return` | `daily_pnl / baseline_nav` |
| Inception baseline | `portfolios.initial_nav` |
| Later baseline | Previous published `portfolio_daily_metrics.closing_nav` |
| Exposure ratios | Corresponding security market value divided by `closing_nav` |

## Completion evidence

The persistent Gold Delta tables are:

- `workspace.devin_market_risk_dev.gold_analytics_runs`
- `workspace.devin_market_risk_dev.gold_position_market_values`
- `workspace.devin_market_risk_dev.gold_portfolio_daily_metrics`

| Check | Instrument market values | Portfolio-daily metrics |
| --- | --- | --- |
| Canonical rows and distinct business keys | 120 and 120 | 8 and 8 |
| Coverage | 2 portfolios × 15 instruments × 4 dates | 2 portfolios × 4 dates |
| Formula reconciliation failures | 0 market-value formula, absolute-value, or sign failures | 0 gross, net, NAV, P&L, baseline, return, or exposure-ratio failures |
| Succeeded audit attempts | 9 | 10 |
| Published partitions | 8 | 8 |
| Unchanged nonpublished reprocesses | 1 | 2 |
| Linked reprocesses | 1 | 2 |

`CORE_15_LONG` began with USD 1,000,000 NAV. January 5 market P&L was positive
USD 7,499.999997; January 6 WMT dividend cash and P&L were positive USD 650.
Ending NAV change and cumulative P&L both equaled USD 8,149.999997.

`LONG_SHORT_130_30` began at 130% long, 30% short magnitude, 160% gross, and
100% net exposure. January 5 P&L was positive USD 40,440; January 6 WMT
short-dividend cash obligation and P&L were negative USD 1,000. Ending NAV change
and cumulative P&L both equaled USD 39,440.

NVDA's 2-for-1 split doubled quantity and halved price, preserving market value
and producing zero split-only P&L. Cumulative daily P&L reconciled exactly to ending
NAV minus initial NAV for both portfolios. Identical reruns created immutable audit
attempts without republishing unchanged canonical Gold rows; SHA-256 idempotency
evidence supports unchanged content rather than row counts alone.

The local quality gate passed Ruff, 198 pytest tests, and 11/11 environment checks.
These local checks complement the supplied runtime and independent reconciliation
evidence; they do not replace Databricks execution.

The learner passed the explain-back on the 120-row versus 8-row grains, signed short
value versus positive short magnitude, gross/net calculations, NAV/P&L/return and
their baselines, long dividends versus short obligations, positive and negative cash
effects on exposure ratios, audit-only reruns, SHA-256 evidence, and split invariance.
See the [Phase 06 handoff](handoffs/phase-06-handoff.md) for the knowledge check and
Phase 07 readiness.

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
