# Phase 03 — Risk Requirements and MVP Scope

## Purpose

This document defines what the first Market Risk Analysis release must answer and
what it deliberately will not attempt. It separates business meaning from later
physical implementation so that Bronze ingestion, Silver validation, Gold metrics,
and Power BI reporting can be tested against the same requirements.

Phase 03 defines contracts and deterministic examples only. It does not call a live
market-data API, ingest production data, create Databricks tables, calculate
production risk metrics, or begin Power BI work.

## Intended users

- A portfolio analyst who needs explainable daily exposure and risk results.
- An analytics engineer who needs precise grains, keys, quality rules, and
  reproducible calculations.
- A reviewer who needs to trace each reported metric to documented source data and
  assumptions.

## MVP analytical questions

The first release must answer these questions for each portfolio and business date:

1. What is the portfolio's net asset value?
2. What are its long, short, gross, and net market exposures?
3. Which instruments, official sectors, and custom risk clusters drive exposure?
4. How concentrated is the portfolio by instrument, sector, and risk cluster?
5. What daily price P&L, dividend P&L, total P&L, and return were produced?
6. What are the portfolio's annualized volatility, drawdown, and maximum drawdown?
7. What are its one-day 95% and 99% historical Value at Risk estimates?
8. How would the portfolio respond to each approved deterministic stress scenario?
9. Is the result current and complete, or is it affected by missing, duplicate,
   invalid, quarantined, or stale source data?

The system must not publish a portfolio metric unless its as-of date, units,
currency, calculation convention, and data-quality state are available.

## Instrument universe

The MVP contains exactly 15 USD-quoted US-listed equities. Custom risk clusters are
analytical groupings and must be stored separately from sourced official sectors.

| Risk cluster | Target weight | Instruments | Selection rationale |
| --- | ---: | --- | --- |
| Systemic Core Technology | 38% | NVDA, AAPL, GOOGL, MSFT | Large-platform and index-concentration sensitivity |
| Semiconductor Infrastructure | 12% | TSM, AVGO | Semiconductor demand and supply-chain sensitivity |
| Consumer and Logistics | 13% | AMZN, WMT | Digital and physical consumer-demand exposure |
| Healthcare and Policy Sensitivity | 11% | LLY, UNH | Healthcare demand, policy, and execution sensitivity |
| Financial Foundations | 12% | JPM, BRK.B | Financial-system and macroeconomic sensitivity |
| Macro and Geopolitical Sensitivity | 14% | XOM, TSLA, LMT | Energy, industrial-transition, and geopolitical sensitivity |

The cluster weights total 100%. These rationales explain why the instruments were
selected; price data alone cannot directly measure capital expenditure, demand
elasticity, clinical pipelines, credit health, or geopolitical escalation.

### Instrument identity

An internal identifier is the stable business identity. Display and provider symbols
are attributes, not permanent keys. For example:

| Field | Berkshire Hathaway Class B value |
| --- | --- |
| `instrument_id` | `BRK_B_US` |
| `display_symbol` | `BRK.B` |
| `yfinance_symbol` | `BRK-B` |

Each instrument must also have a documented exchange MIC, quote currency,
classification source, and classification as-of date before it is active.

## Portfolio definitions

Both portfolios start with an initial NAV of USD 1,000,000 and use a buy-and-hold
strategy. They do not rebalance after inception. Quantities change only for mandatory
corporate actions such as stock splits.

### `CORE_15_LONG`

- Strategy: long-only
- Target long exposure: 100% of initial NAV
- Target short exposure: 0%
- Target gross exposure: 100%
- Target net exposure: 100%
- Allocation rule: preserve cluster weights and weight instruments equally within
  each cluster

| Instruments | Target weight per instrument |
| --- | ---: |
| NVDA, AAPL, GOOGL, MSFT | 9.50% |
| TSM, AVGO | 6.00% |
| AMZN, WMT | 6.50% |
| LLY, UNH | 5.50% |
| JPM, BRK.B | 6.00% |
| XOM, TSLA, LMT | 4.6666666667% |

The stored decimal weights must reconcile to 1.000000 within an absolute tolerance
of 0.000001.

### `LONG_SHORT_130_30`

- Strategy: 130/30 long-short
- Target long exposure: 130% of initial NAV, or USD 1,300,000
- Target short exposure: 30% of initial NAV, or USD 300,000
- Target gross exposure: 160%
- Target net exposure: 100%

| Instrument | Target weight | Instrument | Target weight |
| --- | ---: | --- | ---: |
| AAPL | 24.7% | MSFT | 24.7% |
| GOOGL | -5.7% | NVDA | -5.7% |
| AVGO | 15.6% | TSM | -3.6% |
| WMT | 16.9% | AMZN | -3.9% |
| LLY | 14.3% | UNH | -3.3% |
| JPM | 15.6% | BRK.B | -3.6% |
| XOM | 9.1% | LMT | 9.1% |
| TSLA | -4.2% |  |  |

Positive weights must total 1.300000, negative weights must total -0.300000, gross
weights must total 1.600000, and signed weights must total 1.000000. Each check uses
an absolute tolerance of 0.000001.

## Inception and position convention

The target inception date is 2016-01-04. If any active instrument lacks a valid
closing price on that date, inception moves to the earliest later trading session
with a valid unadjusted close for all 15 instruments.

Initial signed quantity is:

```text
initial_quantity = initial_nav * signed_target_weight / inception_unadjusted_close
```

Quantities use fixed-precision decimals and are not forced to whole shares. Positive
quantity means long, negative quantity means short, and zero positions are not
stored.

One position row represents one portfolio's quantity in one instrument at the close
of one business date. Daily positions are snapshots, not individual trades.

## Valuation and corporate-action conventions

- Unadjusted close values positions on a business date.
- Adjusted historical prices produce the return series used by historical risk
  calculations and reconciliation.
- Explicit corporate-action records drive quantity and dividend treatment.
- A 2-for-1 split doubles quantity, has split ratio `2.0`, and should not materially
  change market value by itself.
- Cash dividends are not reinvested. Long-position dividends increase portfolio
  cash; short-position dividends owed decrease portfolio cash.
- Cash earns no interest in the MVP.
- There are no external deposits or withdrawals after inception.
- Raw close, adjusted history, corporate actions, and cash must not double-count the
  same economic event.

Net asset value is the sum of signed position market values and cash balance.

## Data frequency and freshness

- Frequency: end-of-day only
- Business timezone: `America/New_York`
- Storage timestamps: UTC
- Planned retrieval time: approximately 19:05 America/New_York after a completed US
  trading session
- Freshness deadline: 20:00 America/New_York
- Current batch: all 15 active instruments have an accepted price for the latest
  completed expected trading session

Weekends and exchange holidays do not require new equity prices. If one or more
expected instruments is missing, the batch is `PARTIAL` and portfolio metrics for
that date are not published as complete. A valid late record may be accepted with a
warning and its original event date preserved.

## Required metric conventions

### Exposure and P&L

```text
position_market_value = signed_quantity * unadjusted_close
long_exposure = sum(positive position_market_value)
short_exposure = abs(sum(negative position_market_value))
gross_exposure = long_exposure + short_exposure
net_exposure = long_exposure - short_exposure
price_pnl = current_position_market_value - prior_position_market_value
dividend_pnl = prior_signed_quantity * dividend_per_share
total_pnl = price_pnl + dividend_pnl
daily_return = total_pnl / prior_nav
```

Split-adjusted quantities must be used before price P&L is interpreted. All money
metrics are reported in the portfolio's USD base currency.

### Concentration

- Gross instrument weight is absolute position market value divided by gross
  exposure.
- Sector and risk-cluster views report both signed net exposure and gross exposure.
- Herfindahl-Hirschman concentration is the sum of squared gross instrument weights
  and is reported on a 0-to-1 scale.

### Volatility and drawdown

- Annualized volatility uses the sample standard deviation of 252 daily portfolio
  returns multiplied by the square root of 252.
- Drawdown is `nav / running_peak_nav - 1`.
- Maximum drawdown is the lowest drawdown observed since inception.

### Historical Value at Risk

- Method: historical simulation
- Horizon: one trading day
- Return type: simple adjusted-price return
- Exposure assumption: current signed positions remain fixed across historical
  scenarios
- Primary window: latest 1,260 complete common trading sessions
- Minimum history: 1,000 complete common observations
- Confidence levels: 95% and 99%, with 99% as the headline result
- Missing returns: never replaced with zero
- Output: positive loss amount in USD and as a percentage of current NAV
- Quantile convention: continuous empirical percentile, documented and tested in
  the implementation phase

VaR is an estimate based on observed history, not a maximum possible loss or a
forecast guarantee. Drawdown, stress tests, data completeness, and methodology notes
must be shown with it.

## Deterministic stress requirements

Each active stress scenario must contain exactly one signed decimal shock for each
of the 15 active instruments. Scenario P&L is:

```text
scenario_pnl = sum(current_signed_market_value * instrument_shock)
```

The initial scenario library contains:

1. `BROAD_MARKET_DOWN_10`: every instrument receives a -10% shock.
2. `TECHNOLOGY_CORRECTION`: heavier losses for technology and semiconductor names.
3. `GEOPOLITICAL_SUPPLY_SHOCK`: severe semiconductor losses, with positive XOM and
   LMT shocks.

Shocks are hypothetical analytical inputs, not predictions. A shock below -100%
fails validation. A shock above +100% requires a warning and written justification.

## Data-quality reporting requirements

Every source record must have a deterministic `source_record_id`, ingestion batch,
record hash, ingestion timestamp, and contract version. Quality rules assign one of
these dispositions:

| Disposition | Meaning |
| --- | --- |
| `ACCEPT` | Valid record may become canonical Silver data |
| `WARN` | Usable record with an investigation signal |
| `QUARANTINE` | Record requires resolvable reference or context before use |
| `REJECT` | Invalid record cannot enter Silver |

Bronze preserves every delivered record and batch. Silver holds one current valid
record per business key.

- Same key and same hash in a later overlap window: `UNCHANGED`.
- Identical duplicate within one batch: warn and deduplicate before Silver.
- Same key with conflicting values in one batch: quarantine all conflicting
  candidates.
- Valid changed value in a later batch: accept as a correction and retain history.
- Invalid changed value in a later batch: reject it and retain the prior valid Silver
  row.
- Missing expected instrument: mark the batch `PARTIAL`.

A quarantined record is resolved only by correcting reference data or configuration
and reprocessing the original immutable Bronze record in a new linked batch. Silver
is never repaired by an undocumented manual edit.

## MVP boundaries and non-goals

The MVP deliberately excludes:

- Intraday, streaming, or trading-system latency guarantees
- Price predictions, recommendations, optimization, or automated trading
- Discretionary trades or periodic portfolio rebalancing
- Options, bonds, futures, cryptoassets, and multi-currency valuation
- FX conversion
- Borrow fees, margin interest, transaction costs, taxes, and cash interest
- Regulatory capital or regulatory-grade VaR claims
- Expected Shortfall, parametric VaR, Monte Carlo VaR, and VaR backtesting
- Direct reads from or writes to StockTracker and its PostgreSQL database
- Redistribution of provider market data
- Production availability or service-level guarantees

StockTracker remains a separate application. A future integration may consume
documented curated outputs, but it is not part of this MVP.

## Acceptance criteria

This requirements document is satisfied only when the remaining Phase 03 artifacts:

- Define a precise grain and business key for every required dataset.
- Give every field a meaning, data type, null rule, unit, and valid range where
  applicable.
- Provide deterministic passing and failing examples.
- Validate portfolio weights, stress coverage, conditional corporate-action fields,
  and required contract metadata automatically.
- Document source limitations, licensing boundaries, calendar provenance, and
  Databricks Free Edition constraints.
- Contain no credentials, licensed market history, or unnecessary generated data.
- Pass the repository's local quality gate.
- Are understood and approved before the Phase 03 Git checkpoint is created.
