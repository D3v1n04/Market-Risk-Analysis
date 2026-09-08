# Phase 06 Handoff — Gold Analytics Foundation

## Status

- **Phase:** 06 — Gold Analytics Foundation
- **Status:** Complete
- **Date:** 2026-09-08 (completion evidence recorded)
- **Next phase:** 07 — Risk Measures and Validation (not started)

## Objective

Enable governed deterministic analytics inputs and publish explainable Gold market
values, exposures, NAV, daily P&L, and returns. Reconcile instrument and portfolio
grains, corporate-action effects, baseline conventions, and rerun behavior through
runtime validation, independent checks, and a learner explain-back.

## Completed work

- Phase 6A created governed instruments, target allocations, daily prices,
  corporate actions, a trading calendar, daily positions, and daily cash balances.
- Established both portfolios' `actual_inception_date` as `2016-01-04` through the
  governed correction path; Gold did not invent the date.
- Phase 6B created persistent Gold Delta tables for analytics audit attempts,
  instrument market values, and portfolio-daily metrics.
- Published 120 instrument rows and 8 portfolio rows for two USD portfolios across
  the four approved close-of-business dates `2016-01-04` through `2016-01-07`.
- Reconciled long and short exposure, cash, NAV, P&L, returns, WMT dividends, and
  NVDA's 2-for-1 split.
- Verified immutable audit attempts and linked reprocessing without republishing
  unchanged canonical Gold rows.
- Completed the learner explain-back. These are deterministic learning results;
  VaR and production stress calculations were not implemented in Phase 06.

## Validation evidence

Runtime and independent reconciliation results below are the supplied completion
evidence for checkpoint `632adc0`; the recording date is not a claimed runtime date.

| Check | Command or method | Result | What it proves |
| --- | --- | --- | --- |
| Local quality gate | `make check` | Ruff passed, 198 pytest tests passed, 11/11 environment checks passed | Local quality gate passes; it does not replace runtime execution |
| Instrument grain | Runtime count and distinct-key reconciliation | 120 rows, 120 distinct `portfolio_id × instrument_id × valuation_date` keys | 2 portfolios × 15 instruments per portfolio-date × 4 dates |
| Instrument formulas | Independent reconciliation | Zero market-value formula, absolute-value, or sign failures | Signed valuation and magnitude conventions reconcile |
| Market-value audit | Runtime audit history | 9 succeeded attempts, 8 published partitions, 1 unchanged nonpublished reprocess, 1 linked reprocess | Reprocessing is visible without unnecessary publication |
| Portfolio grain | Runtime count and distinct-key reconciliation | 8 rows, 8 distinct `portfolio_id × valuation_date` keys | 2 portfolios × 4 dates |
| Portfolio formulas | Independent reconciliation | Zero gross, net, NAV, P&L, baseline, return, or exposure-ratio failures | Portfolio measures and baseline dependencies reconcile |
| Portfolio-daily audit | Runtime audit history | 10 succeeded attempts, 8 published partitions, 2 unchanged nonpublished reprocesses, 2 linked reprocesses | Immutable attempts retain reprocessing lineage |
| Long-only economics | Independent reconciliation | Inception NAV USD 1,000,000; January 5 P&L +USD 7,499.999997; January 6 WMT cash and P&L +USD 650 | Ending NAV change and cumulative P&L both equal USD 8,149.999997 |
| Long-short economics | Independent reconciliation | Inception exposures 130% long, 30% short magnitude, 160% gross, 100% net; January 5 P&L +USD 40,440; January 6 WMT cash obligation and P&L −USD 1,000 | Ending NAV change and cumulative P&L both equal USD 39,440 |
| Split invariance | NVDA 2-for-1 split reconciliation | Quantity doubled, price halved, market value unchanged, zero split-only P&L | A split alone does not create economic profit |
| Cumulative P&L | Sum daily P&L versus ending NAV minus initial NAV | Exact match for both portfolios | Daily baselines reconcile over the complete period |
| Idempotency | Identical reruns and SHA-256 evidence | Immutable audit attempts; unchanged canonical content not republished | Audit growth is compatible with canonical idempotency |
| Explain-back | Learner knowledge check | Passed | Learner can explain the implemented metrics and evidence |

Individual run IDs and fingerprints are intentionally omitted from this handoff.
Aggregate audit counts and reconciliations provide the concise completion record,
while the persistent Databricks audit tables retain the detailed identifiers and
hashes. The [Phase 06 guide](../phase-06-gold-analytics-foundation.md) records the
complete metric definitions and result counts.

## Decisions and reasoning

| Decision | Reason | Rejected alternative or tradeoff |
| --- | --- | --- |
| Enable trusted dependencies before Gold | Portfolio configuration alone cannot establish valuation | Inventing prices, positions, cash, or inception would hide missing inputs |
| Allow governed Silver and previously published Gold dependencies | Later NAV baselines require published prior results with explicit lineage | Gold must never read Bronze directly |
| Keep the completed scope USD-only | Quote and base currencies match, so local and base market values are equal | Multi-currency FX requires additional governed inputs and validation |
| Separate signed value from short exposure magnitude | Negative holdings affect net value while positive magnitudes measure exposure | Summing signed shorts into gross exposure would understate it |
| Use initial NAV at inception and previous published closing NAV later | P&L and returns need an explicit, governed baseline | Assuming current NAV equals initial NAV would erase performance |
| Preserve audit attempts on unchanged reruns | Attempt history and canonical publication serve different purposes | Republishing unchanged rows would create unnecessary canonical changes |

## Concepts the learner can explain

- Instrument grain is one portfolio × one instrument × one valuation date;
  portfolio-daily grain is one portfolio × one valuation date.
- `signed_market_value = signed_quantity × close_price`; shorts have negative
  signed value, while `short_market_value` is their positive magnitude.
- Gross market value adds long value and short magnitude. Net security market value
  subtracts short magnitude from long value.
- `closing_nav = net_security_market_value + closing_cash_balance`.
- `daily_pnl = closing_nav - baseline_nav` and
  `daily_return = daily_pnl / baseline_nav`. Inception uses `portfolios.initial_nav`;
  later dates use previous published `portfolio_daily_metrics.closing_nav`.
- Each exposure ratio divides the corresponding security market value by closing
  NAV. At fixed security values and positive NAV, positive cash raises NAV and
  lowers positive exposure ratios; negative cash lowers NAV and raises them.
- Long WMT holdings receive dividend income; short WMT holdings owe dividend cash.
- An unchanged rerun adds audit evidence without republishing canonical rows.
  SHA-256 evidence checks content equality beyond matching row counts.
- A 2-for-1 split doubles quantity and halves price without changing market value
  or creating split-only P&L.

## Knowledge check

The learner passed the Phase 06 explain-back. The following records a concise
assessment of the covered questions, rather than a verbatim answer transcript.

| Question or topic | Assessment |
| --- | --- |
| Why 120 instrument rows but only 8 portfolio rows? | Correctly explained both grains and 2 × 15 × 4 versus 2 × 4 coverage |
| How do shorts affect signed value, gross, and net? | Distinguished negative signed value from positive short magnitude and explained addition versus subtraction |
| How are NAV, P&L, and return calculated? | Explained cash-inclusive NAV, initial NAV at inception, and previous published closing NAV thereafter |
| Why do long and short WMT dividends have opposite signs? | Explained income for long holders and the cash obligation for short holders |
| How does cash affect exposure ratios? | Explained positive and negative cash effects through the closing-NAV denominator |
| Why can audit counts grow on an unchanged rerun? | Explained immutable attempts, unchanged canonical publication, and SHA-256 content evidence |
| Why does the NVDA split create no P&L? | Explained offsetting quantity and price changes and preserved market value |

No Phase 06 explain-back clarification remains open.

## Open questions or blockers

- None for Phase 06 completion.

Phase 07 must agree risk-history requirements and validation conventions before
implementation. Four deterministic dates establish Phase 06 behavior; they do not
establish adequate history for historical VaR.

## Git checkpoint

- **Branch:** `phase-06-gold-analytics-foundation`
- **Starting checkpoint:** `944fc12`
- **Validated implementation checkpoint:** `632adc0` —
  `feat: calculate audited Gold portfolio daily metrics v2`
- **Working tree:** Clean before this update; the four Phase 06 completion
  documentation files are intentionally pending commit. No commit or push was made.
- **Ignored/generated artifacts checked:** The final documentation diff and status
  are checked for scope; generated runtime artifacts are not part of this update.

## Files and platform objects changed

### Repository

The implementation checkpoint includes the following Phase 06 areas; this completion
update changes only the four documentation files listed last.

- `contracts/` — governed analytics input and Gold metric contracts
- `data/fixtures/phase_06_analytics_scenario.yml` — deterministic scenario
- `src/market_risk_analysis/ingestion/phase_06_market_inputs.py` — deterministic
  market-input materialization
- `notebooks/bronze/` and `notebooks/silver/` — Phase 6A landing, ingestion,
  validation, canonicalization, calendar, position, and cash processing
- `sql/bronze/` and `sql/silver/` — Phase 6A table definitions
- `sql/gold/phase_06_create_gold_tables.sql` — Gold Delta definitions
- `notebooks/gold/phase_06_calculate_position_market_values.py` — audited
  instrument valuations
- `notebooks/gold/phase_06_calculate_portfolio_daily_metrics.py` — audited daily
  portfolio metrics
- `tests/` — Phase 06 contract, input, SQL, and notebook checks
- `README.md` — completed current state and handoff links
- `docs/progress.md` — completion checkpoint, evidence, and Phase 07 readiness
- `docs/phase-06-gold-analytics-foundation.md` — historical baseline, completed
  outcome, layer rule, inception correction, and metric definitions
- `docs/handoffs/phase-06-handoff.md` — this handoff

### Databricks

- `workspace.devin_market_risk_dev.gold_analytics_runs` — persistent Delta audit
  history for Gold calculation attempts and publication lineage
- `workspace.devin_market_risk_dev.gold_position_market_values` — persistent Delta
  table with 120 canonical instrument market-value rows
- `workspace.devin_market_risk_dev.gold_portfolio_daily_metrics` — persistent Delta
  table with 8 canonical portfolio-daily rows
- Governed upstream inputs — persistent trusted instruments, allocations, prices,
  corporate actions, trading calendar, positions, cash, and corrected inception

Multi-currency FX, VaR, production stress calculations, Power BI, scheduling, live
retrieval, and StockTracker changes remain outside Phase 06.

## Next-phase readiness

- Governed inputs and reconciled Gold values, NAV, P&L, returns, and exposure ratios
  are available with explicit grains, USD units, dates, dependencies, and lineage.
- Runtime, independent economic reconciliation, local quality checks, rerun
  evidence, and learner explain-back passed.
- Verify the completion documentation commit, branch state, current canonical
  counts, and audit evidence before changing code or platform data.
- Agree historical VaR confidence levels, required history, quantile conventions,
  loss signs, stress assumptions, and independent known examples in Phase 07.
- Preserve the governed Silver and published Gold dependency rule; never read
  Bronze directly from Gold.
- Phase 07 — Risk Measures and Validation has not started.

## Suggested next-chat opening

> We are starting Phase 07 — Risk Measures and Validation. Read `README.md`,
> `docs/project-roadmap.md`, `docs/progress.md`, and
> `docs/handoffs/phase-06-handoff.md` first. Verify the stated Git and platform
> status. Teach and implement only Phase 07 scope, beginning with history needs,
> loss signs, quantile conventions, and validation examples. Keep dependencies and
> lineage explicit, reconcile independently, and require an explain-back before
> completion.
