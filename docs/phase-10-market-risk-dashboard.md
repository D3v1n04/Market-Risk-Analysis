# Phase 10 — Market Risk Dashboard

## Objective

Build a local, interactive Power BI dashboard over the validated Phase 09 semantic
model. The dashboard must answer portfolio performance, exposure concentration, and
downside-risk questions without changing governed Databricks logic or misrepresenting
non-additive VaR and stress outputs.

## Scope and boundaries

The dashboard uses only the existing Import-mode PBIP model and its five approved
Phase 08 serving-view sources. No Bronze, Silver, Gold, or direct source connection
was added. No Power BI Service publishing, scheduled refresh, RLS, forecasting,
backtesting, or pipeline change was introduced.

## Dashboard pages

| Page | Main question answered | Visuals and controls |
| --- | --- | --- |
| Executive Overview | How is the selected portfolio performing, and what is its recent history? | Portfolio and performance-date slicers; Closing NAV, Daily P&L, Daily Return, and Gross Exposure cards; closing-NAV and daily-P&L trends |
| Exposure & Concentration | Where is the selected portfolio concentrated, and how are positions composed? | Portfolio and performance-date slicers; absolute exposure by instrument; long-versus-short gross-exposure donut; position-detail table |
| VaR & Stress Testing | What downside estimate and deterministic scenario impact apply to the selected portfolio and risk run? | Portfolio, Risk As-of Date, VaR Confidence, and Stress Scenario slicers; VaR, Stress P&L, and Stressed NAV cards; VaR-by-confidence and stress-P&L-by-scenario comparisons; interpretation caveat |

## Interaction design

- `CORE_15_LONG` is the opening portfolio selection.
- Executive Overview and Exposure & Concentration share the same Portfolio and
  performance-date selections. KPI cards reflect the selected date; the historical
  trend charts keep the full selected-portfolio history.
- VaR & Stress Testing uses an independent `Risk As-of Date` selection of
  `2016-12-30`. It is not synced with the performance-date slicers because risk
  results are published for a distinct risk-run as-of date.
- VaR Confidence and Stress Scenario slicers are single-select. Their cards show
  one selected context only.
- The VaR comparison chart always shows both 95% and 99% results, and the stress
  comparison chart always shows all three scenarios. These comparisons display
  separate bars; they do not add confidence levels or scenarios.

## Risk interpretation

VaR is a historical-simulation loss estimate at the selected confidence level.
Stress results are deterministic scenarios, not forecasts or evidence that an event
occurred. The trusted model does not contain a backtesting or VaR-breach dataset, so
the dashboard does not claim to identify unexpected realized breaches.

## Validation evidence

| Check | Result | What it proves |
| --- | --- | --- |
| Executive opening state | Pass — `CORE_15_LONG`, January 7, 2016; `$1.01M` closing NAV, `$0.00` daily P&L, `0.00%` daily return, and `$1.01M` gross exposure | The four KPI cards render the selected daily context |
| Performance-date behavior | Pass — January 6 changed the KPI cards to `$650.00` daily P&L and `0.06%` daily return while trends retained the four-date portfolio history | Date scope distinguishes current-context KPIs from history charts |
| Exposure behavior | Pass — Core showed `$1.01M` all-long gross exposure; Long/Short showed `$1.32M` long and `$0.28M` short | Position-level facts filter correctly by portfolio and preserve signed versus absolute values |
| Risk opening state | Pass — Core, risk as-of date December 30, 2016, 95% VaR, and Broad Market Down 10 displayed `$11.92K` VaR, `($107.81K)` stress P&L, and `$970.93K` stressed NAV | Governed risk measures render in one explicit confidence/scenario context |
| Risk interaction behavior | Pass — changing confidence changed only the VaR card; changing scenario changed only stress cards; both comparison charts retained all governed alternatives | Non-additive risk contexts are protected from accidental summation |
| PBIP reopen | Pass | Saved dashboard pages, interactions, and opening selections reopened successfully |
| Local quality gate | Pass — Ruff, 238 pytest tests, and 11/11 environment checks | Existing repository safeguards remain healthy |
| Project hygiene | Pass — `git diff --check`; PBIP credential scan found no credential-bearing terms | Version-controlled report sources remain reviewable and secret-free |

## Decisions

- Use concise KPI cards for current portfolio condition and separate line charts for
  historical performance.
- Use absolute market value for concentration so long and short positions cannot
  cancel each other in the exposure ranking.
- Preserve signed quantity and signed market value in the detail table so short
  positions remain visible.
- Use a local Risk As-of Date rather than the shared performance date because the
  published VaR/stress run is dated December 30, 2016.
- Display VaR confidence levels and stress scenarios side by side only as separate
  comparison bars; never sum them.
