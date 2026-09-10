# Phase 10 Handoff — Market Risk Dashboard

## Status

- **Phase:** 10 — Market Risk Dashboard
- **Status:** Complete
- **Date:** 2026-09-10
- **Next phase:** 11 — Automation, Deployment, and CI

## Objective

Build a local, understandable Power BI dashboard over the approved Phase 09 semantic
model. The dashboard must support portfolio-performance review, exposure
investigation, and controlled downside-risk interpretation without bypassing
governed serving views or changing the data pipeline.

## Completed work

- Built Executive Overview with Portfolio and performance-date controls, four KPI
  cards, and separate Closing NAV and Daily P&L history charts.
- Built Exposure & Concentration with absolute exposure by instrument, long-versus-
  short gross exposure, and signed/absolute position detail.
- Built VaR & Stress Testing with independent Risk As-of Date, VaR Confidence, and
  Stress Scenario controls, three risk KPI cards, two comparison charts, and a
  visible interpretation caveat.
- Kept Portfolio as the shared analytical selector; defaulted it to
  `CORE_15_LONG`.
- Preserved the distinction between performance date (`2016-01-07`) and published
  risk-run as-of date (`2016-12-30`).
- Saved, closed, and successfully reopened the PBIP project.
- Removed automatic Power BI serialization-only semantic-model and report-source
  churn before committing the intended report definitions.

## Validation evidence

| Check | Result | What it proves |
| --- | --- | --- |
| Executive opening state | Pass — Core on January 7 showed `$1.01M` NAV, `$0.00` P&L, `0.00%` return, and `$1.01M` gross exposure | Current-context KPIs render correctly |
| Date behavior | Pass — January 6 changed P&L to `$650.00` and return to `0.06%`, while trend charts retained history | KPI and trend scopes are intentional |
| Exposure behavior | Pass — Long/Short showed `$1.32M` long and `$0.28M` short; signed short rows remained negative | Position grain and sign conventions are preserved |
| Risk opening state | Pass — Core, December 30, 95%, Broad Market Down 10 showed `$11.92K` VaR, `($107.81K)` stress P&L, and `$970.93K` stressed NAV | Selected confidence/scenario measures render correctly |
| Risk interaction behavior | Pass | Confidence changes affected the VaR card only; scenario changes affected stress cards only; comparison charts retained all alternatives |
| PBIP reopen | Pass | Saved report pages and interactions reopen successfully |
| Local quality gate | Pass — Ruff, 238 pytest tests, and 11/11 environment checks | Existing repository safeguards remain healthy |
| Project hygiene | Pass — `git diff --check`; Power BI source credential scan returned no matches | Committed PBIP source is reviewable and secret-free |

## Decisions and reasoning

| Decision | Reason | Tradeoff |
| --- | --- | --- |
| Four overview KPI cards | Shows current NAV, P&L, return, and gross exposure without crowding the page | Net/long/short detail belongs on Exposure |
| Full-history trend charts | Makes the selected daily result interpretable against recent observations | The performance-date slicer intentionally does not reduce chart history |
| Absolute exposure ranking | Prevents long and short positions from offsetting in concentration analysis | A separate table is needed to preserve signs |
| Independent Risk As-of Date | Risk run is published as of December 30, unlike daily performance on January 7 | Risk-page date is intentionally not synced to performance pages |
| Single-select risk controls | VaR confidence levels and scenarios are non-additive | Users compare alternatives through separate bars rather than totals |
| No VaR-breach claim | No governed backtesting or breach dataset exists | Dashboard does not label outcomes as unexpected events |

## Concepts the learner can explain

- A KPI card presents one selected-date result, while a trend chart can preserve the
  selected portfolio’s full history.
- `$0.00` means a valid matching result of zero; `--` means no valid result exists
  in the current context.
- Gross exposure uses absolute value; signed quantity and signed market value retain
  long/short direction.
- VaR at 95% and 99%, and different stress scenarios, must not be summed.
- A risk-run as-of date can differ from the date used to review portfolio
  performance.

## Open questions or blockers

- None for Phase 10.
- Formal VaR backtesting/breach analysis, automated refresh, publishing, and
  scheduled operation remain outside this phase.

## Git checkpoint

- **Branch:** `phase-10-power-bi-dashboard`
- **Implementation commit:** `cc5123a` — `feat: add market risk Power BI dashboard`
- **Working tree:** Clean after the implementation commit.
- **Ignored/generated artifacts checked:** Power BI local `.pbi/` workspace/cache
  remains ignored; no credential-bearing term was found in committed Power BI source.

## Files and platform objects changed

### Repository

- `docs/phase-10-market-risk-dashboard.md` — dashboard design and validation record.
- `powerbi/market_risk_semantic_model.Report/definition/pages/` — durable PBIP
  page, visual, and ordering definitions.
- `docs/handoffs/phase-10-handoff.md` — this handoff.

### Power BI

- `market_risk_semantic_model.pbip` — local persistent dashboard project.
- No Power BI Service publishing, credential persistence, scheduled refresh, or RLS
  was introduced.

## Next-phase readiness

- The local dashboard is saved, reopenable, and validated against the Phase 09
  governed model.
- Phase 11 should verify whether automation/deployment is appropriate for the
  learning MVP before adding workflows, schedules, or cloud publishing.
- Future work must preserve serving-view-only consumption and the non-additive risk
  controls.

## Suggested next-chat opening

> We are starting Phase 11 — Automation, Deployment, and CI. Read `README.md`,
> `docs/project-roadmap.md`, `docs/progress.md`,
> `docs/phase-10-market-risk-dashboard.md`, and
> `docs/handoffs/phase-10-handoff.md` first. Verify the Phase 10 Git checkpoint
> and discuss automation scope before changing any workflow or deployment setting.
