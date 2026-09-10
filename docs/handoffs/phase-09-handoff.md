# Phase 09 Handoff — Power BI Semantic Model

## Status

- **Phase:** 09 — Power BI Semantic Model
- **Status:** Complete
- **Date:** 2026-09-10
- **Next phase:** 10 — Market Risk Dashboard

## Objective

Create a governed Import-mode Power BI semantic model over the approved Phase 08
Databricks serving views, with clear dimensions, intentional relationships, explicit
measures, and reconciled outputs.

## Completed work

- Connected Power BI Desktop to Databricks through OAuth without storing credentials.
- Imported only the five approved Phase 08 serving views.
- Created `DimDate`, `DimPortfolio`, and `DimInstrument`.
- Built and marked a continuous 2016 `DimDate` table, including a
  `Year → Month Name → Date` hierarchy.
- Implemented nine active, single-direction `1:*` relationships.
- Added and formatted seven explicit DAX measures.
- Organized measures into `Portfolio Metrics` and `Risk Measures`.
- Hid the internal risk-run bridge and duplicate fact foreign keys from report view.
- Disabled automatic date/time so `DimDate` is the only calendar table.
- Validated daily analytics, VaR, and stress outputs against Databricks SQL.
- Saved the durable PBIP/TMDL project; the report canvas remains intentionally blank
  for Phase 10.

## Validation evidence

| Check | Result | What it proves |
| --- | --- | --- |
| Power BI refresh | Pass | Import refresh succeeds without an embedded secret |
| Daily reconciliation | Pass — 8 rows; latest-date NAV `$2,047,590.00`; P&L `$47,590.00` | Power BI matches the daily serving view |
| VaR reconciliation | Pass — 4 portfolio/confidence rows | Confidence-level filtering preserves non-additive VaR meaning |
| Stress reconciliation | Pass — 6 portfolio/scenario rows | Scenario-level stress results match the serving view |
| Relationship review | Pass — 9 active, single-direction `1:*` relationships | Dimensions filter facts without fact-to-fact or bidirectional ambiguity |
| Auto-date cleanup | Pass — 8 intended tables only | `DimDate` is the single reporting calendar |
| Local quality gate | Pass — Ruff; 238 pytest tests; 11/11 environment checks | Existing repository safeguards remain healthy |
| Project hygiene | Pass — whitespace check and credential scan | Version-controlled PBIP sources are reviewable and contain no secrets |

## Decisions and reasoning

| Decision | Reason | Tradeoff |
| --- | --- | --- |
| Use Import mode | Small governed views and manual-refresh MVP do not justify live-query latency | Data is refreshed on demand rather than queried live |
| Use serving views only | Centralizes business logic and latest-run control in Databricks | Power BI does not recalculate governed pipeline logic |
| Use `DimDate` | Provides one consistent calendar and filter path | Automatic date tables were disabled |
| Blank ambiguous risk measures | 95%/99% VaR and different stress scenarios are not additive | A user must choose one confidence level or scenario |
| Keep dashboard visuals for Phase 10 | Separates model correctness from presentation design | Report canvas is intentionally empty |

## Concepts the learner can explain

- Import avoids DirectQuery latency for this small, manually refreshed MVP.
- Facts record repeated business events; dimensions provide unique filter keys.
- Different VaR confidence thresholds represent different loss estimates and must not
  be added together.

## Open questions or blockers

- Phase 10 must design report visuals, slicers, and interaction behavior.
- `sql/serving/phase_08_create_risk_serving_views.sql` has a separate pre-existing
  unstaged change and must be validated/committed independently; it is not part of
  Phase 09.

## Git checkpoint

- **Branch:** `phase-09-power-bi-semantic-model`
- **Implementation commit:** `d9061b0` — `feat: add Power BI semantic model`
- **Working tree:** Phase 09 files committed; separate unstaged Phase 08 SQL change
  intentionally remains.
- **Ignored/generated artifacts checked:** Power BI `.pbi/` local cache is ignored;
  PBIP source scan found no credential-bearing terms.

## Files and platform objects changed

### Repository

- `.gitattributes` — normalizes version-controlled PBIP source line endings.
- `.gitignore` — ignores Power BI local `.pbi/` cache.
- `docs/phase-09-power-bi-semantic-model.md` — implementation and validation record.
- `powerbi/market_risk_semantic_model.pbip` and companion Report/SemanticModel
  directories — durable Power BI Project model.
- `docs/handoffs/phase-09-handoff.md` — this handoff.

### Databricks and Power BI

- Five Phase 08 serving views remain the approved persistent Databricks sources.
- `market_risk_semantic_model.pbip` is the persistent local Power BI project.
- No Power BI Service publishing, scheduled refresh, RLS, or credential persistence
  was introduced.

## Next-phase readiness

- The semantic model is saved, refreshable, and reconciled to governed SQL.
- Phase 10 can add visuals using dimensions and explicit measures only.
- Phase 10 must not bypass serving views, create fact-to-fact joins, or sum
  non-additive VaR/stress contexts.

## Suggested next-chat opening

> We are starting Phase 10 — Market Risk Dashboard. Read `README.md`,
> `docs/project-roadmap.md`, `docs/progress.md`, and
> `docs/handoffs/phase-09-handoff.md` first. Verify the Phase 09 Git checkpoint and
> reopen the PBIP model before building visuals.
