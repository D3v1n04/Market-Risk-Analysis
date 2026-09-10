# Phase 09 — Power BI Semantic Model

## Objective

Create a governed, import-mode Power BI semantic model over the approved Phase 08
Databricks serving views. Preserve their grains and risk-rerun controls, expose
friendly dimensions and explicit measures, and validate Power BI outputs against
Databricks SQL.

## Approved sources

Power BI connects through Databricks OAuth to the SQL warehouse and imports only
these persistent serving views in `workspace.devin_market_risk_dev`:

- `vw_portfolio_daily_analytics`
- `vw_position_exposure_detail`
- `vw_latest_published_risk_runs`
- `vw_latest_published_var`
- `vw_latest_published_stress_results`

No Bronze, Silver, Gold table, or credential is embedded in the project.

## Model design

| Role | Power BI table | Purpose |
| --- | --- | --- |
| Dimension | `DimDate` | Continuous calendar from 2016-01-04 through 2016-12-30 |
| Dimension | `DimPortfolio` | Two distinct portfolio identifiers |
| Dimension | `DimInstrument` | Fifteen distinct instrument identifiers |
| Internal bridge | `DimRiskRun` | Governs the selected published risk run |
| Fact | `FactPortfolioDailyAnalytics` | Portfolio/day metrics |
| Fact | `FactPositionExposure` | Portfolio/instrument/day exposure detail |
| Fact | `FactVaR` | Selected-run VaR by confidence level |
| Fact | `FactStressResults` | Selected-run stress output by scenario |

`DimDate` is marked as the date table and includes a `Year → Month Name → Date`
calendar hierarchy. `Month Name` is sorted by `Month Number`.

All nine relationships are active, single-direction, and dimension-to-fact:

- `DimDate` filters `DimRiskRun`, `FactPortfolioDailyAnalytics`, and
  `FactPositionExposure`.
- `DimPortfolio` filters `DimRiskRun`, `FactPortfolioDailyAnalytics`, and
  `FactPositionExposure`.
- `DimInstrument` filters `FactPositionExposure`.
- `DimRiskRun` filters `FactVaR` and `FactStressResults`.

The internal risk-run bridge and duplicate fact foreign keys are hidden from report
view. This keeps report authors on the friendly dimensions without changing model
behavior.

## Measures

| Display folder | Measure | Purpose |
| --- | --- | --- |
| Portfolio Metrics | `Closing NAV (Latest Date)` | Portfolio closing NAV at the latest valuation date in filter context |
| Portfolio Metrics | `Daily P&L` | Sum of daily portfolio P&L |
| Portfolio Metrics | `Gross Exposure (Latest Date)` | Gross exposure at the latest valuation date in filter context |
| Portfolio Metrics | `Daily Return` | Daily return only when one portfolio and one date are in scope |
| Risk Measures | `VaR Amount` | VaR only when one confidence level is selected |
| Risk Measures | `Stress P&L` | Stress P&L only when one scenario is selected |
| Risk Measures | `Stressed NAV` | Stressed NAV only when one scenario is selected |

Currency measures use two decimal places; `Daily Return` uses percentage format.
`confidence_level` uses percentage format and is not summarized.

## Validation evidence

| Check | Result |
| --- | --- |
| Import-mode refresh | Passed without error |
| Daily analytics reconciliation | Eight Power BI rows matched the serving view; latest-date closing NAV total was `$2,047,590.00` and daily P&L total was `$47,590.00` |
| VaR reconciliation | Four portfolio/confidence rows matched Databricks SQL: 95% and 99% values for both portfolios |
| Stress reconciliation | Six portfolio/scenario rows matched Databricks SQL for stress P&L and stressed NAV |
| Model relationship review | Nine active `1:*`, single-direction relationships; no fact-to-fact or bidirectional relationship |
| Local quality gate | Ruff passed; 238 pytest tests passed; environment check 11/11 |
| Project hygiene | `git diff --check` passed; PBIP source scan found no credential-bearing terms |

## Decisions

- Use Import mode because the governed serving views are small, deterministic, and
  refreshed manually for this learning MVP.
- Keep business transformations and latest-published-run logic in Databricks; Power
  BI models and presents that governed output rather than recalculating it.
- Use explicit measures instead of relying on implicit aggregation.
- Return blank for ambiguous VaR confidence-level and stress-scenario contexts rather
  than silently summing non-additive risk outputs.
- Keep the report canvas intentionally empty; dashboard design belongs to Phase 10.

## Persistent project artifact

- `powerbi/market_risk_semantic_model.pbip`
- `powerbi/market_risk_semantic_model.Report/`
- `powerbi/market_risk_semantic_model.SemanticModel/`

The local Power BI `.pbi/` workspace/cache is ignored by Git.
