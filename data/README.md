# Local data workspace

This directory contains deterministic project fixtures and ignored generated data
used by the Market Risk Analysis lakehouse.

## Tracked fixtures

`fixtures/` contains the small deterministic inputs approved by the project data
contracts. These files are version-controlled so tests and demonstrations remain
reproducible.

## Generated data

Generated contents are ignored by Git because they may be large, reproducible,
licensed, sensitive, or environment-specific.

The local layer directories are:

- `raw/`: generated source manifests and locally preserved source artifacts
- `bronze/`: optional local source-aligned ingestion output
- `silver/`: optional local validated and standardized output
- `gold/`: optional local business-ready analytics output

During Phase 04, the portfolio manifest is generated beneath
`data/raw/portfolios/<source-sha256>/manifest.json`. The governed landed copies of
the CSV and manifest are stored in the managed Databricks Unity Catalog Volume
`workspace.devin_market_risk_dev.bronze_landing`.

Do not commit generated, licensed, secret-bearing, or sensitive data.
