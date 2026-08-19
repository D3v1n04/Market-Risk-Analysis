# Local data workspace

This directory will hold local lakehouse data in later parts. Its generated contents
are ignored by Git because datasets can be large, reproducible, licensed, sensitive,
or all four.

The planned layers are:

- `raw/`: source data preserved as received
- `bronze/`: ingested data with technical metadata
- `silver/`: cleaned and standardized data
- `gold/`: business-ready risk measures and aggregates

Only this explanation is committed. Data directories will be created by later jobs.
