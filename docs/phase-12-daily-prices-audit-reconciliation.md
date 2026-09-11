# Phase 12 Daily-Prices Audit Reconciliation

## Incident

During the first manual Yahoo Finance Silver publication, the `DAILY_PRICES`
task encountered a Delta concurrent-append conflict while inserting immutable
source-record outcome evidence into `silver_source_record_outcomes`.

- Parent job run: `182708319944229`
- Failed task run: `775437520269882`
- Error: `DELTA_CONCURRENT_APPEND.WHOLE_TABLE_READ`
- Conflicting table: `workspace.devin_market_risk_dev.silver_source_record_outcomes`
- Cause: the daily-prices and corporate-actions tasks ran concurrently and
  both attempted an insert-only `MERGE` into the shared audit table.

## Verified Canonical State

The failed task had already committed the canonical Daily Prices snapshot before
the audit-table transaction conflict. Subsequent read-only validation confirmed:

| Check | Verified value |
|---|---:|
| Yahoo daily-price rows | 22,620 |
| Trading dates | 1,508 |
| Instruments | 15 |
| First date | 2020-01-02 |
| Last date | 2025-12-31 |

The successful retry and later idempotency rerun both recorded all 22,620 Daily
Prices records as `UNCHANGED`, with no duplicate canonical publication.

## Recovery and Prevention

No historical `ACCEPTED_NEW` processing-run or source-record outcome is
backfilled. Doing so would misrepresent an audit event that was not persisted
at the time of the original publication.

The job was corrected in commit `baee89c7015a69f3c5422bbbfe551e7ca5b23474`
to serialize publication:

1. publish exchange calendar;
2. process Yahoo daily prices;
3. process Yahoo corporate actions.

The corrected job was deployed and its idempotency rerun completed successfully:

- Parent job run: `731181720277002`
- Code version: `baee89c7015a69f3c5422bbbfe551e7ca5b23474`
- Daily Prices: 22,620 `UNCHANGED`, `published = false`
- Corporate Actions: 280 `UNCHANGED`, `published = false`

This document is the immutable Phase 12 reconciliation evidence for the
historical audit gap. Canonical Daily Prices data was not changed during
recovery.
