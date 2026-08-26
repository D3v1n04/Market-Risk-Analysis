# Phase 03 — Human-Readable Data Contracts

## Purpose and authority

This document is the human-readable semantic contract for the Market Risk Analysis
MVP. It defines what one row means, how a row is identified, what every field means,
which values are valid, and what happens when data fails a rule.

The later machine-readable contracts must agree with this document. Automated tests
will verify the parts that can be checked mechanically. If code, SQL, a dashboard, or
a provider response disagrees with this contract, the disagreement must be resolved
explicitly; downstream implementation must not silently invent new meaning.

These are logical contracts. Phase 03 does not create Databricks tables.

## Why grain and keys come first

**Grain** states exactly what one row represents. A **business key** is the smallest
set of business fields that should identify one row at that grain.

Without a declared grain, a join can multiply observations or a calculation can
double count them. Without a key, the pipeline cannot reliably distinguish a new
observation, an identical retry, a conflicting duplicate, or a later correction.

## Contract-wide standards

### Data types

Contracts use Databricks SQL-compatible logical types:

| Type | Use |
| --- | --- |
| `STRING` | IDs, codes, names, controlled values, and explanatory text |
| `DATE` | Business or effective calendar dates without a time-of-day |
| `TIMESTAMP` | An instant stored and interpreted in UTC |
| `BOOLEAN` | `TRUE` or `FALSE`; never `1`, `0`, or text equivalents |
| `BIGINT` | Whole-number counts and market volume |
| `DECIMAL(p,s)` | Money, prices, quantities, ratios, and shocks requiring fixed precision |

`FLOAT` and `DOUBLE` are not contract types for money, quantities, prices, weights,
or ratios because binary floating-point representation can introduce avoidable
rounding differences.

### Null semantics

- `NULL` means unknown, unavailable, not yet completed, or not applicable according
  to the field's documented rule.
- Empty strings, `N/A`, `UNKNOWN`, `-1`, and zero must not substitute for `NULL`.
- Business-key fields are never nullable.
- Conditional fields must be `NULL` when they do not apply.
- A field is not nullable merely because the source sometimes omits it; the contract
  disposition states whether that omission is accepted, warned, quarantined, or
  rejected.

### Units and signs

- Currency code: ISO 4217 uppercase text; the MVP accepts `USD` only.
- Country code: ISO 3166-1 alpha-2 uppercase text.
- Exchange code: ISO 10383 MIC uppercase text.
- Ratios and percentages are stored as decimal ratios: `-0.036` means -3.6%.
- Positive position quantity is long; negative quantity is short.
- Monetary values use the associated currency field and do not embed a currency
  symbol.
- Provider event dates remain business dates; technical event times are UTC.

### Identifier and text conventions

- Project-owned IDs use uppercase `SNAKE_CASE` unless a field explicitly requires a
  UUID.
- Provider symbols are preserved exactly as requested or returned.
- Controlled values use uppercase `SNAKE_CASE`.
- Text is trimmed. Empty strings fail required-field rules.
- Internal IDs remain stable when a display or provider symbol changes.

### Contract version and hashes

- Initial machine-readable contract version: `1.0.0`.
- Contract versions use semantic versioning.
- `record_hash` is a lowercase hexadecimal SHA-256 digest.
- Hash input uses contract field order, UTF-8 text, ISO dates/timestamps, normalized
  decimal strings, explicit null markers, and excludes mutable processing metadata.
- Phase 04 must implement and test the exact canonical serialization before hashes
  are used for deduplication or corrections.

## Dataset inventory

| Dataset | Category | Grain | Business key |
| --- | --- | --- | --- |
| `instruments` | Git-managed reference | One internally identified instrument | `instrument_id` |
| `portfolios` | Git-managed configuration | One portfolio | `portfolio_id` |
| `target_allocations` | Git-managed configuration | One instrument allocation for one portfolio effective on one date | `portfolio_id`, `instrument_id`, `effective_from` |
| `positions` | Derived daily observation | One portfolio's quantity in one instrument at the close of one business date | `portfolio_id`, `instrument_id`, `position_date` |
| `daily_prices` | Sourced observation | One source's end-of-day price observation for one instrument and trading date | `instrument_id`, `price_date`, `source_id` |
| `corporate_actions` | Sourced observation | One sourced corporate action type for one instrument and effective date | `instrument_id`, `effective_date`, `action_type`, `source_id` |
| `trading_calendar` | Versioned reference | One exchange calendar date | `exchange_mic`, `calendar_date` |
| `ingestion_batches` | Operational audit | One attempt to ingest one named dataset from one source for one request window | `batch_id` |
| `data_quality_violations` | Operational audit | One failed or warned rule for one delivered source record | `violation_id` |
| `stress_scenarios` | Git-managed configuration | One stress-scenario definition | `scenario_id` |
| `stress_scenario_shocks` | Git-managed configuration | One instrument shock within one stress scenario | `scenario_id`, `instrument_id` |

## `instruments`

### Purpose

Provides stable internal identity and reviewed classification for the 15-instrument
MVP universe. It prevents provider symbols from becoming permanent business keys.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `instrument_id` | `STRING` | No | Stable project ID; uppercase snake case and unique |
| `display_symbol` | `STRING` | No | Human-facing symbol, such as `BRK.B` |
| `yfinance_symbol` | `STRING` | No | Exact provider mapping, such as `BRK-B`; unique among active instruments |
| `instrument_name` | `STRING` | No | Reviewed legal or commonly recognized security name |
| `asset_class` | `STRING` | No | MVP value `EQUITY` |
| `security_type` | `STRING` | No | Controlled security type, such as `COMMON_STOCK` or `ADR` |
| `exchange_mic` | `STRING` | No | Primary listing exchange MIC used for session expectations |
| `quote_currency` | `STRING` | No | MVP value `USD` |
| `issuer_country_code` | `STRING` | No | Two-letter issuer country code |
| `official_sector_name` | `STRING` | No | Sourced sector classification; separate from custom risk cluster |
| `risk_cluster_id` | `STRING` | No | One approved custom analytical cluster |
| `classification_source` | `STRING` | No | Source used for the official sector and instrument classification |
| `classification_as_of_date` | `DATE` | No | Date on which classification was reviewed |
| `active_from` | `DATE` | No | First date instrument may participate in the MVP universe |
| `active_to` | `DATE` | Yes | Final active date; null while active |
| `is_active` | `BOOLEAN` | No | Whether the instrument is expected in current processing |
| `config_version` | `STRING` | No | Git-managed fixture version |
| `record_hash` | `STRING` | No | SHA-256 digest of stable contract fields |

### Rules

- Exactly 15 instruments are active in contract version `1.0.0`.
- `display_symbol` and `yfinance_symbol` may differ; `instrument_id` does not change
  solely because either symbol changes.
- Active instruments require non-null classification provenance.
- `active_to`, when present, must be on or after `active_from`.
- A source record with an unknown provider symbol is quarantined until a reviewed
  mapping exists.

## `portfolios`

### Purpose

Defines portfolio identity, strategy, capital, currency, and exposure targets without
mixing those attributes into daily positions.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `portfolio_id` | `STRING` | No | Stable project portfolio ID |
| `portfolio_name` | `STRING` | No | Human-readable portfolio name |
| `strategy_code` | `STRING` | No | `LONG_ONLY` or `LONG_SHORT_130_30` |
| `base_currency` | `STRING` | No | MVP value `USD` |
| `target_inception_date` | `DATE` | No | Requested start date, `2016-01-04` |
| `actual_inception_date` | `DATE` | Yes | Earliest verified common session; null until initialization succeeds |
| `initial_nav` | `DECIMAL(18,2)` | No | Initial portfolio NAV; `1000000.00` USD |
| `target_long_ratio` | `DECIMAL(12,10)` | No | Target long exposure divided by initial NAV |
| `target_short_ratio` | `DECIMAL(12,10)` | No | Absolute target short exposure divided by initial NAV; nonnegative |
| `target_gross_ratio` | `DECIMAL(12,10)` | No | Long ratio plus short ratio |
| `target_net_ratio` | `DECIMAL(12,10)` | No | Long ratio minus short ratio |
| `rebalance_policy` | `STRING` | No | MVP value `BUY_AND_HOLD` |
| `cash_policy` | `STRING` | No | MVP value `RETAIN_DIVIDENDS_NO_INTEREST` |
| `is_active` | `BOOLEAN` | No | Whether portfolio is included in current processing |
| `config_version` | `STRING` | No | Git-managed fixture version |
| `record_hash` | `STRING` | No | SHA-256 digest of stable contract fields |

### Rules

- Exactly two portfolios are active: `CORE_15_LONG` and `LONG_SHORT_130_30`.
- `initial_nav` must be positive.
- All target ratios must be nonnegative.
- `target_gross_ratio = target_long_ratio + target_short_ratio`.
- `target_net_ratio = target_long_ratio - target_short_ratio`.
- The long-only portfolio uses ratios `1.0`, `0.0`, `1.0`, and `1.0`.
- The 130/30 portfolio uses ratios `1.3`, `0.3`, `1.6`, and `1.0`.
- Ratio comparisons use absolute tolerance `0.000001`.
- `actual_inception_date` cannot precede `target_inception_date`.

## `target_allocations`

### Purpose

Stores the signed instrument weights used to initialize a portfolio. The effective
date is part of the key so a future approved allocation version does not overwrite
history.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `portfolio_id` | `STRING` | No | References `portfolios.portfolio_id` |
| `instrument_id` | `STRING` | No | References `instruments.instrument_id` |
| `effective_from` | `DATE` | No | First date the target allocation applies |
| `effective_to` | `DATE` | Yes | Final applicable date; null for current configuration |
| `target_weight` | `DECIMAL(12,10)` | No | Signed ratio of initial NAV; negative means short |
| `allocation_version` | `STRING` | No | Project allocation version |
| `record_hash` | `STRING` | No | SHA-256 digest of stable contract fields |

### Rules

- Each active portfolio has exactly 15 allocations for one effective version.
- References must resolve to active portfolio and instrument records.
- `target_weight` cannot be zero.
- Long-only weights cannot be negative.
- Long-only signed and gross weights total `1.000000` within tolerance `0.000001`.
- 130/30 positive weights total `1.300000`, negative weights total `-0.300000`,
  gross weights total `1.600000`, and signed weights total `1.000000` within the
  same tolerance.
- `effective_to`, when present, cannot precede `effective_from`.
- Effective ranges for the same portfolio and instrument cannot overlap.

## `positions`

### Purpose

Provides deterministic daily buy-and-hold position snapshots for valuation and
exposure. It does not represent trades.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `portfolio_id` | `STRING` | No | References `portfolios.portfolio_id` |
| `instrument_id` | `STRING` | No | References `instruments.instrument_id` |
| `position_date` | `DATE` | No | Closing business date represented by the snapshot |
| `quantity` | `DECIMAL(24,8)` | No | Signed split-adjusted quantity; positive long, negative short |
| `position_basis` | `STRING` | No | `INITIAL_ALLOCATION` or `SPLIT_ADJUSTED_CARRY_FORWARD` |
| `allocation_effective_from` | `DATE` | No | Allocation version used to initialize the position |
| `source_record_id` | `STRING` | No | Deterministic ID for this derived snapshot |
| `batch_id` | `STRING` | No | Batch that produced the snapshot |
| `derived_at_utc` | `TIMESTAMP` | No | UTC derivation time |
| `contract_version` | `STRING` | No | Contract used to validate the snapshot |
| `record_hash` | `STRING` | No | SHA-256 digest of stable position fields |

### Rules

- Quantity cannot be zero; zero positions are omitted.
- Long-only quantities must be positive.
- Both signs are allowed for the long-short portfolio according to its allocation.
- Position date must be an expected trading session for the instrument.
- Quantity remains unchanged between sessions unless a documented split applies.
- For a split ratio `r`, post-split quantity equals pre-split quantity multiplied by
  `r` within the fixed-decimal tolerance.
- Every active portfolio requires one valid snapshot for each of its 15 instruments
  before daily portfolio metrics are complete.

## `daily_prices`

### Purpose

Provides sourced, corporate-action-aware end-of-day observations for valuation,
returns, P&L, freshness checks, and historical risk.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `instrument_id` | `STRING` | No | Reviewed internal instrument reference |
| `price_date` | `DATE` | No | Exchange session date represented by the bar |
| `source_id` | `STRING` | No | MVP value `YAHOO_FINANCE` |
| `source_symbol` | `STRING` | No | Exact provider symbol used for retrieval |
| `open_price` | `DECIMAL(20,8)` | No | Raw unadjusted session open in quote currency |
| `high_price` | `DECIMAL(20,8)` | No | Raw unadjusted session high |
| `low_price` | `DECIMAL(20,8)` | No | Raw unadjusted session low |
| `close_price` | `DECIMAL(20,8)` | No | Raw unadjusted official end-of-day close |
| `adjusted_close_price` | `DECIMAL(20,8)` | No | Provider-adjusted close used for return history after reconciliation |
| `volume` | `BIGINT` | Yes | Reported whole-share volume; null is allowed with warning |
| `quote_currency` | `STRING` | No | MVP value `USD` |
| `source_updated_at_utc` | `TIMESTAMP` | Yes | Provider update time only when genuinely supplied |
| `source_record_id` | `STRING` | No | Deterministic ID of the delivered observation |
| `batch_id` | `STRING` | No | References the delivery batch |
| `ingested_at_utc` | `TIMESTAMP` | No | UTC time the project received the record |
| `contract_version` | `STRING` | No | Contract used for validation |
| `record_hash` | `STRING` | No | SHA-256 digest excluding mutable processing metadata |

### Rules

- Key and reference fields are required and instrument mapping must resolve.
- All OHLC and adjusted close values must be greater than zero.
- `high_price` must be at least the maximum of open, low, and close.
- `low_price` must be at most the minimum of open, high, and close.
- Volume cannot be negative. Zero or null volume produces a warning because risk
  metrics do not depend on volume, but the observation remains reviewable.
- `price_date` must be an expected session for the instrument's exchange.
- Quote currency must equal the instrument currency and portfolio-supported `USD`.
- Identical key and hash in a later batch is `UNCHANGED`.
- Identical duplicates in one batch are `DEDUPLICATED` with a warning.
- Conflicting valid candidates for the same key in one batch are quarantined.
- A valid later changed hash is accepted as a correction while history is retained.
- An invalid later correction is rejected and does not replace the prior valid row.

## `corporate_actions`

### Purpose

Stores explicit dividend and split events independently from price bars so quantity,
cash, valuation, and adjusted-return behavior can be reconciled without double
counting.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `instrument_id` | `STRING` | No | Reviewed internal instrument reference |
| `effective_date` | `DATE` | No | Ex-date/effective business date used by the source |
| `action_type` | `STRING` | No | `CASH_DIVIDEND` or `STOCK_SPLIT` |
| `source_id` | `STRING` | No | MVP value `YAHOO_FINANCE` |
| `source_symbol` | `STRING` | No | Exact provider symbol |
| `dividend_amount_per_share` | `DECIMAL(20,8)` | Conditional | Positive cash amount for dividends; otherwise null |
| `dividend_currency` | `STRING` | Conditional | `USD` for dividends; otherwise null |
| `split_ratio` | `DECIMAL(20,10)` | Conditional | New shares per old share; positive and not `1.0`; otherwise null |
| `source_action_id` | `STRING` | Yes | Provider ID only when genuinely supplied |
| `source_updated_at_utc` | `TIMESTAMP` | Yes | Provider update time only when supplied |
| `source_record_id` | `STRING` | No | Deterministic delivered-record ID |
| `batch_id` | `STRING` | No | References the delivery batch |
| `ingested_at_utc` | `TIMESTAMP` | No | UTC receipt time |
| `contract_version` | `STRING` | No | Contract used for validation |
| `record_hash` | `STRING` | No | SHA-256 digest of stable action fields |

### Conditional rules

| Action | Dividend amount | Dividend currency | Split ratio |
| --- | --- | --- | --- |
| `CASH_DIVIDEND` | Required and greater than zero | Required and `USD` | Must be null |
| `STOCK_SPLIT` | Must be null | Must be null | Required, positive, and not `1.0` |

Additional rules:

- Instrument mapping must resolve.
- Effective date must be an expected session unless reviewed source evidence explains
  another convention; otherwise quarantine.
- A 2-for-1 split is represented as `2.0`, not `0.5`.
- Split quantity and raw-price behavior must reconcile without creating artificial
  P&L.
- Long dividends increase portfolio cash; short dividends owed reduce cash.

## `trading_calendar`

### Purpose

Separates expected market closures from missing prices and provides reproducible
session times, early closes, and timezone behavior.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `exchange_mic` | `STRING` | No | Exchange MIC represented by the calendar |
| `calendar_date` | `DATE` | No | Calendar date, including non-trading dates |
| `is_trading_day` | `BOOLEAN` | No | Whether a regular or early-close session exists |
| `market_open_utc` | `TIMESTAMP` | Conditional | Required for trading day; null otherwise |
| `market_close_utc` | `TIMESTAMP` | Conditional | Required for trading day; null otherwise |
| `is_early_close` | `BOOLEAN` | No | True only when close is earlier than standard schedule |
| `holiday_name` | `STRING` | Yes | Reviewed closure name when applicable |
| `exchange_timezone` | `STRING` | No | IANA timezone used to interpret local session rules |
| `calendar_source` | `STRING` | No | Package/project source identifier |
| `calendar_version` | `STRING` | No | Pinned source version |
| `generated_at_utc` | `TIMESTAMP` | No | UTC generation time |
| `record_hash` | `STRING` | No | SHA-256 digest of stable calendar fields |

### Rules

- Trading days require non-null open and close timestamps and close after open.
- Non-trading days require null open and close timestamps and `is_early_close=FALSE`.
- Early close requires `is_trading_day=TRUE`.
- Every active instrument exchange MIC must map to a verified calendar.
- Package upgrades create a new version and require reviewed calendar differences.
- Monday-through-Friday logic is not an acceptable substitute for an exchange
  calendar.

## `ingestion_batches`

### Purpose

Provides one auditable record for each attempt to retrieve or process one dataset and
request window. A retry never overwrites the original attempt.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `batch_id` | `STRING` | No | UUID identifying one attempt |
| `dataset_name` | `STRING` | No | Logical dataset attempted by the batch |
| `source_id` | `STRING` | No | Source or generator identifier |
| `batch_type` | `STRING` | No | `BACKFILL`, `INCREMENTAL`, `RETRY`, or `REPROCESS` |
| `trigger_type` | `STRING` | No | `MANUAL`, `SCHEDULED`, or `RECOVERY` |
| `retry_of_batch_id` | `STRING` | Yes | Prior failed/partial batch when this is a retry |
| `attempt_number` | `BIGINT` | No | Starts at 1 and increments within a retry chain |
| `requested_start_date` | `DATE` | No | Inclusive source request start |
| `requested_end_date` | `DATE` | No | Exclusive source request end |
| `started_at_utc` | `TIMESTAMP` | No | UTC attempt start |
| `completed_at_utc` | `TIMESTAMP` | Yes | UTC completion; null while pending/running |
| `status` | `STRING` | No | `PENDING`, `RUNNING`, `SUCCEEDED`, `SUCCEEDED_WITH_WARNINGS`, `PARTIAL`, or `FAILED` |
| `received_count` | `BIGINT` | No | Delivered source records |
| `accepted_count` | `BIGINT` | No | Records accepted as new/current valid observations |
| `quarantined_count` | `BIGINT` | No | Records awaiting resolvable context |
| `rejected_count` | `BIGINT` | No | Invalid terminal records |
| `unchanged_count` | `BIGINT` | No | Valid overlap records identical to current canonical records |
| `deduplicated_count` | `BIGINT` | No | Redundant same-batch records not processed twice |
| `warning_count` | `BIGINT` | No | Rule warnings; diagnostic and not part of row reconciliation |
| `manifest_sha256` | `STRING` | Yes | Landing-manifest hash once written |
| `error_code` | `STRING` | Yes | Stable failure code for failed/partial attempts |
| `error_message` | `STRING` | Yes | Sanitized diagnostic without credentials |
| `contract_version` | `STRING` | No | Contract used for the batch |

### Rules

- All counts are nonnegative.
- `received_count = accepted_count + quarantined_count + rejected_count +
  unchanged_count + deduplicated_count`.
- `warning_count` is separate because a warned record may also be accepted,
  quarantined, or deduplicated.
- Requested end must be after requested start.
- `completed_at_utc` is null only for `PENDING` or `RUNNING`.
- `SUCCEEDED` requires no quarantined or rejected required records, complete
  expected-symbol coverage, and zero warnings.
- `SUCCEEDED_WITH_WARNINGS` requires complete expected-symbol coverage, zero
  quarantined or rejected records, and at least one warning.
- Missing one or more expected latest prices makes the batch `PARTIAL`.
- `RETRY` requires a valid `retry_of_batch_id` and a higher attempt number.

## `data_quality_violations`

### Purpose

Records one rule result requiring attention for one delivered source record. One
source record may produce multiple violation rows without being counted multiple
times in the batch's received count.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `violation_id` | `STRING` | No | Deterministic unique ID for this batch, record, and rule |
| `batch_id` | `STRING` | No | References `ingestion_batches.batch_id` |
| `dataset_name` | `STRING` | No | Dataset whose record was checked |
| `source_record_id` | `STRING` | No | Delivered record that triggered the rule |
| `rule_id` | `STRING` | No | Stable machine-readable rule identifier |
| `severity` | `STRING` | No | `LOW`, `MEDIUM`, `HIGH`, or `CRITICAL` |
| `disposition` | `STRING` | No | `WARN`, `QUARANTINE`, or `REJECT` |
| `affected_field` | `STRING` | Yes | Specific field, or null for record/cross-record rules |
| `observed_value` | `STRING` | Yes | Sanitized observed representation; may be null |
| `expected_condition` | `STRING` | No | Human-readable valid condition |
| `message` | `STRING` | No | Plain-language reason and impact |
| `detected_at_utc` | `TIMESTAMP` | No | UTC detection time |
| `resolution_status` | `STRING` | No | `OPEN` or `RESOLVED` |
| `resolution_action` | `STRING` | Yes | `ACCEPT_AFTER_FIX`, `REJECT_CONFIRMED`, or `SUPERSEDED` |
| `resolution_batch_id` | `STRING` | Yes | New batch that produced the resolution |
| `resolved_at_utc` | `TIMESTAMP` | Yes | UTC resolution time |
| `resolution_note` | `STRING` | Yes | Sanitized explanation of the resolution |
| `contract_version` | `STRING` | No | Contract used by the rule |

### Rules

- Unique combination: `batch_id`, `source_record_id`, `rule_id`.
- `OPEN` requires all resolution fields to be null.
- `RESOLVED` requires resolution action, batch, timestamp, and note.
- `REJECT` is terminal for the original record.
- `QUARANTINE` may resolve only by correcting reference/configuration context and
  reprocessing the immutable Bronze record in a new linked batch.
- Original violation evidence remains immutable after resolution.
- Silver records are not manually edited to bypass a violation.

## `stress_scenarios`

### Purpose

Defines reviewed hypothetical scenarios independently from their 15 instrument-level
shocks.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `scenario_id` | `STRING` | No | Stable project scenario ID |
| `scenario_name` | `STRING` | No | Human-readable scenario name |
| `scenario_description` | `STRING` | No | What the hypothetical scenario represents |
| `scenario_type` | `STRING` | No | MVP value `DETERMINISTIC_HYPOTHETICAL` |
| `is_active` | `BOOLEAN` | No | Whether scenario must have complete shock coverage |
| `scenario_version` | `STRING` | No | Version of the scenario definition |
| `effective_from` | `DATE` | No | First date definition may be used |
| `effective_to` | `DATE` | Yes | Final date; null while current |
| `record_hash` | `STRING` | No | SHA-256 digest of stable scenario fields |

### Rules

- Initial active IDs are `BROAD_MARKET_DOWN_10`, `TECHNOLOGY_CORRECTION`, and
  `GEOPOLITICAL_SUPPLY_SHOCK`.
- Names and descriptions must call the shocks hypothetical, not forecasts.
- Effective ranges for the same scenario ID cannot overlap.
- Active scenarios require exactly 15 active shock rows.

## `stress_scenario_shocks`

### Purpose

Stores one signed price-return shock for each active instrument in a scenario.

### Fields

| Field | Type | Null | Meaning and validation |
| --- | --- | --- | --- |
| `scenario_id` | `STRING` | No | References `stress_scenarios.scenario_id` |
| `instrument_id` | `STRING` | No | References `instruments.instrument_id` |
| `shock_ratio` | `DECIMAL(12,10)` | No | Signed hypothetical price-return shock |
| `shock_rationale` | `STRING` | No | Plain-language reason for the chosen test input |
| `scenario_version` | `STRING` | No | Must match the parent scenario version |
| `record_hash` | `STRING` | No | SHA-256 digest of stable shock fields |

### Rules

- One row per active scenario and active instrument; no duplicates.
- `shock_ratio` cannot be below `-1.0`.
- A shock above `1.0` is accepted only with a warning and explicit rationale.
- `BROAD_MARKET_DOWN_10` requires `-0.10` for every active instrument.
- Scenario P&L uses current signed market value multiplied by `shock_ratio`.

## Deliberately omitted MVP datasets

### FX rates

An FX-rate dataset is not required because all instrument quote currencies and both
portfolio base currencies are USD. FX must be added with a new reviewed contract
before any non-USD instrument or portfolio is activated. The future contract must
define base/quote direction, rate type, observation time, calendar alignment, and
triangulation policy.

### Trades and rebalances

The portfolios are deterministic buy-and-hold examples. Daily positions are derived
snapshots, not transactions. A trades dataset is required before discretionary
buys, sells, cash flows, or rebalancing are introduced.

### Separate cash input

No external cash-flow source exists in the MVP. Initial financing is defined by the
portfolio and allocation contracts; later cash balance is derived from retained long
dividends and short dividends owed. A separate cash-event contract is required before
deposits, withdrawals, interest, fees, taxes, or other cash events are introduced.

## Stable data-quality rule catalog

| Rule ID | Check | Failure disposition |
| --- | --- | --- |
| `REF_REQUIRED_FIELDS` | Required reference/configuration fields are present and nonempty | Fail Git-managed validation |
| `REF_UNIQUE_BUSINESS_KEY` | Business keys are unique at declared grain | Fail Git-managed validation |
| `REF_VALID_FOREIGN_KEY` | Referenced portfolio, instrument, scenario, or batch exists | Quarantine runtime record; fail Git fixture |
| `ALLOC_PORTFOLIO_TOTALS` | Allocation long, short, gross, and net totals match portfolio targets | Fail |
| `POSITION_NONZERO_QUANTITY` | Position quantity is not zero | Reject |
| `POSITION_STRATEGY_SIGN` | Long-only portfolio contains no negative quantity | Reject |
| `POSITION_DAILY_COVERAGE` | Portfolio has 15 required position snapshots | Partial/fail derived batch |
| `PRICE_REQUIRED_FIELDS` | Required price fields are present | Reject |
| `PRICE_POSITIVE_VALUES` | OHLC and adjusted close are greater than zero | Reject |
| `PRICE_OHLC_CONSISTENCY` | High/low contain open and close | Reject |
| `PRICE_VALID_SESSION` | Price date is an expected exchange session | Reject |
| `PRICE_SUPPORTED_CURRENCY` | Quote currency matches instrument and MVP currency | Quarantine |
| `PRICE_VOLUME_VALIDITY` | Volume is null/zero warning or negative rejection | Warn or reject |
| `PRICE_DUPLICATE_IDENTICAL` | Identical same-batch duplicate exists | Warn and deduplicate |
| `PRICE_DUPLICATE_CONFLICT` | Same-batch key has differing candidate values | Quarantine |
| `PRICE_LATE_ARRIVAL` | Valid record arrives after freshness target | Warn |
| `PRICE_EXPECTED_COVERAGE` | Latest expected session has all 15 active prices | Partial batch |
| `ACTION_TYPE_FIELDS` | Only fields required by the action type are populated | Reject |
| `ACTION_SPLIT_RATIO` | Split ratio is positive and not one | Reject |
| `ACTION_EFFECTIVE_SESSION` | Action date follows verified source/session convention | Quarantine |
| `CALENDAR_CONDITIONAL_TIMES` | Session/closure timestamps follow conditional rules | Fail reference generation |
| `BATCH_COUNT_RECONCILIATION` | Mutually exclusive outcome counts sum to received count | Fail batch |
| `STRESS_ACTIVE_COVERAGE` | Active scenario has exactly 15 unique active shocks | Fail Git-managed validation |
| `STRESS_SHOCK_RANGE` | Shock is not below -100%; gains above 100% are justified | Fail or warn |

## Deterministic acceptance examples

| Example | Expected outcome | Reason |
| --- | --- | --- |
| MSFT raw close `425.50` with valid OHLC on an expected session | Accept | Required values are positive and consistent |
| MSFT raw close `-425.50` | Reject | Prices cannot be negative |
| High `100`, low `90`, open `95`, close `105` | Reject | Close exceeds reported high |
| Valid MSFT price dated Sunday | Reject | Equity price date is not an expected session |
| Provider symbol has no internal instrument mapping | Quarantine | Identity and joins cannot be trusted yet |
| Valid price reports non-USD currency | Quarantine | MVP valuation cannot use unsupported currency |
| Same key and identical hash appears twice in one batch | Warn and deduplicate | One observation must not be processed twice |
| Same key has close `500` and `501` in the same batch | Quarantine both candidates | Arrival order is not a valid tie-breaker within one batch |
| Later batch provides valid corrected close `501` | Accept correction | Later valid source version may supersede current Silver row |
| Later batch provides invalid corrected close `-501` | Reject correction | Prior valid Silver row must remain current |
| `CORE_15_LONG` quantity is `-200` | Reject | Long-only positions cannot be short |
| `LONG_SHORT_130_30` quantity is `-200` for an approved short allocation | Accept | Negative quantity is valid for the long-short strategy |
| Allocation weights total `0.995` | Fail | Portfolio is underallocated beyond tolerance |
| 2-for-1 split has ratio `2.0` and dividend fields null | Accept | Conditional action fields are consistent |
| Split row also contains a dividend amount | Reject | One row cannot mix action-type grains |
| NYSE holiday has `is_trading_day=FALSE` and null open/close | Accept | Closure representation is correct |
| Completed batch has null `completed_at_utc` | Fail | Completed status and timestamp contradict |
| One Bronze record fails three rules | One received record and three violation rows | Violations do not multiply batch input counts |
| Active stress scenario has only 14 shocks | Fail | Complete comparison requires all 15 active instruments |
| Broad -10% shock on the 130/30 portfolio | Expected P&L `-100000` at inception | Long loss `-130000` plus short gain `30000` |

## Analyst and analytics-engineer views

| Analyst view | Analytics-engineer view |
| --- | --- |
| What does this row mean? | Which grain and business key enforce that meaning? |
| Can I trust the latest price? | Did expected-session, freshness, mapping, and correction rules pass? |
| Why did NAV move? | Did raw price, adjusted return, split quantity, dividend cash, and batch lineage reconcile? |
| Is the portfolio concentrated? | Are gross denominators, classifications, and effective dates complete and nonduplicating? |
| What failed? | Which stable rule, disposition, violation row, and retry chain explain the failure? |

Both views depend on the same contract. Clear business meaning without enforceable
rules is fragile; enforceable rules without clear meaning can validate the wrong
thing.

## Implementation handoff requirements

Before Phase 04 uses these contracts:

- Create matching machine-readable YAML contracts with version `1.0.0`.
- Create exact deterministic reference/configuration fixtures.
- Implement tests for required fields, types, null rules, keys, totals, foreign keys,
  conditional fields, stress coverage, and stable rule IDs.
- Prove SHA-256 canonical serialization with fixed examples.
- Validate the 15 provider-symbol mappings and classifications with source evidence.
- Run the full repository quality gate.
- Do not create Bronze tables until the contracts and fixtures pass.
