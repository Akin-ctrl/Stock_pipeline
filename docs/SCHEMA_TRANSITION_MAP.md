# Schema Transition Map

This document maps the current schema to the redesign that stays inside the
existing tables instead of duplicating them.

## Goal

The redesign is in-place.

We keep the current schema shape and make the live tables financially safer and
clearer to use.

## Current To Target Mapping

### Keep As-Is

#### `dim_sectors`

- remains the sector master

#### `dim_stocks`

- remains the stock/security master

#### `fact_technical_indicators`

- remains the indicator storage table
- should eventually calculate only from trusted rows in `fact_daily_prices`

#### `fact_recommendations`

- remains the recommendation output table
- should later separate short-term and long-term strategy lineage more clearly

#### `alert_rules`

- remains the alert configuration table

#### `alert_history`

- remains the alert history table

### Keep, But Redesign In Place

#### `staging_daily_prices`

Current role:

- load source observations before reconciliation

Target role:

- remain the single staging table for all incoming market-price observations

Changes:

- remove single-source enforcement
- preserve better reconciliation lineage
- keep raw source observations separate from promoted production facts

#### `staging_audit_log`

Current role:

- reconciliation audit trail

Target role:

- remain the reconciliation decision log
- become the main explanation layer for how production prices were chosen

#### `fact_daily_prices`

Current role:

- promoted daily close records
- derived change fields
- source reference
- strategy input base

Target role:

- remain the single canonical production daily-price table

Changes:

- keep one row per stock/date
- make `source` the selected production source
- add trust/status metadata directly on the row
- use `bar_status`, `source_count`, `is_official`, and `confidence_score`
- retain `data_quality_flag` and `has_complete_data`

## Explicit Non-Goal

The redesign does not create duplicate production price tables such as:

- `market_quote_snapshots`
- `market_daily_bars`

Those concepts overlap too heavily with the current `staging_daily_prices` and
`fact_daily_prices` flow for the project's present needs.

## Summary Table

| Current Table | Decision | Target |
|---|---|---|
| `dim_sectors` | Keep | `dim_sectors` |
| `dim_stocks` | Keep | `dim_stocks` |
| `staging_daily_prices` | Keep and generalize | `staging_daily_prices` |
| `staging_audit_log` | Keep and strengthen | `staging_audit_log` |
| `fact_daily_prices` | Keep and redesign in place | `fact_daily_prices` |
| `fact_technical_indicators` | Keep | `fact_technical_indicators` |
| `fact_recommendations` | Keep and enrich | `fact_recommendations` |
| `alert_rules` | Keep | `alert_rules` |
| `alert_history` | Keep | `alert_history` |

## Migration Priority

1. make `staging_daily_prices` source-agnostic
2. enrich `fact_daily_prices` with trust and promotion metadata
3. filter indicators and strategy logic by trusted production rows
4. separate short-term and long-term recommendation behavior more explicitly
5. add fundamentals and official NGX document ingestion only when a real source is available
