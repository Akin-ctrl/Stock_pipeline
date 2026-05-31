# Schema Transition Map

Status: Superseded by the current schema implementation as of 2026-05-31.

This document originally mapped the current schema to a redesign that stayed
inside the existing tables instead of duplicating them. Most of the core
transition has now been implemented. Keep this file as a historical transition
record unless the remaining future items are moved into a current backlog.

## Original Goal

The redesign is in-place.

We keep the current schema shape and make the live tables financially safer and
clearer to use.

## Implemented Schema Decisions

### Keep As-Is

#### `dim_sectors`

- remains the sector master

#### `dim_stocks`

- remains the stock/security master

#### `fact_technical_indicators`

- remains the indicator storage table
- historical indicator backfill now calculates from trusted rows in
  `fact_daily_prices`

#### `fact_recommendations`

- remains the recommendation output table
- now stores model-aligned recommendation fields such as `action_type`,
  `technical_signal_type`, `signal_agreement`,
  `predicted_probability_10d_up`, `heuristic_score`, policy outputs, and
  outcome fields
- short-term versus long-term strategy lineage remains future work

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

Implemented direction:

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

Implemented direction:

- keep one row per stock/date
- make `source` the selected production source
- add trust/status metadata directly on the row
- use `bar_status`, `source_count`, `is_official`, and `confidence_score`
- retain `data_quality_flag` and `has_complete_data`

### Dashboard Semantic Layer

The current dashboard-facing redesign uses semantic views instead of another
physical daily recommendation table.

Current views include:

- `vw_market_overview`
- `vw_stock_price_panel`
- `vw_recommendation_board`
- `vw_daily_recommendation_board`
- `vw_sector_performance`
- `vw_model_health`
- `vw_backtest_equity_curve`
- `vw_data_quality_monitor`

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

## Remaining Future Work

1. continue strengthening source-agnostic staging as additional real sources are added
2. continue filtering indicators and strategy logic by trusted production rows
3. separate short-term and long-term recommendation behavior more explicitly
4. add fundamentals and official NGX document ingestion only when a real source is available
5. add corporate actions when a reliable source and adjustment policy exist
