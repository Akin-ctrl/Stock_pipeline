# System Overview

## What The Project Is

This project is a Nigerian equities market-data and screening pipeline.

Today, it is best understood as:

- a PostgreSQL-backed NGX data pipeline
- a staging and reconciliation workflow
- a technical indicator engine
- an alert and recommendation engine in validation and calibration mode
- a dashboard semantic layer for finance-style reporting
- a trimmed CLI for pipeline runs and aligned read-only inspection

It is not yet a complete investment-grade decision system.

## What The Code Currently Does

Current end-to-end flow:

1. fetch current NGX quote data from Afrimarket
2. load source observations into `staging_daily_prices`
3. record reconciliation decisions in `staging_audit_log`
4. promote reconciled rows into `fact_daily_prices`
5. calculate indicators into `fact_technical_indicators`
6. evaluate alert rules
7. generate recommendations into `fact_recommendations`
8. expose dashboard-ready semantic views

Historical price data can also be backfilled into staging through the dedicated
backfill DAG and script. Historical technical indicators are backfilled
separately from trusted `fact_daily_prices` rows.

## Current Source Coverage

Operational source coverage in the live code:

- current quotes: Afrimarket
- historical backfill: Afrimarket

The current production workflow is effectively single-source, even though the staging design leaves room for broader reconciliation later.

## Current Data Model

Main entities in the live code:

- `dim_sectors`
- `dim_stocks`
- `staging_daily_prices`
- `staging_audit_log`
- `fact_daily_prices`
- `fact_technical_indicators`
- `fact_recommendations`
- `alert_rules`
- `alert_history`

## Current Analytics Layer

Indicators currently stored:

- `ma_7`
- `ma_30`
- `ma_90`
- `rsi_14`
- `macd`
- `macd_signal`
- `macd_histogram`
- `bollinger_upper`
- `bollinger_middle`
- `bollinger_lower`
- `volatility_30`
- `atr_14`
- `ma_crossover_signal`
- `trend_strength`

Recommendation profile currently supported:

- `steady_20p_10d`

## Current Operations Stack

Runtime stack in the repository:

- Python application container
- PostgreSQL 16
- Airflow webserver
- Airflow scheduler
- Airflow init job
- PgAdmin
- Superset

Operational surfaces:

- Airflow for scheduled and manual pipeline execution
- a trimmed CLI for aligned pipeline runs and inspection
- direct database inspection for validation
- Superset dashboards and BI tools through dashboard semantic views

## Current Airflow Workflows

Scheduled DAGs:

- `nigerian_stock_pipeline_v2`
- `weekly_steady_backtest`

Manual or triggered DAGs:

- `backfill_historical_data`
- `daily_steady_snapshot`

The daily stock pipeline is scheduled for `0 17 * * 1-5`, meaning 5:00 PM
Africa/Lagos on weekdays. It starts paused on creation and triggers
`daily_steady_snapshot` after successful completion. The weekly steady backtest
runs Fridays at 8:00 PM Africa/Lagos.

## Current Limitations

The most important current limitations are:

- source concentration around Afrimarket
- official NGX documents and corporate actions are not yet ingested
- full fundamentals are not yet available
- recommendation thresholds still need calibration from refreshed historical outcomes
- long-term investing support is not fundamentally grounded yet
- the CLI is intentionally narrower than older docs implied

## Recommended Interpretation

Today, the project is most useful for:

- collecting and organizing NGX price history
- ranking stocks for manual review
- running alerts and technical screening
- preparing for a better multi-layer market-data architecture

It should not yet be treated as a highly accurate autonomous investing system.
