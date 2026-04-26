# System Overview

## What The Project Is

This project is a Nigerian equities market-data and screening pipeline.

Today, it is best understood as:

- a PostgreSQL-backed NGX data pipeline
- a staging and reconciliation workflow
- a technical indicator engine
- an alert and recommendation engine under active redesign

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

Historical data can also be backfilled into staging through the dedicated backfill DAG and script.

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
- `ma_crossover_signal`

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
- Metabase

## Current Airflow Workflows

Scheduled DAG:

- `nigerian_stock_pipeline_v2`

Manual DAG:

- `backfill_historical_data`

The daily DAG is scheduled for `0 14 * * *` and starts paused on creation.

## Current Limitations

The most important current limitations are:

- source concentration around Afrimarket
- `fact_daily_prices` still needs stronger trust metadata for investment use
- technical indicators computed from limited field coverage
- recommendations still driven mainly by technical heuristics
- long-term investing support is not fundamentally grounded yet
- parts of the CLI still reference older repository/model APIs

## Recommended Interpretation

Today, the project is most useful for:

- collecting and organizing NGX price history
- ranking stocks for manual review
- running alerts and technical screening
- preparing for a better multi-layer market-data architecture

It should not yet be treated as a highly accurate autonomous investing system.
