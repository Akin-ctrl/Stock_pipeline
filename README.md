# Nigerian Stock Pipeline

Daily NGX market-data pipeline with staging, reconciliation, indicators, alerts, recommendations, and historical backfill support.

## Current State

This repository is in active refactor.

What the codebase currently does:

- fetches current NGX quotes from Afrimarket
- stages fetched records in PostgreSQL
- reconciles staged records through a staging audit workflow
- loads reconciled daily price facts into one production price table
- computes technical indicators from stored close-price history
- evaluates alert rules
- generates profile-based recommendations
- supports historical backfill from Afrimarket history into staging

What it does not currently do well enough yet:

- broad multi-source reconciliation
- robust fundamentals-driven investing
- investment-grade confidence calibration
- fully trustworthy CLI coverage across all commands

## Data Reality

The current live source mix is constrained by NGX data availability:

- current quotes: Afrimarket adapter
- historical backfill: Afrimarket history
- official NGX PDFs and filing ingestion: not yet implemented in the live pipeline

Because of that, the project should currently be treated as:

- a market-data and screening platform under active redesign
- not a fully validated automated investing engine

## Current Architecture

```text
Afrimarket current quotes
    -> staging_daily_prices
    -> staging_audit_log
    -> fact_daily_prices
    -> fact_technical_indicators
    -> alert_history / fact_recommendations
```

Core tables in the current code:

- `dim_sectors`
- `dim_stocks`
- `staging_daily_prices`
- `staging_audit_log`
- `fact_daily_prices`
- `fact_technical_indicators`
- `fact_recommendations`
- `alert_rules`
- `alert_history`

## Current Indicators

The live indicator model stores:

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

## Current Recommendation Profile

The recommendation engine now uses a single steady profile:

- `steady_20p_10d`

This profile biases the model toward stable 10-day momentum and moderate volatility rather than extreme spikes.

## Known Limitations

The current codebase still has important gaps:

- the pipeline is operationally single-source even though staging supports reconciliation
- `fact_daily_prices` still needs stronger trust/status semantics for investment use
- historical data quality varies by field
- some docs previously described older schemas and indicators
- parts of `app/cli.py` still reference older repository APIs and should be treated as in transition

See:

- [Pipeline Corrections Todo](./docs/PIPELINE_CORRECTIONS_TODO.md)
- [Architecture Redesign Proposal](./docs/ARCHITECTURE_REDESIGN_PROPOSAL.md)
- [Schema Transition Map](./docs/SCHEMA_TRANSITION_MAP.md)

## Quick Start

### Docker

```bash
docker compose up -d --build
```

Current service ports from `docker-compose.yml`:

- Airflow UI: `http://localhost:8080`
- PostgreSQL: `localhost:5433`
- PgAdmin: `http://localhost:5051`
- Metabase: `http://localhost:3000`

### Airflow DAGs

Current DAGs:

- `nigerian_stock_pipeline_v2`
- `backfill_historical_data`
- `weekly_steady_backtest`
- `daily_steady_snapshot`

The daily pipeline DAG is defined with:

- schedule: `0 14 * * *`
- timezone intent: 3:00 PM WAT / 2:00 PM UTC
- `is_paused_upon_creation=True`

### Historical Backfill

Manual trigger example:

```bash
docker compose exec airflow-scheduler airflow dags trigger backfill_historical_data \
  --conf '{"years": 5}'
```

## Recommended Usage Right Now

Prefer:

- Airflow DAG execution for the main workflow
- historical backfill script/DAG for data accumulation
- direct inspection of PostgreSQL tables and logs

Use caution with:

- older CLI commands that may still reference pre-refactor repository methods

## Documentation

- [Docs Index](./docs/README.md)
- [System Overview](./docs/1_SYSTEM_OVERVIEW.md)
- [Technical Architecture](./docs/2_TECHNICAL_ARCHITECTURE.md)
- [Deployment Guide](./docs/3_DEPLOYMENT_GUIDE.md)
- [User Guide](./docs/4_USER_GUIDE.md)

## Weekly Backtest + Dashboard

Run the weekly report once (full run):

```bash
docker compose exec -T airflow-webserver sh -lc "python /Stock_pipeline/scripts/weekly_backtest_report.py"
```

Smoke test (fast, 30 days, subset):

```bash
docker compose exec -T airflow-webserver sh -lc "python /Stock_pipeline/scripts/weekly_backtest_report.py --smoke"
```

Then open Metabase and connect it to the `stock_pipeline` database to visualize:

- `backtest_runs`
- `backtest_trades`
- `recommendation_snapshots`

Metabase seed dashboard (template):

```bash
python scripts/generate_metabase_seed.py
```
- [Architecture Redesign Proposal](./docs/ARCHITECTURE_REDESIGN_PROPOSAL.md)
- [Schema Transition Map](./docs/SCHEMA_TRANSITION_MAP.md)

## Next Direction

The current redesign path is:

1. keep the existing dimension, staging, indicator, alert, and recommendation concepts
2. make `staging_daily_prices` truly source-agnostic
3. make `fact_daily_prices` the single canonical daily-price table with clearer trust metadata
4. tighten indicator and recommendation inputs around trusted production rows
5. separate short-term and long-term strategy outputs
6. bring official NGX documents into the ingestion model when a real source is available
