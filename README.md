# Nigerian Stock Pipeline

Daily NGX market-data pipeline with staging, reconciliation, indicators, alerts, recommendations, and historical backfill support.

## Current State

What the codebase currently does:

- fetches current NGX quotes from Afrimarket
- stages fetched records in PostgreSQL
- reconciles staged records through a staging audit workflow
- loads reconciled daily price facts into one production price table
- computes technical indicators from stored close-price history
- evaluates alert rules
- generates profile-based recommendations
- supports historical backfill from Afrimarket history into staging
- supports historical indicator backfill from trusted stored price history
- exposes dashboard-ready semantic views for recommendations, model health,
  backtests, and data quality

What still needs validation or future source expansion:

- broad multi-source reconciliation
- robust fundamentals-driven investing
- evidence-based threshold calibration on refreshed historical data
- official NGX document, corporate-action, and fundamentals ingestion

## Data Reality

The current live source mix is constrained by NGX data availability:

- current quotes: Afrimarket adapter
- historical backfill: Afrimarket history
- official NGX PDFs and filing ingestion: not yet implemented in the live pipeline

Because of that, the project should currently be treated as:

- a market-data and screening platform in validation and dashboard-prep mode
- not a fully validated automated investing engine

## Current Architecture

```text
Afrimarket current quotes
    -> staging_daily_prices
    -> staging_audit_log
    -> fact_daily_prices
    -> fact_technical_indicators
    -> alert_history / fact_recommendations
    -> dashboard semantic views
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
- `atr_14`
- `ma_crossover_signal`
- `trend_strength`

## Current Recommendation Profile

The recommendation engine now uses a single steady profile:

- `steady_20p_10d`

This profile biases the model toward stable 10-day momentum and moderate volatility rather than extreme spikes.

## Known Limitations

The current codebase still has important gaps:

- the pipeline is operationally single-source even though staging supports reconciliation
- historical data quality varies by field
- source trust is constrained until official NGX or paid-source validation is added
- recommendation thresholds still need calibration against refreshed historical outcomes
- current recommendations are screening candidates, not autonomous trade instructions

See:

- [Model Redesign Backlog](./docs/MODEL_REDESIGN_BACKLOG.md)
- [Cleanup Plan](./docs/CLEANUP_PLAN.md)
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

- schedule: `0 17 * * 1-5`
- timezone intent: 5:00 PM Africa/Lagos, Monday-Friday
- follow-up: triggers `daily_steady_snapshot` after successful completion
- `is_paused_upon_creation=True`

### Historical Backfill

Manual trigger example:

```bash
docker compose exec airflow-scheduler airflow dags trigger backfill_historical_data \
  --conf '{"years": 5}'
```

Price backfill loads historical Afrimarket price observations into staging and
then through the main promotion path. Indicator history is backfilled separately
from trusted `fact_daily_prices` rows with:

```bash
python scripts/backfill_historical_indicators.py
```

## Recommended Usage Right Now

Prefer:

- Airflow DAG execution for the main workflow
- historical backfill script/DAG for data accumulation
- historical indicator backfill after large price reloads
- direct inspection of PostgreSQL tables and logs

Use the CLI as a trimmed companion surface for aligned pipeline runs and
read-only inspection. Airflow remains the primary operational control plane.

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

- dashboard semantic views such as `vw_daily_recommendation_board`,
  `vw_model_health`, `vw_backtest_equity_curve`, and `vw_data_quality_monitor`
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

The current next direction is:

1. validate the refreshed historical dataset and model behavior
2. calibrate score and probability thresholds from real backtest results
3. polish dashboard semantic views for finance-style reporting
4. keep documentation aligned with the implemented schema and DAGs
5. bring official NGX documents, corporate actions, and fundamentals into the
   ingestion model when real sources are available
