# User Guide

## What You Should Use This Project For Today

Current best uses:

- collecting NGX price history
- staging and reconciling daily quote records
- running technical indicator calculations
- generating watchlists and alerts for manual review

Current bad uses:

- fully automated real-money trading
- long-term investment decisions based purely on the current recommendation engine
- treating recommendation confidence as true predictive probability

## Main Workflows

### 1. Run The Daily Pipeline

Use Airflow and trigger or enable:

- `nigerian_stock_pipeline_v2`

This runs:

- fetch
- staging
- reconciliation
- price load
- indicators
- alerts
- recommendations

### 2. Backfill Historical Data

Use:

- `backfill_historical_data`

This is the preferred way to accumulate longer close-price history for indicator and backtest work.

### 3. Review Data In PostgreSQL

Current important tables to inspect:

- `staging_daily_prices`
- `staging_audit_log`
- `fact_daily_prices`
- `fact_technical_indicators`
- `alert_history`
- `fact_recommendations`

## Recommendation Profile

The system uses a single steady profile:

- `steady_20p_10d`

This profile biases the model toward stable momentum and lower volatility rather than extreme spikes.

## Notifications

The system supports:

- email
- Slack

Notification behavior depends on environment configuration and alert results.

## Weekly Backtest + Metabase

Weekly backtest results and recommendation snapshots are stored in:

- `backtest_runs`
- `backtest_trades`
- `recommendation_snapshots`
- `daily_recommendation_snapshots`
- `decision_signals`

To populate the tables once:

```bash
docker compose exec -T airflow-webserver sh -lc "python /Stock_pipeline/scripts/weekly_backtest_report.py"
```

Smoke test (fast run):

```bash
docker compose exec -T airflow-webserver sh -lc "python /Stock_pipeline/scripts/weekly_backtest_report.py --smoke"
```

Metabase runs on `http://localhost:3000` and can connect to the `stock_pipeline` database to visualize these tables.

You can generate a starter dashboard template with:

```bash
python scripts/generate_metabase_seed.py
```

## What The Recommendation Engine Currently Looks At

Current recommendation inputs include:

- `rsi_14`
- `macd`
- `macd_signal`
- `ma_7`
- `ma_30`
- `ma_90`
- `volatility_30`
- current price
- recent volume ratio when volume history is available
- daily price change when available

## What It Does Not Yet Reliably Incorporate

- official NGX corporate actions
- full fundamentals
- earnings quality
- balance-sheet metrics
- true multi-source validation

## Current Trust Model

Treat current outputs like this:

- price history: useful, but source-constrained
- indicators: useful for screening, not authoritative by themselves
- alerts: good for monitoring rules
- recommendations: candidates for human review, not trade instructions

## Practical Review Loop

Recommended daily process:

1. run or review the daily DAG
2. check whether staging reconciled cleanly
3. inspect price and indicator coverage
4. review alerts
5. review top recommendations manually
6. validate any trade idea outside the system before execution

## Current User Warning

The project is in the middle of an architecture correction and redesign.

That means the safest way to use it right now is:

- as a market-data and screening assistant
- not as a source of highly accurate investment predictions
