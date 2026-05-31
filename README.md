# Nigerian Stock Pipeline

An end-to-end market data and screening platform for Nigerian Exchange (NGX) equities. Built on a dimensional data model with a full staging and reconciliation layer, a technical indicator engine, an alert and recommendation system with backtesting, and a semantic dashboard view layer. Orchestrated with Apache Airflow and visualised through Metabase.

---

## What This System Does

The pipeline runs automatically every weekday at 5:00 PM Africa/Lagos time. From a single trigger it:

1. Fetches current NGX quote data from Afrimarket
2. Loads source observations into a staging layer with full audit tracking
3. Reconciles staged records and promotes trusted rows to production fact tables
4. Computes 14 technical indicators across the full price history
5. Evaluates configurable alert rules across the stock universe
6. Generates profile-based screening recommendations with risk/reward metadata
7. Refreshes dashboard-ready semantic views for BI reporting

Historical price data can be backfilled via a dedicated DAG. Technical indicators are backfilled separately from trusted production price rows, keeping reruns idempotent.

---

## Architecture

```
Afrimarket (current + historical)
        │
        ▼
staging_daily_prices  ──►  staging_audit_log
        │
        ▼ (reconciliation + promotion)
fact_daily_prices
        │
        ├──►  fact_technical_indicators
        │
        ├──►  alert_rules / alert_history
        │
        └──►  fact_recommendations
                    │
                    ▼
          Dashboard semantic views
          (Metabase / any BI tool)
```

The application layer is structured in six explicit layers: configuration, SQLAlchemy models, repository, service, pipeline orchestration, and Airflow DAGs. The staging design is built for future multi-source reconciliation even though the current ingestion path is single-source.

---

## Data Model

### Dimension Tables
| Table | Purpose |
|---|---|
| `dim_sectors` | Canonical sector master list |
| `dim_stocks` | Security master — all NGX-tracked stocks with listing/delisting metadata |

### Staging Tables
| Table | Purpose |
|---|---|
| `staging_daily_prices` | Source-aware raw observations before reconciliation |
| `staging_audit_log` | Reconciliation decisions, source sets, severity records |

### Fact Tables
| Table | Purpose |
|---|---|
| `fact_daily_prices` | Canonical production daily prices with trust, lineage, and promotion metadata |
| `fact_technical_indicators` | 14 computed indicators per stock per day |
| `fact_recommendations` | Scored recommendations with outcome tracking |
| `alert_rules` / `alert_history` | Configurable alert definitions and trigger history |

### Semantic View Layer

Dashboard reporting is implemented as database views rather than additional physical tables. Current views:

- `vw_market_overview`
- `vw_stock_price_panel`
- `vw_recommendation_board`
- `vw_daily_recommendation_board`
- `vw_sector_performance`
- `vw_model_health`
- `vw_backtest_equity_curve`
- `vw_data_quality_monitor`

Raw backtest artifacts are stored in `backtest_runs`, `backtest_trades`, `recommendation_snapshots`, and `decision_signals`.

---

## Technical Indicators

All 14 indicators are computed from trusted `fact_daily_prices` rows and stored per stock per trading day:

| Indicator | Description |
|---|---|
| `ma_7`, `ma_30`, `ma_90` | Simple moving averages |
| `rsi_14` | Relative Strength Index |
| `macd`, `macd_signal`, `macd_histogram` | MACD components |
| `bollinger_upper`, `bollinger_middle`, `bollinger_lower` | Bollinger Bands |
| `volatility_30` | 30-day rolling volatility |
| `atr_14` | Average True Range |
| `ma_crossover_signal` | MA crossover detection |
| `trend_strength` | Composite trend score |

---

## Recommendation Engine

The recommendation engine separates concerns clearly:

- **`action_type`** — long-only user-facing action: `STRONG_BUY`, `BUY`, `HOLD`, `AVOID`, `STRONGLY_AVOID`
- **`technical_signal_type`** — underlying technical signal: `STRONG_BUY`, `BUY`, `HOLD`, `SELL`, `STRONG_SELL`
- **`signal_agreement`** — heuristic signal agreement, not predictive probability
- **`predicted_probability_10d_up`** — model-estimated probability of a positive 10-trading-day move
- **`heuristic_score`** — composite score across technical, momentum, volatility, trend, and volume sub-scores
- **`risk_reward_ratio`**, **`policy_target_price`**, **`policy_stop_loss`** — policy-level position parameters

An outlier guard excludes model dataset rows and backtest trade windows above 50% absolute return to prevent split-like or bad-data windows from distorting calibration.

Current active profile: `steady_20p_10d` — biased toward stable 10-day momentum with moderate volatility tolerance.

---

## Alert Types

Supported alert rule types: `PRICE_MOVEMENT`, `MA_CROSSOVER`, `VOLATILITY`, `VOLUME_SPIKE`, `RSI`, `MACD`, `CUSTOM`

---

## Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Database | PostgreSQL 16 |
| Application | Python (SQLAlchemy, layered service architecture) |
| Dashboard | Metabase |
| DB Admin | PgAdmin |
| Containerisation | Docker Compose |

---

## Quick Start

### Prerequisites
- Docker and Docker Compose
- A `.env` file configured from `.env.example`

### Start the stack

```bash
docker compose up -d --build
docker compose ps
```

### Service ports

| Service | Port |
|---|---|
| Airflow UI | `http://localhost:8080` |
| Metabase | `http://localhost:3000` |
| PgAdmin | `http://localhost:5051` |
| PostgreSQL | `localhost:5433` |

### Validate deployment

Once the stack is up, confirm the following before running the pipeline:

1. PostgreSQL is healthy
2. Airflow webserver and scheduler are both running
3. All four DAGs are visible in the Airflow UI
4. Dashboard semantic views exist after migrations apply

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `nigerian_stock_pipeline_v2` | `0 17 * * 1-5` (5 PM WAT, weekdays) | Main daily pipeline — fetch, stage, reconcile, indicators, alerts, recommendations |
| `daily_steady_snapshot` | Triggered by main DAG | Verifies recommendations are dashboard-ready after each run |
| `weekly_steady_backtest` | `0 20 * * 5` (8 PM WAT, Fridays) | Refreshes weekly backtest results and snapshot artifacts |
| `backfill_historical_data` | Manual trigger only | Loads historical price data into staging |

The main DAG starts paused on creation. Unpause it in the Airflow UI when ready to go live.

---

## Historical Backfill

Trigger a 5-year price backfill:

```bash
docker compose exec airflow-scheduler airflow dags trigger backfill_historical_data \
  --conf '{"years": 5}'
```

Backfill technical indicators after a large price reload:

```bash
python scripts/backfill_historical_indicators.py
```

Price history and indicator history are backfilled separately by design. This keeps reruns safe and idempotent.

---

## Weekly Backtest

Full run:

```bash
docker compose exec -T airflow-webserver sh -lc \
  "python /Stock_pipeline/scripts/weekly_backtest_report.py"
```

Smoke test (30 days, subset):

```bash
docker compose exec -T airflow-webserver sh -lc \
  "python /Stock_pipeline/scripts/weekly_backtest_report.py --smoke"
```

Generate Metabase seed dashboard:

```bash
python scripts/generate_metabase_seed.py
```

---

## Environment Variables

The settings loader expects these variables in `.env`:

```
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

Email and Slack notification variables are also supported. See `.env.example` for the full reference.

---

## Common Commands

```bash
# Logs
docker compose logs -f airflow-scheduler
docker compose logs -f app

# Enter containers
docker compose exec app bash
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"

# Restart services
docker compose restart airflow-scheduler
docker compose restart app
```

---

## Documentation

| Document | Contents |
|---|---|
| [System Overview](docs/1_SYSTEM_OVERVIEW.md) | What the system does, data model, current scope |
| [Technical Architecture](docs/2_TECHNICAL_ARCHITECTURE.md) | Layer breakdown, schema detail, service classes |
| [Deployment Guide](docs/3_DEPLOYMENT_GUIDE.md) | Docker setup, DAG reference, operational commands |
| [User Guide](docs/4_USER_GUIDE.md) | How to use the pipeline, dashboard views, CLI |
| [Architecture Redesign Proposal](docs/ARCHITECTURE_REDESIGN_PROPOSAL.md) | Multi-source expansion roadmap |
| [Schema Transition Map](docs/SCHEMA_TRANSITION_MAP.md) | Schema evolution and migration reference |
| [Model Redesign Backlog](docs/MODEL_REDESIGN_BACKLOG.md) | Recommendation model improvement backlog |

---

## Roadmap

- Multi-source reconciliation (official NGX data, paid feeds)
- Corporate actions and fundamentals ingestion
- Evidence-based recommendation threshold calibration
- Dashboard presentation layer polish
- Orchestrator refactor into cleaner stage services

---

## Scope Note

The current system is a **market-data and screening platform**, not a fully validated automated investing engine. Recommendations are screening candidates for manual review. Validate outputs at the table, view, log, and backtest-result level before drawing conclusions from dashboard displays.
