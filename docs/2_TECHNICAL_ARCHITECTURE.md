# Technical Architecture

## Current Architectural Shape

The current codebase uses a layered structure:

- configuration layer
- SQLAlchemy model layer
- repository layer
- service layer
- pipeline orchestration layer
- Airflow DAG layer

The live implementation is centered on one operational source adapter plus a staging workflow.

## Current Database Model

### Dimension Tables

#### `dim_sectors`

Defined in [dimension.py](../app/models/dimension.py).

Purpose:

- canonical sector master list

Key fields:

- `sector_id`
- `sector_name`
- `description`

#### `dim_stocks`

Defined in [dimension.py](../app/models/dimension.py).

Purpose:

- canonical security master for stocks tracked by the system

Key fields:

- `stock_id`
- `stock_code`
- `company_name`
- `sector_id`
- `exchange`
- `listing_date`
- `delisting_date`
- `is_active`
- `metadata`

### Fact Tables

#### `fact_daily_prices`

Defined in [fact.py](../app/models/fact.py).

Purpose in the current system:

- stores promoted daily close-price records and derived daily/YTD change fields
- acts as the canonical production daily-price table
- carries trust and promotion metadata used by downstream analytics

Current fields:

- `stock_id`
- `price_date`
- `close_price`
- `volume`
- `change_1d_pct`
- `change_ytd_pct`
- `source`
- `source_count`
- `bar_status`
- `is_official`
- `confidence_score`
- `data_quality_flag`
- `has_complete_data`
- `ingestion_timestamp`

Important note:

The redesign now keeps this table as the single production daily-price table
instead of splitting it into duplicated quote/bar tables. The goal is to make
the existing fact table financially safer by improving:

- trust status
- source lineage
- promotion semantics
- downstream filtering for indicators and strategy logic

#### `fact_technical_indicators`

Defined in [fact.py](../app/models/fact.py).

Current stored indicators:

- `ma_7`
- `ma_30`
- `ma_90`
- `rsi_14`
- `macd`
- `macd_signal`
- `macd_histogram`
- `volatility_30`
- `atr_14`
- `bollinger_upper`
- `bollinger_middle`
- `bollinger_lower`
- `ma_crossover_signal`
- `trend_strength`

#### `fact_recommendations`

Defined in [fact.py](../app/models/fact.py).

Purpose:

- stores generated recommendations and outcome tracking

Key fields:

- `action_type`
- `technical_signal_type`
- `signal_agreement`
- `predicted_probability_10d_up`
- `heuristic_score`
- `heuristic_score_category`
- `current_price`
- `policy_target_price`
- `policy_stop_loss`
- `policy_upside_pct`
- `policy_downside_pct`
- `risk_reward_ratio`
- `heuristic_risk_level`
- `reasons`
- `technical_score`
- `momentum_score`
- `volatility_score`
- `trend_score`
- `volume_score`
- `rsi_14`
- `macd`
- `model_version`
- `is_active`
- `outcome`
- `outcome_date`
- `actual_return_pct`

Important semantic split:

- `action_type` is the long-only user-facing action: `STRONG_BUY`, `BUY`,
  `HOLD`, `AVOID`, or `STRONGLY_AVOID`
- `technical_signal_type` preserves the underlying technical signal:
  `STRONG_BUY`, `BUY`, `HOLD`, `SELL`, or `STRONG_SELL`
- `signal_agreement` is heuristic signal agreement, not predictive probability
- `predicted_probability_10d_up` is the model-estimated probability of a
  positive 10-trading-day move when enough history is available

### Alert Tables

Defined in [alert.py](../app/models/alert.py).

- `alert_rules`
- `alert_history`

Current alert rule types in code:

- `PRICE_MOVEMENT`
- `MA_CROSSOVER`
- `VOLATILITY`
- `VOLUME_SPIKE`
- `RSI`
- `MACD`
- `CUSTOM`

### Staging Tables

Defined in [staging.py](../app/models/staging.py).

#### `staging_daily_prices`

Purpose:

- raw-ish staging for source observations before reconciliation and promotion

Current fields:

- `stock_code`
- `source`
- `price_date`
- `close_price`
- `change_1d_pct`
- `change_ytd_pct`
- `volume`
- `loaded_at`
- `reconciled`
- `promoted_at`
- `reconciliation_notes`

Important note:

`staging_daily_prices` remains intentionally source-aware so future multi-source
expansion is possible, but the current normal ingestion path is Afrimarket-only.

#### `staging_audit_log`

Purpose:

- records reconciliation method, source set, selected price, and severity

## Current Pipeline Flow

### Daily Pipeline

Implemented primarily in [orchestrator.py](../app/pipelines/orchestrator.py) and the `nigerian_stock_pipeline_v2` Airflow DAG.

Current daily flow:

1. fetch current prices from Afrimarket
2. update stock master if needed
3. insert fetched rows into `staging_daily_prices`
4. reconcile staging rows
5. pull reconciled rows
6. transform and deduplicate
7. load promoted daily prices into `fact_daily_prices`
8. calculate technical indicators
9. evaluate alerts
10. generate recommendations

### Historical Backfill

Implemented through:

- `scripts/backfill_historical_afrimarket.py`
- `backfill_historical_data` DAG
- `scripts/backfill_historical_indicators.py`

Purpose:

- load longer close-price history into staging
- recompute historical indicators from trusted promoted price facts

The price backfill and indicator backfill are intentionally separate. Price
history is loaded and promoted first; indicators are then upserted from
`fact_daily_prices` so reruns remain idempotent.

## Current Service Layer

### Data Sources

- `AfrimarketDataSource`

Current behavior:

- current fetch includes `stock_code`, `company_name`, `exchange`, `price_date`, `close_price`, `volume`, `change_1d_pct`
- historical fetch returns mainly `price_date` and `close_price`
- no NGX-specific source adapter is part of the current supported runtime path

### Processors

- `DataValidator`
- `DataTransformer`
- `ReconciliationEngine`

### Analytics

- `IndicatorCalculator`
- `SignalGenerator`
- `StockScorer`
- `StockScreener`
- `RecommendationBacktester`
- steady profile: `steady_20p_10d`

The current recommendation model separates:

- long-only action semantics from technical sell/avoid signals
- heuristic score from signal agreement
- signal agreement from predicted 10-day probability
- policy target and stop outputs from model forecasts

Model dataset building and backtesting include an outlier guard by default:

- anchor-day and forward-return model rows above 50 percent absolute return are
  excluded from model datasets
- backtest trade windows above 50 percent absolute gross return are excluded
  from accuracy metrics

This guard is intended to remove split-like or bad-data windows from model
calibration without deleting the underlying price history.

### Dashboard Semantic Layer

The dashboard layer is implemented as database views rather than another
physical daily recommendation table.

Current semantic views include:

- `vw_market_overview`
- `vw_stock_price_panel`
- `vw_recommendation_board`
- `vw_daily_recommendation_board`
- `vw_sector_performance`
- `vw_model_health`
- `vw_backtest_equity_curve`
- `vw_data_quality_monitor`

Raw weekly backtest artifacts remain in:

- `backtest_runs`
- `backtest_trades`
- `recommendation_snapshots`
- `decision_signals`

## Known Architectural Gaps

The biggest current architectural issues are:

- single-source dependence in practice
- official NGX documents, corporate actions, and fundamentals are not ingested yet
- recommendation thresholds still need evidence-based validation
- dashboard presentation still needs product-level design polish
- the orchestrator remains large and should eventually be split into clearer stage services

## Why The Schema Is Being Redesigned

The redesign is not starting from zero.

It was based on the existing model set:

- keep the dimension tables
- keep the staging and audit concept
- keep indicators, alerts, and recommendations
- `fact_daily_prices` has been redesigned in place as the one canonical production price table
- trust and promotion metadata have been added to existing rows
- later add corporate actions and fundamental snapshots

See:

- [Architecture Redesign Proposal](./archive/ARCHITECTURE_REDESIGN_PROPOSAL.md)
