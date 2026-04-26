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

- `signal_type`
- `confidence_score`
- `overall_score`
- `score_category`
- `current_price`
- `target_price`
- `stop_loss`
- `risk_level`
- `recommendation_reason`
- `technical_score`
- `momentum_score`
- `volatility_score`
- `trend_score`
- `volume_score`
- `outcome`
- `actual_return_pct`

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

`staging_daily_prices` is being generalized in-place so it can support multiple
real sources later instead of being structurally tied to Afrimarket.

Important note:

The current redesign removes single-source enforcement from the staging model,
but the live pipeline still operates with Afrimarket as its only normal source.

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

Purpose:

- load longer close-price history into staging

## Current Service Layer

### Data Sources

- `AfrimarketDataSource`

Current behavior:

- current fetch includes `stock_code`, `company_name`, `exchange`, `price_date`, `close_price`, `volume`, `change_1d_pct`
- historical fetch returns mainly `price_date` and `close_price`

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

## Known Architectural Gaps

The biggest current architectural issues are:

- single-source dependence in practice
- mixed source and canonical responsibilities in `fact_daily_prices`
- recommendations still depend mainly on technical heuristics
- some docs and CLI paths were previously ahead of the implementation

## Why The Schema Is Being Redesigned

The redesign is not starting from zero.

It is based on the existing model set:

- keep the dimension tables
- keep the staging and audit concept
- keep indicators, alerts, and recommendations
- redesign `fact_daily_prices` in place as the one canonical production price table
- add stronger trust and promotion metadata to existing rows
- later add corporate actions and fundamental snapshots

See:

- [Architecture Redesign Proposal](./ARCHITECTURE_REDESIGN_PROPOSAL.md)
- [Schema Transition Map](./SCHEMA_TRANSITION_MAP.md)
