# 🏗️ Technical Architecture

> Deep dive into database schema, OOP design patterns, and implementation details

---

## 📋 Table of Contents
1. [Database Schema](#database-schema)
2. [Application Architecture](#application-architecture)
3. [Data Flow](#data-flow)
4. [Design Patterns](#design-patterns)

---

## 1. Database Schema

### 1.1 Entity Relationship Diagram

```
┌─────────────────┐         ┌──────────────────────┐
│   dim_sectors   │         │     dim_stocks       │
│─────────────────│         │──────────────────────│
│ PK sector_id    │◄────────│ PK stock_id          │
│    sector_name  │         │    stock_code        │
│    description  │         │ FK sector_id         │
└─────────────────┘         │    company_name      │
                            │    exchange          │
                            │    is_active         │
                            └──────────────────────┘
                                       △
                                       │
                     ┌─────────────────┴─────────────────┐
                     │                                   │
          ┌──────────────────────┐          ┌────────────────────────┐
          │ fact_daily_prices    │          │ fact_technical_        │
          │──────────────────────│          │     indicators         │
          │ PK price_id          │          │────────────────────────│
          │ FK stock_id          │          │ PK indicator_id        │
          │    price_date        │          │ FK stock_id            │
          │    close_price       │          │    calculation_date    │
          │    change_1d_pct     │          │    sma_20              │
          │    change_ytd_pct    │          │    sma_50              │
          │    market_cap        │          │    rsi_14              │
          │    source            │          │    macd                │
          │    data_quality_flag │          │    macd_signal         │
          │    has_complete_data │          │    volatility_30       │
          │    created_at        │          │    created_at          │
          └──────────────────────┘          └────────────────────────┘
                     △                                 △
                     │                                 │
                     └─────────────────┬───────────────┘
                                       │
                          ┌────────────────────────┐
                          │   alert_history        │
                          │────────────────────────│
                          │ PK alert_id            │
                          │ FK stock_id            │
                          │    alert_date          │
                          │    alert_type          │
                          │    severity            │
                          │    message             │
                          │    notification_sent   │
                          │    created_at          │
                          └────────────────────────┘
```

### 1.2 Table Definitions (Production Schema)

#### **dim_sectors** (Master Sector Reference)
```sql
CREATE TABLE dim_sectors (
    sector_id SERIAL PRIMARY KEY,
    sector_name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Production Data: 12 NGX Sectors
INSERT INTO dim_sectors (sector_name, description) VALUES
    ('Agriculture', 'Agricultural production and services'),
    ('Construction/Real Estate', 'Construction and real estate'),
    ('Consumer Goods', 'Food, beverages, manufacturing'),
    ('Financial Services', 'Banking, insurance, investment'),
    ('Healthcare', 'Pharmaceuticals, hospitals'),
    ('ICT', 'Telecoms, IT services'),
    ('Industrial Goods', 'Manufacturing, engineering'),
    ('Natural Resources', 'Oil & gas, mining'),
    ('Oil & Gas', 'Energy sector'),
    ('Services', 'Business and consumer services'),
    ('Utilities', 'Power, water, infrastructure'),
    ('Conglomerates', 'Diversified holdings');
```

#### **dim_stocks** (Master Stock Reference)
```sql
CREATE TABLE dim_stocks (
    stock_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(50) UNIQUE NOT NULL,  -- Handles codes like 'SEPL.L', 'GTCO.L'
    company_name VARCHAR(255) NOT NULL,
    sector_id INTEGER REFERENCES dim_sectors(sector_id),
    exchange VARCHAR(10) NOT NULL DEFAULT 'NGX',
    listing_date DATE,
    delisting_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stock_code ON dim_stocks(stock_code);
CREATE INDEX idx_active_stocks ON dim_stocks(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_sector ON dim_stocks(sector_id);
```

**Production Stats:**
- 154 active stocks loaded
- Stock codes: Alphanumeric + dots (SEPL.L, GTCO.L supported)
- All stocks marked `is_active = TRUE`

#### **fact_daily_prices** (Time-Series Price Data)
```sql
CREATE TABLE fact_daily_prices (
    price_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    
    -- NGX-Specific Fields (No OHLCV)
    close_price NUMERIC(18, 4) NOT NULL,
    change_1d_pct NUMERIC(10, 4),          -- Daily % change
    change_ytd_pct NUMERIC(10, 4),         -- Year-to-date %
    market_cap VARCHAR(100),                -- Market capitalization
    
    -- Metadata
    source VARCHAR(50) NOT NULL DEFAULT 'ngx',
    data_quality_flag VARCHAR(20) DEFAULT 'GOOD',
    has_complete_data BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT uq_stock_price_date UNIQUE (stock_id, price_date),
    CONSTRAINT chk_price_positive CHECK (close_price > 0),
    CONSTRAINT chk_data_quality CHECK (
        data_quality_flag IN ('GOOD', 'INCOMPLETE', 'POOR', 'SUSPICIOUS', 'MISSING', 'STALE')
    )
);

CREATE INDEX idx_price_date ON fact_daily_prices(price_date DESC);
CREATE INDEX idx_stock_date ON fact_daily_prices(stock_id, price_date DESC);
CREATE INDEX idx_quality_issues ON fact_daily_prices(data_quality_flag) 
    WHERE data_quality_flag != 'GOOD';
```

**Production Stats:**
- 154 prices loaded per day
- Source: All records have `source='ngx'`
- Quality: 50% GOOD (all fields), 50% INCOMPLETE (missing 1D%)
- Upsert logic: `ON CONFLICT (stock_id, price_date) DO UPDATE`

**Data Quality Logic:**
```python
# GOOD: All 4 NGX fields present
if close_price and change_1d_pct and change_ytd_pct and market_cap:
    flag = 'GOOD'

# INCOMPLETE: Missing 1D% only (common for low-volume stocks)
elif close_price and change_ytd_pct and market_cap:
    flag = 'INCOMPLETE'

# POOR: Missing close price (critical field)
elif not close_price:
    flag = 'POOR'
```

#### **fact_technical_indicators** (Calculated Metrics)
```sql
CREATE TABLE fact_technical_indicators (
    indicator_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    calculation_date DATE NOT NULL,
    
    -- Moving Averages
    sma_20 NUMERIC(18, 4),
    sma_50 NUMERIC(18, 4),
    
    -- Momentum
    rsi_14 NUMERIC(5, 2),              -- 0-100 range
    
    -- Trend Following
    macd NUMERIC(18, 4),
    macd_signal NUMERIC(18, 4),
    macd_histogram NUMERIC(18, 4),
    
    -- Volatility
    volatility_30 NUMERIC(10, 4),      -- 30-day annualized
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_stock_indicator_date UNIQUE (stock_id, calculation_date),
    CONSTRAINT chk_rsi_range CHECK (rsi_14 BETWEEN 0 AND 100)
);

CREATE INDEX idx_indicator_date ON fact_technical_indicators(calculation_date DESC);
CREATE INDEX idx_stock_calc_date ON fact_technical_indicators(stock_id, calculation_date DESC);
```

**Activation Timeline:**
- **Days 1-20**: Table empty (requires 20+ days price history)
- **Day 21+**: Indicators calculated automatically
- **Formula requirements**:
  - SMA20: Needs 20 days of close prices
  - SMA50: Needs 50 days
  - RSI: Needs 14 days
  - MACD: Needs 26 days
  - Volatility: Needs 30 days

#### **alert_history** (Investment Signals)
```sql
CREATE TABLE alert_history (
    alert_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    alert_date DATE NOT NULL,
    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    alert_type VARCHAR(50) NOT NULL,       -- 'PRICE_SPIKE', 'MA_CROSSOVER', etc.
    severity VARCHAR(20) NOT NULL,         -- 'INFO', 'WARNING', 'CRITICAL'
    trigger_value NUMERIC(18, 4),
    message TEXT NOT NULL,
    
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_channels VARCHAR(100),    -- 'email,slack'
    
    CONSTRAINT chk_alert_severity CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL'))
);

CREATE INDEX idx_alert_date ON alert_history(alert_date DESC);
CREATE INDEX idx_stock_alerts ON alert_history(stock_id, alert_date DESC);
```

**Alert Triggers:**
- Price movement > 5% (WARNING)
- Price movement > 10% (CRITICAL)
- Golden Cross: SMA20 > SMA50 (INFO)
- Death Cross: SMA20 < SMA50 (WARNING)
- RSI < 30 or > 70 (INFO)
- Volume spike > 2× average (INFO)

---

## 2. Application Architecture

### 2.1 Layer Overview

```
┌─────────────────────────────────────────────────────────┐
│                    CLI Layer (app/cli.py)               │
│  30+ commands for manual operations                     │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│             Orchestration Layer (app/pipelines/)        │
│  PipelineOrchestrator - 8 ETL stages                    │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│               Service Layer (app/services/)             │
│  ├── data_sources/  (NGX scraper)                       │
│  ├── processors/    (validation, transformation)        │
│  ├── indicators/    (technical analysis)                │
│  ├── alerts/        (evaluation + notifications)        │
│  └── advisory/      (recommendations)                   │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│           Repository Layer (app/repositories/)          │
│  Data access abstraction (5 repositories)               │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│              Model Layer (app/models/)                  │
│  SQLAlchemy ORM (6 tables)                              │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│                   PostgreSQL Database                   │
└─────────────────────────────────────────────────────────┘
```

### 2.2 Core Classes

#### **NGXDataSource** (app/services/data_sources/ngx_source.py)
```python
class NGXDataSource(DataSource):
    """Scrapes NGX stocks from african-markets.com"""
    
    def fetch_stocks(self, date: datetime) -> pd.DataFrame:
        """Returns DataFrame with columns:
        - Company Name
        - Sector
        - Price (₦)
        - 1D (%)
        - YTD (%)
        - Mkt Cap
        - Date
        """
        
    def _clean_price(self, value: str) -> Optional[float]:
        """Removes ₦, NGN, commas: '₦1,234.56' → 1234.56"""
        
    def _clean_percentage(self, value: str) -> Optional[float]:
        """Removes %, +, -, commas: '+1,354.00%' → 1354.00"""
```

**Key Features:**
- Retry logic with exponential backoff
- Comma removal for large values
- Source attribution (`source='ngx'`)
- Error handling for missing fields

#### **DataValidator** (app/services/processors/validator.py)
```python
class DataValidator:
    """Validates data quality and assigns flags"""
    
    def validate_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Returns DataFrame with added columns:
        - data_quality_flag: GOOD | INCOMPLETE | POOR
        - has_complete_data: bool
        """
        
    def _check_ngx_completeness(self, row) -> str:
        """NGX-specific validation:
        - GOOD: All 4 fields (close, 1D%, YTD%, cap)
        - INCOMPLETE: Missing 1D% only
        - POOR: Missing close or critical fields
        """
```

#### **IndicatorCalculator** (app/services/indicators/calculator.py)
```python
class IndicatorCalculator:
    """Calculates technical indicators from price history"""
    
    def calculate_all(self, stock_id: int, end_date: date) -> Dict[str, float]:
        """Requires 50+ days of price history. Returns:
        {
            'sma_20': float,
            'sma_50': float,
            'rsi_14': float,
            'macd': float,
            'macd_signal': float,
            'macd_histogram': float,
            'volatility_30': float
        }
        """
```

**Activation Requirements:**
- SMA20: 20 days minimum
- SMA50: 50 days minimum
- RSI: 14 days minimum
- MACD: 26 days minimum
- Volatility: 30 days minimum

**Implementation**: Vectorized pandas calculations for performance

#### **InvestmentAdvisor** (app/services/advisory/advisor.py)
```python
class InvestmentAdvisor:
    """Generates BUY/SELL/HOLD recommendations"""
    
    def generate_recommendations(
        self,
        recommendation_date: date,
        min_score: float = 40.0,
        min_confidence: float = 0.6
    ) -> List[StockRecommendation]:
        """Returns recommendations with:
        - signal_type: STRONG_BUY | BUY | HOLD | SELL | STRONG_SELL
        - score: 0-100 composite score
        - confidence: 0-1 signal confidence
        - target_price: +10-15% for buys
        - stop_loss: -5-7% protection
        - risk_level: LOW | MEDIUM | HIGH
        - reasoning: Detailed explanation
        """
```

**Scoring Components:**
1. **Technical** (30%): RSI positioning, MACD signals
2. **Momentum** (25%): Price trends vs moving averages
3. **Volatility** (20%): Price stability (lower = better)
4. **Trend** (15%): Golden/Death cross status
5. **Volume** (10%): Volume confirmation *(Future: requires volume data)*

#### **PipelineOrchestrator** (app/pipelines/orchestrator.py)
```python
class PipelineOrchestrator:
    """Main ETL workflow coordinator"""
    
    def run(self, execution_date: date, config: PipelineConfig) -> PipelineResult:
        """8-stage pipeline:
        1. _fetch_data()          # Scrape NGX
        2. _validate_data()       # Quality checks
        3. _transform_data()      # Clean & standardize
        4. _load_stocks()         # Upsert dim_stocks
        5. _load_prices()         # Bulk upsert fact_daily_prices
        6. _calculate_indicators() # Technical analysis (day 21+)
        7. _evaluate_alerts()     # Check alert conditions (day 21+)
        8. _generate_recommendations() # Investment signals (day 21+)
        
        Returns: PipelineResult with metrics
        """
```

**Error Handling:**
- Stage-level try/catch with logging
- Continues on non-critical failures
- Rollback on database errors
- Notification failures don't break pipeline

---

## 3. Data Flow

### 3.1 Daily Pipeline Execution

```
3:00 PM WAT (Airflow Trigger)
         ↓
┌────────────────────────────────────────┐
│  Stage 1: Fetch NGX Data               │
│  - Scrape african-markets.com          │
│  - Extract 154 stocks × 6 fields       │
│  - Save raw CSV to data/raw/           │
│  Duration: ~15 seconds                 │
└────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────┐
│  Stage 2: Validate Data                │
│  - Check required fields               │
│  - Assign quality flags                │
│  - Log validation errors               │
│  Duration: ~2 seconds                  │
└────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────┐
│  Stage 3: Transform Data               │
│  - Clean prices (remove ₦, commas)     │
│  - Clean percentages (handle 1,354%)   │
│  - Standardize stock codes             │
│  - Add source='ngx' field              │
│  Duration: ~3 seconds                  │
└────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────┐
│  Stage 4: Load Stocks                  │
│  - Upsert into dim_stocks              │
│  - Update company names if changed     │
│  - Create new stocks if detected       │
│  Duration: ~2 seconds                  │
└────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────┐
│  Stage 5: Load Prices                  │
│  - Bulk upsert (50/batch)              │
│  - ON CONFLICT DO UPDATE               │
│  - 154 prices in 4 batches             │
│  Duration: ~10 seconds                 │
└────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────┐
│  Stage 6: Calculate Indicators         │
│  - Skip if < 20 days history           │
│  - Compute SMA, RSI, MACD, Volatility  │
│  - Bulk insert indicators              │
│  Duration: ~3 seconds (after day 21)   │
└────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────┐
│  Stage 7: Evaluate Alerts              │
│  - Skip if no indicators               │
│  - Check 8 alert conditions            │
│  - Save to alert_history               │
│  - Send notifications (Email/Slack)    │
│  Duration: ~2 seconds (after day 21)   │
└────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────┐
│  Stage 8: Generate Recommendations     │
│  - Skip if no indicators               │
│  - Score all stocks 0-100              │
│  - Generate BUY/SELL/HOLD signals      │
│  - Calculate target prices             │
│  Duration: ~2 seconds (after day 21)   │
└────────────────────────────────────────┘
         ↓
┌────────────────────────────────────────┐
│  Pipeline Complete                     │
│  Total: ~37 seconds                    │
│  Success: True                         │
│  Errors: 0                             │
└────────────────────────────────────────┘
```

### 3.2 Bulk Upsert Logic

**Problem**: Daily pipeline re-runs can create duplicate (stock_id, price_date) records  
**Solution**: PostgreSQL `ON CONFLICT DO UPDATE` strategy

```python
# app/repositories/price_repository.py
def bulk_insert_prices(self, prices: List[Dict]) -> int:
    """Idempotent bulk upsert with conflict resolution"""
    
    stmt = insert(FactDailyPrice).values(prices)
    
    # Update all fields on conflict (handles retries)
    stmt = stmt.on_conflict_do_update(
        index_elements=['stock_id', 'price_date'],
        set_={
            'close_price': stmt.excluded.close_price,
            'change_1d_pct': stmt.excluded.change_1d_pct,
            'change_ytd_pct': stmt.excluded.change_ytd_pct,
            'market_cap': stmt.excluded.market_cap,
            'source': stmt.excluded.source,
            'data_quality_flag': stmt.excluded.data_quality_flag,
            'has_complete_data': stmt.excluded.has_complete_data
        }
    )
    
    session.execute(stmt)
    session.commit()
```

**Benefits:**
- Pipeline can retry safely
- No duplicate key errors
- Latest data always wins
- Transactional integrity maintained

---

## 4. Design Patterns

### 4.1 Repository Pattern
**Purpose**: Abstract database operations from business logic

```python
class BaseRepository(ABC):
    """Base class for all repositories"""
    
    def __init__(self, db: DatabaseConnection):
        self.db = db
        
    def get_by_id(self, id: int): ...
    def get_all(self): ...
    def create(self, entity): ...
    def update(self, entity): ...
    def delete(self, id: int): ...
```

**Implementation Examples:**
- `StockRepository`: Stock CRUD operations
- `PriceRepository`: Price history, bulk upsert
- `IndicatorRepository`: Indicator calculations, historical queries
- `AlertRepository`: Alert creation, filtering, status updates

### 4.2 Dependency Injection
**Purpose**: Flexible service configuration, easier testing

```python
class PipelineOrchestrator:
    def __init__(
        self,
        db: DatabaseConnection,
        config: PipelineConfig,
        data_source: Optional[DataSource] = None,
        validator: Optional[DataValidator] = None
    ):
        # Inject dependencies (real or mocks for testing)
        self.db = db
        self.config = config
        self.data_source = data_source or NGXDataSource()
        self.validator = validator or DataValidator()
```

### 4.3 Strategy Pattern
**Purpose**: Swappable data sources without code changes

```python
class DataSource(ABC):
    @abstractmethod
    def fetch_stocks(self, date: datetime) -> pd.DataFrame:
        pass

class NGXDataSource(DataSource):
    def fetch_stocks(self, date: datetime) -> pd.DataFrame:
        # NGX-specific implementation
        ...

# Future: Add new sources without modifying orchestrator
# Example: Alternative exchanges, APIs, or data providers
```

### 4.4 Dataclass Configuration
**Purpose**: Type-safe, immutable configuration

```python
@dataclass(frozen=True)
class PipelineConfig:
    fetch_ngx: bool = True
    validate_data: bool = True
    load_stocks: bool = True
    load_prices: bool = True
    calculate_indicators: bool = True
    evaluate_alerts: bool = True
    generate_recommendations: bool = True
    batch_size: int = 50
    max_errors: int = 10
    lookback_days: int = 30
```

**Benefits:**
- Immutable (frozen=True)
- Type hints for IDE support
- Default values
- Easy serialization

---

## 📊 Performance Characteristics

### Database
- **Query response**: < 500ms for typical queries
- **Bulk insert**: 154 prices in ~10 seconds (4 batches)
- **Indicator calculation**: ~3 seconds for all stocks
- **Connection pooling**: 5 connections, reused

### Pipeline
- **Total execution**: ~37 seconds end-to-end
- **Data fetching**: ~15 seconds (network I/O)
- **Processing**: ~22 seconds (CPU-bound)
- **Memory usage**: < 500MB peak

### Scalability
- **Current**: 154 stocks, 154 prices/day
- **Capacity**: 500+ stocks supported
- **Storage**: ~180MB/year (price data only)
- **Horizontal scaling**: Multi-container support ready

---

**Last Updated**: December 31, 2025  
**Version**: 1.0.0
