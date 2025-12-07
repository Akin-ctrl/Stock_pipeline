# 🏗️ Stock Pipeline - Enterprise Architecture Design

## 📋 Table of Contents
1. [System Overview](#system-overview)
2. [Database Schema Design](#database-schema-design)
3. [Application Architecture](#application-architecture)
4. [Design Patterns](#design-patterns)
5. [Data Flow](#data-flow)
6. [Investment Advisory Features](#investment-advisory-features)

---

## 1. System Overview

### Business Purpose
**Production-ready investment advisory system** for Nigerian Stock Exchange (NGX) with:
- Daily automated data collection (156 NGX stocks + 2 LSE Nigerian stocks)
- Technical indicator calculation (MA, RSI, Volatility)
- Smart alert system for investment opportunities
- Portfolio analysis and recommendations
- Historical data accumulation for backtesting

### Key Stakeholders
- **Investor (You)**: Primary user seeking actionable investment insights
- **Data Engineer**: Maintains pipeline reliability
- **System**: Automated advisory that runs 24/7

### Non-Functional Requirements
- **Reliability**: 99.5% uptime, automated error recovery
- **Performance**: Query response < 500ms, process 156 stocks in < 2 min
- **Scalability**: Support 500+ stocks, 5+ years historical data
- **Maintainability**: Clean OOP, 80%+ test coverage, comprehensive logging
- **Security**: Environment-based configs, no hardcoded secrets

---

## 2. Database Schema Design

### 2.1 Entity Relationship Overview

```
┌─────────────────┐         ┌──────────────────────┐
│   dim_sectors   │         │     dim_stocks       │
│─────────────────│         │──────────────────────│
│ PK sector_id    │◄────────│ PK stock_id          │
│    sector_name  │         │    stock_code        │
│    description  │         │ FK sector_id         │
└─────────────────┘         │    company_name      │
                            │    exchange          │
                            │    listing_date      │
                            │    is_active         │
                            │    created_at        │
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
          │    open_price        │          │    calculation_date    │
          │    high_price        │          │    ma_7                │
          │    low_price         │          │    ma_30               │
          │    close_price       │          │    rsi_14              │
          │    volume            │          │    volatility_30       │
          │    change_1d_pct     │          │    bollinger_upper     │
          │    change_ytd_pct    │          │    bollinger_lower     │
          │    market_cap        │          │    created_at          │
          │    source            │          └────────────────────────┘
          │    data_quality_flag │                     △
          │    created_at        │                     │
          └──────────────────────┘                     │
                     △                                 │
                     └─────────────────┬───────────────┘
                                       │
                          ┌────────────────────────┐
                          │   alert_history        │
                          │────────────────────────│
                          │ PK alert_id            │
                          │ FK stock_id            │
                          │ FK rule_id             │
                          │    alert_date          │
                          │    alert_type          │
                          │    severity            │
                          │    trigger_value       │
                          │    message             │
                          │    is_resolved         │
                          │    created_at          │
                          └────────────────────────┘
                                       △
                                       │
                          ┌────────────────────────┐
                          │   alert_rules          │
                          │────────────────────────│
                          │ PK rule_id             │
                          │    rule_name           │
                          │    rule_type           │
                          │    condition_sql       │
                          │    threshold_value     │
                          │    severity            │
                          │    is_active           │
                          │    description         │
                          └────────────────────────┘
```

### 2.2 Detailed Table Definitions

#### **dim_sectors** (Dimension Table - Sector Master)
```sql
CREATE TABLE dim_sectors (
    sector_id SERIAL PRIMARY KEY,
    sector_name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sector_name ON dim_sectors(sector_name);

-- Reference Data
INSERT INTO dim_sectors (sector_name, description) VALUES
    ('Financials', 'Banks, Insurance, Mortgage, Asset Management'),
    ('Consumer Goods', 'Food, Beverages, Manufacturing'),
    ('Consumer Services', 'Transport, Hospitality, Media'),
    ('Technology', 'IT Services, Software, Telecoms'),
    ('Basic Materials', 'Chemicals, Construction Materials'),
    ('Industrials', 'Manufacturing, Engineering, Construction'),
    ('Oil & Gas', 'Exploration, Production, Distribution'),
    ('Healthcare', 'Pharmaceuticals, Hospitals, Equipment'),
    ('Utilities', 'Power, Water, Infrastructure');
```

#### **dim_stocks** (Dimension Table - Stock Master)
```sql
CREATE TABLE dim_stocks (
    stock_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(20) UNIQUE NOT NULL,  -- e.g., 'AIRTELAFRI', 'DANGCEM'
    company_name VARCHAR(255) NOT NULL,
    sector_id INTEGER REFERENCES dim_sectors(sector_id),
    exchange VARCHAR(10) NOT NULL,  -- 'NGX', 'LSE'
    listing_date DATE,
    delisting_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB,  -- Additional info: website, CEO, market_cap_category
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_exchange CHECK (exchange IN ('NGX', 'LSE'))
);

CREATE INDEX idx_stock_code ON dim_stocks(stock_code);
CREATE INDEX idx_exchange ON dim_stocks(exchange);
CREATE INDEX idx_sector ON dim_stocks(sector_id);
CREATE INDEX idx_active_stocks ON dim_stocks(is_active) WHERE is_active = TRUE;
```

#### **fact_daily_prices** (Fact Table - Time Series Prices)
```sql
CREATE TABLE fact_daily_prices (
    price_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    
    -- OHLCV Data
    open_price NUMERIC(18, 4),
    high_price NUMERIC(18, 4),
    low_price NUMERIC(18, 4),
    close_price NUMERIC(18, 4) NOT NULL,
    volume BIGINT,
    
    -- Calculated Fields
    change_1d_pct NUMERIC(10, 4),  -- Daily percentage change
    change_ytd_pct NUMERIC(10, 4),  -- Year-to-date change
    market_cap VARCHAR(50),
    
    -- Metadata
    source VARCHAR(50) NOT NULL,  -- 'african-markets.com', 'yahoo_finance'
    data_quality_flag VARCHAR(20) DEFAULT 'GOOD',  -- 'GOOD', 'SUSPICIOUS', 'MISSING'
    has_complete_data BOOLEAN DEFAULT TRUE,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite unique constraint (one price per stock per day)
    CONSTRAINT uq_stock_price_date UNIQUE (stock_id, price_date),
    CONSTRAINT chk_price_positive CHECK (close_price > 0),
    CONSTRAINT chk_data_quality CHECK (data_quality_flag IN ('GOOD', 'SUSPICIOUS', 'MISSING', 'STALE'))
);

-- Performance indexes for common queries
CREATE INDEX idx_price_date ON fact_daily_prices(price_date DESC);
CREATE INDEX idx_stock_date ON fact_daily_prices(stock_id, price_date DESC);
CREATE INDEX idx_recent_prices ON fact_daily_prices(price_date DESC) WHERE price_date >= CURRENT_DATE - INTERVAL '90 days';
CREATE INDEX idx_quality ON fact_daily_prices(data_quality_flag) WHERE data_quality_flag != 'GOOD';
```

#### **fact_technical_indicators** (Fact Table - Computed Metrics)
```sql
CREATE TABLE fact_technical_indicators (
    indicator_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    calculation_date DATE NOT NULL,
    
    -- Moving Averages
    ma_7 NUMERIC(18, 4),
    ma_30 NUMERIC(18, 4),
    ma_90 NUMERIC(18, 4),
    
    -- Momentum Indicators
    rsi_14 NUMERIC(5, 2),  -- Relative Strength Index (0-100)
    macd NUMERIC(18, 4),   -- MACD line
    macd_signal NUMERIC(18, 4),
    macd_histogram NUMERIC(18, 4),
    
    -- Volatility
    volatility_30 NUMERIC(10, 4),  -- 30-day rolling standard deviation
    atr_14 NUMERIC(18, 4),  -- Average True Range
    
    -- Bollinger Bands
    bollinger_upper NUMERIC(18, 4),
    bollinger_middle NUMERIC(18, 4),
    bollinger_lower NUMERIC(18, 4),
    
    -- Trading Signals
    ma_crossover_signal VARCHAR(10),  -- 'BULLISH', 'BEARISH', 'NEUTRAL'
    trend_strength NUMERIC(5, 2),  -- 0-100 scale
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_stock_indicator_date UNIQUE (stock_id, calculation_date),
    CONSTRAINT chk_rsi_range CHECK (rsi_14 BETWEEN 0 AND 100),
    CONSTRAINT chk_trend_range CHECK (trend_strength BETWEEN 0 AND 100)
);

CREATE INDEX idx_indicator_date ON fact_technical_indicators(calculation_date DESC);
CREATE INDEX idx_stock_calc_date ON fact_technical_indicators(stock_id, calculation_date DESC);
CREATE INDEX idx_ma_crossover ON fact_technical_indicators(ma_crossover_signal) WHERE ma_crossover_signal != 'NEUTRAL';
```

#### **alert_rules** (Configuration Table)
```sql
CREATE TABLE alert_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) UNIQUE NOT NULL,
    rule_type VARCHAR(50) NOT NULL,  -- 'PRICE_MOVEMENT', 'MA_CROSSOVER', 'VOLATILITY', 'VOLUME_SPIKE'
    condition_sql TEXT,  -- SQL expression for evaluation
    threshold_value NUMERIC(10, 4),
    severity VARCHAR(20) DEFAULT 'INFO',  -- 'INFO', 'WARNING', 'CRITICAL'
    is_active BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_severity CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL'))
);

-- Pre-defined Investment Rules
INSERT INTO alert_rules (rule_name, rule_type, threshold_value, severity, description) VALUES
    ('Daily_Change_Significant', 'PRICE_MOVEMENT', 4.0, 'WARNING', 'Daily price change exceeds ±4%'),
    ('Daily_Change_Extreme', 'PRICE_MOVEMENT', 8.0, 'CRITICAL', 'Daily price change exceeds ±8%'),
    ('MA_Bullish_Crossover', 'MA_CROSSOVER', 0, 'INFO', '7-day MA crosses above 30-day MA'),
    ('MA_Bearish_Crossover', 'MA_CROSSOVER', 0, 'WARNING', '7-day MA crosses below 30-day MA'),
    ('Volatility_Spike', 'VOLATILITY', 2.0, 'WARNING', 'Volatility exceeds 2x 30-day average'),
    ('Volume_Surge', 'VOLUME_SPIKE', 2.5, 'INFO', 'Volume exceeds 2.5x average'),
    ('RSI_Oversold', 'RSI', 30, 'INFO', 'RSI below 30 (potential buy signal)'),
    ('RSI_Overbought', 'RSI', 70, 'WARNING', 'RSI above 70 (potential sell signal)');
```

#### **alert_history** (Fact Table - Investment Alerts)
```sql
CREATE TABLE alert_history (
    alert_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    rule_id INTEGER NOT NULL REFERENCES alert_rules(rule_id),
    alert_date DATE NOT NULL,
    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    trigger_value NUMERIC(18, 4),
    message TEXT NOT NULL,
    
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolution_notes TEXT,
    
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_channels VARCHAR(100),  -- 'email,slack,sms'
    
    CONSTRAINT chk_alert_severity CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL'))
);

CREATE INDEX idx_alert_date ON alert_history(alert_date DESC);
CREATE INDEX idx_stock_alerts ON alert_history(stock_id, alert_date DESC);
CREATE INDEX idx_unresolved ON alert_history(is_resolved, severity) WHERE is_resolved = FALSE;
CREATE INDEX idx_recent_alerts ON alert_history(alert_timestamp DESC) WHERE alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days';
```

### 2.3 Analytical Views

#### **vw_latest_stock_prices** (Current Market View)
```sql
CREATE OR REPLACE VIEW vw_latest_stock_prices AS
SELECT 
    s.stock_id,
    s.stock_code,
    s.company_name,
    sec.sector_name,
    s.exchange,
    p.price_date,
    p.close_price,
    p.change_1d_pct,
    p.change_ytd_pct,
    p.volume,
    p.market_cap,
    p.data_quality_flag
FROM dim_stocks s
JOIN dim_sectors sec ON s.sector_id = sec.sector_id
JOIN LATERAL (
    SELECT *
    FROM fact_daily_prices
    WHERE stock_id = s.stock_id
    ORDER BY price_date DESC
    LIMIT 1
) p ON TRUE
WHERE s.is_active = TRUE
ORDER BY s.stock_code;
```

#### **vw_investment_dashboard** (Comprehensive View with Indicators)
```sql
CREATE OR REPLACE VIEW vw_investment_dashboard AS
SELECT 
    s.stock_code,
    s.company_name,
    sec.sector_name,
    p.price_date,
    p.close_price,
    p.change_1d_pct,
    p.change_ytd_pct,
    i.ma_7,
    i.ma_30,
    i.rsi_14,
    i.volatility_30,
    i.ma_crossover_signal,
    i.trend_strength,
    -- Alert summary
    (SELECT COUNT(*) 
     FROM alert_history ah 
     WHERE ah.stock_id = s.stock_id 
       AND ah.alert_date = p.price_date
       AND ah.is_resolved = FALSE
    ) AS active_alerts_count,
    (SELECT STRING_AGG(ah.severity || ': ' || ah.message, '; ')
     FROM alert_history ah
     WHERE ah.stock_id = s.stock_id
       AND ah.alert_date = p.price_date
       AND ah.is_resolved = FALSE
    ) AS alert_messages
FROM dim_stocks s
JOIN dim_sectors sec ON s.sector_id = sec.sector_id
JOIN LATERAL (
    SELECT * FROM fact_daily_prices
    WHERE stock_id = s.stock_id
    ORDER BY price_date DESC LIMIT 1
) p ON TRUE
LEFT JOIN LATERAL (
    SELECT * FROM fact_technical_indicators
    WHERE stock_id = s.stock_id
    ORDER BY calculation_date DESC LIMIT 1
) i ON TRUE
WHERE s.is_active = TRUE;
```

---

## 3. Application Architecture

### 3.1 Project Structure

```
Stock_pipeline/
├── app/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── settings.py          # Environment configs
│   │   └── database.py          # DB connection management
│   ├── models/
│   │   ├── __init__.py
│   │   ├── base.py              # SQLAlchemy declarative base
│   │   ├── dimension.py         # Dimension table models
│   │   ├── fact.py              # Fact table models
│   │   └── alert.py             # Alert models
│   ├── repositories/
│   │   ├── __init__.py
│   │   ├── base.py              # Generic repository interface
│   │   ├── stock_repository.py
│   │   ├── price_repository.py
│   │   ├── indicator_repository.py
│   │   └── alert_repository.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── data_sources/
│   │   │   ├── __init__.py
│   │   │   ├── base.py          # Abstract DataSource
│   │   │   ├── ngx_source.py    # african-markets scraper
│   │   │   └── yahoo_source.py  # Yahoo Finance API
│   │   ├── processors/
│   │   │   ├── __init__.py
│   │   │   ├── base.py          # Abstract DataProcessor
│   │   │   ├── price_processor.py
│   │   │   └── validator.py     # Data quality checks
│   │   ├── indicators/
│   │   │   ├── __init__.py
│   │   │   ├── calculator.py    # Technical indicators
│   │   │   └── strategies.py    # Trading strategies
│   │   ├── alerts/
│   │   │   ├── __init__.py
│   │   │   ├── evaluator.py     # Alert rule engine
│   │   │   └── notifier.py      # Notification service
│   │   └── advisory/
│   │       ├── __init__.py
│   │       ├── advisor.py       # Investment recommendations
│   │       └── portfolio.py     # Portfolio analysis
│   ├── pipelines/
│   │   ├── __init__.py
│   │   ├── orchestrator.py      # Main ETL orchestration
│   │   ├── ingestion.py
│   │   ├── processing.py
│   │   └── loading.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── logger.py            # Structured logging
│   │   ├── exceptions.py        # Custom exceptions
│   │   └── decorators.py        # Retry, timing decorators
│   └── api/
│       ├── __init__.py
│       └── endpoints.py         # FastAPI REST endpoints (optional)
├── airflow/
│   └── dags/
│       └── ngx_investment_pipeline.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── docs/
│   ├── API.md
│   └── DEPLOYMENT.md
├── scripts/
│   ├── init_db.py               # Create tables and seed data
│   └── backfill.py              # Historical data backfill
├── .env.example
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml               # Poetry/Black/MyPy config
└── README.md
```

### 3.2 Layer Responsibilities

#### **Configuration Layer** (`config/`)
- Environment variable management (dev/staging/prod)
- Database connection pooling
- API credentials management
- Feature flags

#### **Models Layer** (`models/`)
- SQLAlchemy ORM definitions
- Data validation (Pydantic integration)
- Business logic methods
- Relationship mappings

#### **Repository Layer** (`repositories/`)
- Data access abstraction
- Query optimization
- Transaction management
- Caching strategies

#### **Service Layer** (`services/`)
- Business logic implementation
- Data transformation
- External API integration
- Algorithm implementation

#### **Pipeline Layer** (`pipelines/`)
- Workflow orchestration
- Error handling & retry
- Data flow coordination
- Dependency management

---

## 4. Design Patterns (Applied from reference.py)

### 4.1 OOP Principles Applied

#### **Encapsulation**
```python
class DataSource(ABC):
    """Abstract base for all data sources"""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self._name = name
        self._config = config
        self._session: Optional[requests.Session] = None
    
    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        """Fetch data from source"""
        pass
    
    def _create_session(self) -> requests.Session:
        """Private method for session setup"""
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1)
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session
```

#### **Inheritance & Polymorphism**
```python
class NGXDataSource(DataSource):
    """Concrete implementation for NGX"""
    
    def fetch(self) -> pd.DataFrame:
        # NGX-specific scraping logic
        pass

class YahooFinanceSource(DataSource):
    """Concrete implementation for Yahoo Finance"""
    
    def fetch(self) -> pd.DataFrame:
        # Yahoo Finance API logic
        pass

# Usage - polymorphic behavior
sources: List[DataSource] = [
    NGXDataSource("NGX", ngx_config),
    YahooFinanceSource("LSE", lse_config)
]

for source in sources:
    df = source.fetch()  # Each uses its own implementation
```

#### **Dependency Injection**
```python
class ETLOrchestrator:
    """Orchestrates entire pipeline with injected dependencies"""
    
    def __init__(
        self,
        data_source: DataSource,
        processor: DataProcessor,
        indicator_calc: IndicatorCalculator,
        alert_evaluator: AlertEvaluator,
        repository: PriceRepository
    ):
        self._source = data_source
        self._processor = processor
        self._indicator = indicator_calc
        self._alerts = alert_evaluator
        self._repo = repository
    
    def run(self) -> None:
        # 1. Fetch
        raw_df = self._source.fetch()
        
        # 2. Process
        clean_df = self._processor.process(raw_df)
        
        # 3. Calculate indicators
        indicators_df = self._indicator.calculate_all(clean_df)
        
        # 4. Evaluate alerts
        alerts = self._alerts.evaluate(indicators_df)
        
        # 5. Save to database
        self._repo.bulk_insert(clean_df)
```

### 4.2 Design Patterns

#### **Factory Pattern** (Data Source Creation)
```python
class DataSourceFactory:
    """Creates appropriate data source based on type"""
    
    @staticmethod
    def create(source_type: str, config: Dict) -> DataSource:
        if source_type == "NGX":
            return NGXDataSource("NGX", config)
        elif source_type == "YAHOO":
            return YahooFinanceSource("Yahoo", config)
        else:
            raise ValueError(f"Unknown source: {source_type}")
```

#### **Strategy Pattern** (Alert Rules)
```python
class AlertStrategy(ABC):
    @abstractmethod
    def evaluate(self, data: Dict) -> Optional[Alert]:
        pass

class PriceMovementStrategy(AlertStrategy):
    def evaluate(self, data: Dict) -> Optional[Alert]:
        if abs(data['change_1d_pct']) > 4.0:
            return Alert(severity='WARNING', message='Significant price movement')

class MACrossoverStrategy(AlertStrategy):
    def evaluate(self, data: Dict) -> Optional[Alert]:
        if data['ma_7'] > data['ma_30'] and data['prev_ma_7'] <= data['prev_ma_30']:
            return Alert(severity='INFO', message='Bullish MA crossover')
```

#### **Repository Pattern** (Data Access)
```python
class Repository(Generic[T], ABC):
    """Generic repository interface"""
    
    @abstractmethod
    def get_by_id(self, id: int) -> Optional[T]:
        pass
    
    @abstractmethod
    def get_all(self) -> List[T]:
        pass
    
    @abstractmethod
    def save(self, entity: T) -> T:
        pass

class PriceRepository(Repository[FactDailyPrice]):
    """Concrete implementation for prices"""
    
    def get_price_history(
        self, 
        stock_id: int, 
        start_date: date, 
        end_date: date
    ) -> List[FactDailyPrice]:
        """Custom query method"""
        pass
```

#### **Observer Pattern** (Alerts & Notifications)
```python
class AlertObserver(ABC):
    @abstractmethod
    def on_alert_triggered(self, alert: Alert) -> None:
        pass

class EmailNotifier(AlertObserver):
    def on_alert_triggered(self, alert: Alert) -> None:
        # Send email
        pass

class SlackNotifier(AlertObserver):
    def on_alert_triggered(self, alert: Alert) -> None:
        # Send Slack message
        pass

class AlertEvaluator:
    def __init__(self):
        self._observers: List[AlertObserver] = []
    
    def attach(self, observer: AlertObserver) -> None:
        self._observers.append(observer)
    
    def _notify(self, alert: Alert) -> None:
        for observer in self._observers:
            observer.on_alert_triggered(alert)
```

---

## 5. Data Flow

### 5.1 Daily Pipeline Execution

```
┌─────────────────────────────────────────────────────────────────────┐
│                      AIRFLOW SCHEDULER (3:00 PM WAT)                │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 1: Health Check                                               │
│  - Check database connectivity                                      │
│  - Verify data sources are reachable                                │
│  - Validate previous run completed successfully                     │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 2: Ingest NGX Data                                            │
│  - Scrape african-markets.com (156 stocks)                          │
│  - Save to: data/raw/ngx/YYYY-MM-DD/                                │
│  - Log: Record count, missing data, response time                   │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 3: Ingest LSE Data (Parallel)                                 │
│  - Fetch Yahoo Finance (SEPL.L, GTCO.L)                             │
│  - Save to: data/raw/lse/YYYY-MM-DD/                                │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 4: Data Validation & Cleaning                                 │
│  - Check for nulls, outliers, duplicates                            │
│  - Convert data types                                               │
│  - Flag suspicious data (quality_flag = 'SUSPICIOUS')               │
│  - Save to: data/processed/YYYY-MM-DD/                              │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 5: Load to Database                                           │
│  - Upsert dim_stocks (handle new/delisted stocks)                   │
│  - Insert fact_daily_prices (skip duplicates)                       │
│  - Log: Rows inserted, skipped, failed                              │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 6: Calculate Technical Indicators                             │
│  - MA (7, 30, 90 day)                                               │
│  - RSI, MACD, Bollinger Bands                                       │
│  - Volatility, ATR                                                  │
│  - Insert into fact_technical_indicators                            │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 7: Evaluate Alert Rules                                       │
│  - Apply all active rules from alert_rules table                    │
│  - Generate alerts for threshold breaches                           │
│  - Insert into alert_history                                        │
│  - Deduplicate (don't re-alert same condition)                      │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 8: Send Notifications                                         │
│  - Email: CRITICAL alerts                                           │
│  - Slack: WARNING + CRITICAL                                        │
│  - SMS: CRITICAL only (optional)                                    │
│  - Mark notification_sent = TRUE                                    │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 9: Generate Investment Advisory Report                        │
│  - Top 10 performers (1-day, YTD)                                   │
│  - Top 10 losers                                                    │
│  - Stocks with bullish signals                                      │
│  - Portfolio summary                                                │
│  - Save to: reports/daily/advisory_YYYY-MM-DD.html                  │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  TASK 10: Cleanup & Archival                                        │
│  - Archive old raw files (>90 days)                                 │
│  - Update pipeline run metadata                                     │
│  - Log pipeline completion time                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 Query Patterns (Optimized)

#### **Get Today's Top Movers**
```sql
SELECT 
    stock_code, 
    company_name, 
    close_price, 
    change_1d_pct
FROM vw_latest_stock_prices
WHERE ABS(change_1d_pct) > 4.0
ORDER BY ABS(change_1d_pct) DESC
LIMIT 10;

-- Uses index: idx_recent_prices
-- Execution time: ~15ms
```

#### **Get Stock with Active Alerts**
```sql
SELECT 
    s.stock_code,
    s.company_name,
    ah.severity,
    ah.message,
    ah.alert_timestamp
FROM alert_history ah
JOIN dim_stocks s ON ah.stock_id = s.stock_id
WHERE ah.is_resolved = FALSE
  AND ah.alert_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY ah.severity DESC, ah.alert_timestamp DESC;

-- Uses index: idx_unresolved
-- Execution time: ~20ms
```

---

## 6. Investment Advisory Features

### 6.1 Advisory Capabilities

#### **Portfolio Analysis**
- **Sector Allocation**: Diversification analysis across 9 sectors
- **Risk Metrics**: Portfolio volatility, beta, Sharpe ratio
- **Performance**: 1D, 7D, 30D, YTD returns
- **Top Holdings**: Concentration risk assessment

#### **Stock Recommendations**
- **Buy Signals**: RSI < 30, bullish MA crossover, oversold conditions
- **Sell Signals**: RSI > 70, bearish crossover, overbought
- **Hold Signals**: Neutral indicators, consolidation patterns

#### **Alert System**
- **CRITICAL**: >8% daily move, extreme volatility
- **WARNING**: 4-8% move, bearish crossover, 2x volatility
- **INFO**: Bullish crossover, volume spike, RSI levels

### 6.2 API Endpoints (FastAPI - Optional)

```python
# GET /api/v1/stocks
# GET /api/v1/stocks/{stock_code}/latest
# GET /api/v1/stocks/{stock_code}/history?start_date=2025-01-01
# GET /api/v1/stocks/{stock_code}/indicators
# GET /api/v1/alerts/active
# GET /api/v1/portfolio/summary
# GET /api/v1/advisory/recommendations
# POST /api/v1/alerts/resolve/{alert_id}
```

---

## 7. Next Steps

1. ✅ **Review this architecture document**
2. ⏳ **Initialize database schema** (run SQL scripts)
3. ⏳ **Create base classes** (models, repositories, services)
4. ⏳ **Implement data sources** (NGX + Yahoo)
5. ⏳ **Build indicators calculator**
6. ⏳ **Create alert engine**
7. ⏳ **Refactor ETL pipeline**
8. ⏳ **Build Airflow DAG**
9. ⏳ **Add tests & documentation**
10. ⏳ **Deploy to production**

---

**Document Version**: 1.0  
**Last Updated**: 2025-12-06  
**Status**: Design Phase - Pending Implementation
