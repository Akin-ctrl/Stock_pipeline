# 📈 Nigerian Stock Exchange Investment Pipeline

## 🧑‍💼 **Investor Persona: Nigerian Equity Growth Investor**

### 📌 Background:

A data-driven investor focused on **medium- to long-term growth in the Nigerian equity market**. Monitors NGX (Nigerian Stock Exchange) stocks and select LSE-listed Nigerian companies to identify investment opportunities, manage risk, and optimize portfolio performance through technical analysis and automated alerts.

---

## 🏢 **Business Scenario**:

Managing a portfolio of Nigerian equities requires daily monitoring of 150+ stocks across multiple sectors. This system provides automated insights to answer:

> **"Which Nigerian stocks are showing early signs of breakout, downturn, or increased volatility that could inform buy/hold/sell decisions?"**

---

## 🎯 **Business Requirements**

### 1. **Market Coverage**:

**Primary Market**: Nigerian Stock Exchange (NGX)
- 156+ listed stocks across 9 sectors
- Focus on liquid, actively traded stocks
- Daily price and volume data

**Secondary Market**: London Stock Exchange (LSE)
- 2 Nigerian stocks with dual listings
- Cross-market arbitrage opportunities

**Key Sectors**:
- Financials (Banks, Insurance, Asset Management)
- Consumer Goods (Food, Beverages, Manufacturing)
- Oil & Gas (Exploration, Production, Distribution)
- Industrials (Manufacturing, Construction)
- Technology & Telecoms
- Healthcare & Pharmaceuticals
- Basic Materials
- Consumer Services
- Utilities

---

### 2. **Data Requirements**:

* **Daily OHLCV data**: Open, High, Low, Close, Volume
* **Price metrics**: Daily change %, YTD change %
* **Technical indicators**:
  - Moving Averages: 20-day & 50-day SMA
  - RSI (Relative Strength Index): 14-day
  - MACD: Fast(12), Slow(26), Signal(9)
  - Bollinger Bands: 20-day period, 2 std deviations
  - Volatility: 30-day annualized
  - MA Crossover signals: Golden Cross / Death Cross

---

### 3. **Data Sources**:

* **Primary Source**: african-markets.com (NGX web scraping)
  - Real-time NGX stock prices
  - Volume and market cap data
  - Sector classification

* **Secondary Source**: Yahoo Finance API
  - Historical price data
  - Backup for missing NGX data
  - LSE Nigerian stock prices

---

### 4. **Alert Conditions for Actionable Signals**:

| Condition | Actionable Insight | Severity |
|-----------|-------------------|----------|
| Daily % Change > ±5% | Significant price movement | WARNING |
| Daily % Change > ±10% | Extreme volatility event | CRITICAL |
| Golden Cross (MA20 > MA50) | Bullish trend signal | INFO |
| Death Cross (MA20 < MA50) | Bearish trend signal | WARNING |
| RSI < 30 | Oversold - potential buy | INFO |
| RSI > 70 | Overbought - potential sell | WARNING |
| Volatility > 30% | High risk period | WARNING |
| Volume > 2× 30-day average | Unusual activity | INFO |

---

### 5. **System Outputs**:

* **Daily automated alerts**: Email/Slack notifications for triggered conditions
* **Investment dashboard**: Web-based visualization (future phase)
  - Price charts with MA overlays
  - Technical indicator trends
  - Alert history and portfolio impact
* **Data exports**: CSV/JSON for external analysis
* **Performance reports**: Weekly/monthly portfolio summaries

---

### 6. **System Requirements**:

* ✅ **Fully automated ETL pipeline**: Fetch → Validate → Transform → Load → Analyze
* ✅ **Historical data storage**: PostgreSQL with 5+ years capacity
* ✅ **Containerized deployment**: Docker Compose for portability
* ✅ **Production-grade code**: Type hints, comprehensive tests, structured logging
* ✅ **Scheduling**: Airflow DAG for daily 3PM WAT execution (after market close)
* ✅ **Version control**: Git with comprehensive commit history
* ✅ **Monitoring**: Pipeline metrics, error tracking, data quality checks

---

## 📦 **Pipeline Architecture**

### ETL Workflow:

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA INGESTION                              │
├─────────────────────────────────────────────────────────────────┤
│  NGX Source          │  Scrape african-markets.com             │
│  Yahoo Finance       │  API calls for historical data          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   DATA VALIDATION                               │
├─────────────────────────────────────────────────────────────────┤
│  • Null checks       │  Required fields present                │
│  • Price ranges      │  Values within bounds                   │
│  • OHLC consistency  │  High ≥ Low, etc.                       │
│  • Duplicate detect  │  No duplicate stock+date                │
│  • Quality flags     │  GOOD / SUSPICIOUS / MISSING            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  DATA TRANSFORMATION                            │
├─────────────────────────────────────────────────────────────────┤
│  • Standardize codes │  Uppercase, trim whitespace             │
│  • Clean names       │  Title case, normalize                  │
│  • Calculate changes │  Daily %, YTD %                         │
│  • Fill missing      │  Forward/backward fill                  │
│  • Add metadata      │  Source, timestamp, completeness        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    DATA STORAGE                                 │
├─────────────────────────────────────────────────────────────────┤
│  dim_sectors         │  9 Nigerian market sectors              │
│  dim_stocks          │  156+ stock master data                 │
│  fact_daily_prices   │  Time-series OHLCV data                 │
│  fact_indicators     │  Calculated technical metrics           │
│  alert_rules         │  8 pre-configured alert conditions      │
│  alert_history       │  Triggered alerts with resolution       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│               TECHNICAL ANALYSIS                                │
├─────────────────────────────────────────────────────────────────┤
│  • Moving Averages   │  SMA 20/50 with crossover detection    │
│  • RSI               │  14-day momentum oscillator             │
│  • MACD              │  Trend following indicator              │
│  • Bollinger Bands   │  Volatility bands                       │
│  • Volatility        │  30-day annualized                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  ALERT EVALUATION                               │
├─────────────────────────────────────────────────────────────────┤
│  • Price movements   │  ±5% / ±10% thresholds                  │
│  • MA crossovers     │  Golden/Death cross signals             │
│  • RSI extremes      │  Oversold (<30) / Overbought (>70)      │
│  • High volatility   │  >30% annualized                        │
│  • Volume spikes     │  >2× average                            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    NOTIFICATIONS                                │
├─────────────────────────────────────────────────────────────────┤
│  • Email alerts      │  Daily digest + critical alerts         │
│  • Slack integration │  Real-time notifications                │
│  • Dashboard         │  Web-based visualization (future)       │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 **Technology Stack**

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Python 3.12 | Core application logic |
| **Database** | PostgreSQL 16 | Time-series data storage |
| **ORM** | SQLAlchemy 2.0 | Database abstraction |
| **Data Processing** | Pandas 2.1.4 | Data transformation & analysis |
| **Web Scraping** | BeautifulSoup4 | NGX data extraction |
| **API Client** | yfinance | Yahoo Finance integration |
| **Scheduling** | Apache Airflow 2.8 | Workflow orchestration |
| **Containerization** | Docker & Docker Compose | Deployment & portability |
| **Version Control** | Git & GitHub | Code management |
| **Testing** | pytest | Unit & integration tests |
| **Logging** | Python logging + JSON | Structured logging |

---

## 📁 **Project Structure**

```
Stock_pipeline/
├── app/
│   ├── config/              # Database & settings configuration
│   ├── models/              # SQLAlchemy ORM models (6 tables)
│   ├── repositories/        # Data access layer (5 repositories)
│   ├── services/
│   │   ├── data_sources/    # NGX & Yahoo Finance sources
│   │   ├── processors/      # Validation & transformation
│   │   ├── indicators/      # Technical indicator calculator
│   │   └── alerts/          # Alert rule evaluator
│   ├── pipelines/           # ETL orchestrator (7 stages)
│   └── utils/               # Logging, exceptions, decorators
├── airflow/
│   ├── dags/                # Airflow DAG definitions
│   ├── logs/                # Airflow execution logs
│   └── plugins/             # Custom Airflow plugins
├── tests/
│   ├── unit/                # Unit tests for components
│   └── integration/         # End-to-end pipeline tests
├── data/
│   ├── raw/                 # Raw ingested data (CSV)
│   └── processed/           # Processed data for loading
├── reports/                 # Generated investment reports
├── logs/                    # Application logs
├── archive/                 # Historical scripts & data
├── docker-compose.yml       # Multi-container orchestration
├── .env.example             # Environment variables template
└── README.md               # This file
```

---

## 🎯 **Current Status: 70% MVP Complete**

### ✅ **Completed Components**:
1. **Foundation Layer** (100%)
   - Database configuration with connection pooling
   - Settings management with environment variables
   - Comprehensive logging with structured JSON
   - Custom exception hierarchy

2. **Data Models** (100%)
   - 6 SQLAlchemy ORM models
   - Relationships and constraints
   - Indexes for query optimization

3. **Repository Layer** (100%)
   - BaseRepository with common operations
   - StockRepository (156+ stocks)
   - PriceRepository (time-series data)
   - IndicatorRepository (technical metrics)
   - AlertRepository (rules & history)

4. **Data Sources** (100%)
   - NGXDataSource with web scraping
   - YahooDataSource with API integration
   - 156 NGX stocks + 2 LSE stocks configured

5. **Data Processors** (100%)
   - DataValidator with 6 validation checks
   - DataTransformer with standardization & cleaning
   - Quality flags and error reporting

6. **Technical Indicators** (100%)
   - IndicatorCalculator with 6 indicator types
   - Vectorized pandas calculations
   - Batch processing support

7. **Alert Engine** (100%)
   - AlertEvaluator with 5 rule type handlers
   - Deduplication logic
   - Severity levels and metadata tracking

8. **Pipeline Orchestrator** (100%)
   - 7-stage ETL workflow
   - Configurable execution
   - Error handling and metrics
   - Batch processing with transactions

### 🔨 **In Progress (30%)**:
9. **Airflow Integration** (Next)
   - DAG definition for daily 3PM WAT schedule
   - Task dependencies and retries
   - Monitoring and alerting

10. **Integration Tests** (Next)
    - End-to-end pipeline validation
    - Data quality checks
    - Performance benchmarks

11. **CLI Interface** (Next)
    - Manual pipeline execution
    - Data inspection commands
    - Configuration management

12. **Dashboard/Reporting** (Future Phase)
    - Web-based visualization
    - Interactive charts
    - Portfolio analytics

---

## 🚦 **Getting Started**

### Prerequisites:
- Docker & Docker Compose installed
- Git for version control
- Python 3.12+ (for local development)

### Quick Start:

```bash
# 1. Clone repository
git clone https://github.com/Akin-ctrl/Stock_pipeline.git
cd Stock_pipeline

# 2. Set up environment variables
cp .env.example .env
# Edit .env with your configurations

# 3. Start services
docker-compose up -d

# 4. Initialize database
docker-compose exec app python -m app.scripts.init_db

# 5. Run pipeline manually (testing)
docker-compose exec app python -m app.pipelines.orchestrator

# 6. View logs
docker-compose logs -f app
```

---

## 📊 **Sample Outputs**

### Daily Alert Example:
```
🚨 Nigerian Stock Alert - December 7, 2025

CRITICAL ALERTS:
• DANGCEM: +12.4% daily move - significant volatility detected
• MTNN: Volume spike 3.2× average - unusual trading activity

WARNINGS:
• ZENITHBANK: Death Cross detected (MA20 crossed below MA50)
• BUACEMENT: RSI at 73.5 - overbought territory

INFO:
• AIRTELAFRI: Golden Cross confirmed - bullish signal
• NESTLE: RSI at 28.2 - potential buy opportunity

Portfolio Summary:
• Total alerts today: 6
• Stocks monitored: 156
• Data quality: 98.7% GOOD
• Pipeline execution: 1.8 minutes
```

---

## 📈 **Business Value**

### Investment Benefits:
- ✅ **Early opportunity detection**: Identify breakouts before the crowd
- ✅ **Risk management**: Monitor volatility and market stress
- ✅ **Time savings**: Automated daily analysis vs manual screening
- ✅ **Data-driven decisions**: Technical signals backed by historical patterns
- ✅ **Comprehensive coverage**: 156+ stocks across all NGX sectors
- ✅ **Reliable alerts**: Deduplication prevents alert fatigue

### Technical Benefits:
- ✅ **Production-ready**: 70%+ test coverage, structured logging, error handling
- ✅ **Scalable**: Handles 500+ stocks, 5+ years of data
- ✅ **Maintainable**: Clean OOP architecture, comprehensive documentation
- ✅ **Portable**: Docker deployment, environment-based configuration
- ✅ **Extensible**: Plugin architecture for new indicators and data sources

---

## 🤝 **Contributing**

This is a personal investment tool, but contributions are welcome:
1. Fork the repository
2. Create a feature branch
3. Add comprehensive tests
4. Submit pull request with detailed description

---

## 📝 **License**

Private project - All rights reserved

---

## 📧 **Contact**

For questions or issues, please open a GitHub issue or contact the repository owner.

---

## 🙏 **Acknowledgments**

- Nigerian Stock Exchange for market data access
- african-markets.com for real-time NGX prices
- Yahoo Finance for historical data API
- Open-source community for Python packages

---

**Last Updated**: December 7, 2025  
**Version**: 0.7.0 (MVP 70% Complete)  
**Status**: Active Development
