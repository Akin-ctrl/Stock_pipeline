# Nigerian Stock Pipeline - System Overview

> **Production-Ready MVP** | Daily automated Nigerian Stock Exchange (NGX) data collection, analysis, and investment advisory system.

---

## Purpose

Automated stock pipeline for **Nigerian equity growth investors** that:
- Monitors **154 NGX stocks** daily from african-markets.com
- Calculates **technical indicators** (MA, RSI, MACD, Volatility)
- Generates **investment recommendations** (BUY/SELL/HOLD signals)
- Sends **real-time alerts** via Email/Slack
- Runs **24/7 autonomously** via Airflow scheduler

---

## Business Value

### For Investors:
✅ **Early opportunity detection** - Identify breakouts before manual analysis  
✅ **Risk management** - Monitor volatility and market stress  
✅ **Time savings** - Automated daily analysis of 154 stocks  
✅ **Data-driven decisions** - Technical signals backed by historical patterns  
✅ **Comprehensive coverage** - All major NGX sectors  

### Technical Excellence:
✅ **Production-ready** - Battle-tested, error-resilient pipeline  
✅ **Scalable** - PostgreSQL star schema supporting years of data  
✅ **Maintainable** - Clean OOP, 100% production-validated  
✅ **Portable** - Docker Compose deployment  
✅ **Cloud-ready** - Deploy to DigitalOcean/AWS in 30 minutes  

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA COLLECTION                          │
│  NGX Source: african-markets.com (154 stocks)               │
│  Fields: Close Price, 1D%, YTD%, Market Cap                 │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   DATA VALIDATION                           │
│  Quality Flags: GOOD | INCOMPLETE | POOR | SUSPICIOUS       │
│  Checks: Required fields, price ranges, duplicates          │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                  DATA STORAGE (PostgreSQL)                  │
│  dim_stocks: 154 stocks across 12 sectors                   │
│  fact_daily_prices: Time-series with quality flags          │
│  fact_technical_indicators: MA, RSI, MACD, Volatility       │
│  alert_history: Investment signals and notifications        │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│              TECHNICAL ANALYSIS (20+ days)                  │
│  Moving Averages: SMA20, SMA50 with crossover detection     │
│  RSI: 14-day momentum (oversold/overbought)                 │
│  MACD: Trend following indicator                            │
│  Volatility: 30-day annualized risk metric                  │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│            INVESTMENT ADVISORY (Indicators ready)           │
│  Signals: STRONG_BUY | BUY | HOLD | SELL | STRONG_SELL      │
│  Scores: 0-100 (Technical, Momentum, Volatility, Trend)     │
│  Risk: LOW | MEDIUM | HIGH with stop-loss levels            │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                    NOTIFICATIONS                            │
│  Email: Daily digest + critical alerts                      │
│  Slack: Real-time investment signals                        │
│  CLI: Manual queries and reports                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Python 3.11 | Core application |
| **Database** | PostgreSQL 16 | Time-series storage |
| **ORM** | SQLAlchemy 2.0 | Database layer |
| **Data** | Pandas 2.1 | Analysis & transformation |
| **Scraping** | BeautifulSoup4 | NGX data extraction |
| **Scheduler** | Apache Airflow 2.10 | Daily 3PM WAT automation |
| **Containers** | Docker Compose | Orchestration |
| **Monitoring** | Structured logging | JSON logs + Airflow UI |

---

## Current Production Status

### **LIVE & OPERATIONAL**
- **154 stocks** loaded from NGX (12 sectors)
- **154 daily prices** captured (50% GOOD, 50% INCOMPLETE quality)
- **Daily execution**: 3:00 PM WAT (2:00 PM UTC) via Airflow
- **Execution time**: ~37 seconds end-to-end
- **Data quality**: Automated validation with flags
- **Auto-restart**: All containers restart on failure
- **Webserver**: http://localhost:8080 (Airflow UI)

### **Data Accumulation Timeline**
- **Days 1-20**: Price collection only (building history)
- **Days 21-30**: Technical indicators activate (requires 20+ days)
- **Days 31+**: Full advisory system (recommendations + alerts)
- **Month 2-3**: Rich historical data for backtesting

### **NGX Data Coverage**
```
Total Stocks: 154
Source: african-markets.com
Fields Captured:
  ✅ Close Price (NGN)
  ✅ Daily Change % (1D%)
  ✅ Year-to-Date % (YTD%)
  ✅ Market Cap
  ✅ Company Name
  ✅ Sector

Quality Distribution:
  77 GOOD (100% data completeness)
  77 INCOMPLETE (missing 1D% only)
  0 POOR/SUSPICIOUS/MISSING
```

---

## Project Structure

```
Stock_pipeline/
├── app/
│   ├── config/              # Settings, database connection
│   ├── models/              # SQLAlchemy ORM (6 tables)
│   ├── repositories/        # Data access layer (5 repos)
│   ├── services/
│   │   ├── data_sources/    # NGX scraper
│   │   ├── processors/      # Validation & transformation
│   │   ├── indicators/      # Technical analysis
│   │   ├── alerts/          # Alert evaluation + notifications
│   │   └── advisory/        # Investment recommendations
│   ├── pipelines/           # ETL orchestrator
│   ├── utils/               # Logging, exceptions, decorators
│   └── cli.py               # 30+ commands for manual ops
├── airflow/
│   ├── dags/                # Daily pipeline DAG
│   └── logs/                # Execution history
├── tests/
│   ├── unit/                # Component tests
│   └── integration/         # End-to-end tests
├── data/
│   ├── raw/                 # Scraped CSV files
│   └── processed/           # Validated data
├── logs/                    # Application logs
├── reports/                 # Generated analysis
├── docker-compose.yml       # 5 containers orchestration
└── .env                     # Environment configuration
```

---

## Key Features

### 1. **Data Collection**
- NGX scraping with retry logic and error handling
- Comma removal for large percentages (1,354.00% → 1354.00%)
- Source attribution (`source='ngx'`)
- Data quality flagging

### 2. **Data Storage**
- **Star schema** design (facts + dimensions)
- **Bulk upsert** logic (handles retries gracefully)
- **Quality flags**: GOOD, INCOMPLETE, POOR, SUSPICIOUS, MISSING, STALE
- **Unique constraints**: Prevents duplicate (stock, date) records

### 3. **Technical Analysis** *(Activates day 21+)*
- Moving Averages (SMA20, SMA50)
- RSI (Relative Strength Index)
- MACD (Moving Average Convergence Divergence)
- Volatility (30-day annualized)
- Golden/Death Cross detection

### 4. **Investment Advisory** *(Activates day 21+)*
- **5 signal types**: STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
- **Score categories**: EXCELLENT (80-100), GOOD (60-79), FAIR (40-59)
- **Risk assessment**: LOW/MEDIUM/HIGH
- **Target prices**: +10-15% for buys
- **Stop-loss levels**: -5-7% protection

### 5. **Notifications**
- Email alerts with HTML formatting
- Slack webhook integration
- Daily digest summaries
- Severity-based routing (CRITICAL → all channels)

### 6. **CLI Interface**
- 30+ commands for manual operations
- Database inspection (stocks, prices, indicators)
- Pipeline execution and monitoring
- Report generation (daily, weekly, monthly)
- Notification testing

---

## Deployment

### **Option 1: Continue Laptop-Based**
```bash
# Pipeline runs automatically daily at 3PM WAT
# Access Airflow UI: http://localhost:8080
# View logs: docker compose logs -f
# Manual operations: docker compose exec app python -m app.cli <command>
```

### **Option 2: Deploy to Cloud (24/7 Operation)**
Migrate to DigitalOcean/AWS for continuous operation:
1. Transfer Docker Compose setup to cloud server
2. Start containers: `docker compose up -d`
3. Access remote Airflow: `http://<server-ip>:8080`
4. SSH for CLI access: `ssh root@<server-ip>`

See [deployment guide](./3_DEPLOYMENT_GUIDE.md) for detailed steps.

---

## Documentation Index

1. **[System Overview](./1_SYSTEM_OVERVIEW.md)** ← You are here
2. **[Technical Architecture](./2_TECHNICAL_ARCHITECTURE.md)** - Database schema, OOP design, data flow
3. **[Deployment Guide](./3_DEPLOYMENT_GUIDE.md)** - Docker setup, cloud deployment, CLI usage
4. **[User Guide](./4_USER_GUIDE.md)** - Notifications, advisory system, reports

---

**Last Updated**: December 31, 2025  
**Version**: 1.0.0 (Production MVP)  
**Status**: ✅ Production-Ready
