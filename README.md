# Nigerian Stock Pipeline

> **Production-Ready MVP** | Daily automated Nigerian Stock Exchange (NGX) data collection, technical analysis, and stock screening system.

[![Status](https://img.shields.io/badge/status-production-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.12-blue)]()
[![Docker](https://img.shields.io/badge/docker-ready-blue)]()
[![License](https://img.shields.io/badge/license-private-red)]()

---

## What It Does

Automated 24/7 stock pipeline for Nigerian equity investors:
- Monitors 154 NGX stocks daily from african-markets.com
- Calculates technical indicators (MA, RSI, MACD, Volatility)
- Generates investment recommendations (BUY/SELL/HOLD signals)
- Sends real-time alerts via Email/Slack
- Runs autonomously via Airflow scheduler (3:00 PM WAT)

---

## Quick Start

### Local Docker Setup (5 minutes)

```bash
# 1. Clone and configure
git clone https://github.com/Akin-ctrl/Stock_pipeline.git
cd Stock_pipeline
cp .env.example .env
# Edit .env with your settings

# 2. Start all services
docker compose up -d --build

# 3. Access Airflow UI
open http://localhost:8080
# Username: admin | Password: admin

# 4. Verify pipeline ran
docker compose logs -f app
```
---
## Current Status

### PRODUCTION READY (other features need accumulation of data)
- **154 stocks** loaded (12 NGX sectors)
- **Daily execution**: 3:00 PM WAT (2:00 PM UTC)
- **Execution time**: ~37 seconds end-to-end
- **Quality**: 50% GOOD, 50% INCOMPLETE (normal for NGX)
- **Webserver**: http://localhost:8080

### Data Accumulation Timeline
- **Days 1-20**: Price collection (building history)
- **Days 21-30**: Technical indicators activate
- **Days 31+**: Full advisory with recommendations
- **Month 2-3**: Rich data for backtesting

## Architecture

```
NGX Scraper → Validation → PostgreSQL → Technical Analysis
                                ↓
                    Stock Screening → Notifications
                                ↓
                         Email + Slack + CLI
```

**Tech Stack:**
- Python 3.12, PostgreSQL 16, Airflow 2.10
- Docker Compose orchestration
- SQLAlchemy ORM, Pandas analysis
- BeautifulSoup4 web scraping

---

## Documentation

| Document | Description |
|----------|-------------|
| **[1. System Overview](./docs/1_SYSTEM_OVERVIEW.md)** | Business purpose, features, project structure |
| **[2. Technical Architecture](./docs/2_TECHNICAL_ARCHITECTURE.md)** | Database schema, OOP design, data flow |
| **[3. Deployment Guide](./docs/3_DEPLOYMENT_GUIDE.md)** | Docker setup, cloud deployment, CLI usage |
| **[4. User Guide](./docs/4_USER_GUIDE.md)** | Notifications, advisory system, reports |

---

## Key Features

### 1. Data Collection
✅ NGX scraping with retry logic  
✅ Quality validation (GOOD/INCOMPLETE/POOR flags)  
✅ Bulk upsert (idempotent, handles retries)  

### 2. Technical Analysis *(Day 21+)*
✅ SMA20, SMA50 with crossover detection  
✅ RSI (14-day momentum)  
✅ MACD (trend following)  
✅ Volatility (30-day annualized)  

### 3. Stock Screening & Signals *(Day 21+)*
✅ **5 signal types**: STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL  
✅ **Scores 0-100**: Technical, Momentum, Volatility, Trend  
✅ **Risk assessment**: LOW, MEDIUM, HIGH  
✅ **Target prices**: +10-15% for buys  
✅ **Stop-loss levels**: -5-7% protection  

### 4. Notifications
✅ Email alerts (HTML + plain text)  
✅ Slack webhook integration  
✅ Daily digest summaries  
✅ Severity-based routing  

### 5. CLI Interface 
```bash
# Database inspection
docker compose exec app python -m app.cli list-stocks
docker compose exec app python -m app.cli check-data-quality

# Manual operations
docker compose exec app python -m app.cli run-pipeline
docker compose exec app python -m app.cli top-picks --signal BUY --count 10

# Reports
docker compose exec app python -m app.cli generate-report --type weekly
docker compose exec app python -m app.cli test-email
```

---

## Project Structure

```
Stock_pipeline/
├── app/
│   ├── config/              # Settings, database connection
│   ├── models/              # SQLAlchemy ORM (6 tables)
│   ├── repositories/        # Data access layer
│   ├── services/
│   │   ├── data_sources/    # NGX scraper
│   │   ├── processors/      # Validation & transformation
│   │   ├── indicators/      # Technical analysis
│   │   ├── alerts/          # Evaluation + notifications
│   │   └── screening/       # Stock screening & signals
│   ├── pipelines/           # ETL orchestrator
│   └── cli.py               # 30+ CLI commands
├── airflow/
│   └── dags/                # Daily pipeline DAG
├── docs/                    # Comprehensive documentation
├── tests/                   # Unit & integration tests
└── docker-compose.yml       # 5-container orchestration
```

---

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **PgAdmin** | http://localhost:5050 | admin@stockpipeline.com / admin |
| **PostgreSQL** | localhost:5432 | stockuser / changeme |

---

## System Requirements

**Local Docker:**
- 4GB RAM minimum
- 10GB disk space
- Docker 20.10+ & Docker Compose 2.0+

---

## Business Value

**For Investors:**
- ✅ Early opportunity detection before manual analysis
- ✅ Risk management with volatility monitoring
- ✅ Time savings (automated analysis of 154 stocks)
- ✅ Data-driven decisions backed by technical signals
- ✅ Comprehensive coverage of all NGX sectors

**Technical Excellence:**
- ✅ Production-ready (100% validated)
- ✅ Scalable (PostgreSQL star schema, 500+ stock capacity)
- ✅ Maintainable (Clean OOP, comprehensive docs)
- ✅ Portable (Docker Compose deployment)
- ✅ Cloud-ready (Deploy in 30 minutes)

---

## Database Schema

**Star Schema** design with 6 tables:
- `dim_sectors` - 12 NGX sectors
- `dim_stocks` - 154 active stocks
- `fact_daily_prices` - Time-series (close_price, 1D%, YTD%, market_cap)
- `fact_technical_indicators` - SMA, RSI, MACD, Volatility
- `alert_history` - Investment signals and notifications

**Quality Flags:** GOOD, INCOMPLETE, POOR, SUSPICIOUS, MISSING, STALE

See **[Technical Architecture](./docs/2_TECHNICAL_ARCHITECTURE.md)** for detailed schema.

---

## Daily Pipeline (8 Stages)

```
1. Fetch NGX Data        (~15s) - Scrape african-markets.com
2. Validate Data         (~2s)  - Quality checks & flags
3. Transform Data        (~3s)  - Clean & standardize
4. Load Stocks          (~2s)  - Upsert dim_stocks
5. Load Prices          (~10s) - Bulk upsert (154 prices)
6. Calculate Indicators (~3s)  - SMA, RSI, MACD (day 21+)
7. Evaluate Alerts      (~2s)  - Check conditions (day 21+)
8. Generate Recommendations (~2s) - BUY/SELL/HOLD (day 21+)

Total: ~37 seconds
```

**Execution:** Daily at 3:00 PM WAT (after NGX market close)

---

## Notifications

### Email Setup (Gmail)
```bash
# 1. Get Gmail app password
# Visit: https://myaccount.google.com/security → 2-Step → App passwords

# 2. Configure .env
NOTIFICATION_EMAIL_ENABLED=true
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_16_char_app_password
NOTIFICATION_EMAILS=recipient@example.com

# 3. Test
docker compose exec app python -m app.cli test-email
```

### Slack Setup
```bash
# 1. Create webhook: https://api.slack.com/messaging/webhooks
# 2. Configure .env
NOTIFICATION_SLACK_ENABLED=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# 3. Test
docker compose exec app python -m app.cli test-slack
```

**See [User Guide](./docs/4_USER_GUIDE.md) for detailed setup.**

---

## Investment Advisory (Day 21+)

**Signals Generated:**
- **STRONG_BUY** - Multiple bullish indicators, +15% target
- **BUY** - Bullish signals, +10% target
- **HOLD** - Mixed/neutral signals
- **SELL** - Bearish indicators
- **STRONG_SELL** - Multiple bearish indicators

**Scoring (0-100):**
- Technical (30%): RSI, MACD analysis
- Momentum (25%): Price vs moving averages
- Volatility (20%): Price stability
- Trend (15%): Golden/Death cross
- Volume (10%): Confirmation signals

**Example:**
```bash
docker compose exec app python -m app.cli top-picks --signal BUY --count 5

# Output:
# 1. MTNN - STRONG_BUY
#    Score: 82.5 (EXCELLENT), Confidence: 87%
#    Target: ₦269.68 (+15%), Stop Loss: ₦221.78 (-5%)
#    Risk: LOW
```

---

## Sample Outputs

### Daily Pipeline Log
```
2025-12-31 15:00:00 - Pipeline started (execution_date=2025-12-31)
2025-12-31 15:00:15 - Fetched 154 stocks from NGX
2025-12-31 15:00:17 - Validated: 77 GOOD, 77 INCOMPLETE
2025-12-31 15:00:20 - Transformed and cleaned data
2025-12-31 15:00:22 - Loaded 154 stocks (0 new, 154 updated)
2025-12-31 15:00:32 - Bulk loaded 154 prices (4 batches)
2025-12-31 15:00:33 - Indicators skipped (requires 20+ days)
2025-12-31 15:00:34 - Alerts skipped (no indicators yet)
2025-12-31 15:00:35 - Recommendations skipped (no indicators yet)
2025-12-31 15:00:37 - Pipeline completed successfully
                     Execution time: 37 seconds
```

### Email Alert (Day 21+)
```
Subject: CRITICAL: DANGCEM Price Spike

DANGCEM (Dangote Cement PLC)
Price: ₦1,234.56
Change: +12.4% (1D)
YTD: +240.5%
Market Cap: 2.1T

Alert: Daily price movement exceeds 10%
Severity: CRITICAL
Recommendation: Review position immediately

---

```

---

## Troubleshooting

### Pipeline Failed
```bash
# 1. Check logs
docker compose logs app | tail -100

# 2. Verify database
docker compose exec app python -m app.cli check-db

# 3. Manual retry
docker compose exec app python -m app.cli run-pipeline
```

### No Data Loaded
```bash
# Test NGX scraper
docker compose exec app python -m app.cli sync-stocks

# Check data quality
docker compose exec app python -m app.cli check-data-quality
```

### Notifications Not Sending
```bash
# Test email
docker compose exec app python -m app.cli test-email

# Test Slack
docker compose exec app python -m app.cli test-slack
```

**See [Deployment Guide](./docs/3_DEPLOYMENT_GUIDE.md) for more troubleshooting.**

---

## Next Steps (currently ongoing)

### Days 1-20: Data Accumulation
```bash
# Monitor daily execution
docker compose logs -f app

# Check data quality
docker compose exec app python -m app.cli check-data-quality
```

### Days 21+: Full System Active
```bash
# View top buy recommendations
docker compose exec app python -m app.cli top-picks --signal BUY --count 10

# Check recent alerts
docker compose exec app python -m app.cli recent-alerts --days 7

# Generate weekly report
docker compose exec app python -m app.cli generate-report --type weekly
```

### Cloud Deployment (24/7 Operation)
```bash
# Transfer to DigitalOcean/AWS
# See: docs/3_DEPLOYMENT_GUIDE.md

# Benefits:
# ✅ Runs without laptop on
# ✅ 99.9% uptime
# ✅ Remote access from anywhere
# ✅ Professional infrastructure
```

---

## Support & Contributing

**Documentation:**
- [System Overview](./docs/1_SYSTEM_OVERVIEW.md)
- [Technical Architecture](./docs/2_TECHNICAL_ARCHITECTURE.md)
- [Deployment Guide](./docs/3_DEPLOYMENT_GUIDE.md)
- [User Guide](./docs/4_USER_GUIDE.md)

**Issues:** [GitHub Issues](https://github.com/Akin-ctrl/Stock_pipeline/issues)

**Contributing:**
1. Fork repository
2. Create feature branch
3. Add comprehensive tests
4. Submit pull request

---

## License

Private project - All rights reserved

---

## Acknowledgments

- Nigerian Stock Exchange for market data access
- african-markets.com for real-time NGX prices
- Open-source Python community

---

**Last Updated**: December 31, 2025  
**Version**: 1.0.0 (Production MVP)  
**Status**: ✅ Production-Ready
