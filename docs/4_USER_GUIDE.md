# 📊 User Guide - Notifications, Advisory & Reports

> How to use the notification system, investment advisory, and reporting features

---

## 📋 Table of Contents
1. [Notification System](#notification-system)
2. [Investment Advisory](#investment-advisory)
3. [Reports & Analysis](#reports--analysis)
4. [Data Quality](#data-quality)

---

## 1. Notification System

### 1.1 Overview

The system sends **automated notifications** for investment signals via:
- 📧 **Email** - Daily digest + critical alerts
- 💬 **Slack** - Real-time notifications
- 🔔 **Future**: SMS via Twilio

### 1.2 Email Setup (Gmail)

#### Step 1: Generate App Password
```bash
# 1. Visit: https://myaccount.google.com/security
# 2. Enable "2-Step Verification" if not already enabled
# 3. Click "2-Step Verification" → "App passwords"
# 4. Select "Mail" → Generate
# 5. Copy the 16-character password (e.g., "abcd efgh ijkl mnop")
```

#### Step 2: Configure .env
```bash
# Edit .env file
NOTIFICATION_EMAIL_ENABLED=true
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=abcdefghijklmnop  # 16-char app password (no spaces)
NOTIFICATION_FROM_EMAIL=nigerian.stocks@yourdomain.com
NOTIFICATION_EMAILS=investor1@email.com,investor2@email.com
```

#### Step 3: Test Email
```bash
docker compose exec app python -m app.cli test-email

# Expected output:
# ✅ Email configuration loaded
# ✅ Email sent successfully to: investor1@email.com, investor2@email.com
```

### 1.3 Slack Setup

#### Step 1: Create Webhook
```bash
# 1. Visit: https://api.slack.com/messaging/webhooks
# 2. Create new Slack app (or use existing workspace)
# 3. Enable "Incoming Webhooks"
# 4. Add webhook to workspace
# 5. Select channel (e.g., #stock-alerts)
# 6. Copy webhook URL
```

#### Step 2: Configure .env
```bash
NOTIFICATION_SLACK_ENABLED=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXX
```

#### Step 3: Test Slack
```bash
docker compose exec app python -m app.cli test-slack

# Expected output:
# ✅ Slack configuration loaded
# ✅ Slack message sent successfully
```

### 1.4 Notification Types

#### Critical Alerts (Immediate Email + Slack)
Triggers:
- Daily price change > ±10%
- MACD bullish/bearish crossover
- RSI extreme levels (< 20 or > 80)

**Example Email:**
```
Subject: 🚨 CRITICAL: DANGCEM Price Spike

DANGCEM (Dangote Cement PLC)
Price: ₦1,234.56
Change: +12.4% (1D)
Severity: CRITICAL

Alert: Daily price movement exceeds 10%
Recommendation: Review position immediately

---
Nigerian Stock Pipeline
Generated: 2025-12-31 15:05:00 WAT
```

#### Daily Digest (Email Only)
Sent after each pipeline run (3:00 PM WAT)

**Content:**
- Summary statistics (stocks processed, prices loaded)
- All alerts generated today
- Quality report (GOOD/INCOMPLETE distribution)
- Execution time and status

**Example:**
```
Subject: 📊 Daily Stock Digest - December 31, 2025

Pipeline Execution Summary:
✅ Status: Success
⏱️ Execution Time: 37 seconds
📈 Stocks Processed: 154
💰 Prices Loaded: 154

Data Quality:
✅ GOOD: 77 (50%)
⚠️ INCOMPLETE: 77 (50%)
❌ POOR: 0 (0%)

Alerts Generated: 0
Indicators Calculated: 0 (requires 20+ days)
Recommendations: 0 (requires indicators)

---
Next Run: January 1, 2026 at 3:00 PM WAT
```

### 1.5 Customizing Notifications

#### Multiple Recipients
```bash
# Add multiple emails (comma-separated)
NOTIFICATION_EMAILS=trader@company.com,analyst@company.com,portfolio.manager@company.com
```

#### Email-Only Mode
```bash
NOTIFICATION_EMAIL_ENABLED=true
NOTIFICATION_SLACK_ENABLED=false
```

#### Slack-Only Mode
```bash
NOTIFICATION_EMAIL_ENABLED=false
NOTIFICATION_SLACK_ENABLED=true
```

#### Disable All Notifications
```bash
NOTIFICATION_EMAIL_ENABLED=false
NOTIFICATION_SLACK_ENABLED=false
```

**Note**: Pipeline still runs successfully even if notifications are disabled

---

## 2. Investment Advisory

### 2.1 Overview

The advisory system generates **BUY/SELL/HOLD recommendations** using:
- Technical indicator analysis (RSI, MACD, MA)
- Multi-dimensional scoring (0-100)
- Risk assessment (LOW/MEDIUM/HIGH)
- Target prices and stop-loss levels

**⏳ Activation Timeline**: Requires 20+ days of price history

### 2.2 Recommendation Signals

| Signal | Emoji | Meaning | Action | Target Return |
|--------|-------|---------|--------|---------------|
| **STRONG_BUY** | 🚀 | Multiple strong bullish signals | Aggressive buy | +15% |
| **BUY** | 📈 | Bullish indicators | Buy | +10% |
| **HOLD** | ⏸️ | Mixed/neutral signals | Wait | - |
| **SELL** | 📉 | Bearish indicators | Sell | - |
| **STRONG_SELL** | ⚠️ | Multiple strong bearish signals | Urgent sell | - |

### 2.3 Scoring System

Each stock receives a **composite score (0-100)** across 5 dimensions:

#### 1. Technical Score (30% weight)
- RSI positioning (30-70 = healthy)
- MACD bullish/bearish crossover
- Indicator alignment

#### 2. Momentum Score (25% weight)
- Price vs SMA20 (short-term trend)
- Price vs SMA50 (medium-term trend)
- Trend strength

#### 3. Volatility Score (20% weight)
- 30-day price stability
- Lower volatility = higher score
- Risk-adjusted performance

#### 4. Trend Score (15% weight)
- Golden Cross (SMA20 > SMA50) = bullish
- Death Cross (SMA20 < SMA50) = bearish
- Trend momentum

#### 5. Volume Score (10% weight)
- *Future enhancement* (requires volume data)
- Currently set to neutral (50)

**Score Categories:**
- ⭐⭐⭐⭐⭐ **EXCELLENT** (80-100) - Top-tier stocks
- ⭐⭐⭐⭐ **GOOD** (60-79) - Strong candidates
- ⭐⭐⭐ **FAIR** (40-59) - Moderate quality
- ⭐⭐ **POOR** (20-39) - Weak stocks
- ⭐ **VERY_POOR** (0-19) - Avoid

### 2.4 Using the Advisory System

#### View Top Buy Recommendations
```bash
docker compose exec app python -m app.cli top-picks --signal BUY --count 10

# Example output:
# Top 10 BUY Recommendations (2025-12-31)
# 
# 1. MTNN (MTN Nigeria) - STRONG_BUY
#    Score: 82.5 (EXCELLENT)
#    Confidence: 87%
#    Current: ₦234.50
#    Target: ₦269.68 (+15%)
#    Stop Loss: ₦221.78 (-5%)
#    Risk: LOW
#    Reasoning: Strong bullish momentum, RSI at 45 (healthy), 
#               Golden Cross confirmed, low volatility
# 
# 2. AIRTELAFRI (Airtel Africa) - BUY
#    Score: 76.3 (GOOD)
#    ...
```

#### View Top Sell Signals
```bash
docker compose exec app python -m app.cli top-picks --signal SELL --count 5

# Shows stocks with bearish signals
```

#### View All Recommendations
```bash
docker compose exec app python -m app.cli generate-recommendations

# Generates fresh recommendations for all stocks
# Applies default filters:
# - Min Score: 40.0
# - Min Confidence: 60%
```

#### Stock-Specific Recommendation
```bash
docker compose exec app python -m app.cli stock-recommendation DANGCEM

# Output:
# DANGCEM (Dangote Cement PLC)
# Signal: HOLD
# Score: 58.2 (FAIR)
# Confidence: 65%
# 
# Technical Breakdown:
# - Technical: 62/100 (RSI neutral, MACD flat)
# - Momentum: 55/100 (Price near SMA20)
# - Volatility: 48/100 (Moderate volatility)
# - Trend: 60/100 (Neutral trend)
# - Volume: 50/100 (Average)
# 
# Risk: MEDIUM
# Reasoning: Mixed signals, wait for clearer trend direction
```

### 2.5 Risk Assessment

**LOW Risk:**
- Low volatility (< 20% annualized)
- Strong trend confirmation
- High confidence signals (> 80%)
- Example: Large-cap banks, telecoms

**MEDIUM Risk:**
- Moderate volatility (20-30%)
- Mixed indicators
- Medium confidence (60-79%)
- Example: Mid-cap industrials

**HIGH Risk:**
- High volatility (> 30%)
- Conflicting signals
- Low confidence (< 60%)
- Example: Small-cap stocks, high-beta sectors

### 2.6 Target Prices & Stop Loss

**For BUY Signals:**
```python
# Target Price (+10-15% upside)
if signal == STRONG_BUY:
    target = current_price * 1.15  # +15%
else:
    target = current_price * 1.10  # +10%

# Stop Loss (-5-7% downside protection)
if volatility < 20%:
    stop_loss = current_price * 0.95  # -5%
else:
    stop_loss = current_price * 0.93  # -7%
```

**Example:**
```
Stock: ZENITHBANK
Current: ₦45.00
Signal: BUY
Target: ₦49.50 (+10%)
Stop Loss: ₦42.75 (-5%)
```

### 2.7 Advisory Workflow

```
Day 1-20: Data Accumulation
└─> No recommendations (building price history)

Day 21: Indicators Activate
└─> calculate_indicators() runs successfully
    └─> SMA20, SMA50, RSI, MACD, Volatility ready

Day 22+: Full Advisory
└─> generate_recommendations() runs daily
    └─> Score all stocks → Generate signals
        └─> Send top picks via email/Slack (if configured)
            └─> Save to database for CLI access
```

---

## 3. Reports & Analysis

### 3.1 Daily Report

```bash
docker compose exec app python -m app.cli generate-report --type daily

# Output file: /app/reports/daily_report_20251231.txt
```

**Contents:**
- Pipeline execution summary
- Data quality breakdown
- Top movers (biggest % gainers/losers)
- New alerts generated
- Top buy/sell recommendations *(if available)*

### 3.2 Weekly Report

```bash
docker compose exec app python -m app.cli generate-report --type weekly

# Output file: /app/reports/weekly_report_20251231.txt
```

**Contents:**
- Week-over-week price changes
- Most active stocks (by alerts)
- Quality trend analysis
- Indicator trends (MA crossovers, RSI extremes)
- Portfolio performance summary

### 3.3 Monthly Report

```bash
docker compose exec app python -m app.cli generate-report --type monthly

# Output file: /app/reports/monthly_report_202512.txt
```

**Contents:**
- Month-over-month returns
- Sector performance analysis
- Best/worst performing stocks
- Alert frequency by stock
- Recommendation accuracy tracking *(future)*

### 3.4 Custom Queries

#### Price History Export
```bash
docker compose exec app python -m app.cli export-data --stock MTNN --days 90 --output /app/reports/mtnn_90days.csv

# CSV columns: date, close_price, change_1d_pct, change_ytd_pct, market_cap, data_quality_flag
```

#### Indicator History
```bash
docker compose exec app python -m app.cli indicator-history DANGCEM --days 30

# Shows 30 days of SMA20, SMA50, RSI, MACD, Volatility
```

#### Alert History
```bash
docker compose exec app python -m app.cli stock-alerts ZENITHBANK --days 30

# Shows all alerts triggered for ZENITHBANK in last 30 days
```

---

## 4. Data Quality

### 4.1 Quality Flags

| Flag | Meaning | Action |
|------|---------|--------|
| **GOOD** | All 4 NGX fields present | Use for analysis |
| **INCOMPLETE** | Missing 1D% only | Usable, some limitations |
| **POOR** | Missing close price or critical data | Exclude from analysis |
| **SUSPICIOUS** | Anomalous values detected | Manual review needed |
| **MISSING** | No data for date | Investigate source |
| **STALE** | Data older than expected | Check scraper |

### 4.2 Quality Report

```bash
docker compose exec app python -m app.cli check-data-quality

# Output:
# Data Quality Report (2025-12-31)
# 
# Total Records: 154
# 
# Distribution:
# ✅ GOOD: 77 (50.0%)
# ⚠️ INCOMPLETE: 77 (50.0%)
# ❌ POOR: 0 (0.0%)
# ⚠️ SUSPICIOUS: 0 (0.0%)
# ❌ MISSING: 0 (0.0%)
# 
# Stocks with Quality Issues:
# (Lists stocks with INCOMPLETE/POOR/SUSPICIOUS flags)
```

### 4.3 Handling INCOMPLETE Data

**INCOMPLETE** records (missing 1D% only) are **safe to use** for:
- ✅ Price tracking
- ✅ YTD performance analysis
- ✅ Market cap trends
- ✅ Long-term technical indicators (SMA20, SMA50)

**Limited use for:**
- ⚠️ Daily momentum analysis
- ⚠️ Intraday volatility calculations
- ⚠️ Short-term trading signals

**Impact**: ~50% of NGX stocks have no 1D% data (low trading volume)

### 4.4 Quality Monitoring

```bash
# Daily quality check
docker compose exec app python -m app.cli quality-report

# View quality trend over time
docker compose exec app python -m app.cli quality-history --days 30

# Identify stocks with persistent quality issues
docker compose exec app python -m app.cli quality-issues --threshold 0.7
# Shows stocks with < 70% GOOD quality rate
```

---

## 📊 Typical User Workflows

### Morning Routine (5 minutes)
```bash
# 1. Check overnight alerts
docker compose exec app python -m app.cli recent-alerts --days 1

# 2. View top buy recommendations
docker compose exec app python -m app.cli top-picks --signal BUY --count 5

# 3. Check pipeline status
docker compose exec app python -m app.cli pipeline-status
```

### Weekly Review (15 minutes)
```bash
# 1. Generate weekly report
docker compose exec app python -m app.cli generate-report --type weekly

# 2. Review alert history
docker compose exec app python -m app.cli recent-alerts --days 7

# 3. Check MA crossovers
docker compose exec app python -m app.cli ma-crossovers --signal golden

# 4. View quality trends
docker compose exec app python -m app.cli quality-report
```

### Monthly Analysis (30 minutes)
```bash
# 1. Generate monthly report
docker compose exec app python -m app.cli generate-report --type monthly

# 2. Review top performers
docker compose exec app python -m app.cli top-movers --days 30

# 3. Sector analysis
docker compose exec app python -m app.cli sector-performance --days 30

# 4. Export data for external analysis
docker compose exec app python -m app.cli export-data --all --days 90 --output /app/reports/q4_data.csv
```

---

## 🎯 Tips & Best Practices

### Notifications
- ✅ Use email for daily digest, Slack for critical alerts
- ✅ Add multiple recipients for team collaboration
- ✅ Test notifications after every configuration change
- ✅ Check spam folder if emails not arriving

### Advisory
- ✅ Wait until day 21+ for reliable recommendations
- ✅ Consider risk level when acting on signals
- ✅ Use stop-loss levels to protect downside
- ✅ Combine with fundamental analysis (not automated)
- ✅ Track recommendation accuracy over time

### Data Quality
- ✅ Review quality report weekly
- ✅ INCOMPLETE data is normal for low-volume stocks
- ✅ Investigate POOR/SUSPICIOUS flags immediately
- ✅ Monitor quality trends (should stay > 80% GOOD+INCOMPLETE)

### Reports
- ✅ Generate reports on weekends for weekly review
- ✅ Export data monthly for backup
- ✅ Compare reports over time to spot trends
- ✅ Share reports with team/advisor

---

**Last Updated**: December 31, 2025  
**Version**: 1.0.0
