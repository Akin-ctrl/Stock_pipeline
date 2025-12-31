# 📊 Investment Advisory System - Quick Reference

## 🚀 Quick Start

### Run Automatic Recommendations (via Pipeline)
```bash
# Generates recommendations as part of daily pipeline
python -m app.pipelines.orchestrator
```

### Test the System
```bash
# Test recommendation generation
python app/test_advisory.py
```

---

## 📋 System Overview

The advisory system generates **AI-powered stock recommendations** with:
- 🎯 **BUY/SELL/HOLD signals** (5 types: STRONG_BUY → STRONG_SELL)
- 📊 **Quality scores** (0-100 across 5 dimensions)
- 💰 **Target prices** (+10-15% for buys)
- 🛡️ **Stop-loss levels** (-5-7% protection)
- ⚖️ **Risk assessment** (LOW/MEDIUM/HIGH)
- 🔍 **Detailed reasoning** (why this recommendation)

---

## 🎯 Signal Types

| Signal | Emoji | Meaning | Action |
|--------|-------|---------|--------|
| STRONG_BUY | 🚀 | Multiple strong bullish indicators | Aggressive buy |
| BUY | 📈 | Bullish indicators | Buy |
| HOLD | ⏸️ | Mixed/neutral signals | Wait |
| SELL | 📉 | Bearish indicators | Sell |
| STRONG_SELL | ⚠️ | Multiple strong bearish indicators | Urgent sell |

---

## 📊 Score Breakdown

Each stock gets scored 0-100 in 5 areas:

1. **Technical** (30%) - RSI, MACD indicators
2. **Momentum** (25%) - Price trends
3. **Volatility** (20%) - Price stability (lower = better)
4. **Trend** (15%) - Golden/Death cross
5. **Volume** (10%) - Volume confirmation

**Categories:**
- ⭐⭐⭐⭐⭐ EXCELLENT (80-100)
- ⭐⭐⭐⭐ GOOD (60-79)
- ⭐⭐⭐ FAIR (40-59)
- ⭐⭐ POOR (20-39)
- ⭐ VERY_POOR (0-19)

---

## 💻 Code Examples

### Generate Recommendations Manually
```python
from app.config.database import get_db
from app.services.advisory import InvestmentAdvisor, SignalType
from datetime import date

db = get_db()
with db.get_session() as session:
    advisor = InvestmentAdvisor(session)
    
    # Generate recommendations
    recommendations = advisor.generate_recommendations(
        recommendation_date=date.today(),
        min_score=60.0,  # Only good stocks
        min_confidence=0.7  # High confidence
    )
    
    # Get top buy picks
    top_buys = advisor.get_top_picks(
        recommendations,
        signal_filter=SignalType.BUY,
        top_n=10
    )
    
    # Display
    for rec in top_buys:
        print(f"{rec.stock_code}: {rec.signal_type.value} "
              f"Score={rec.score:.1f} "
              f"Confidence={rec.confidence*100:.0f}%")
```

### Query Database
```python
from app.repositories import RecommendationRepository
from datetime import date

with db.get_session() as session:
    repo = RecommendationRepository(session)
    
    # Get today's recommendations
    recs = repo.get_recommendations_by_date(date.today())
    
    # Get top 5 buy picks
    top_picks = repo.get_top_picks(
        recommendation_date=date.today(),
        signal_type='BUY',
        top_n=5
    )
    
    for rec in top_picks:
        print(f"{rec.stock.symbol}: "
              f"Score={rec.overall_score} "
              f"Target=₦{rec.target_price}")
    
    # Get performance stats
    stats = repo.get_performance_stats()
    print(f"Win Rate: {stats['win_rate_pct']:.1f}%")
    print(f"Avg Return: {stats['average_return_pct']:.2f}%")
```

### Track Outcomes
```python
# Update when target price reached
repo.update_recommendation_outcome(
    recommendation_id=123,
    outcome='HIT_TARGET',
    outcome_date=date.today(),
    actual_return_pct=Decimal('12.5')
)

# Update when stop-loss hit
repo.update_recommendation_outcome(
    recommendation_id=124,
    outcome='HIT_STOP_LOSS',
    outcome_date=date.today(),
    actual_return_pct=Decimal('-6.8')
)

# Mark old recommendations as expired (30+ days)
expired = repo.mark_expired(
    cutoff_date=date.today(),
    max_days=30
)
print(f"Expired {expired} old recommendations")
```

---

## 📂 Database Schema

**Table:** `fact_recommendations`

**Key Fields:**
```sql
recommendation_id    BIGINT PRIMARY KEY
stock_id            INT (FK to dim_stocks)
recommendation_date DATE
signal_type         VARCHAR  -- STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
confidence_score    DECIMAL  -- 0-100
overall_score       DECIMAL  -- 0-100
score_category      VARCHAR  -- EXCELLENT, GOOD, FAIR, POOR, VERY_POOR
current_price       DECIMAL
target_price        DECIMAL
stop_loss           DECIMAL
risk_level          VARCHAR  -- LOW, MEDIUM, HIGH
is_active           BOOLEAN
outcome             VARCHAR  -- HIT_TARGET, HIT_STOP_LOSS, EXPIRED, ONGOING
```

---

## 🔍 How It Works

### 1. Signal Generation
```
Input: Technical indicators (RSI, MACD, SMA50, SMA200, Volume)

Process:
1. Analyze RSI → Oversold/Overbought
2. Analyze MACD → Bullish/Bearish crossover
3. Analyze MAs → Golden/Death cross
4. Analyze Volume → Confirmation
5. Aggregate all signals
6. Calculate confidence

Output: STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL + confidence %
```

### 2. Stock Scoring
```
Input: Technical indicators + price data

Process:
1. Score Technical (RSI, MACD)
2. Score Momentum (price change %)
3. Score Volatility (stability)
4. Score Trend (MA crossovers)
5. Score Volume (support)
6. Weighted average

Output: Score 0-100 + category
```

### 3. Recommendation
```
Input: Signal + Score + Current price

Process:
1. Filter by min score & confidence
2. Calculate target price (+10-15%)
3. Calculate stop loss (-5-7%)
4. Assess risk level
5. Build reasoning list

Output: Complete StockRecommendation
```

---

## ⚙️ Configuration

### Minimum Thresholds (in orchestrator)
```python
recommendations = advisor.generate_recommendations(
    min_score=40.0,      # Change to 60.0 for quality only
    min_confidence=0.5   # Change to 0.7 for high confidence
)
```

### Score Weights (in scoring.py)
```python
scorer = StockScorer(
    technical_weight=0.30,   # 30%
    momentum_weight=0.25,    # 25%
    volatility_weight=0.20,  # 20%
    trend_weight=0.15,       # 15%
    volume_weight=0.10       # 10%
)
```

### RSI Thresholds (in signals.py)
```python
generator = SignalGenerator(
    rsi_oversold=30.0,          # Buy signal
    rsi_overbought=70.0,        # Sell signal
    rsi_strong_oversold=20.0,   # Strong buy
    rsi_strong_overbought=80.0  # Strong sell
)
```

---

## 📊 Performance Metrics

### View Stats
```python
stats = repo.get_performance_stats(
    start_date=date(2025, 1, 1),
    end_date=date.today()
)

print(f"Total Recommendations: {stats['total_recommendations']}")
print(f"Win Rate: {stats['win_rate_pct']:.1f}%")
print(f"Average Return: {stats['average_return_pct']:.2f}%")
print(f"Wins: {stats['wins']}")
print(f"Losses: {stats['losses']}")

for outcome, count in stats['outcomes'].items():
    print(f"{outcome}: {count}")
```

**Expected Metrics:**
- Win Rate: 60-70% (good system)
- Average Return: 5-10%
- Hit Target: Higher than Hit Stop Loss

---

## 🎯 Common Queries

### Today's Top 10 Buy Picks
```python
top_buys = repo.get_top_picks(
    recommendation_date=date.today(),
    signal_type='BUY',
    top_n=10
)
```

### This Week's Recommendations
```python
from datetime import timedelta

week_ago = date.today() - timedelta(days=7)
recs = repo.get_recommendations_by_date_range(
    start_date=week_ago,
    end_date=date.today()
)
```

### Stock History
```python
stock_recs = repo.get_recommendations_by_stock(
    stock_id=5,
    limit=10  # Last 10 recommendations
)
```

### Active Buy Recommendations
```python
active_buys = repo.get_active_recommendations(
    signal_type='BUY'
)
```

---

## 🛠️ Troubleshooting

### No Recommendations Generated
**Problem:** `No recommendations generated for today`

**Solutions:**
1. Run the pipeline first:
   ```bash
   python -m app.pipelines.orchestrator
   ```

2. Check if indicators exist:
   ```python
   from app.repositories import IndicatorRepository
   indicators = indicator_repo.get_latest_indicators(stock_id, date.today())
   ```

3. Lower thresholds:
   ```python
   recs = advisor.generate_recommendations(
       min_score=30.0,      # Lower from 40
       min_confidence=0.3   # Lower from 0.5
   )
   ```

### All Scores Too Low
**Problem:** No stocks meeting minimum score

**Check:**
- Market conditions (bearish market = lower scores)
- Indicator data quality
- Adjust score weights if needed

---

## 📚 Related Documentation

- **Full Implementation:** [ADVISORY_SYSTEM_SUMMARY.md](ADVISORY_SYSTEM_SUMMARY.md)
- **Notification System:** [NOTIFICATION_QUICKSTART.md](NOTIFICATION_QUICKSTART.md)
- **Pipeline Guide:** [README.md](README.md)

---

## 🎓 Example Workflow

### Daily Workflow
```bash
# 1. Run pipeline (generates recommendations automatically)
python -m app.pipelines.orchestrator

# 2. View top picks
python app/test_advisory.py

# 3. Query database for specific needs
python -c "
from app.config.database import get_db
from app.repositories import RecommendationRepository
from datetime import date

db = get_db()
with db.get_session() as session:
    repo = RecommendationRepository(session)
    
    top_picks = repo.get_top_picks(
        recommendation_date=date.today(),
        signal_type='BUY',
        top_n=5
    )
    
    for rec in top_picks:
        print(f'{rec.stock.symbol}: Score={rec.overall_score} Target=₦{rec.target_price}')
"
```

### Weekly Review
```python
# Check performance
stats = repo.get_performance_stats(
    start_date=week_ago,
    end_date=date.today()
)

# Update outcomes for active recommendations
active = repo.get_active_recommendations()
for rec in active:
    # Check current price and update if target/stop-loss hit
    # (implement monitoring script)

# Mark expired
expired = repo.mark_expired(date.today(), max_days=30)
```

---

## ✅ Quick Checklist

Before using the advisory system:
- ✅ Database initialized (`fact_recommendations` table exists)
- ✅ Pipeline has run (stocks, prices, indicators loaded)
- ✅ Technical indicators calculated for recent dates
- ✅ Price data available for target date

To verify:
```bash
# Check if recommendations exist
python -c "
from app.config.database import get_db
from app.repositories import RecommendationRepository
from datetime import date

db = get_db()
with db.get_session() as session:
    repo = RecommendationRepository(session)
    count = len(repo.get_recommendations_by_date(date.today()))
    print(f'Recommendations today: {count}')
"
```

---

**System Status:** ✅ FULLY OPERATIONAL

**Ready for:** Production use, dashboard integration, performance tracking
