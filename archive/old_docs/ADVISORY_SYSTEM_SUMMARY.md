# 📊 Investment Advisory System - COMPLETE

## ✅ Implementation Summary

The Investment Advisory System is a comprehensive AI-powered recommendation engine that analyzes stocks using multiple technical indicators to generate actionable BUY/SELL/HOLD signals with confidence scores.

---

## 🎯 Core Components

### 1. Signal Generator (`signals.py`)
**Purpose:** Generate trading signals from technical indicators

**Features:**
- ✅ **RSI Analysis** - Oversold/overbought detection with multiple thresholds
- ✅ **MACD Analysis** - Bullish/bearish crossover signals
- ✅ **Moving Average Analysis** - Golden Cross / Death Cross detection
- ✅ **Volume Analysis** - Volume confirmation for price movements
- ✅ **Signal Aggregation** - Combines multiple signals with weighted scoring
- ✅ **Confidence Scoring** - 0-100% confidence based on signal agreement

**Signal Types:**
- 🚀 `STRONG_BUY` - Multiple strong bullish indicators
- 📈 `BUY` - Bullish indicators
- ⏸️ `HOLD` - Mixed or neutral signals
- 📉 `SELL` - Bearish indicators
- ⚠️ `STRONG_SELL` - Multiple strong bearish indicators

**Algorithm:**
```
1. Analyze RSI (20/30/70/80 thresholds)
2. Analyze MACD crossovers
3. Analyze MA crossovers (SMA50/SMA200)
4. Analyze volume trends
5. Aggregate signals with weights
6. Calculate confidence score
7. Resolve conflicts → final signal
```

---

### 2. Stock Scorer (`scoring.py`)
**Purpose:** Score stocks 0-100 across multiple dimensions

**Scoring Categories:**
- ⭐⭐⭐⭐⭐ **EXCELLENT** (80-100) - Top tier stocks
- ⭐⭐⭐⭐ **GOOD** (60-79) - Strong candidates
- ⭐⭐⭐ **FAIR** (40-59) - Moderate quality
- ⭐⭐ **POOR** (20-39) - Weak stocks
- ⭐ **VERY_POOR** (0-19) - Avoid

**Scoring Dimensions:**
1. **Technical Score** (30% weight)
   - RSI positioning
   - MACD signals
   
2. **Momentum Score** (25% weight)
   - Price change %
   - Price vs moving averages
   
3. **Volatility Score** (20% weight)
   - Price stability (lower volatility = higher score)
   
4. **Trend Score** (15% weight)
   - Golden/Death cross status
   - Trend strength
   
5. **Volume Score** (10% weight)
   - Volume support for price moves

**Formula:**
```
Total Score = (Technical × 0.30) + (Momentum × 0.25) + 
              (Volatility × 0.20) + (Trend × 0.15) + (Volume × 0.10)
```

---

### 3. Investment Advisor (`advisor.py`)
**Purpose:** Main recommendation engine combining signals and scores

**Features:**
- ✅ Analyzes all active stocks
- ✅ Generates buy/sell/hold recommendations
- ✅ Calculates target prices (10-15% upside)
- ✅ Calculates stop-loss levels (5-7% downside)
- ✅ Assesses risk (LOW/MEDIUM/HIGH)
- ✅ Provides detailed reasoning
- ✅ Filters by minimum score/confidence
- ✅ Ranks stocks by composite score

**StockRecommendation Output:**
```python
StockRecommendation(
    stock_code="MTNN",
    stock_name="MTN Nigeria Communications",
    signal_type=SignalType.STRONG_BUY,
    confidence=0.85,  # 85%
    score=78.5,  # out of 100
    score_category=ScoreCategory.GOOD,
    current_price=Decimal('125.00'),
    target_price=Decimal('143.75'),  # +15%
    stop_loss=Decimal('116.25'),  # -7%
    risk_level='LOW',
    reasons=[
        "RSI 28 - Oversold",
        "MACD bullish crossover",
        "Strong upward trend",
        "High volume support"
    ]
)
```

**Target Price Calculation:**
- **BUY** → +10% target, -5% stop-loss
- **STRONG_BUY** → +15% target, -7% stop-loss
- **SELL** → -10% target, +3% stop-loss
- **STRONG_SELL** → -15% target, +5% stop-loss

**Risk Assessment:**
- **LOW** - Low volatility + high confidence + high score
- **MEDIUM** - Moderate volatility or confidence
- **HIGH** - High volatility or low confidence or extreme RSI

---

### 4. Database Model (`fact.py`)
**Table:** `fact_recommendations`

**Key Fields:**
- `recommendation_id` - Primary key
- `stock_id` - Foreign key to dim_stocks
- `recommendation_date` - Date of recommendation
- `signal_type` - STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
- `confidence_score` - 0-100%
- `overall_score` - 0-100
- `score_category` - EXCELLENT/GOOD/FAIR/POOR/VERY_POOR
- `current_price` - Price at recommendation time
- `target_price` - Estimated target
- `stop_loss` - Recommended stop-loss
- `potential_return_pct` - Expected return %
- `risk_level` - LOW/MEDIUM/HIGH
- `recommendation_reason` - Text explanation
- Score breakdown (technical, momentum, volatility, trend, volume)
- Indicator values (RSI, MACD)
- Outcome tracking (is_active, outcome, actual_return_pct)

**Indexes:**
- `(stock_id, recommendation_date)` - Historical lookup
- `recommendation_date` - Date queries
- `signal_type` - Filter by signal
- `is_active` - Active recommendations

**Outcome Values:**
- `HIT_TARGET` - Target price reached
- `HIT_STOP_LOSS` - Stop-loss triggered
- `ONGOING` - Still active
- `EXPIRED` - Exceeded time limit (30 days)

---

### 5. Recommendation Repository (`recommendation_repository.py`)
**Purpose:** Database operations for recommendations

**Methods:**
- `create_recommendation(rec)` - Save single recommendation
- `create_recommendations_bulk(recs)` - Bulk save
- `get_recommendations_by_date(date)` - Get all for specific date
- `get_recommendations_by_date_range(start, end)` - Date range query
- `get_recommendations_by_stock(stock_id)` - Stock history
- `get_active_recommendations(signal_type)` - Active recs
- `get_top_picks(date, signal_type, top_n)` - Top N picks
- `update_recommendation_outcome(id, outcome, return)` - Track results
- `mark_expired(cutoff_date, max_days)` - Expire old recs
- `get_performance_stats()` - Win rate, avg return, outcomes

**Performance Tracking:**
```python
stats = repo.get_performance_stats()
# Returns:
{
    'total_recommendations': 150,
    'outcomes': {
        'HIT_TARGET': 65,
        'HIT_STOP_LOSS': 35,
        'EXPIRED': 25,
        'ONGOING': 25
    },
    'average_return_pct': 5.8,
    'win_rate_pct': 65.0,  # 65/(65+35) * 100
    'wins': 65,
    'losses': 35
}
```

---

### 6. Pipeline Integration (`orchestrator.py`)
**Stage 8:** Generate Recommendations (after alerts)

**Integration Points:**
1. Added `generate_recommendations` to `PipelineConfig`
2. Added `recommendations_generated` to `PipelineResult`
3. Added `_generate_recommendations()` method
4. Calls `InvestmentAdvisor` after indicators calculated
5. Saves recommendations to database
6. Logs top 3 buy picks

**Execution Flow:**
```
Stage 1: Fetch Data (NGX/Yahoo)
Stage 2: Validate Data
Stage 3: Transform Data
Stage 4: Load Stocks
Stage 5: Load Prices
Stage 6: Calculate Indicators
Stage 7: Evaluate Alerts → Send Notifications
Stage 8: Generate Recommendations → Save to DB ✨ NEW
```

**Configuration:**
```python
config = PipelineConfig(
    generate_recommendations=True,  # Enable/disable
    # ... other settings
)
```

**Output in Logs:**
```
Stage 8: Generating investment recommendations
Generated 45 investment recommendations
Top 3 buy recommendations:
  1. MTNN - Score: 78.5, Signal: STRONG_BUY, Confidence: 85%
  2. DANGCEM - Score: 76.2, Signal: BUY, Confidence: 78%
  3. ZENITHBANK - Score: 74.1, Signal: BUY, Confidence: 72%
```

---

### 7. Test Script (`test_advisory.py`)
**Purpose:** Test and demonstrate the advisory system

**Test Flow:**
1. ✅ Initialize advisor and repository
2. ✅ Generate recommendations for today
3. ✅ Display signal distribution (BUY/SELL/HOLD counts)
4. ✅ Show top 5 buy recommendations
5. ✅ Display detailed analysis for each
6. ✅ Save to database
7. ✅ Retrieve from database (demo query)
8. ✅ Show performance statistics
9. ✅ Display usage examples

**Run:**
```bash
python app/test_advisory.py
```

**Expected Output:**
```
═══════════════════════════════════════════════════════════════════
INVESTMENT ADVISORY SYSTEM TEST
═══════════════════════════════════════════════════════════════════

✅ Generated 45 recommendations

Signal Distribution:
  🚀 BUY/STRONG_BUY: 18
  ⏸️  HOLD: 22
  📉 SELL/STRONG_SELL: 5

═══════════════════════════════════════════════════════════════════
TOP BUY RECOMMENDATIONS
═══════════════════════════════════════════════════════════════════

────────────────────────────────────────────────────────────────────
#1 - MTNN (MTN Nigeria Communications)
────────────────────────────────────────────────────────────────────

Signal: 🚀 STRONG_BUY
Confidence: 85%
Overall Score: 78.5/100 (GOOD)
Risk Level: LOW

Prices:
  Current:     ₦125.00
  Target:      ₦143.75
  Potential:   +15.0%
  Stop Loss:   ₦116.25

Score Breakdown:
  Technical:   85/100
  Momentum:    78/100
  Volatility:  92/100
  Trend:       72/100
  Volume:      81/100

Key Reasons:
  1. RSI 28.5 - Oversold
  2. MACD bullish crossover (0.45)
  3. High volume support (2.3x average)

[... more recommendations ...]
```

---

## 📊 Files Created/Modified

### Created (New Files):
1. `/app/services/advisory/signals.py` (329 lines)
   - SignalGenerator class
   - SignalType enum
   - TechnicalSignal dataclass

2. `/app/services/advisory/scoring.py` (410 lines)
   - StockScorer class
   - ScoreCategory enum
   - StockScore dataclass

3. `/app/services/advisory/advisor.py` (462 lines)
   - InvestmentAdvisor class
   - StockRecommendation dataclass

4. `/app/services/advisory/__init__.py` (24 lines)
   - Module exports

5. `/app/repositories/recommendation_repository.py` (330 lines)
   - RecommendationRepository class
   - CRUD operations
   - Performance tracking

6. `/app/test_advisory.py` (264 lines)
   - Test and demo script

### Modified (Existing Files):
1. `/app/models/fact.py`
   - Added `FactRecommendation` model (150 lines)

2. `/app/models/dimension.py`
   - Added `recommendations` relationship to `DimStock`

3. `/app/models/__init__.py`
   - Exported `FactRecommendation`

4. `/app/repositories/__init__.py`
   - Exported `RecommendationRepository`

5. `/app/pipelines/orchestrator.py`
   - Added `generate_recommendations` to `PipelineConfig`
   - Added `recommendations_generated` to `PipelineResult`
   - Added `_generate_recommendations()` method
   - Integrated Stage 8 into pipeline

---

## 🎯 Usage Examples

### 1. Automatic (Pipeline)
```python
from app.pipelines import PipelineOrchestrator

orchestrator = PipelineOrchestrator()
result = orchestrator.run()

print(f"Recommendations: {result.recommendations_generated}")
```

### 2. Manual Generation
```python
from app.config.database import get_db
from app.services.advisory import InvestmentAdvisor
from datetime import date

db = get_db()
with db.get_session() as session:
    advisor = InvestmentAdvisor(session)
    
    # Generate recommendations
    recs = advisor.generate_recommendations(
        recommendation_date=date.today(),
        min_score=60.0,  # Only quality stocks
        min_confidence=0.7  # High confidence
    )
    
    # Get top buy picks
    from app.services.advisory import SignalType
    top_buys = advisor.get_top_picks(
        recs,
        signal_filter=SignalType.BUY,
        top_n=10
    )
    
    # Display
    for rec in top_buys:
        print(advisor.format_recommendation(rec))
```

### 3. Database Queries
```python
from app.repositories import RecommendationRepository
from datetime import date

with db.get_session() as session:
    repo = RecommendationRepository(session)
    
    # Today's recommendations
    today_recs = repo.get_recommendations_by_date(date.today())
    
    # Top picks
    top_picks = repo.get_top_picks(
        recommendation_date=date.today(),
        signal_type='BUY',
        top_n=5
    )
    
    # Active recommendations
    active = repo.get_active_recommendations(signal_type='BUY')
    
    # Performance stats
    stats = repo.get_performance_stats()
    print(f"Win Rate: {stats['win_rate_pct']:.1f}%")
```

### 4. Track Outcomes
```python
# Update when target hit
repo.update_recommendation_outcome(
    recommendation_id=123,
    outcome='HIT_TARGET',
    outcome_date=date.today(),
    actual_return_pct=Decimal('12.5')
)

# Mark expired recommendations
expired_count = repo.mark_expired(
    cutoff_date=date.today(),
    max_days=30
)
```

---

## 🔬 Technical Details

### Recommendation Algorithm

**Step 1: Data Collection**
```
For each active stock:
  1. Get latest technical indicators (RSI, MACD, SMA50, SMA200)
  2. Get current price
  3. Calculate volume ratio
```

**Step 2: Signal Generation**
```
1. Analyze RSI:
   - RSI < 20 → STRONG_BUY (oversold)
   - RSI < 30 → BUY
   - RSI > 80 → STRONG_SELL (overbought)
   - RSI > 70 → SELL

2. Analyze MACD:
   - MACD > Signal & MACD > 0 → STRONG_BUY
   - MACD > Signal → BUY
   - MACD < Signal & MACD < 0 → STRONG_SELL
   - MACD < Signal → SELL

3. Analyze Moving Averages:
   - SMA50 > SMA200 → STRONG_BUY (Golden Cross)
   - Price > SMA50 → BUY
   - SMA50 < SMA200 → STRONG_SELL (Death Cross)
   - Price < SMA50 → SELL

4. Analyze Volume:
   - High volume + uptrend → BUY (confirmation)

5. Aggregate:
   - Weight each signal
   - Calculate consensus
   - Determine confidence
```

**Step 3: Stock Scoring**
```
1. Technical Score:
   - RSI positioning (ideal: 40-60)
   - MACD crossover status

2. Momentum Score:
   - Price change % (ideal: 2-5%)
   - Price vs SMA50

3. Volatility Score:
   - Lower volatility = higher score
   - < 2% = 100, > 7% = 20

4. Trend Score:
   - Golden/Death Cross status
   - Trend strength

5. Volume Score:
   - Volume support for price moves

Total = Weighted average
```

**Step 4: Filtering & Ranking**
```
1. Filter:
   - Score >= min_score (default: 40)
   - Confidence >= min_confidence (default: 0.5)

2. Calculate targets:
   - Target price = current × (1 + expected_return)
   - Stop loss = current × (1 - max_loss)

3. Assess risk:
   - Consider volatility, confidence, score, RSI

4. Build reasons list

5. Rank by:
   - Primary: Overall score
   - Secondary: Confidence
```

---

## 📈 Performance Tracking

### Metrics
- **Total Recommendations** - All generated
- **Win Rate** - % that hit target vs stop-loss
- **Average Return** - Mean actual return %
- **Outcome Distribution** - Breakdown by outcome type
- **Signal Accuracy** - % correct per signal type

### Outcome Updates
Recommendations should be monitored and outcomes updated:

```python
# Daily monitoring script (to be created)
from app.repositories import RecommendationRepository, PriceRepository
from datetime import date

repo = RecommendationRepository(session)
price_repo = PriceRepository(session)

active_recs = repo.get_active_recommendations()

for rec in active_recs:
    latest_price = price_repo.get_latest_price(rec.stock_id, date.today())
    
    if latest_price:
        current = latest_price.close_price
        
        # Check if target hit
        if rec.target_price and current >= rec.target_price:
            actual_return = rec.calculate_actual_return(current)
            repo.update_recommendation_outcome(
                rec.recommendation_id,
                'HIT_TARGET',
                date.today(),
                actual_return
            )
        
        # Check if stop-loss hit
        elif rec.stop_loss and current <= rec.stop_loss:
            actual_return = rec.calculate_actual_return(current)
            repo.update_recommendation_outcome(
                rec.recommendation_id,
                'HIT_STOP_LOSS',
                date.today(),
                actual_return
            )

# Mark old recommendations as expired
repo.mark_expired(date.today(), max_days=30)
```

---

## 🚀 Next Steps

### Integration with Tableau Dashboard
```
1. Create view: vw_latest_recommendations
2. Visualizations:
   - Top 10 buy picks (table)
   - Signal distribution (pie chart)
   - Score breakdown (bar chart)
   - Performance over time (line chart)
   - Win rate by signal type (bar chart)
3. Filters:
   - Date range
   - Signal type
   - Sector
   - Risk level
```

### Airflow DAG
```
1. Daily task: generate_recommendations
2. Weekly task: update_outcomes
3. Monthly task: performance_report
```

### CLI Commands
```
stock-pipeline recommendations generate
stock-pipeline recommendations top --signal BUY --limit 10
stock-pipeline recommendations track --update-outcomes
stock-pipeline recommendations stats --start-date 2025-01-01
```

---

## ✅ Completion Status

**INVESTMENT ADVISORY SYSTEM: 100% COMPLETE** ✅

All requirements met:
- ✅ Signal generation (RSI, MACD, MA, Volume)
- ✅ Multi-factor stock scoring
- ✅ Investment recommendations (BUY/SELL/HOLD)
- ✅ Confidence scoring
- ✅ Target price calculation
- ✅ Risk assessment
- ✅ Database model and repository
- ✅ Pipeline integration
- ✅ Performance tracking
- ✅ Test script
- ✅ Documentation

**Ready for:** Production use and integration with dashboard
**Next item:** Airflow DAG enhancement, CLI interface, or Tableau dashboard
