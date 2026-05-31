"""
Stock scoring system for investment analysis.

Scores stocks using technical and market-derived factors available in the
current pipeline. This is not yet a full fundamentals-driven scoring engine.
"""

from dataclasses import dataclass
from typing import Dict, Optional
from enum import Enum


class ScoreCategory(Enum):
    """Score categories."""
    EXCELLENT = "EXCELLENT"  # 80-100
    GOOD = "GOOD"           # 60-79
    FAIR = "FAIR"           # 40-59
    POOR = "POOR"           # 20-39
    VERY_POOR = "VERY_POOR" # 0-19


@dataclass
class StockScore:
    """
    Comprehensive stock score.
    
    Attributes:
        total_score: Overall heuristic score (0-100)
        category: Heuristic score category
        technical_score: Technical analysis score (0-100)
        momentum_score: Momentum score (0-100)
        volatility_score: Volatility/risk score (0-100)
        trend_score: Trend strength score (0-100)
        volume_score: Volume analysis score (0-100)
        breakdown: Dict of individual metric scores
    """
    total_score: float
    category: ScoreCategory
    technical_score: float
    momentum_score: float
    volatility_score: float
    trend_score: float
    volume_score: float
    breakdown: Dict[str, float]

    @property
    def heuristic_score(self) -> float:
        """Explicit alias for the handcrafted heuristic score."""
        return self.total_score

    @property
    def heuristic_score_category(self) -> ScoreCategory:
        """Explicit alias for the handcrafted heuristic score category."""
        return self.category


class StockScorer:
    """
    Scores stocks based on multiple factors.
    
    Analyzes technical indicators, momentum, volatility, and trends
    to produce a comprehensive score for investment ranking.
    """
    
    def __init__(
        self,
        technical_weight: float = 0.30,
        momentum_weight: float = 0.25,
        volatility_weight: float = 0.20,
        trend_weight: float = 0.15,
        volume_weight: float = 0.10
    ):
        """
        Initialize scorer with category weights.
        
        Args:
            technical_weight: Weight for technical indicators
            momentum_weight: Weight for momentum metrics
            volatility_weight: Weight for volatility/risk
            trend_weight: Weight for trend strength
            volume_weight: Weight for volume analysis
        """
        self.weights = {
            'technical': technical_weight,
            'momentum': momentum_weight,
            'volatility': volatility_weight,
            'trend': trend_weight,
            'volume': volume_weight
        }
        
        # Normalize weights
        total = sum(self.weights.values())
        self.weights = {k: v/total for k, v in self.weights.items()}
    
    def calculate_score(
        self,
        indicators: Dict[str, float],
        price_history: Optional[list] = None
    ) -> StockScore:
        """
        Calculate comprehensive stock score.
        
        Args:
            indicators: Dict of technical indicators
                - rsi_14: 14-day RSI
                - macd: MACD value
                - macd_signal: MACD signal line
                - ma_30: 30-day SMA
                - ma_90: 90-day SMA
                - current_price: Current price
                - volume_ratio: Volume vs average
                - volatility: Price volatility
                - price_change_pct: Recent price change %
            price_history: Optional list of recent prices
            
        Returns:
            StockScore with detailed breakdown
        """
        breakdown = {}
        
        # Technical Score (0-100)
        tech_score = self._score_technical_indicators(indicators, breakdown)
        
        # Momentum Score (0-100)
        momentum_score = self._score_momentum(indicators, breakdown)
        
        # Volatility Score (0-100) - Lower volatility = higher score
        volatility_score = self._score_volatility(indicators, breakdown)
        
        # Trend Score (0-100)
        trend_score = self._score_trend(indicators, breakdown)
        
        # Volume Score (0-100)
        volume_score = self._score_volume(indicators, breakdown)
        
        # Calculate weighted total
        base_total_score = (
            tech_score * self.weights['technical'] +
            momentum_score * self.weights['momentum'] +
            volatility_score * self.weights['volatility'] +
            trend_score * self.weights['trend'] +
            volume_score * self.weights['volume']
        )

        trust_multiplier = self._calculate_trust_multiplier(
            indicators,
            price_history,
            breakdown,
        )
        total_score = base_total_score * trust_multiplier
        
        # Determine category
        category = self._categorize_score(total_score)
        
        return StockScore(
            total_score=round(total_score, 2),
            category=category,
            technical_score=round(tech_score, 2),
            momentum_score=round(momentum_score, 2),
            volatility_score=round(volatility_score, 2),
            trend_score=round(trend_score, 2),
            volume_score=round(volume_score, 2),
            breakdown=breakdown
        )

    def _calculate_trust_multiplier(
        self,
        indicators: Dict[str, float],
        price_history: Optional[list],
        breakdown: Dict[str, float],
    ) -> float:
        """Penalize scores when the underlying market data is weak or too short."""
        trusted_history_days = indicators.get('trusted_history_days')
        if trusted_history_days is None:
            trusted_history_days = len(price_history) if price_history is not None else 0

        if trusted_history_days >= 90:
            history_multiplier = 1.0
        elif trusted_history_days >= 60:
            history_multiplier = 0.85
        elif trusted_history_days >= 30:
            history_multiplier = 0.65
        else:
            history_multiplier = 0.40

        quality_flag = indicators.get('price_quality_flag')
        if quality_flag == 'GOOD':
            quality_multiplier = 1.0
        elif quality_flag == 'INCOMPLETE':
            quality_multiplier = 0.9
        else:
            quality_multiplier = 0.5

        confidence_score = indicators.get('price_confidence_score')
        if confidence_score is None:
            confidence_multiplier = 0.85
        elif confidence_score >= 80:
            confidence_multiplier = 1.0
        elif confidence_score >= 70:
            confidence_multiplier = 0.9
        elif confidence_score >= 60:
            confidence_multiplier = 0.75
        else:
            confidence_multiplier = 0.5

        bar_status = indicators.get('bar_status')
        if bar_status in ('RECONCILED', 'OFFICIAL'):
            bar_status_multiplier = 1.0
        elif bar_status == 'OBSERVED':
            bar_status_multiplier = 0.8
        else:
            bar_status_multiplier = 0.6

        trust_multiplier = (
            history_multiplier *
            quality_multiplier *
            confidence_multiplier *
            bar_status_multiplier
        )
        breakdown['history_multiplier'] = round(history_multiplier, 2)
        breakdown['quality_multiplier'] = round(quality_multiplier, 2)
        breakdown['confidence_multiplier'] = round(confidence_multiplier, 2)
        breakdown['bar_status_multiplier'] = round(bar_status_multiplier, 2)
        breakdown['trust_multiplier'] = round(trust_multiplier, 2)

        return trust_multiplier
    
    def _score_technical_indicators(
        self,
        indicators: Dict[str, float],
        breakdown: Dict[str, float]
    ) -> float:
        """Score based on technical indicators."""
        scores = []
        
        # RSI Score
        rsi = indicators.get('rsi_14')
        if rsi is not None:
            # Ideal RSI between 40-60 (neutral to slightly bullish)
            if 40 <= rsi <= 60:
                rsi_score = 100
            elif 30 <= rsi < 40 or 60 < rsi <= 70:
                rsi_score = 80
            elif 20 <= rsi < 30 or 70 < rsi <= 80:
                rsi_score = 60
            elif rsi < 20:
                rsi_score = 40  # Oversold - potential bounce
            else:  # rsi > 80
                rsi_score = 20  # Overbought - risky
            
            scores.append(rsi_score)
            breakdown['rsi_score'] = rsi_score
        
        # MACD Score
        macd = indicators.get('macd')
        macd_signal = indicators.get('macd_signal')
        if macd is not None and macd_signal is not None:
            diff = macd - macd_signal
            if diff > 0 and macd > 0:
                macd_score = 100  # Strong bullish
            elif diff > 0:
                macd_score = 80   # Bullish
            elif diff < 0 and macd < 0:
                macd_score = 20   # Strong bearish
            elif diff < 0:
                macd_score = 40   # Bearish
            else:
                macd_score = 60   # Neutral
            
            scores.append(macd_score)
            breakdown['macd_score'] = macd_score
        
        return sum(scores) / len(scores) if scores else 50.0
    
    def _score_momentum(
        self,
        indicators: Dict[str, float],
        breakdown: Dict[str, float]
    ) -> float:
        """Score based on price momentum."""
        scores = []
        
        # Price Change Score
        price_change = indicators.get('price_change_pct')
        if price_change is not None:
            # Positive momentum is good, but not extreme
            if 2 <= price_change <= 5:
                momentum_score = 100
            elif 5 < price_change <= 10:
                momentum_score = 80
            elif 0 <= price_change < 2:
                momentum_score = 70
            elif 10 < price_change <= 15:
                momentum_score = 60
            elif price_change > 15:
                momentum_score = 40  # Too hot - potential pullback
            elif -2 <= price_change < 0:
                momentum_score = 50
            elif -5 <= price_change < -2:
                momentum_score = 30
            else:  # price_change < -5
                momentum_score = 20
            
            scores.append(momentum_score)
            breakdown['momentum_score'] = momentum_score

        change_10d = indicators.get('price_change_10d')
        if change_10d is not None:
            if 5 <= change_10d <= 20:
                score_10d = 100
            elif 20 < change_10d <= 30:
                score_10d = 80
            elif 0 <= change_10d < 5:
                score_10d = 70
            elif 30 < change_10d <= 40:
                score_10d = 60
            elif change_10d > 40:
                score_10d = 40  # Too hot - pullback risk
            elif -5 <= change_10d < 0:
                score_10d = 50
            elif -10 <= change_10d < -5:
                score_10d = 30
            else:
                score_10d = 20
            scores.append(score_10d)
            breakdown['momentum_10d_score'] = score_10d

        change_20d = indicators.get('price_change_20d')
        if change_20d is not None:
            if 8 <= change_20d <= 25:
                score_20d = 100
            elif 25 < change_20d <= 35:
                score_20d = 80
            elif 0 <= change_20d < 8:
                score_20d = 70
            elif 35 < change_20d <= 45:
                score_20d = 60
            elif change_20d > 45:
                score_20d = 40
            elif -5 <= change_20d < 0:
                score_20d = 50
            elif -12 <= change_20d < -5:
                score_20d = 30
            else:
                score_20d = 20
            scores.append(score_20d)
            breakdown['momentum_20d_score'] = score_20d
        
        # Price vs Moving Averages
        price = indicators.get('current_price')
        ma_30 = indicators.get('ma_30')
        
        if price is not None and ma_30 is not None:
            price_vs_ma30 = ((price - ma_30) / ma_30) * 100
            if price_vs_ma30 > 0:
                ma_score = min(100, 70 + price_vs_ma30 * 3)
            else:
                ma_score = max(20, 50 + price_vs_ma30 * 2)
            
            scores.append(ma_score)
            breakdown['ma_momentum_score'] = ma_score
        
        return sum(scores) / len(scores) if scores else 50.0
    
    def _score_volatility(
        self,
        indicators: Dict[str, float],
        breakdown: Dict[str, float]
    ) -> float:
        """Score based on volatility (lower is better for stability)."""
        volatility = indicators.get('volatility')
        
        if volatility is None:
            return 50.0
        
        # Volatility is annualized decimal (for example 0.30 = 30%).
        # Lower volatility = higher score
        if volatility < 0.20:
            vol_score = 100  # Very stable
        elif volatility < 0.30:
            vol_score = 85   # Stable
        elif volatility < 0.40:
            vol_score = 70   # Moderate
        elif volatility < 0.50:
            vol_score = 55   # Somewhat volatile
        elif volatility < 0.70:
            vol_score = 40   # Volatile
        else:
            vol_score = 20   # Highly volatile
        
        breakdown['volatility_score'] = vol_score
        return vol_score
    
    def _score_trend(
        self,
        indicators: Dict[str, float],
        breakdown: Dict[str, float]
    ) -> float:
        """Score based on trend strength."""
        scores = []
        
        ma_30 = indicators.get('ma_30')
        ma_90 = indicators.get('ma_90')
        
        if ma_30 is not None and ma_90 is not None:
            # Trend comparison using the actual 30/90-day averages produced by the indicator layer
            ma_diff_pct = ((ma_30 - ma_90) / ma_90) * 100
            
            if ma_diff_pct > 5:
                trend_score = 100  # Strong uptrend
            elif ma_diff_pct > 2:
                trend_score = 85   # Uptrend
            elif ma_diff_pct > 0:
                trend_score = 70   # Weak uptrend
            elif ma_diff_pct > -2:
                trend_score = 50   # Weak downtrend
            elif ma_diff_pct > -5:
                trend_score = 35   # Downtrend
            else:
                trend_score = 20   # Strong downtrend
            
            scores.append(trend_score)
            breakdown['trend_score'] = trend_score
        
        # ADX-like trend strength (if available)
        # For now, use price vs moving averages as proxy
        price = indicators.get('current_price')
        if price is not None and ma_30 is not None and ma_90 is not None:
            if price > ma_30 > ma_90:
                strength_score = 100  # Clear uptrend
            elif price > ma_30 or price > ma_90:
                strength_score = 70   # Mixed trend
            elif price < ma_30 < ma_90:
                strength_score = 20   # Clear downtrend
            else:
                strength_score = 40   # Mixed trend
            
            scores.append(strength_score)
            breakdown['trend_strength_score'] = strength_score
        
        return sum(scores) / len(scores) if scores else 50.0
    
    def _score_volume(
        self,
        indicators: Dict[str, float],
        breakdown: Dict[str, float]
    ) -> float:
        """Score based on volume analysis."""
        volume_ratio = indicators.get('volume_ratio')
        
        if volume_ratio is None:
            return 50.0
        
        # Higher volume on uptrends is positive
        price_change = indicators.get('price_change_pct', 0)
        
        if price_change > 0:
            # Upward price movement
            if volume_ratio > 2.0:
                vol_score = 100  # Strong volume support
            elif volume_ratio > 1.5:
                vol_score = 85   # Good volume
            elif volume_ratio > 1.0:
                vol_score = 70   # Average volume
            else:
                vol_score = 50   # Weak volume (concerning)
        else:
            # Downward price movement
            if volume_ratio > 2.0:
                vol_score = 30   # High volume selling (bad)
            elif volume_ratio > 1.5:
                vol_score = 40   # Increased selling
            elif volume_ratio > 1.0:
                vol_score = 55   # Normal volume
            else:
                vol_score = 70   # Low volume decline (less concerning)
        
        breakdown['volume_score'] = vol_score
        return vol_score
    
    def _categorize_score(self, score: float) -> ScoreCategory:
        """Categorize numerical score."""
        if score >= 80:
            return ScoreCategory.EXCELLENT
        elif score >= 60:
            return ScoreCategory.GOOD
        elif score >= 40:
            return ScoreCategory.FAIR
        elif score >= 20:
            return ScoreCategory.POOR
        else:
            return ScoreCategory.VERY_POOR
    
    def get_score_summary(self, score: StockScore) -> str:
        """
        Get human-readable score summary.
        
        Args:
            score: StockScore object
            
        Returns:
            Summary string
        """
        emoji_map = {
            ScoreCategory.EXCELLENT: "⭐⭐⭐⭐⭐",
            ScoreCategory.GOOD: "⭐⭐⭐⭐",
            ScoreCategory.FAIR: "⭐⭐⭐",
            ScoreCategory.POOR: "⭐⭐",
            ScoreCategory.VERY_POOR: "⭐"
        }
        
        stars = emoji_map.get(score.category, "")
        
        summary = f"{stars} Overall Score: {score.total_score:.1f}/100 ({score.category.value})\n"
        summary += "\nScore Breakdown:\n"
        summary += f"  Technical:  {score.technical_score:.1f}/100\n"
        summary += f"  Momentum:   {score.momentum_score:.1f}/100\n"
        summary += f"  Volatility: {score.volatility_score:.1f}/100\n"
        summary += f"  Trend:      {score.trend_score:.1f}/100\n"
        summary += f"  Volume:     {score.volume_score:.1f}/100\n"
        
        return summary
