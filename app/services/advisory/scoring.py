"""
Stock scoring system for investment analysis.

Scores stocks based on multiple technical and fundamental factors.
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
        total_score: Overall score (0-100)
        category: Score category
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
                - sma_50: 50-day SMA
                - sma_200: 200-day SMA
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
        total_score = (
            tech_score * self.weights['technical'] +
            momentum_score * self.weights['momentum'] +
            volatility_score * self.weights['volatility'] +
            trend_score * self.weights['trend'] +
            volume_score * self.weights['volume']
        )
        
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
        
        # Price vs Moving Averages
        price = indicators.get('current_price')
        sma_50 = indicators.get('sma_50')
        sma_200 = indicators.get('sma_200')
        
        if price and sma_50:
            price_vs_sma50 = ((price - sma_50) / sma_50) * 100
            if price_vs_sma50 > 0:
                ma_score = min(100, 70 + price_vs_sma50 * 3)
            else:
                ma_score = max(20, 50 + price_vs_sma50 * 2)
            
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
        
        # Lower volatility = higher score
        # Typical stock volatility: 20-40% annually (1.6-3.3% monthly)
        if volatility < 2.0:
            vol_score = 100  # Very stable
        elif volatility < 3.0:
            vol_score = 85   # Stable
        elif volatility < 4.0:
            vol_score = 70   # Moderate
        elif volatility < 5.0:
            vol_score = 55   # Somewhat volatile
        elif volatility < 7.0:
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
        
        sma_50 = indicators.get('sma_50')
        sma_200 = indicators.get('sma_200')
        
        if sma_50 and sma_200:
            # Golden Cross / Death Cross
            sma_diff_pct = ((sma_50 - sma_200) / sma_200) * 100
            
            if sma_diff_pct > 5:
                trend_score = 100  # Strong uptrend
            elif sma_diff_pct > 2:
                trend_score = 85   # Uptrend
            elif sma_diff_pct > 0:
                trend_score = 70   # Weak uptrend
            elif sma_diff_pct > -2:
                trend_score = 50   # Weak downtrend
            elif sma_diff_pct > -5:
                trend_score = 35   # Downtrend
            else:
                trend_score = 20   # Strong downtrend
            
            scores.append(trend_score)
            breakdown['trend_score'] = trend_score
        
        # ADX-like trend strength (if available)
        # For now, use price vs moving averages as proxy
        price = indicators.get('current_price')
        if price and sma_50 and sma_200:
            if price > sma_50 > sma_200:
                strength_score = 100  # Clear uptrend
            elif price > sma_50 or price > sma_200:
                strength_score = 70   # Mixed trend
            elif price < sma_50 < sma_200:
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
