"""
Stock screening service.

Generates trading signals and stock scores by combining
technical signals, stock scores, and risk analysis.

**DISCLAIMER**: This is a technical analysis screening tool for educational purposes.
Not financial advice. Always do your own research.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional, Dict
from decimal import Decimal

from app.services.advisory.signals import (
    SignalGenerator, SignalType, TechnicalSignal
)
from app.services.advisory.scoring import StockScorer, StockScore, ScoreCategory
from app.repositories import IndicatorRepository, PriceRepository, StockRepository
from app.utils import get_logger


@dataclass
class StockRecommendation:
    """
    Complete stock recommendation.
    
    Attributes:
        stock_id: Stock ID
        stock_code: Stock symbol/code
        stock_name: Stock company name
        recommendation_date: Date of recommendation
        signal_type: Trading signal (BUY/SELL/HOLD)
        confidence: Signal confidence (0-1)
        score: Overall stock score (0-100)
        score_category: Score category
        current_price: Current stock price
        target_price: Estimated target price
        stop_loss: Recommended stop-loss price
        risk_level: Risk assessment (LOW/MEDIUM/HIGH)
        reasons: List of recommendation reasons
        indicators: Dict of indicator values
        technical_signal: Full TechnicalSignal object
        stock_score: Full StockScore object
    """
    stock_id: int
    stock_code: str
    stock_name: str
    recommendation_date: date
    signal_type: SignalType
    confidence: float
    score: float
    score_category: ScoreCategory
    current_price: Decimal
    target_price: Optional[Decimal]
    stop_loss: Optional[Decimal]
    risk_level: str
    reasons: List[str]
    indicators: Dict[str, float]
    technical_signal: TechnicalSignal
    stock_score: StockScore


class StockScreener:
    """
    Generates stock screening signals and scores.
    
    Combines technical analysis, scoring, and risk assessment
    to produce BUY/SELL/HOLD signals with confidence levels.
    
    **NOT INVESTMENT ADVICE** - For screening and analysis only.
    """
    
    def __init__(self, db_session):
        """
        Initialize advisor.
        
        Args:
            db_session: Database session
        """
        self.db = db_session
        self.logger = get_logger("stock_screener")
        
        self.signal_generator = SignalGenerator()
        self.stock_scorer = StockScorer()
        
        self.stock_repo = StockRepository(db_session)
        self.price_repo = PriceRepository(db_session)
        self.indicator_repo = IndicatorRepository(db_session)
    
    def generate_recommendations(
        self,
        recommendation_date: Optional[date] = None,
        stock_codes: Optional[List[str]] = None,
        min_score: float = 40.0,
        min_confidence: float = 0.5
    ) -> List[StockRecommendation]:
        """
        Generate recommendations for stocks.
        
        Args:
            recommendation_date: Date to generate recommendations for
            stock_codes: Specific stocks to analyze (None = all active)
            min_score: Minimum score threshold
            min_confidence: Minimum confidence threshold
            
        Returns:
            List of StockRecommendation objects
        """
        if recommendation_date is None:
            recommendation_date = date.today()
        
        self.logger.info(
            f"Generating recommendations for {recommendation_date}",
            extra={"date": str(recommendation_date)}
        )
        
        # Get stocks to analyze
        if stock_codes:
            stocks = [self.stock_repo.get_by_code(code) for code in stock_codes]
            stocks = [s for s in stocks if s is not None]
        else:
            stocks = self.stock_repo.get_all_active()
        
        recommendations = []
        
        for stock in stocks:
            try:
                recommendation = self._analyze_stock(
                    stock,
                    recommendation_date,
                    min_score,
                    min_confidence
                )
                
                if recommendation:
                    recommendations.append(recommendation)
                    
            except Exception as e:
                self.logger.warning(
                    f"Failed to analyze {stock.stock_code}: {str(e)}"
                )
        
        # Sort by score (descending)
        recommendations.sort(key=lambda x: x.score, reverse=True)
        
        self.logger.info(
            f"Generated {len(recommendations)} recommendations",
            extra={"recommendations": len(recommendations)}
        )
        
        return recommendations
    
    def _analyze_stock(
        self,
        stock,
        recommendation_date: date,
        min_score: float,
        min_confidence: float
    ) -> Optional[StockRecommendation]:
        """Analyze individual stock and generate recommendation."""
        
        # Get latest indicators
        indicators_data = self.indicator_repo.get_latest(
            stock.stock_id
        )
        
        if not indicators_data:
            self.logger.debug(
                f"No indicators found for {stock.stock_code}"
            )
            return None
        
        # Get current price
        latest_price = self.price_repo.get_latest_price(
            stock.stock_id,
            recommendation_date
        )
        
        if not latest_price:
            self.logger.debug(
                f"No price data for {stock.stock_code}"
            )
            return None
        
        # Build indicators dict
        indicators = self._build_indicators_dict(
            indicators_data,
            latest_price
        )
        
        # Generate signal
        signal = self.signal_generator.generate_signal(indicators)
        
        # Calculate score
        score = self.stock_scorer.calculate_score(indicators)
        
        # Filter by thresholds
        if score.total_score < min_score:
            self.logger.debug(
                f"{stock.stock_code} score {score.total_score:.1f} below threshold {min_score}"
            )
            return None
        
        if signal.confidence < min_confidence:
            self.logger.debug(
                f"{stock.stock_code} confidence {signal.confidence:.2f} below threshold {min_confidence}"
            )
            return None
        
        # Calculate target price and stop loss
        target_price, stop_loss = self._calculate_price_targets(
            float(latest_price.close_price),
            signal.signal_type,
            indicators
        )
        
        # Assess risk
        risk_level = self._assess_risk(indicators, signal, score)
        
        # Build combined reasons
        reasons = self._build_reasons(signal, score, indicators)
        
        return StockRecommendation(
            stock_id=stock.stock_id,
            stock_code=stock.stock_code,
            stock_name=stock.stock_name,
            recommendation_date=recommendation_date,
            signal_type=signal.signal_type,
            confidence=signal.confidence,
            score=score.total_score,
            score_category=score.category,
            current_price=latest_price.close_price,
            target_price=Decimal(str(target_price)) if target_price else None,
            stop_loss=Decimal(str(stop_loss)) if stop_loss else None,
            risk_level=risk_level,
            reasons=reasons,
            indicators=indicators,
            technical_signal=signal,
            stock_score=score
        )
    
    def _build_indicators_dict(
        self,
        indicators_data: list,
        latest_price
    ) -> Dict[str, float]:
        """Build indicators dictionary from database records."""
        indicators = {
            'current_price': float(latest_price.close_price),
            'volume_ratio': 1.0  # Default
        }
        
        for ind in indicators_data:
            if ind.indicator_type == 'RSI':
                indicators['rsi_14'] = float(ind.indicator_value)
            elif ind.indicator_type == 'SMA_50':
                indicators['sma_50'] = float(ind.indicator_value)
            elif ind.indicator_type == 'SMA_200':
                indicators['sma_200'] = float(ind.indicator_value)
            elif ind.indicator_type == 'MACD':
                indicators['macd'] = float(ind.indicator_value)
            elif ind.indicator_type == 'MACD_SIGNAL':
                indicators['macd_signal'] = float(ind.indicator_value)
            elif ind.indicator_type == 'VOLATILITY':
                indicators['volatility'] = float(ind.indicator_value)
        
        # Calculate volume ratio if data available
        if latest_price.volume:
            # This is simplified - in production, calculate avg volume
            indicators['volume_ratio'] = 1.2  # Placeholder
        
        # Calculate price change percentage
        if 'sma_50' in indicators:
            price_diff = indicators['current_price'] - indicators['sma_50']
            indicators['price_change_pct'] = (price_diff / indicators['sma_50']) * 100
        
        return indicators
    
    def _calculate_price_targets(
        self,
        current_price: float,
        signal_type: SignalType,
        indicators: Dict[str, float]
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Calculate target price and stop loss.
        
        Returns:
            Tuple of (target_price, stop_loss)
        """
        if signal_type in [SignalType.BUY, SignalType.STRONG_BUY]:
            # Target: 10-15% above current price
            target_multiplier = 1.15 if signal_type == SignalType.STRONG_BUY else 1.10
            target_price = current_price * target_multiplier
            
            # Stop loss: 5-7% below current price
            stop_loss_multiplier = 0.93 if signal_type == SignalType.STRONG_BUY else 0.95
            stop_loss = current_price * stop_loss_multiplier
            
            return target_price, stop_loss
            
        elif signal_type in [SignalType.SELL, SignalType.STRONG_SELL]:
            # For sell signals, target is lower price
            target_multiplier = 0.85 if signal_type == SignalType.STRONG_SELL else 0.90
            target_price = current_price * target_multiplier
            
            # Stop loss: price increase (exit short position)
            stop_loss_multiplier = 1.05 if signal_type == SignalType.STRONG_SELL else 1.03
            stop_loss = current_price * stop_loss_multiplier
            
            return target_price, stop_loss
        
        return None, None
    
    def _assess_risk(
        self,
        indicators: Dict[str, float],
        signal: TechnicalSignal,
        score: StockScore
    ) -> str:
        """
        Assess investment risk level.
        
        Returns:
            Risk level: LOW, MEDIUM, or HIGH
        """
        risk_factors = []
        
        # Volatility risk
        volatility = indicators.get('volatility', 3.0)
        if volatility > 5.0:
            risk_factors.append('high_volatility')
        elif volatility < 2.5:
            risk_factors.append('low_volatility')
        
        # Confidence risk
        if signal.confidence < 0.6:
            risk_factors.append('low_confidence')
        
        # Score risk
        if score.total_score < 50:
            risk_factors.append('low_score')
        
        # RSI extremes
        rsi = indicators.get('rsi_14')
        if rsi and (rsi < 25 or rsi > 75):
            risk_factors.append('extreme_rsi')
        
        # Determine risk level
        if len(risk_factors) >= 3 or 'high_volatility' in risk_factors:
            return 'HIGH'
        elif len(risk_factors) >= 1:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _build_reasons(
        self,
        signal: TechnicalSignal,
        score: StockScore,
        indicators: Dict[str, float]
    ) -> List[str]:
        """Build comprehensive list of recommendation reasons."""
        reasons = []
        
        # Add signal reasons
        reasons.extend(signal.reasons)
        
        # Add score-based reasons
        if score.category == ScoreCategory.EXCELLENT:
            reasons.append(f"Excellent overall score ({score.total_score:.0f}/100)")
        elif score.category == ScoreCategory.GOOD:
            reasons.append(f"Good overall score ({score.total_score:.0f}/100)")
        
        # Add specific insights
        if score.momentum_score >= 80:
            reasons.append("Strong price momentum")
        
        if score.volatility_score >= 85:
            reasons.append("Low volatility - stable investment")
        
        if score.trend_score >= 80:
            reasons.append("Strong upward trend")
        
        if score.volume_score >= 80:
            reasons.append("Healthy volume support")
        
        return reasons
    
    def get_top_picks(
        self,
        recommendations: List[StockRecommendation],
        signal_filter: Optional[SignalType] = None,
        top_n: int = 10
    ) -> List[StockRecommendation]:
        """
        Get top stock picks from recommendations.
        
        Args:
            recommendations: List of recommendations
            signal_filter: Filter by signal type (e.g., only BUY)
            top_n: Number of top picks to return
            
        Returns:
            Filtered and sorted top picks
        """
        filtered = recommendations
        
        if signal_filter:
            filtered = [r for r in filtered if r.signal_type == signal_filter]
        
        # Sort by composite score (score + confidence)
        filtered.sort(
            key=lambda x: (x.score * x.confidence),
            reverse=True
        )
        
        return filtered[:top_n]
    
    def format_recommendation(self, rec: StockRecommendation) -> str:
        """
        Format recommendation for display.
        
        Args:
            rec: StockRecommendation object
            
        Returns:
            Formatted string
        """
        emoji_map = {
            SignalType.STRONG_BUY: "🚀",
            SignalType.BUY: "📈",
            SignalType.HOLD: "⏸️",
            SignalType.SELL: "📉",
            SignalType.STRONG_SELL: "⚠️"
        }
        
        risk_emoji = {
            'LOW': '🟢',
            'MEDIUM': '🟡',
            'HIGH': '🔴'
        }
        
        emoji = emoji_map.get(rec.signal_type, "")
        risk = risk_emoji.get(rec.risk_level, "")
        
        output = f"\n{'='*60}\n"
        output += f"{emoji} {rec.stock_code} - {rec.stock_name}\n"
        output += f"{'='*60}\n\n"
        
        output += f"Recommendation: {rec.signal_type.value}\n"
        output += f"Confidence: {rec.confidence*100:.0f}%\n"
        output += f"Overall Score: {rec.score:.1f}/100 ({rec.score_category.value})\n"
        output += f"Risk Level: {risk} {rec.risk_level}\n\n"
        
        output += f"Price Information:\n"
        output += f"  Current: ₦{rec.current_price:,.2f}\n"
        if rec.target_price:
            output += f"  Target:  ₦{rec.target_price:,.2f}\n"
        if rec.stop_loss:
            output += f"  Stop Loss: ₦{rec.stop_loss:,.2f}\n"
        
        if rec.target_price and rec.current_price:
            potential_gain = ((rec.target_price - rec.current_price) / rec.current_price) * 100
            output += f"  Potential: {potential_gain:+.1f}%\n"
        
        output += f"\nReasons:\n"
        for i, reason in enumerate(rec.reasons[:5], 1):
            output += f"  {i}. {reason}\n"
        
        output += f"\nScore Breakdown:\n"
        output += f"  Technical:  {rec.stock_score.technical_score:.0f}/100\n"
        output += f"  Momentum:   {rec.stock_score.momentum_score:.0f}/100\n"
        output += f"  Volatility: {rec.stock_score.volatility_score:.0f}/100\n"
        output += f"  Trend:      {rec.stock_score.trend_score:.0f}/100\n"
        output += f"  Volume:     {rec.stock_score.volume_score:.0f}/100\n"
        
        return output
    
    def close(self):
        """Close database session."""
        # Session managed externally
        pass
