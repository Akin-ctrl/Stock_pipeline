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
from enum import Enum

from app.services.advisory.signals import (
    SignalGenerator, SignalType, TechnicalSignal
)
from app.services.advisory.scoring import StockScorer, StockScore, ScoreCategory
from app.repositories import IndicatorRepository, PriceRepository, StockRepository
from app.utils import get_logger


class RecommendationProfile(Enum):
    """Strategy profile for recommendation tuning."""
    STEADY_20P_10D = "steady_20p_10d"


@dataclass(frozen=True)
class RecommendationProfileConfig:
    """Configuration bundle for profile-specific scoring and signal thresholds."""
    technical_weight: float
    momentum_weight: float
    volatility_weight: float
    trend_weight: float
    volume_weight: float
    rsi_oversold: float
    rsi_overbought: float
    rsi_strong_oversold: float
    rsi_strong_overbought: float
    min_score: float
    min_confidence: float
    target_upside_buy: float
    target_upside_strong: float
    stop_loss_buy: float
    stop_loss_strong: float
    min_price: float
    max_volatility: float
    min_volume_ratio: float
    rsi_min: float
    rsi_max: float
    buy_only: bool
    min_trusted_history_days: int
    min_price_confidence_score: float
    require_complete_data: bool
    require_official: bool


PROFILE_CONFIGS: Dict[RecommendationProfile, RecommendationProfileConfig] = {
    RecommendationProfile.STEADY_20P_10D: RecommendationProfileConfig(
        technical_weight=0.25,
        momentum_weight=0.32,
        volatility_weight=0.23,
        trend_weight=0.15,
        volume_weight=0.05,
        rsi_oversold=35.0,
        rsi_overbought=72.0,
        rsi_strong_oversold=25.0,
        rsi_strong_overbought=80.0,
        min_score=55.0,
        min_confidence=0.60,
        target_upside_buy=1.20,
        target_upside_strong=1.25,
        stop_loss_buy=0.94,
        stop_loss_strong=0.92,
        min_price=1.0,
        max_volatility=0.75,
        min_volume_ratio=0.8,
        rsi_min=40.0,
        rsi_max=75.0,
        buy_only=True,
        min_trusted_history_days=30,
        min_price_confidence_score=60.0,
        require_complete_data=True,
        require_official=False,
    ),
}


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
    
    def __init__(self, db_session, strategy_profile: str = RecommendationProfile.STEADY_20P_10D.value):
        """
        Initialize advisor.
        
        Args:
            db_session: Database session
        """
        self.db = db_session
        self.logger = get_logger("stock_screener")
        
        self.strategy_profile = self._parse_profile(strategy_profile)
        self.profile_config = PROFILE_CONFIGS[self.strategy_profile]
        self.signal_generator = SignalGenerator()
        self.stock_scorer = StockScorer()
        self._apply_profile(self.strategy_profile)
        
        self.stock_repo = StockRepository(db_session)
        self.price_repo = PriceRepository(db_session)
        self.indicator_repo = IndicatorRepository(db_session)

    def _parse_profile(self, profile: str) -> RecommendationProfile:
        """Parse profile string with safe fallback to steady_20p_10d."""
        normalized = (profile or RecommendationProfile.STEADY_20P_10D.value).strip().lower()
        for option in RecommendationProfile:
            if option.value == normalized:
                return option
        self.logger.warning(
            f"Unknown strategy profile '{profile}', defaulting to steady_20p_10d"
        )
        return RecommendationProfile.STEADY_20P_10D

    def _apply_profile(self, profile: RecommendationProfile) -> None:
        """Apply profile config to signal/scoring engines."""
        self.strategy_profile = profile
        self.profile_config = PROFILE_CONFIGS[profile]

        cfg = self.profile_config
        self.signal_generator = SignalGenerator(
            rsi_oversold=cfg.rsi_oversold,
            rsi_overbought=cfg.rsi_overbought,
            rsi_strong_oversold=cfg.rsi_strong_oversold,
            rsi_strong_overbought=cfg.rsi_strong_overbought,
        )
        self.stock_scorer = StockScorer(
            technical_weight=cfg.technical_weight,
            momentum_weight=cfg.momentum_weight,
            volatility_weight=cfg.volatility_weight,
            trend_weight=cfg.trend_weight,
            volume_weight=cfg.volume_weight,
        )
    
    def generate_recommendations(
        self,
        recommendation_date: Optional[date] = None,
        stock_codes: Optional[List[str]] = None,
        min_score: Optional[float] = None,
        min_confidence: Optional[float] = None,
        strategy_profile: Optional[str] = None,
    ) -> List[StockRecommendation]:
        """
        Generate recommendations for stocks.
        
        Args:
            recommendation_date: Date to generate recommendations for
            stock_codes: Specific stocks to analyze (None = all active)
            min_score: Minimum score threshold (defaults to profile)
            min_confidence: Minimum confidence threshold (defaults to profile)
            strategy_profile: steady_20p_10d
            
        Returns:
            List of StockRecommendation objects
        """
        if recommendation_date is None:
            recommendation_date = date.today()

        if strategy_profile is not None:
            self._apply_profile(self._parse_profile(strategy_profile))

        effective_min_score = (
            self.profile_config.min_score if min_score is None else min_score
        )
        effective_min_confidence = (
            self.profile_config.min_confidence
            if min_confidence is None
            else min_confidence
        )
        
        self.logger.info(
            f"Generating recommendations for {recommendation_date}",
            extra={
                "date": str(recommendation_date),
                "profile": self.strategy_profile.value,
                "min_score": effective_min_score,
                "min_confidence": effective_min_confidence,
            }
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
                    effective_min_score,
                    effective_min_confidence,
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
            extra={
                "recommendations": len(recommendations),
                "profile": self.strategy_profile.value,
            }
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
        indicators_data = self.indicator_repo.get_latest_by_code(
            stock.stock_code,
            recommendation_date
        )
        
        if not indicators_data:
            self.logger.debug(
                f"No indicators found for {stock.stock_code}"
            )
            return None
        
        # Get current price
        latest_price = self.price_repo.get_latest_trusted_price(
            stock.stock_id,
            recommendation_date
        )
        
        if not latest_price:
            self.logger.debug(
                f"No price data for {stock.stock_code}"
            )
            return None
        
        if hasattr(indicators_data, 'calculation_date') and indicators_data.calculation_date != latest_price.price_date:
            self.logger.debug(
                f"Skipping {stock.stock_code}: latest indicator date "
                f"{indicators_data.calculation_date} does not match trusted price date "
                f"{latest_price.price_date}"
            )
            return None

        # Build indicators dict
        indicators, trusted_price_history = self._build_indicators_dict(
            stock.stock_id,
            recommendation_date,
            indicators_data,
            latest_price
        )
        
        # Generate signal
        signal = self.signal_generator.generate_signal(indicators)

        # Calculate score
        score = self.stock_scorer.calculate_score(
            indicators,
            price_history=trusted_price_history,
        )

        # Profile guardrails for steady investing
        cfg = self.profile_config
        if cfg.buy_only and signal.signal_type in [SignalType.SELL, SignalType.STRONG_SELL]:
            return None

        if indicators.get('current_price') is not None and indicators['current_price'] < cfg.min_price:
            return None

        if indicators.get('volatility') is not None and indicators['volatility'] > cfg.max_volatility:
            return None

        if indicators.get('volume_ratio') is not None and indicators['volume_ratio'] < cfg.min_volume_ratio:
            return None

        if indicators.get('trusted_history_days') is not None and indicators['trusted_history_days'] < cfg.min_trusted_history_days:
            return None

        if (
            indicators.get('price_confidence_score') is not None
            and indicators['price_confidence_score'] < cfg.min_price_confidence_score
        ):
            return None

        if cfg.require_complete_data and not indicators.get('has_complete_data', False):
            return None

        if cfg.require_official and not indicators.get('is_official', False):
            return None

        rsi_val = indicators.get('rsi_14')
        if rsi_val is not None and (rsi_val < cfg.rsi_min or rsi_val > cfg.rsi_max):
            return None

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
            stock_name=stock.company_name,
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
        stock_id,
        as_of_date,
        indicators_data,
        latest_price
    ) -> tuple[Dict[str, float], list]:
        """Build indicators dictionary from database records."""
        indicators = {
            'current_price': float(latest_price.close_price),
            'volume_ratio': 1.0,  # Default
            'price_confidence_score': (
                float(latest_price.confidence_score)
                if latest_price.confidence_score is not None
                else None
            ),
            'price_quality_flag': latest_price.data_quality_flag,
            'bar_status': latest_price.bar_status,
            'has_complete_data': bool(latest_price.has_complete_data),
            'is_official': bool(latest_price.is_official),
        }

        # Current schema: one wide FactTechnicalIndicator row per stock/date
        if hasattr(indicators_data, 'rsi_14'):
            if indicators_data.rsi_14 is not None:
                indicators['rsi_14'] = float(indicators_data.rsi_14)
            if indicators_data.macd is not None:
                indicators['macd'] = float(indicators_data.macd)
            if indicators_data.macd_signal is not None:
                indicators['macd_signal'] = float(indicators_data.macd_signal)
            if indicators_data.volatility_30 is not None:
                indicators['volatility'] = float(indicators_data.volatility_30)
            if indicators_data.ma_7 is not None:
                indicators['ma_7'] = float(indicators_data.ma_7)
            if indicators_data.ma_30 is not None:
                indicators['ma_30'] = float(indicators_data.ma_30)
            if indicators_data.ma_90 is not None:
                indicators['ma_90'] = float(indicators_data.ma_90)

        # Backward compatibility: older long-format indicator rows
        elif isinstance(indicators_data, list):
            for ind in indicators_data:
                if ind.indicator_type == 'RSI':
                    indicators['rsi_14'] = float(ind.indicator_value)
                elif ind.indicator_type == 'SMA_50':
                    indicators['ma_30'] = float(ind.indicator_value)
                elif ind.indicator_type == 'SMA_200':
                    indicators['ma_90'] = float(ind.indicator_value)
                elif ind.indicator_type == 'MACD':
                    indicators['macd'] = float(ind.indicator_value)
                elif ind.indicator_type == 'MACD_SIGNAL':
                    indicators['macd_signal'] = float(ind.indicator_value)
                elif ind.indicator_type == 'VOLATILITY':
                    indicators['volatility'] = float(ind.indicator_value)
        
        recent_prices = self.price_repo.get_trusted_price_history(
            stock_id,
            end_date=as_of_date,
            limit=90
        )
        indicators['trusted_history_days'] = len(recent_prices)

        if recent_prices:
            latest_record = recent_prices[0]
            recent_volume_window = recent_prices[:21]
            historical_volumes = [
                p.volume for p in recent_volume_window[1:]
                if p.volume is not None
            ]
            if latest_record.volume is not None and historical_volumes:
                avg_volume = sum(historical_volumes) / len(historical_volumes)
                if avg_volume > 0:
                    indicators['volume_ratio'] = float(latest_record.volume / avg_volume)

            if latest_record.change_1d_pct is not None:
                indicators['price_change_pct'] = float(latest_record.change_1d_pct)
            elif len(recent_prices) > 1:
                previous_close = recent_prices[1].close_price
                if previous_close is not None and float(previous_close) > 0:
                    indicators['price_change_pct'] = (
                        (float(latest_record.close_price) - float(previous_close))
                        / float(previous_close)
                    ) * 100

            if len(recent_prices) > 10:
                close_10d = recent_prices[10].close_price
                if close_10d is not None and float(close_10d) > 0:
                    indicators['price_change_10d'] = (
                        (float(latest_record.close_price) - float(close_10d))
                        / float(close_10d)
                    ) * 100

            if len(recent_prices) > 20:
                close_20d = recent_prices[20].close_price
                if close_20d is not None and float(close_20d) > 0:
                    indicators['price_change_20d'] = (
                        (float(latest_record.close_price) - float(close_20d))
                        / float(close_20d)
                    ) * 100
        
        return indicators, recent_prices
    
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
        cfg = self.profile_config
        if signal_type in [SignalType.BUY, SignalType.STRONG_BUY]:
            target_multiplier = (
                cfg.target_upside_strong
                if signal_type == SignalType.STRONG_BUY
                else cfg.target_upside_buy
            )
            target_price = current_price * target_multiplier

            stop_loss_multiplier = (
                cfg.stop_loss_strong
                if signal_type == SignalType.STRONG_BUY
                else cfg.stop_loss_buy
            )
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
        # Volatility is stored as annualized decimal (for example 0.30 = 30%)
        volatility = indicators.get('volatility', 0.30)
        if volatility > 0.50:
            risk_factors.append('high_volatility')
        elif volatility < 0.20:
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
