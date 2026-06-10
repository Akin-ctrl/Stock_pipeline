"""
Stock screening service.

Generates trading signals and stock scores by combining
technical signals, stock scores, and risk analysis.

**DISCLAIMER**: This is a technical analysis screening tool for educational purposes.
Not financial advice. Always do your own research.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Optional, Dict
from decimal import Decimal
from enum import Enum

from app.services.advisory.eligibility import (
    EligibilityConfig,
    RecommendationEligibilityEvaluator,
)
from app.services.advisory.selection import (
    RecommendationSelectionEvaluator,
    SelectionConfig,
)
from app.services.advisory.signals import (
    SignalGenerator, SignalType, TechnicalSignal
)
from app.services.advisory.policy import RecommendationPolicyEngine
from app.services.advisory.scoring import StockScorer, StockScore, ScoreCategory
from app.services.modeling.feature_engineering import build_historical_feature_snapshot
from app.services.modeling import (
    HistoricalLogisticProbabilityEstimator,
    NullProbabilityEstimator,
)
from app.repositories import IndicatorRepository, PriceRepository, StockRepository
from app.utils import get_logger


class RecommendationProfile(Enum):
    """Strategy profile for recommendation tuning."""
    STEADY_20P_10D = "steady_20p_10d"
    STEADY_20P_10D_V2 = "steady_20p_10d_v2"


class RecommendationAction(Enum):
    """Long-only recommendation actions shown to users and downstream tools."""

    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    AVOID = "AVOID"
    STRONGLY_AVOID = "STRONGLY_AVOID"


@dataclass(frozen=True)
class SignalConfig:
    """Configuration for heuristic technical-signal generation."""

    rsi_oversold: float
    rsi_overbought: float
    rsi_strong_oversold: float
    rsi_strong_overbought: float


@dataclass(frozen=True)
class ScoringConfig:
    """Configuration for heuristic score weighting."""

    technical_weight: float
    momentum_weight: float
    volatility_weight: float
    trend_weight: float
    volume_weight: float


@dataclass(frozen=True)
class PolicyConfig:
    """Configuration for strategy-policy target and stop outputs."""

    target_upside_buy: float
    target_upside_strong: float
    stop_loss_buy: float
    stop_loss_strong: float


@dataclass(frozen=True)
class RecommendationProfileConfig:
    """Composition root for prediction-adjacent advisory configuration layers."""

    signal_config: SignalConfig
    scoring_config: ScoringConfig
    eligibility_config: EligibilityConfig
    selection_config: SelectionConfig
    policy_config: PolicyConfig


PROFILE_CONFIGS: Dict[RecommendationProfile, RecommendationProfileConfig] = {
    RecommendationProfile.STEADY_20P_10D: RecommendationProfileConfig(
        signal_config=SignalConfig(
            rsi_oversold=35.0,
            rsi_overbought=72.0,
            rsi_strong_oversold=25.0,
            rsi_strong_overbought=80.0,
        ),
        scoring_config=ScoringConfig(
            technical_weight=0.25,
            momentum_weight=0.32,
            volatility_weight=0.23,
            trend_weight=0.15,
            volume_weight=0.05,
        ),
        eligibility_config=EligibilityConfig(
            min_price=8.0,
            max_volatility=0.75,
            min_volume_ratio=0.8,
            rsi_min=40.0,
            rsi_max=75.0,
            min_trusted_history_days=30,
            min_price_confidence_score=60.0,
            require_complete_data=True,
            require_official=False,
            min_drawdown_20d_pct=1.0,
            max_price_change_20d_pct=12.0,
        ),
        selection_config=SelectionConfig(
            min_heuristic_score=70.0,
            min_signal_agreement=0.60,
            buy_only=True,
        ),
        policy_config=PolicyConfig(
            target_upside_buy=1.20,
            target_upside_strong=1.25,
            stop_loss_buy=0.94,
            stop_loss_strong=0.92,
        ),
    ),
    RecommendationProfile.STEADY_20P_10D_V2: RecommendationProfileConfig(
        signal_config=SignalConfig(
            rsi_oversold=35.0,
            rsi_overbought=74.0,
            rsi_strong_oversold=25.0,
            rsi_strong_overbought=84.0,
        ),
        scoring_config=ScoringConfig(
            technical_weight=0.24,
            momentum_weight=0.34,
            volatility_weight=0.18,
            trend_weight=0.16,
            volume_weight=0.08,
        ),
        eligibility_config=EligibilityConfig(
            min_price=0.20,
            max_volatility=2.00,
            min_volume_ratio=0.0,
            rsi_min=0.0,
            rsi_max=100.0,
            min_trusted_history_days=30,
            min_price_confidence_score=60.0,
            require_complete_data=True,
            require_official=False,
            min_drawdown_20d_pct=None,
            max_price_change_20d_pct=None,
        ),
        selection_config=SelectionConfig(
            min_heuristic_score=68.0,
            min_signal_agreement=0.40,
            buy_only=True,
        ),
        policy_config=PolicyConfig(
            target_upside_buy=1.16,
            target_upside_strong=1.20,
            stop_loss_buy=0.93,
            stop_loss_strong=0.91,
        ),
    ),
}


@dataclass
class RecommendationAuditEntry:
    """Terminal candidate-funnel state for one stock in one recommendation run."""

    stock_id: int
    stock_code: str
    recommendation_date: date
    profile: str
    stage_reached: str
    rejection_reason: Optional[str] = None
    price_date: Optional[date] = None
    indicator_date: Optional[date] = None
    current_price: Optional[float] = None
    action_type: Optional[str] = None
    technical_signal_type: Optional[str] = None
    signal_agreement: Optional[float] = None
    predicted_probability_10d_up: Optional[float] = None
    heuristic_score: Optional[float] = None
    heuristic_score_category: Optional[str] = None
    eligible: bool = False
    selected: bool = False
    portfolio_approved: bool = False
    portfolio_rejection_reason: Optional[str] = None
    portfolio_rank: Optional[int] = None
    candidate_tier: str = "blocked"
    indicators: Dict[str, object] = field(default_factory=dict)
    score_breakdown: Dict[str, object] = field(default_factory=dict)
    model_version: Optional[str] = None


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
        action_type: Long-only recommendation action
        signal_agreement: Heuristic signal-agreement score (0-1)
        predicted_probability_10d_up: Optional model probability for target_up_10d
        heuristic_score: Heuristic technical score (0-100)
        heuristic_score_category: Heuristic score category
        policy_target_price: Strategy-policy target price
        policy_stop_loss: Strategy-policy stop price
        heuristic_risk_level: Heuristic risk label
        current_price: Current stock price
        target_price: Backward-compatible alias for the policy target price
        stop_loss: Backward-compatible alias for the policy stop price
        risk_level: Backward-compatible alias for the heuristic risk label
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
    action_type: RecommendationAction
    signal_agreement: float
    predicted_probability_10d_up: Optional[float]
    heuristic_score: float
    heuristic_score_category: ScoreCategory
    policy_target_price: Optional[Decimal]
    policy_stop_loss: Optional[Decimal]
    heuristic_risk_level: str
    current_price: Decimal
    reasons: List[str]
    indicators: Dict[str, float]
    technical_signal: TechnicalSignal
    stock_score: StockScore
    strategy_profile: str = RecommendationProfile.STEADY_20P_10D.value
    portfolio_approved: bool = False
    portfolio_rejection_reason: Optional[str] = None
    portfolio_rank: Optional[int] = None
    portfolio_position_size_pct: Optional[float] = None
    portfolio_policy_version: Optional[str] = None
    portfolio_open_positions_before: Optional[int] = None
    portfolio_available_slots_before: Optional[int] = None
    portfolio_max_concurrent_positions: Optional[int] = None
    portfolio_max_entries_per_day: Optional[int] = None

    @property
    def confidence(self) -> float:
        """Backward-compatible alias for legacy heuristic confidence access."""
        return self.signal_agreement

    @property
    def score(self) -> float:
        """Backward-compatible alias for the heuristic score."""
        return self.heuristic_score

    @property
    def score_category(self) -> ScoreCategory:
        """Backward-compatible alias for the heuristic score category."""
        return self.heuristic_score_category

    @property
    def target_price(self) -> Optional[Decimal]:
        """Backward-compatible alias for the strategy-policy target price."""
        return self.policy_target_price

    @property
    def stop_loss(self) -> Optional[Decimal]:
        """Backward-compatible alias for the strategy-policy stop price."""
        return self.policy_stop_loss

    @property
    def risk_level(self) -> str:
        """Backward-compatible alias for the heuristic risk label."""
        return self.heuristic_risk_level

    @property
    def is_actionable(self) -> bool:
        """Return True when the recommendation represents a long entry action."""
        return self.action_type in {
            RecommendationAction.BUY,
            RecommendationAction.STRONG_BUY,
        }


class StockScreener:
    """
    Generates stock screening signals and scores.
    
    Combines technical analysis, scoring, and risk assessment
    to produce BUY/SELL/HOLD signals with confidence levels.
    
    **NOT INVESTMENT ADVICE** - For screening and analysis only.
    """
    
    def __init__(
        self,
        db_session,
        strategy_profile: str = RecommendationProfile.STEADY_20P_10D.value,
        probability_estimator=None,
    ):
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
        self.probability_estimator = (
            probability_estimator
            if probability_estimator is not None
            else HistoricalLogisticProbabilityEstimator(db_session)
        )
        self.policy_engine = RecommendationPolicyEngine(self.profile_config.policy_config)
        self.eligibility_evaluator = RecommendationEligibilityEvaluator(
            self.profile_config.eligibility_config
        )
        self.selection_evaluator = RecommendationSelectionEvaluator(
            self.profile_config.selection_config
        )
        self.last_audit_entries: List[RecommendationAuditEntry] = []
        
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
        signal_cfg = cfg.signal_config
        scoring_cfg = cfg.scoring_config
        self.signal_generator = SignalGenerator(
            rsi_oversold=signal_cfg.rsi_oversold,
            rsi_overbought=signal_cfg.rsi_overbought,
            rsi_strong_oversold=signal_cfg.rsi_strong_oversold,
            rsi_strong_overbought=signal_cfg.rsi_strong_overbought,
        )
        self.stock_scorer = StockScorer(
            technical_weight=scoring_cfg.technical_weight,
            momentum_weight=scoring_cfg.momentum_weight,
            volatility_weight=scoring_cfg.volatility_weight,
            trend_weight=scoring_cfg.trend_weight,
            volume_weight=scoring_cfg.volume_weight,
        )
        self.policy_engine = RecommendationPolicyEngine(cfg.policy_config)
        self.eligibility_evaluator = RecommendationEligibilityEvaluator(
            cfg.eligibility_config
        )
        self.selection_evaluator = RecommendationSelectionEvaluator(
            cfg.selection_config
        )
    
    def generate_recommendations(
        self,
        recommendation_date: Optional[date] = None,
        stock_codes: Optional[List[str]] = None,
        min_score: Optional[float] = None,
        min_confidence: Optional[float] = None,
        min_signal_agreement: Optional[float] = None,
        min_predicted_probability: Optional[float] = None,
        strategy_profile: Optional[str] = None,
        capture_audit: bool = False,
    ) -> List[StockRecommendation]:
        """
        Generate recommendations for stocks.
        
        Args:
            recommendation_date: Date to generate recommendations for
            stock_codes: Specific stocks to analyze (None = all active)
            min_score: Legacy alias for minimum heuristic score threshold
            min_confidence: Legacy alias for minimum signal-agreement threshold
            min_signal_agreement: Minimum heuristic signal-agreement threshold
            min_predicted_probability: Minimum predicted 10-day up probability
            strategy_profile: steady_20p_10d
            
        Returns:
            List of StockRecommendation objects
        """
        if recommendation_date is None:
            recommendation_date = date.today()

        if strategy_profile is not None:
            self._apply_profile(self._parse_profile(strategy_profile))

        self.last_audit_entries = []

        effective_min_score = (
            self.profile_config.selection_config.min_heuristic_score
            if min_score is None
            else min_score
        )
        effective_min_signal_agreement = (
            self.profile_config.selection_config.min_signal_agreement
            if min_signal_agreement is None and min_confidence is None
            else (
                min_signal_agreement
                if min_signal_agreement is not None
                else min_confidence
            )
        )
        effective_min_predicted_probability = (
            self.profile_config.selection_config.min_predicted_probability
            if min_predicted_probability is None
            else min_predicted_probability
        )
        
        self.logger.info(
            f"Generating recommendations for {recommendation_date}",
            extra={
                "date": str(recommendation_date),
                "profile": self.strategy_profile.value,
                "min_heuristic_score": effective_min_score,
                "min_signal_agreement": effective_min_signal_agreement,
                "min_predicted_probability": effective_min_predicted_probability,
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
                    effective_min_signal_agreement,
                    effective_min_predicted_probability,
                    self.last_audit_entries if capture_audit else None,
                )
                
                if recommendation:
                    recommendations.append(recommendation)
                    
            except Exception as e:
                self.logger.warning(
                    f"Failed to analyze {stock.stock_code}: {str(e)}"
                )
        
        # Keep model probability diagnostic until ranking is proven out-of-sample.
        recommendations.sort(key=self._recommendation_rank_key, reverse=True)
        
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
        min_signal_agreement: float,
        min_predicted_probability: Optional[float],
        audit_entries: Optional[List[RecommendationAuditEntry]] = None,
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
            self._append_audit_entry(
                audit_entries,
                stock=stock,
                recommendation_date=recommendation_date,
                stage_reached="no_indicator",
                rejection_reason="no_indicator",
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
            self._append_audit_entry(
                audit_entries,
                stock=stock,
                recommendation_date=recommendation_date,
                stage_reached="no_trusted_price",
                rejection_reason="no_trusted_price",
                indicator_date=getattr(indicators_data, 'calculation_date', None),
            )
            return None
        
        if hasattr(indicators_data, 'calculation_date') and indicators_data.calculation_date != latest_price.price_date:
            self.logger.debug(
                f"Skipping {stock.stock_code}: latest indicator date "
                f"{indicators_data.calculation_date} does not match trusted price date "
                f"{latest_price.price_date}"
            )
            self._append_audit_entry(
                audit_entries,
                stock=stock,
                recommendation_date=recommendation_date,
                stage_reached="indicator_price_date_mismatch",
                rejection_reason="indicator_price_date_mismatch",
                price_date=latest_price.price_date,
                indicator_date=getattr(indicators_data, 'calculation_date', None),
                current_price=float(latest_price.close_price),
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
        action_type = self._map_signal_to_action(signal.signal_type)

        # Calculate score
        score = self.stock_scorer.calculate_score(
            indicators,
            price_history=trusted_price_history,
        )
        self._apply_profile_score_adjustments(score, indicators)

        predicted_probability_10d_up = self.probability_estimator.estimate_probability_10d_up(
            {
                "stock_id": stock.stock_id,
                "stock_code": stock.stock_code,
                "recommendation_date": recommendation_date,
                "signal_type": signal.signal_type.value,
                "signal_agreement": signal.signal_agreement,
                "heuristic_score": score.total_score,
                "heuristic_score_category": score.category.value,
                "indicators": indicators,
            }
        )

        eligibility_decision = self.eligibility_evaluator.evaluate(indicators)
        if not eligibility_decision.eligible:
            self.logger.debug(
                f"{stock.stock_code} failed eligibility: "
                f"{eligibility_decision.rejection_reason}"
            )
            self._append_audit_entry(
                audit_entries,
                stock=stock,
                recommendation_date=recommendation_date,
                stage_reached="eligibility_failed",
                rejection_reason=eligibility_decision.rejection_reason,
                price_date=latest_price.price_date,
                indicator_date=getattr(indicators_data, 'calculation_date', None),
                current_price=float(latest_price.close_price),
                action_type=action_type.value,
                technical_signal_type=signal.signal_type.value,
                signal_agreement=signal.signal_agreement,
                predicted_probability_10d_up=predicted_probability_10d_up,
                heuristic_score=score.total_score,
                heuristic_score_category=score.category.value,
                eligible=False,
                selected=False,
                indicators=indicators,
                score_breakdown=score.breakdown,
                model_version=self._audit_model_version(predicted_probability_10d_up),
            )
            return None

        selection_decision = self.selection_evaluator.evaluate(
            action_value=action_type.value,
            heuristic_score=score.total_score,
            signal_agreement=signal.signal_agreement,
            predicted_probability_10d_up=predicted_probability_10d_up,
            min_heuristic_score=min_score,
            min_signal_agreement=min_signal_agreement,
            min_predicted_probability=min_predicted_probability,
        )
        if not selection_decision.selected:
            self.logger.debug(
                f"{stock.stock_code} failed selection: "
                f"{selection_decision.rejection_reason}"
            )
            self._append_audit_entry(
                audit_entries,
                stock=stock,
                recommendation_date=recommendation_date,
                stage_reached="selection_failed",
                rejection_reason=selection_decision.rejection_reason,
                price_date=latest_price.price_date,
                indicator_date=getattr(indicators_data, 'calculation_date', None),
                current_price=float(latest_price.close_price),
                action_type=action_type.value,
                technical_signal_type=signal.signal_type.value,
                signal_agreement=signal.signal_agreement,
                predicted_probability_10d_up=predicted_probability_10d_up,
                heuristic_score=score.total_score,
                heuristic_score_category=score.category.value,
                eligible=True,
                selected=False,
                indicators=indicators,
                score_breakdown=score.breakdown,
                model_version=self._audit_model_version(predicted_probability_10d_up),
            )
            return None

        policy_output = self.policy_engine.build_policy_output(
            current_price=float(latest_price.close_price),
            action_value=action_type.value,
            indicators=indicators,
            signal_agreement=signal.signal_agreement,
            heuristic_score=score.total_score,
        )
        
        # Build combined reasons
        reasons = self._build_reasons(signal, score, indicators)

        self._append_audit_entry(
            audit_entries,
            stock=stock,
            recommendation_date=recommendation_date,
            stage_reached="selected",
            rejection_reason=None,
            price_date=latest_price.price_date,
            indicator_date=getattr(indicators_data, 'calculation_date', None),
            current_price=float(latest_price.close_price),
            action_type=action_type.value,
            technical_signal_type=signal.signal_type.value,
            signal_agreement=signal.signal_agreement,
            predicted_probability_10d_up=predicted_probability_10d_up,
            heuristic_score=score.total_score,
            heuristic_score_category=score.category.value,
            eligible=True,
            selected=True,
            indicators=indicators,
            score_breakdown=score.breakdown,
            model_version=self._audit_model_version(predicted_probability_10d_up),
        )
        
        return StockRecommendation(
            stock_id=stock.stock_id,
            stock_code=stock.stock_code,
            stock_name=stock.company_name,
            recommendation_date=recommendation_date,
            signal_type=signal.signal_type,
            action_type=action_type,
            signal_agreement=signal.signal_agreement,
            predicted_probability_10d_up=predicted_probability_10d_up,
            heuristic_score=score.total_score,
            heuristic_score_category=score.category,
            policy_target_price=(
                Decimal(str(policy_output.policy_target_price))
                if policy_output.policy_target_price is not None
                else None
            ),
            policy_stop_loss=(
                Decimal(str(policy_output.policy_stop_loss))
                if policy_output.policy_stop_loss is not None
                else None
            ),
            heuristic_risk_level=policy_output.heuristic_risk_level,
            current_price=latest_price.close_price,
            reasons=reasons,
            indicators=indicators,
            technical_signal=signal,
            stock_score=score,
            strategy_profile=self.strategy_profile.value,
        )

    def apply_portfolio_audit(
        self,
        recommendations: List[StockRecommendation],
    ) -> None:
        """Update selected audit entries with portfolio-gate outcomes."""
        recommendations_by_stock = {
            recommendation.stock_id: recommendation
            for recommendation in recommendations
        }

        for entry in self.last_audit_entries:
            recommendation = recommendations_by_stock.get(entry.stock_id)
            if recommendation is None or not entry.selected:
                continue

            entry.stage_reached = "portfolio_evaluated"
            entry.portfolio_approved = bool(recommendation.portfolio_approved)
            entry.portfolio_rejection_reason = (
                recommendation.portfolio_rejection_reason
            )
            entry.portfolio_rank = recommendation.portfolio_rank
            if not recommendation.portfolio_approved:
                entry.rejection_reason = recommendation.portfolio_rejection_reason
                entry.candidate_tier = "watchlist"
            else:
                entry.candidate_tier = "approved"

    def _append_audit_entry(
        self,
        audit_entries: Optional[List[RecommendationAuditEntry]],
        *,
        stock,
        recommendation_date: date,
        stage_reached: str,
        rejection_reason: Optional[str],
        price_date: Optional[date] = None,
        indicator_date: Optional[date] = None,
        current_price: Optional[float] = None,
        action_type: Optional[str] = None,
        technical_signal_type: Optional[str] = None,
        signal_agreement: Optional[float] = None,
        predicted_probability_10d_up: Optional[float] = None,
        heuristic_score: Optional[float] = None,
        heuristic_score_category: Optional[str] = None,
        eligible: bool = False,
        selected: bool = False,
        candidate_tier: Optional[str] = None,
        indicators: Optional[Dict[str, object]] = None,
        score_breakdown: Optional[Dict[str, object]] = None,
        model_version: Optional[str] = None,
    ) -> None:
        """Append one audit entry when candidate-funnel capture is enabled."""
        if audit_entries is None:
            return

        audit_entries.append(
            RecommendationAuditEntry(
                stock_id=stock.stock_id,
                stock_code=stock.stock_code,
                recommendation_date=recommendation_date,
                profile=self.strategy_profile.value,
                stage_reached=stage_reached,
                rejection_reason=rejection_reason,
                price_date=price_date,
                indicator_date=indicator_date,
                current_price=current_price,
                action_type=action_type,
                technical_signal_type=technical_signal_type,
                signal_agreement=signal_agreement,
                predicted_probability_10d_up=predicted_probability_10d_up,
                heuristic_score=heuristic_score,
                heuristic_score_category=heuristic_score_category,
                eligible=eligible,
                selected=selected,
                candidate_tier=(
                    candidate_tier
                    or self._candidate_tier(
                        stage_reached=stage_reached,
                        rejection_reason=rejection_reason,
                        selected=selected,
                        portfolio_approved=False,
                        score=heuristic_score,
                        action_type=action_type,
                    )
                ),
                indicators=indicators,
                score_breakdown=score_breakdown,
                model_version=model_version,
            )
        )

    def _apply_profile_score_adjustments(
        self,
        score: StockScore,
        indicators: Dict[str, float],
    ) -> None:
        """Apply v2 risk penalties after the base heuristic score."""
        if self.strategy_profile != RecommendationProfile.STEADY_20P_10D_V2:
            return

        penalty_reasons: Dict[str, float] = {}

        current_price = indicators.get("current_price")
        if current_price is not None:
            if current_price < 1.0:
                penalty_reasons["low_price_penalty"] = 18.0
            elif current_price < 3.0:
                penalty_reasons["low_price_penalty"] = 10.0
            elif current_price < 8.0:
                penalty_reasons["low_price_penalty"] = 5.0

        volatility = indicators.get("volatility")
        if volatility is not None:
            if volatility > 1.25:
                penalty_reasons["high_volatility_penalty"] = 18.0
            elif volatility > 0.90:
                penalty_reasons["high_volatility_penalty"] = 10.0
            elif volatility > 0.75:
                penalty_reasons["high_volatility_penalty"] = 5.0

        volume_ratio = indicators.get("volume_ratio")
        if volume_ratio is not None:
            if volume_ratio < 0.25:
                penalty_reasons["low_volume_penalty"] = 12.0
            elif volume_ratio < 0.50:
                penalty_reasons["low_volume_penalty"] = 7.0
            elif volume_ratio < 0.80:
                penalty_reasons["low_volume_penalty"] = 3.0

        drawdown_20d_pct = indicators.get("drawdown_20d_pct")
        if drawdown_20d_pct is not None and drawdown_20d_pct < 1.0:
            penalty_reasons["no_pullback_penalty"] = 4.0

        price_change_20d = indicators.get("price_change_20d")
        if price_change_20d is not None:
            if price_change_20d > 45:
                penalty_reasons["extended_run_penalty"] = 14.0
            elif price_change_20d > 30:
                penalty_reasons["extended_run_penalty"] = 8.0
            elif price_change_20d > 20:
                penalty_reasons["extended_run_penalty"] = 4.0

        rsi = indicators.get("rsi_14")
        if rsi is not None:
            if rsi > 85 or rsi < 20:
                penalty_reasons["extreme_rsi_penalty"] = 12.0
            elif rsi > 78 or rsi < 30:
                penalty_reasons["extreme_rsi_penalty"] = 6.0

        penalty = sum(penalty_reasons.values())
        adjusted_score = max(0.0, min(100.0, score.total_score - penalty))
        score.breakdown.update(penalty_reasons)
        score.breakdown["v2_total_penalty"] = round(penalty, 2)
        score.breakdown["pre_penalty_total_score"] = score.total_score
        score.total_score = round(adjusted_score, 2)
        score.category = self.stock_scorer._categorize_score(score.total_score)

    @staticmethod
    def _candidate_tier(
        *,
        stage_reached: str,
        rejection_reason: Optional[str],
        selected: bool,
        portfolio_approved: bool,
        score: Optional[float],
        action_type: Optional[str] = None,
    ) -> str:
        """Classify candidate state for dashboard-facing funnel analysis."""
        if portfolio_approved:
            return "approved"
        if selected:
            return "watchlist"
        if stage_reached in {
            "no_indicator",
            "no_trusted_price",
            "indicator_price_date_mismatch",
        }:
            return "blocked"
        if rejection_reason in {
            "requires_complete_data",
            "requires_official_data",
            "below_min_price_confidence",
            "insufficient_trusted_history",
        }:
            return "blocked"
        if action_type in {"AVOID", "STRONGLY_AVOID"}:
            return "avoid"
        if score is not None and score >= 68:
            return "watchlist"
        return "avoid"

    @staticmethod
    def _audit_model_version(predicted_probability_10d_up: Optional[float]) -> Optional[str]:
        """Return the audit model label for persisted diagnostics."""
        return (
            "historical_logistic_v1"
            if predicted_probability_10d_up is not None
            else None
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
            chronological_prices = list(reversed(recent_prices))
            engineered_features = build_historical_feature_snapshot(
                history_through_anchor=chronological_prices,
                current_price=latest_record.close_price,
                ma_7=indicators.get('ma_7'),
                ma_30=indicators.get('ma_30'),
                ma_90=indicators.get('ma_90'),
            )

            indicators['volume_ratio'] = (
                engineered_features.volume_ratio
                if engineered_features.volume_ratio is not None
                else indicators['volume_ratio']
            )
            indicators['price_change_pct'] = engineered_features.price_change_pct
            indicators['price_change_3d'] = engineered_features.price_change_3d
            indicators['price_change_5d'] = engineered_features.price_change_5d
            indicators['price_change_10d'] = engineered_features.price_change_10d
            indicators['price_change_20d'] = engineered_features.price_change_20d
            indicators['price_change_30d'] = engineered_features.price_change_30d
            indicators['price_change_60d'] = engineered_features.price_change_60d
            indicators['close_vs_20d_high_pct'] = engineered_features.close_vs_20d_high_pct
            indicators['close_vs_60d_high_pct'] = engineered_features.close_vs_60d_high_pct
            indicators['close_vs_20d_low_pct'] = engineered_features.close_vs_20d_low_pct
            indicators['close_vs_60d_low_pct'] = engineered_features.close_vs_60d_low_pct
            indicators['drawdown_20d_pct'] = engineered_features.drawdown_20d_pct
            indicators['drawdown_60d_pct'] = engineered_features.drawdown_60d_pct
            indicators['rebound_20d_pct'] = engineered_features.rebound_20d_pct
            indicators['rebound_60d_pct'] = engineered_features.rebound_60d_pct
            indicators['volatility_10d'] = engineered_features.volatility_10d
            indicators['volatility_20d'] = engineered_features.volatility_20d
            indicators['downside_volatility_20d'] = engineered_features.downside_volatility_20d
            indicators['average_volume_20d'] = engineered_features.average_volume_20d
            indicators['volume_trend_ratio'] = engineered_features.volume_trend_ratio

        return indicators, recent_prices
    
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
        filtered = [r for r in filtered if r.is_actionable]
        
        # Sort by heuristic conviction first; probability remains diagnostic.
        filtered.sort(key=self._recommendation_rank_key, reverse=True)
        
        return filtered[:top_n]

    @staticmethod
    def _recommendation_rank_key(recommendation: StockRecommendation) -> float:
        """Rank recommendations by heuristic conviction, with probability as tie-breaker."""
        probability = (
            recommendation.predicted_probability_10d_up
            if recommendation.predicted_probability_10d_up is not None
            else 0.0
        )
        return (
            recommendation.heuristic_score * 1_000
            + recommendation.signal_agreement * 100
            + probability
        )

    @staticmethod
    def _map_signal_to_action(signal_type: SignalType) -> RecommendationAction:
        """Map technical signal direction to the current long-only action language."""
        mapping = {
            SignalType.STRONG_BUY: RecommendationAction.STRONG_BUY,
            SignalType.BUY: RecommendationAction.BUY,
            SignalType.HOLD: RecommendationAction.HOLD,
            SignalType.SELL: RecommendationAction.AVOID,
            SignalType.STRONG_SELL: RecommendationAction.STRONGLY_AVOID,
        }
        return mapping[signal_type]
    
    def format_recommendation(self, rec: StockRecommendation) -> str:
        """
        Format recommendation for display.
        
        Args:
            rec: StockRecommendation object
            
        Returns:
            Formatted string
        """
        emoji_map = {
            RecommendationAction.STRONG_BUY: "🚀",
            RecommendationAction.BUY: "📈",
            RecommendationAction.HOLD: "⏸️",
            RecommendationAction.AVOID: "🛑",
            RecommendationAction.STRONGLY_AVOID: "⚠️",
        }
        
        risk_emoji = {
            'LOW': '🟢',
            'MEDIUM': '🟡',
            'HIGH': '🔴'
        }
        
        emoji = emoji_map.get(rec.action_type, "")
        risk = risk_emoji.get(rec.heuristic_risk_level, "")
        
        output = f"\n{'='*60}\n"
        output += f"{emoji} {rec.stock_code} - {rec.stock_name}\n"
        output += f"{'='*60}\n\n"
        
        output += f"Action: {rec.action_type.value}\n"
        output += f"Technical Signal: {rec.signal_type.value}\n"
        output += f"Signal Agreement: {rec.signal_agreement*100:.0f}%\n"
        if rec.predicted_probability_10d_up is not None:
            output += (
                f"Predicted 10D Up Probability: "
                f"{rec.predicted_probability_10d_up*100:.0f}%\n"
            )
        output += (
            f"Heuristic Score: {rec.heuristic_score:.1f}/100 "
            f"({rec.heuristic_score_category.value})\n"
        )
        output += f"Heuristic Risk: {risk} {rec.heuristic_risk_level}\n\n"
        
        output += f"Price Information:\n"
        output += f"  Current: ₦{rec.current_price:,.2f}\n"
        if rec.policy_target_price:
            output += f"  Policy Target: ₦{rec.policy_target_price:,.2f}\n"
        if rec.policy_stop_loss:
            output += f"  Policy Stop:   ₦{rec.policy_stop_loss:,.2f}\n"
        if rec.policy_target_price and rec.current_price:
            potential_gain = ((rec.policy_target_price - rec.current_price) / rec.current_price) * 100
            output += f"  Policy Upside: {potential_gain:+.1f}%\n"
        
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
