"""
Stock screening services.

Generates stock screening signals based on technical analysis.

**DISCLAIMER**: For educational/analysis purposes only. Not investment advice.
"""

from app.services.advisory.signals import (
    SignalGenerator, SignalType, TechnicalSignal
)
from app.services.advisory.scoring import (
    StockScorer, StockScore, ScoreCategory
)
from app.services.advisory.eligibility import (
    EligibilityConfig,
    EligibilityDecision,
    RecommendationEligibilityEvaluator,
)
from app.services.advisory.policy import RecommendationPolicyEngine, RecommendationPolicyOutput
from app.services.advisory.selection import (
    RecommendationSelectionEvaluator,
    SelectionConfig,
    SelectionDecision,
)
from app.services.advisory.advisor import (
    StockScreener,
    StockRecommendation,
    RecommendationAction,
    RecommendationProfile,
    RecommendationProfileConfig,
    PolicyConfig,
    ScoringConfig,
    SignalConfig,
)

__all__ = [
    'SignalGenerator',
    'SignalType',
    'TechnicalSignal',
    'StockScorer',
    'StockScore',
    'ScoreCategory',
    'EligibilityConfig',
    'EligibilityDecision',
    'RecommendationEligibilityEvaluator',
    'RecommendationPolicyEngine',
    'RecommendationPolicyOutput',
    'RecommendationSelectionEvaluator',
    'SelectionConfig',
    'SelectionDecision',
    'StockScreener',
    'StockRecommendation',
    'RecommendationAction',
    'RecommendationProfile',
    'RecommendationProfileConfig',
    'PolicyConfig',
    'ScoringConfig',
    'SignalConfig',
]
