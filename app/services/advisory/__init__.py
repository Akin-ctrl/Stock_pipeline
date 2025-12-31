"""
Investment advisory services.

Generates stock recommendations based on technical analysis.
"""

from app.services.advisory.signals import (
    SignalGenerator, SignalType, TechnicalSignal
)
from app.services.advisory.scoring import (
    StockScorer, StockScore, ScoreCategory
)
from app.services.advisory.advisor import (
    InvestmentAdvisor, StockRecommendation
)

__all__ = [
    'SignalGenerator',
    'SignalType',
    'TechnicalSignal',
    'StockScorer',
    'StockScore',
    'ScoreCategory',
    'InvestmentAdvisor',
    'StockRecommendation',
]
