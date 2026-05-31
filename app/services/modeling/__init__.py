"""Modeling utilities for dataset construction and target definitions."""

from app.services.modeling.dataset_builder import (
    ModelingDatasetBuilder,
    ModelingDatasetConfig,
    ModelingDatasetRow,
    ModelingDatasetSummary,
)
from app.services.modeling.feature_engineering import PROBABILITY_FEATURE_NAMES
from app.services.modeling.feature_validation import (
    FeatureAlignmentReport,
    FeatureHealthStat,
    FeatureValidationSummary,
    summarize_probability_features,
    validate_probability_feature_alignment,
)
from app.services.modeling.model_validation import (
    BaselineComparison,
    FoldWindow,
    ModelValidationReport,
    ProbabilityBucketStat,
    ValidationPrediction,
    WalkForwardFoldResult,
    WalkForwardModelValidator,
    build_probability_bucket_stats,
    compare_momentum_baseline,
    generate_walk_forward_windows,
)
from app.services.modeling.probability_estimator import (
    HistoricalLogisticProbabilityEstimator,
    LogisticProbabilityModel,
    NullProbabilityEstimator,
    ProbabilityEstimator,
)
from app.services.modeling.targets import (
    DEFAULT_DIRECTION_TARGET,
    DirectionTargetDefinition,
    ForwardReturnLabel,
    build_forward_return_label,
    calculate_forward_return_pct,
)
from app.services.modeling.trust_validation import (
    TrustCohortStat,
    TrustFilterComparison,
    TrustValidationReport,
    TrustValidator,
    build_confidence_band_stats,
    build_standard_trust_filter_comparisons,
)

__all__ = [
    "DEFAULT_DIRECTION_TARGET",
    "DirectionTargetDefinition",
    "FoldWindow",
    "FeatureAlignmentReport",
    "FeatureHealthStat",
    "FeatureValidationSummary",
    "ForwardReturnLabel",
    "HistoricalLogisticProbabilityEstimator",
    "LogisticProbabilityModel",
    "ModelValidationReport",
    "ModelingDatasetBuilder",
    "ModelingDatasetConfig",
    "ModelingDatasetRow",
    "ModelingDatasetSummary",
    "NullProbabilityEstimator",
    "PROBABILITY_FEATURE_NAMES",
    "ProbabilityBucketStat",
    "ProbabilityEstimator",
    "TrustCohortStat",
    "TrustFilterComparison",
    "TrustValidationReport",
    "TrustValidator",
    "ValidationPrediction",
    "WalkForwardFoldResult",
    "WalkForwardModelValidator",
    "BaselineComparison",
    "build_forward_return_label",
    "build_confidence_band_stats",
    "build_probability_bucket_stats",
    "build_standard_trust_filter_comparisons",
    "calculate_forward_return_pct",
    "compare_momentum_baseline",
    "generate_walk_forward_windows",
    "summarize_probability_features",
    "validate_probability_feature_alignment",
]
