# Model Redesign Reconciliation Status

Date: 2026-05-31

## Purpose

This document replaces the original model redesign backlog.

The original backlog is stale because the recommendation model has already been redesigned to a significant degree. This file records which redesign items have been implemented in the codebase, which are only partially complete, and what still needs to be operationalized.

## Current Conclusion

The original model redesign backlog should no longer be treated as a pending implementation plan.

Most of the architectural redesign has been completed. The remaining work is no longer "design the model"; it is now:

- validate the model on refreshed historical prices and indicators
- calibrate thresholds from actual results
- decide whether to persist trained model artifacts
- update dashboard and documentation around the new semantics

Recommended handling:

- keep this file temporarily as a redesign completion record
- move remaining operational items into the active backlog
- then archive or delete this document

## Verification Scope

This reconciliation was checked against:

- `app/services/advisory/advisor.py`
- `app/services/advisory/signals.py`
- `app/services/advisory/scoring.py`
- `app/services/advisory/eligibility.py`
- `app/services/advisory/selection.py`
- `app/services/advisory/policy.py`
- `app/services/modeling/targets.py`
- `app/services/modeling/dataset_builder.py`
- `app/services/modeling/feature_engineering.py`
- `app/services/modeling/feature_extractor.py`
- `app/services/modeling/probability_estimator.py`
- `app/services/modeling/model_validation.py`
- `app/services/modeling/trust_validation.py`
- `app/services/backtesting/recommendation_backtester.py`
- `app/repositories/recommendation_repository.py`
- `app/models/fact.py`
- `scripts/backtest_recommendations.py`
- `scripts/weekly_backtest_report.py`
- `tests/unit/*model*`
- `tests/unit/test_advisory_*`
- `tests/unit/test_signal_agreement_semantics.py`
- `tests/integration/test_modeling_dataset_builder.py`
- `tests/integration/test_probability_baseline.py`
- `tests/integration/test_model_validation.py`
- `tests/integration/test_trust_validation.py`
- `tests/integration/test_recommendation_path.py`

## Redesign Item Status

### 1. Explicit Prediction Target

Status: complete.

The canonical target is now defined in `app/services/modeling/targets.py`.

Current target:

`P(10-trading-day return > 0)`

The modeling dataset stores both:

- `target_up_10d`
- `forward_return_10d`

### 2. Sell Labels Are Not First-Class Trading Actions

Status: complete.

The system now separates technical signals from user-facing long-only actions.

Current mapping:

- `STRONG_BUY` -> `STRONG_BUY`
- `BUY` -> `BUY`
- `HOLD` -> `HOLD`
- `SELL` -> `AVOID`
- `STRONG_SELL` -> `STRONGLY_AVOID`

The active selection layer is long-only and excludes non-buy actions when `buy_only` is enabled.

### 3. Confidence Split Into Signal Agreement And Prediction Probability

Status: complete with backward-compatible aliases.

The recommendation object and fact table now separate:

- `signal_agreement`
- `predicted_probability_10d_up`

Legacy names such as `confidence` still exist as aliases in some paths, but the current semantic meaning is signal agreement, not predictive confidence.

### 4. Outcome-Linked Probability Baseline

Status: substantially complete.

The repo now contains a lightweight logistic probability baseline:

- `LogisticProbabilityModel`
- `HistoricalLogisticProbabilityEstimator`
- probability-aware recommendation ranking
- `min_predicted_probability` filtering
- integration tests showing non-null probabilities when historical data is sufficient

Remaining work:

- run validation on the refreshed real database after historical prices and
  indicators have been loaded
- calibrate thresholds from actual market history
- decide whether trained model artifacts should be persisted instead of retrained on demand

### 5. Target, Stop, And Risk Are Policy Outputs

Status: complete.

Policy outputs have been separated into `RecommendationPolicyEngine`.

Current outputs include:

- `policy_target_price`
- `policy_stop_loss`
- `policy_upside_pct`
- `policy_downside_pct`
- `risk_reward_ratio`
- `heuristic_risk_level`

These are strategy-policy outputs, not claimed model forecasts.

### 6. Feature Set Expansion

Status: complete for the first redesigned baseline.

The model feature layer now includes:

- multi-horizon return features
- moving-average distance features
- rolling high/low distance features
- drawdown and rebound features
- volatility and downside-volatility features
- volume-ratio and volume-trend features
- technical indicator features
- trust and data-quality features

Remaining work:

- measure feature stability and usefulness on refreshed production-like data
- remove or revise weak features after validation, not by guesswork
- keep the current outlier guard under review as validation results improve

### 7. Strict Validation Framework

Status: implemented.

The repo now has walk-forward validation in `WalkForwardModelValidator`.

It reports:

- hit rate
- Brier score
- average 10-day forward return
- top-k hit rate
- top-k average forward return
- probability bucket stats
- comparison against a simple 10-day momentum baseline

Remaining work:

- run operational validation reports against the refreshed historical dataset
- use the report to select thresholds before dashboarding model quality

### 8. Trust Logic Repositioned And Validated

Status: implemented as tooling; not yet operationally interpreted.

Trust is now represented separately through:

- price trust metadata
- eligibility checks
- model features
- `TrustValidator`

The validation helper can compare trust cohorts and stricter filters against realized 10-day outcomes.

Remaining work:

- run trust validation on the refreshed database
- decide which trust filters improve results enough to keep

### 9. Profile Concerns Split Into Cleaner Layers

Status: substantially complete.

The active profile now composes separate layers:

- signal configuration
- heuristic scoring configuration
- eligibility configuration
- selection configuration
- policy configuration
- probability estimator interface

Remaining work:

- keep only one profile until the first profile is validated
- avoid adding more profiles before threshold calibration is evidence-based

## What Is Still Not Done

The model redesign is not finished in the financial sense until it has been validated against refreshed real data.

Open work:

- run walk-forward validation on refreshed historical rows
- run trust validation on refreshed historical rows
- choose score/probability thresholds from validation results
- compare against the simple momentum baseline
- decide whether probability models should be persisted, versioned, or retrained on demand
- update dashboard views and docs to show probability, signal agreement, policy, and realized performance separately

Recently completed or substantially completed:

- historical price data was reloaded through the current pipeline path
- historical indicators were backfilled from trusted `fact_daily_prices`
- indicator writes were changed to upsert rather than skip existing rows
- model dataset building now excludes extreme anchor-day and forward-return rows
  above 50 percent absolute return by default
- recommendation backtests now exclude extreme gross-return windows above
  50 percent absolute return by default

## Delete Or Keep?

Do not delete this file yet.

The old backlog is complete enough that it should not guide implementation anymore, but the remaining operational items should be moved to a current model-validation backlog first.

After those remaining items are moved, this document can be deleted or archived as a historical redesign record.
