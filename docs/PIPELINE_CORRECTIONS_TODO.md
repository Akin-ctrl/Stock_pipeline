# Pipeline Corrections Todo

Status: Superseded / mostly completed as of 2026-05-31.

This file is no longer an active implementation checklist. It records problems
that were identified during cleanup and model-correction work. Use the current
code, migrations, tests, and operational docs as the implementation authority.

## Historical Problems

- `volume` exists in staging but is lost before the price fact load. The staging model and staging repository still carry it, but `fact_daily_prices` does not.
- `fact_daily_prices` is missing fields that downstream code still expects in places, so the model/repository contract is inconsistent.
- The recommendation layer maps indicator horizons incorrectly: `ma_30` is treated as `sma_50`, and `ma_90` is treated as `sma_200`.
- `volume_ratio` is currently a placeholder in the advisor, so volume-based signals and scores are not based on real history.
- `price_change_pct` is sometimes derived from price versus SMA instead of actual day-over-day movement, which distorts momentum scoring.
- RSI, Bollinger Bands, and volatility handling need tighter mathematical treatment for investment-grade accuracy.
- Alert evaluation does not fully cover every supported rule type, specifically `MACD`.
- Recommendation generation can become date-inconsistent if indicators are not fetched with the same as-of cutoff as prices.

## Current Resolution Status

- Volume now exists on `fact_daily_prices` and is carried through staging,
  promotion, repositories, and downstream features.
- The canonical `FactDailyPrice` model is close-price-centric with volume,
  change fields, and trust metadata.
- Advisor logic now uses the current `ma_7`, `ma_30`, and `ma_90` indicator
  horizons directly.
- Volume ratio and volume-trend features are calculated from historical volume
  where available.
- Recommendation analysis is date-bounded so the selected price and indicator
  rows use the same as-of boundary.
- Technical sell and strong-sell signals are preserved as technical signals but
  mapped to long-only `AVOID` and `STRONGLY_AVOID` user actions.
- Indicator persistence and historical indicator backfill use upsert behavior;
  reruns should update existing keys rather than skip them.

## Rectification Steps

Historical sequence, retained for audit only:

1. Restore end-to-end `volume` support from staging through loading into the price fact layer.
2. Align `FactDailyPrice`, `PriceRepository`, and any schema migration so the model, queries, and database all agree.
3. Correct the indicator-to-recommendation mapping so each feature uses the intended lookback window.
4. Replace placeholder `volume_ratio` logic with a real historical volume comparison.
5. Compute `price_change_pct` from actual daily price movement only, or omit it when the needed input is unavailable.
6. Rework indicator formulas to canonical finance definitions and handle warm-up periods explicitly.
7. Add missing alert rule evaluation paths, starting with `MACD`.
8. Ensure recommendation analysis uses the same date boundary for both prices and indicators to avoid look-ahead bias.
9. Add or update tests for schema parity, indicator math, alert triggers, and recommendation outputs.

## Working Checklist

- [x] Restore `volume` in the price fact schema
- [x] Update load/repository code to persist `volume`
- [x] Fix indicator horizon mappings in advisor logic
- [x] Replace placeholder volume scoring with real calculation
- [x] Remove or correct derived momentum fallback logic
- [x] Tighten indicator warm-up handling for historical backfill
- [x] Implement missing alert rule coverage for current supported rules
- [x] Enforce date-consistent recommendation analysis
- [ ] Continue expanding regression tests around indicator math, alert triggers,
  idempotent writes, and recommendation outputs
