-- Add production portfolio-gate fields for daily recommendations.
--
-- Run after 2026_05_30_recommendation_dashboard_schema.sql if the dashboard
-- schema already exists. This migration is idempotent for repeated deploys.

BEGIN;

DROP VIEW IF EXISTS vw_daily_recommendation_board;
DROP VIEW IF EXISTS vw_sector_performance;
DROP VIEW IF EXISTS vw_recommendation_board;
DROP VIEW IF EXISTS vw_model_health;

ALTER TABLE fact_recommendations
    ADD COLUMN IF NOT EXISTS portfolio_approved BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS portfolio_rejection_reason VARCHAR(50),
    ADD COLUMN IF NOT EXISTS portfolio_rank INTEGER,
    ADD COLUMN IF NOT EXISTS portfolio_position_size_pct NUMERIC(6, 4),
    ADD COLUMN IF NOT EXISTS portfolio_policy_version VARCHAR(50),
    ADD COLUMN IF NOT EXISTS portfolio_open_positions_before INTEGER,
    ADD COLUMN IF NOT EXISTS portfolio_available_slots_before INTEGER,
    ADD COLUMN IF NOT EXISTS portfolio_max_concurrent_positions INTEGER,
    ADD COLUMN IF NOT EXISTS portfolio_max_entries_per_day INTEGER;

CREATE INDEX IF NOT EXISTS ix_fact_recommendations_portfolio_approved
    ON fact_recommendations (recommendation_date, profile, portfolio_approved);

CREATE OR REPLACE VIEW vw_recommendation_board AS
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY r.recommendation_date, r.profile
        ORDER BY
            r.portfolio_approved DESC,
            r.portfolio_rank ASC NULLS LAST,
            CASE WHEN r.predicted_probability_10d_up IS NULL THEN 0 ELSE 1 END DESC,
            r.predicted_probability_10d_up DESC NULLS LAST,
            r.heuristic_score DESC,
            r.signal_agreement DESC,
            s.stock_code
    ) AS recommendation_rank,
    r.recommendation_id,
    r.recommendation_date,
    r.profile,
    s.stock_id,
    s.stock_code,
    s.company_name,
    sec.sector_name,
    r.action_type,
    r.technical_signal_type,
    r.signal_agreement,
    ROUND((r.signal_agreement * 100.0)::numeric, 2) AS signal_agreement_pct,
    r.predicted_probability_10d_up,
    ROUND((r.predicted_probability_10d_up * 100.0)::numeric, 2) AS predicted_probability_10d_up_pct,
    r.heuristic_score,
    r.heuristic_score_category,
    r.current_price,
    r.policy_target_price,
    r.policy_stop_loss,
    r.policy_upside_pct,
    r.policy_downside_pct,
    r.risk_reward_ratio,
    r.heuristic_risk_level,
    r.technical_score,
    r.momentum_score,
    r.volatility_score,
    r.trend_score,
    r.volume_score,
    r.rsi_14,
    r.macd,
    r.portfolio_approved,
    r.portfolio_rejection_reason,
    r.portfolio_rank,
    r.portfolio_position_size_pct,
    r.portfolio_policy_version,
    r.portfolio_open_positions_before,
    r.portfolio_available_slots_before,
    r.portfolio_max_concurrent_positions,
    r.portfolio_max_entries_per_day,
    r.reasons,
    r.model_version,
    r.created_at
FROM fact_recommendations r
JOIN dim_stocks s ON s.stock_id = r.stock_id
LEFT JOIN dim_sectors sec ON sec.sector_id = s.sector_id;

CREATE OR REPLACE VIEW vw_daily_recommendation_board AS
SELECT *
FROM vw_recommendation_board
WHERE recommendation_date = (
    SELECT MAX(recommendation_date)
    FROM fact_recommendations
)
AND portfolio_approved;

CREATE OR REPLACE VIEW vw_sector_performance AS
WITH latest_date AS (
    SELECT MAX(price_date) AS market_date
    FROM fact_daily_prices
),
latest_prices AS (
    SELECT
        f.stock_id,
        f.change_1d_pct,
        f.change_ytd_pct,
        f.volume
    FROM fact_daily_prices f
    JOIN latest_date ld ON ld.market_date = f.price_date
),
latest_recommendations AS (
    SELECT r.*
    FROM fact_recommendations r
    WHERE r.recommendation_date = (
        SELECT MAX(recommendation_date)
        FROM fact_recommendations
    )
)
SELECT
    ld.market_date,
    sec.sector_name,
    COUNT(DISTINCT s.stock_id) AS active_stocks,
    ROUND(AVG(lp.change_1d_pct), 4) AS average_1d_return_pct,
    ROUND(AVG(lp.change_ytd_pct), 4) AS average_ytd_return_pct,
    SUM(lp.volume) AS total_volume,
    COUNT(lr.recommendation_id) AS recommendation_count,
    COUNT(lr.recommendation_id) FILTER (
        WHERE lr.action_type IN ('BUY', 'STRONG_BUY')
        AND lr.portfolio_approved
    ) AS actionable_count,
    ROUND(AVG(lr.heuristic_score), 2) AS average_heuristic_score,
    ROUND(AVG(lr.predicted_probability_10d_up * 100.0), 2) AS average_probability_pct
FROM latest_date ld
CROSS JOIN dim_sectors sec
LEFT JOIN dim_stocks s ON s.sector_id = sec.sector_id AND s.is_active
LEFT JOIN latest_prices lp ON lp.stock_id = s.stock_id
LEFT JOIN latest_recommendations lr ON lr.stock_id = s.stock_id
GROUP BY ld.market_date, sec.sector_name;

CREATE OR REPLACE VIEW vw_model_health AS
SELECT
    br.run_id,
    br.run_date,
    br.profile,
    br.start_date,
    br.end_date,
    br.horizon_days,
    br.total_trades,
    br.win_rate_pct,
    br.average_return_pct,
    br.average_win_pct,
    br.average_loss_pct,
    br.profit_factor,
    br.directional_accuracy_pct,
    br.max_drawdown_pct,
    (br.run_metadata->'portfolio'->>'total_return_pct')::numeric AS portfolio_return_pct,
    (br.run_metadata->'portfolio'->>'max_drawdown_pct')::numeric AS portfolio_max_drawdown_pct,
    (br.run_metadata->'portfolio'->>'win_rate_pct')::numeric AS portfolio_win_rate_pct,
    (br.run_metadata->'portfolio'->>'profit_factor')::numeric AS portfolio_profit_factor,
    ds.status AS decision_status,
    ds.lookback_runs,
    ds.rationale,
    br.run_metadata,
    br.created_at
FROM backtest_runs br
LEFT JOIN decision_signals ds
    ON ds.run_date = br.run_date
    AND ds.profile = br.profile;

COMMIT;
