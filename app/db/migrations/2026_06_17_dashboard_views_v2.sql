-- Dashboard views v2: fix multi-profile, recommendation board, and command center.
--
-- Fixes:
--   1. vw_daily_recommendation_board: show ALL candidates (not just approved)
--   2. vw_sector_performance: partition by profile to prevent double-counting
--   3. vw_latest_model_verdict: show latest per-profile (not random LIMIT 1)
--   4. vw_dashboard_command_center: count approved picks from fact_recommendations

BEGIN;

-- Drop dependent views first (reverse dependency order)
DROP VIEW IF EXISTS vw_dashboard_command_center;
DROP VIEW IF EXISTS vw_daily_recommendation_board;
DROP VIEW IF EXISTS vw_sector_performance;
DROP VIEW IF EXISTS vw_latest_model_verdict;
DROP VIEW IF EXISTS vw_model_health;

-- 1. vw_daily_recommendation_board: show ALL candidates for the latest date.
--    The portfolio_approved column remains for filtering/coloring in charts.
CREATE OR REPLACE VIEW vw_daily_recommendation_board AS
SELECT *
FROM vw_recommendation_board
WHERE recommendation_date = (
    SELECT MAX(recommendation_date)
    FROM fact_recommendations
);

-- 2. vw_sector_performance: add profile to GROUP BY to prevent double-counting.
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
    lr.profile,
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
CROSS JOIN (SELECT DISTINCT profile FROM fact_recommendations) pr
LEFT JOIN dim_stocks s ON s.sector_id = sec.sector_id AND s.is_active
LEFT JOIN latest_prices lp ON lp.stock_id = s.stock_id
LEFT JOIN latest_recommendations lr
    ON lr.stock_id = s.stock_id
    AND lr.profile = pr.profile
GROUP BY ld.market_date, lr.profile, sec.sector_name;

-- 3. vw_latest_model_verdict: show latest run per profile (not random LIMIT 1).
CREATE OR REPLACE VIEW vw_latest_model_verdict AS
WITH ranked_runs AS (
    SELECT
        br.*,
        ROW_NUMBER() OVER (
            PARTITION BY br.profile
            ORDER BY br.run_date DESC, br.created_at DESC, br.run_id DESC
        ) AS rn
    FROM backtest_runs br
    WHERE br.run_type = 'full_validation'
)
SELECT
    lr.run_id,
    lr.run_date,
    lr.profile,
    lr.run_type,
    lr.start_date,
    lr.end_date,
    lr.horizon_days,
    lr.total_trades AS raw_trade_count,
    lr.win_rate_pct AS raw_win_rate_pct,
    lr.profit_factor AS raw_profit_factor,
    lr.max_drawdown_pct AS raw_max_drawdown_pct,
    (lr.run_metadata->'portfolio'->>'realized_trade_count')::integer AS portfolio_trade_count,
    (lr.run_metadata->'portfolio'->>'total_return_pct')::numeric AS portfolio_return_pct,
    (lr.run_metadata->'portfolio'->>'max_drawdown_pct')::numeric AS portfolio_max_drawdown_pct,
    (lr.run_metadata->'portfolio'->>'win_rate_pct')::numeric AS portfolio_win_rate_pct,
    (lr.run_metadata->'portfolio'->>'profit_factor')::numeric AS portfolio_profit_factor,
    ds.status AS decision_status,
    ds.rationale AS decision_rationale,
    lr.run_metadata
FROM ranked_runs lr
LEFT JOIN decision_signals ds
    ON ds.run_date = lr.run_date
    AND ds.profile = lr.profile
    AND ds.run_type = lr.run_type
WHERE lr.rn = 1;

-- 4. Recreate vw_model_health (depends on backtest_runs + decision_signals,
--    unchanged but must be recreated because we dropped it).
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

-- 5. vw_dashboard_command_center: count approved picks from fact_recommendations
--    directly (not from the daily board view which now includes rejected rows).
CREATE OR REPLACE VIEW vw_dashboard_command_center AS
SELECT
    mv.run_id,
    mv.run_date,
    mv.profile,
    mv.start_date,
    mv.end_date,
    mv.decision_status,
    mv.portfolio_trade_count,
    mv.portfolio_return_pct,
    mv.portfolio_max_drawdown_pct,
    mv.portfolio_win_rate_pct,
    mv.portfolio_profit_factor,
    mo.market_date,
    mo.priced_stocks,
    mo.advancers,
    mo.decliners,
    mo.good_quality_pct,
    COALESCE(dr.approved_recommendations, 0) AS approved_recommendations
FROM vw_latest_model_verdict mv
LEFT JOIN vw_market_overview mo ON TRUE
LEFT JOIN (
    SELECT profile, COUNT(*) AS approved_recommendations
    FROM fact_recommendations
    WHERE recommendation_date = (
        SELECT MAX(recommendation_date)
        FROM fact_recommendations
    )
    AND portfolio_approved
    GROUP BY profile
) dr ON dr.profile = mv.profile;

COMMIT;
