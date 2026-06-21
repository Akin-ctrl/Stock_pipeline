-- Recommendation model and dashboard semantic-layer migration.
--
-- Preconditions:
-- - Run against the stock_pipeline database only.
-- - The operational fact/staging tables have already been cleared.
-- - dim_stocks and dim_sectors are preserved.

BEGIN;

DROP VIEW IF EXISTS vw_data_quality_monitor;
DROP VIEW IF EXISTS vw_backtest_equity_curve;
DROP VIEW IF EXISTS vw_model_health;
DROP VIEW IF EXISTS vw_sector_performance;
DROP VIEW IF EXISTS vw_daily_recommendation_board;
DROP VIEW IF EXISTS vw_recommendation_board;
DROP VIEW IF EXISTS vw_stock_price_panel;
DROP VIEW IF EXISTS vw_market_overview;

DROP TABLE IF EXISTS daily_recommendation_snapshots CASCADE;
DROP TABLE IF EXISTS fact_recommendations CASCADE;

CREATE TABLE fact_recommendations (
    recommendation_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    recommendation_date DATE NOT NULL,
    profile VARCHAR(50) NOT NULL DEFAULT 'steady_20p_10d',

    action_type VARCHAR(20) NOT NULL,
    technical_signal_type VARCHAR(20) NOT NULL,
    signal_agreement NUMERIC(6, 4) NOT NULL,
    predicted_probability_10d_up NUMERIC(6, 4),
    heuristic_score NUMERIC(6, 2) NOT NULL,
    heuristic_score_category VARCHAR(20) NOT NULL,

    current_price NUMERIC(18, 4) NOT NULL,
    policy_target_price NUMERIC(18, 4),
    policy_stop_loss NUMERIC(18, 4),
    policy_upside_pct NUMERIC(8, 4),
    policy_downside_pct NUMERIC(8, 4),
    risk_reward_ratio NUMERIC(10, 4),

    heuristic_risk_level VARCHAR(10) NOT NULL,
    reasons JSONB DEFAULT '[]'::jsonb,

    technical_score NUMERIC(5, 2),
    momentum_score NUMERIC(5, 2),
    volatility_score NUMERIC(5, 2),
    trend_score NUMERIC(5, 2),
    volume_score NUMERIC(5, 2),
    rsi_14 NUMERIC(5, 2),
    macd NUMERIC(18, 4),

    portfolio_approved BOOLEAN NOT NULL DEFAULT FALSE,
    portfolio_rejection_reason VARCHAR(50),
    portfolio_rank INTEGER,
    portfolio_position_size_pct NUMERIC(6, 4),
    portfolio_policy_version VARCHAR(50),
    portfolio_open_positions_before INTEGER,
    portfolio_available_slots_before INTEGER,
    portfolio_max_concurrent_positions INTEGER,
    portfolio_max_entries_per_day INTEGER,

    model_version VARCHAR(50),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    outcome VARCHAR(20),
    outcome_date DATE,
    actual_return_pct NUMERIC(7, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_recommendation_action_type
        CHECK (action_type IN ('STRONG_BUY', 'BUY', 'HOLD', 'AVOID', 'STRONGLY_AVOID')),
    CONSTRAINT chk_recommendation_technical_signal_type
        CHECK (technical_signal_type IN ('STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL')),
    CONSTRAINT chk_recommendation_signal_agreement
        CHECK (signal_agreement >= 0 AND signal_agreement <= 1),
    CONSTRAINT chk_recommendation_predicted_probability
        CHECK (
            predicted_probability_10d_up IS NULL
            OR predicted_probability_10d_up BETWEEN 0 AND 1
        ),
    CONSTRAINT chk_recommendation_heuristic_score
        CHECK (heuristic_score >= 0 AND heuristic_score <= 100),
    CONSTRAINT chk_recommendation_risk_level
        CHECK (heuristic_risk_level IN ('LOW', 'MEDIUM', 'HIGH')),
    CONSTRAINT chk_recommendation_current_price_positive
        CHECK (current_price > 0),
    CONSTRAINT chk_recommendation_score_category
        CHECK (heuristic_score_category IN ('EXCELLENT', 'GOOD', 'FAIR', 'POOR', 'VERY_POOR')),
    CONSTRAINT chk_recommendation_outcome
        CHECK (
            outcome IS NULL
            OR outcome IN ('HIT_TARGET', 'HIT_STOP_LOSS', 'ONGOING', 'EXPIRED')
        )
);

CREATE UNIQUE INDEX ux_recommendation_stock_date_profile
    ON fact_recommendations (stock_id, recommendation_date, profile);
CREATE INDEX ix_fact_recommendations_recommendation_date
    ON fact_recommendations (recommendation_date);
CREATE INDEX ix_fact_recommendations_profile
    ON fact_recommendations (profile);
CREATE INDEX ix_fact_recommendations_action_type
    ON fact_recommendations (action_type);
CREATE INDEX ix_fact_recommendations_active
    ON fact_recommendations (is_active);
CREATE INDEX ix_fact_recommendations_portfolio_approved
    ON fact_recommendations (recommendation_date, profile, portfolio_approved);
CREATE INDEX ix_fact_recommendations_rank
    ON fact_recommendations (
        recommendation_date DESC,
        profile,
        action_type,
        predicted_probability_10d_up DESC,
        heuristic_score DESC
    );

CREATE OR REPLACE VIEW vw_market_overview AS
WITH latest_date AS (
    SELECT MAX(price_date) AS market_date
    FROM fact_daily_prices
),
latest_prices AS (
    SELECT f.*
    FROM fact_daily_prices f
    JOIN latest_date ld ON ld.market_date = f.price_date
)
SELECT
    ld.market_date,
    COUNT(lp.stock_id) AS priced_stocks,
    COUNT(*) FILTER (WHERE lp.change_1d_pct > 0) AS advancers,
    COUNT(*) FILTER (WHERE lp.change_1d_pct < 0) AS decliners,
    COUNT(lp.stock_id) FILTER (
        WHERE lp.change_1d_pct = 0 OR lp.change_1d_pct IS NULL
    ) AS unchanged,
    ROUND(AVG(lp.change_1d_pct), 4) AS average_1d_return_pct,
    SUM(lp.volume) AS total_volume,
    COUNT(*) FILTER (WHERE lp.data_quality_flag = 'GOOD') AS good_quality_rows,
    COUNT(*) FILTER (WHERE lp.has_complete_data) AS complete_rows,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE lp.data_quality_flag = 'GOOD')
        / NULLIF(COUNT(*), 0),
        2
    ) AS good_quality_pct
FROM latest_date ld
LEFT JOIN latest_prices lp ON TRUE
GROUP BY ld.market_date;

CREATE OR REPLACE VIEW vw_stock_price_panel AS
SELECT
    f.price_date,
    s.stock_id,
    s.stock_code,
    s.company_name,
    sec.sector_name,
    f.close_price,
    f.volume,
    f.change_1d_pct,
    f.change_ytd_pct,
    f.bar_status,
    f.confidence_score AS price_confidence_score,
    f.data_quality_flag,
    f.has_complete_data,
    i.ma_7,
    i.ma_30,
    i.ma_90,
    i.rsi_14,
    i.macd,
    i.macd_signal,
    i.macd_histogram,
    i.volatility_30,
    i.bollinger_upper,
    i.bollinger_middle,
    i.bollinger_lower,
    i.trend_strength
FROM fact_daily_prices f
JOIN dim_stocks s ON s.stock_id = f.stock_id
LEFT JOIN dim_sectors sec ON sec.sector_id = s.sector_id
LEFT JOIN fact_technical_indicators i
    ON i.stock_id = f.stock_id
    AND i.calculation_date = f.price_date;

CREATE OR REPLACE VIEW vw_recommendation_board AS
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY r.recommendation_date, r.profile
        ORDER BY
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

CREATE OR REPLACE VIEW vw_backtest_equity_curve AS
WITH ordered_trades AS (
    SELECT
        bt.run_id,
        br.profile,
        br.run_date,
        bt.trade_id,
        bt.stock_code,
        bt.entry_date,
        bt.exit_date,
        bt.signal_type,
        bt.net_return_pct,
        SUM(
            CASE
                WHEN bt.net_return_pct <= -100 THEN NULL
                ELSE LN(1 + (bt.net_return_pct / 100.0))
            END
        ) OVER (
            PARTITION BY bt.run_id
            ORDER BY bt.entry_date, bt.trade_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_log_return
    FROM backtest_trades bt
    JOIN backtest_runs br ON br.run_id = bt.run_id
)
SELECT
    run_id,
    profile,
    run_date,
    trade_id,
    stock_code,
    entry_date,
    exit_date,
    signal_type,
    net_return_pct,
    ROUND(((EXP(cumulative_log_return) - 1) * 100.0)::numeric, 4) AS cumulative_return_pct
FROM ordered_trades;

CREATE OR REPLACE VIEW vw_data_quality_monitor AS
WITH staging_by_date AS (
    SELECT
        price_date,
        COUNT(*) AS staged_rows,
        COUNT(*) FILTER (WHERE reconciled) AS reconciled_rows,
        COUNT(*) FILTER (WHERE close_price IS NULL OR close_price <= 0) AS invalid_staging_prices,
        COUNT(*) FILTER (WHERE change_1d_pct IS NULL) AS missing_staging_1d_change,
        COUNT(*) FILTER (WHERE change_ytd_pct IS NULL) AS missing_staging_ytd_change
    FROM staging_daily_prices
    GROUP BY price_date
),
fact_by_date AS (
    SELECT
        price_date,
        COUNT(*) AS fact_rows,
        COUNT(*) FILTER (WHERE close_price IS NULL OR close_price <= 0) AS invalid_fact_prices,
        COUNT(*) FILTER (WHERE data_quality_flag = 'GOOD') AS good_quality_rows,
        COUNT(*) FILTER (WHERE has_complete_data) AS complete_rows,
        COUNT(*) FILTER (WHERE bar_status IN ('RECONCILED', 'OFFICIAL')) AS trusted_rows
    FROM fact_daily_prices
    GROUP BY price_date
),
audit_by_date AS (
    SELECT
        price_date,
        COUNT(*) AS audit_rows,
        COUNT(*) FILTER (WHERE conflict_severity IN ('high', 'critical')) AS high_conflict_rows
    FROM staging_audit_log
    GROUP BY price_date
)
SELECT
    COALESCE(s.price_date, f.price_date, a.price_date) AS market_date,
    COALESCE(s.staged_rows, 0) AS staged_rows,
    COALESCE(s.reconciled_rows, 0) AS reconciled_rows,
    COALESCE(f.fact_rows, 0) AS fact_rows,
    COALESCE(a.audit_rows, 0) AS audit_rows,
    COALESCE(s.invalid_staging_prices, 0) AS invalid_staging_prices,
    COALESCE(f.invalid_fact_prices, 0) AS invalid_fact_prices,
    COALESCE(s.missing_staging_1d_change, 0) AS missing_staging_1d_change,
    COALESCE(s.missing_staging_ytd_change, 0) AS missing_staging_ytd_change,
    COALESCE(f.good_quality_rows, 0) AS good_quality_rows,
    COALESCE(f.complete_rows, 0) AS complete_rows,
    COALESCE(f.trusted_rows, 0) AS trusted_rows,
    COALESCE(a.high_conflict_rows, 0) AS high_conflict_rows,
    ROUND(100.0 * COALESCE(s.reconciled_rows, 0) / NULLIF(s.staged_rows, 0), 2) AS reconciliation_pct,
    ROUND(100.0 * COALESCE(f.good_quality_rows, 0) / NULLIF(f.fact_rows, 0), 2) AS good_quality_pct,
    ROUND(100.0 * COALESCE(f.complete_rows, 0) / NULLIF(f.fact_rows, 0), 2) AS completeness_pct
FROM staging_by_date s
FULL OUTER JOIN fact_by_date f ON f.price_date = s.price_date
FULL OUTER JOIN audit_by_date a
    ON a.price_date = COALESCE(s.price_date, f.price_date);

COMMIT;
