-- Add weekly recommendation candidate board.
--
-- Weekly recommendations are intentionally separate from fact_recommendations:
-- daily facts remain strict execution/portfolio-approved signals, while this
-- table exposes slower weekly watchlist candidates with explicit gate reasons.

BEGIN;

CREATE TABLE IF NOT EXISTS weekly_recommendations (
    weekly_recommendation_id BIGSERIAL PRIMARY KEY,
    week_start_date DATE NOT NULL,
    week_end_date DATE NOT NULL,
    recommendation_date DATE NOT NULL,
    profile VARCHAR(50) NOT NULL,

    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    stock_code VARCHAR(20) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    sector_name VARCHAR(100),

    rank INTEGER NOT NULL,
    weekly_status VARCHAR(30) NOT NULL,
    candidate_tier VARCHAR(20) NOT NULL,
    action_type VARCHAR(20) NOT NULL,
    technical_signal_type VARCHAR(20),
    rejection_reason VARCHAR(100),

    signal_agreement NUMERIC(6, 4),
    heuristic_score NUMERIC(6, 2) NOT NULL,
    current_price NUMERIC(18, 4) NOT NULL,
    rsi_14 NUMERIC(5, 2),
    volatility NUMERIC(10, 4),
    volume_ratio NUMERIC(12, 4),
    price_change_20d NUMERIC(10, 4),
    drawdown_20d_pct NUMERIC(10, 4),

    rationale JSONB DEFAULT '[]'::jsonb,
    source VARCHAR(50) NOT NULL DEFAULT 'recommendation_audit',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_weekly_recommendation_status
        CHECK (
            weekly_status IN (
                'APPROVED',
                'WATCHLIST',
                'WAIT_FOR_PULLBACK',
                'WAIT_FOR_VOLUME',
                'HIGH_RISK_WATCHLIST',
                'SPECULATIVE_WATCHLIST'
            )
        ),
    CONSTRAINT chk_weekly_recommendation_candidate_tier
        CHECK (candidate_tier IN ('approved', 'watchlist')),
    CONSTRAINT chk_weekly_recommendation_action_type
        CHECK (action_type IN ('STRONG_BUY', 'BUY')),
    CONSTRAINT chk_weekly_recommendation_rank_positive
        CHECK (rank > 0)
);

CREATE INDEX IF NOT EXISTS idx_weekly_recommendation_week
    ON weekly_recommendations (week_end_date, profile);
CREATE INDEX IF NOT EXISTS idx_weekly_recommendation_stock
    ON weekly_recommendations (stock_code);
CREATE UNIQUE INDEX IF NOT EXISTS ux_weekly_recommendation_week_profile_stock
    ON weekly_recommendations (week_end_date, profile, stock_id);

CREATE OR REPLACE VIEW vw_weekly_recommendation_board AS
SELECT
    wr.weekly_recommendation_id,
    wr.week_start_date,
    wr.week_end_date,
    wr.recommendation_date,
    wr.profile,
    wr.rank,
    wr.weekly_status,
    wr.candidate_tier,
    wr.stock_code,
    wr.company_name,
    wr.sector_name,
    wr.action_type,
    wr.technical_signal_type,
    wr.rejection_reason,
    wr.heuristic_score,
    wr.signal_agreement,
    wr.current_price,
    wr.rsi_14,
    wr.volatility,
    wr.volume_ratio,
    wr.price_change_20d,
    wr.drawdown_20d_pct,
    wr.rationale,
    wr.source,
    wr.created_at
FROM weekly_recommendations wr;

COMMIT;
