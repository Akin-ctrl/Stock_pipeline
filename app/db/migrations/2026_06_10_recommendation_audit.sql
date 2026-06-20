-- Add per-stock recommendation audit persistence.
--
-- This table captures the full candidate funnel for each recommendation run
-- so zero-recommendation days can be explained and visualized.

BEGIN;

CREATE TABLE IF NOT EXISTS fact_recommendation_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id),
    recommendation_date DATE NOT NULL,
    profile VARCHAR(50) NOT NULL DEFAULT 'steady_20p_10d',
    price_date DATE,
    indicator_date DATE,
    current_price NUMERIC(18, 4),
    stage_reached VARCHAR(50) NOT NULL,
    rejection_reason VARCHAR(100),
    eligible BOOLEAN NOT NULL DEFAULT FALSE,
    selected BOOLEAN NOT NULL DEFAULT FALSE,
    candidate_tier VARCHAR(20) NOT NULL DEFAULT 'blocked',
    portfolio_approved BOOLEAN NOT NULL DEFAULT FALSE,
    portfolio_rejection_reason VARCHAR(50),
    portfolio_rank INTEGER,
    action_type VARCHAR(20),
    technical_signal_type VARCHAR(20),
    signal_agreement NUMERIC(6, 4),
    predicted_probability_10d_up NUMERIC(6, 4),
    heuristic_score NUMERIC(6, 2),
    heuristic_score_category VARCHAR(20),
    rsi_14 NUMERIC(5, 2),
    volatility NUMERIC(10, 4),
    volume_ratio NUMERIC(12, 4),
    price_change_20d NUMERIC(10, 4),
    drawdown_20d_pct NUMERIC(10, 4),
    trusted_history_days INTEGER,
    price_quality_flag VARCHAR(20),
    bar_status VARCHAR(20),
    has_complete_data BOOLEAN,
    is_official BOOLEAN,
    score_breakdown JSONB DEFAULT '{}'::jsonb,
    indicators JSONB DEFAULT '{}'::jsonb,
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT chk_recommendation_audit_stage CHECK (
        stage_reached IN (
            'stock_loaded',
            'no_indicator',
            'no_trusted_price',
            'indicator_price_date_mismatch',
            'scored',
            'eligibility_failed',
            'selection_failed',
            'selected',
            'portfolio_evaluated'
        )
    ),
    CONSTRAINT chk_recommendation_audit_candidate_tier CHECK (
        candidate_tier IN ('approved', 'watchlist', 'avoid', 'blocked')
    ),
    CONSTRAINT chk_recommendation_audit_signal_agreement CHECK (
        signal_agreement IS NULL OR signal_agreement BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_recommendation_audit_predicted_probability CHECK (
        predicted_probability_10d_up IS NULL
        OR predicted_probability_10d_up BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_recommendation_audit_heuristic_score CHECK (
        heuristic_score IS NULL OR heuristic_score BETWEEN 0 AND 100
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_recommendation_audit_stock_date_profile
    ON fact_recommendation_audit (stock_id, recommendation_date, profile);

CREATE INDEX IF NOT EXISTS ix_recommendation_audit_date_profile_stage
    ON fact_recommendation_audit (recommendation_date, profile, stage_reached);

CREATE INDEX IF NOT EXISTS ix_recommendation_audit_rejection_reason
    ON fact_recommendation_audit (rejection_reason);

CREATE OR REPLACE VIEW vw_recommendation_candidate_funnel AS
SELECT
    recommendation_date,
    profile,
    stage_reached,
    rejection_reason,
    candidate_tier,
    COUNT(*) AS stock_count
FROM fact_recommendation_audit
GROUP BY recommendation_date, profile, stage_reached, rejection_reason, candidate_tier;

CREATE OR REPLACE VIEW vw_latest_recommendation_candidate_funnel AS
SELECT *
FROM vw_recommendation_candidate_funnel
WHERE recommendation_date = (
    SELECT MAX(recommendation_date)
    FROM fact_recommendation_audit
);

COMMIT;
