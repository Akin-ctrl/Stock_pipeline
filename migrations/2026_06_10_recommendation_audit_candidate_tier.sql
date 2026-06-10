-- Add dashboard candidate tiers to recommendation audit rows.

BEGIN;

DROP VIEW IF EXISTS vw_latest_recommendation_candidate_funnel;
DROP VIEW IF EXISTS vw_recommendation_candidate_funnel;

ALTER TABLE fact_recommendation_audit
    ADD COLUMN IF NOT EXISTS candidate_tier VARCHAR(20) NOT NULL DEFAULT 'blocked';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_recommendation_audit_candidate_tier'
    ) THEN
        ALTER TABLE fact_recommendation_audit
            ADD CONSTRAINT chk_recommendation_audit_candidate_tier
            CHECK (candidate_tier IN ('approved', 'watchlist', 'avoid', 'blocked'));
    END IF;
END $$;

CREATE OR REPLACE VIEW vw_recommendation_candidate_funnel AS
SELECT
    recommendation_date,
    profile,
    stage_reached,
    rejection_reason,
    candidate_tier,
    COUNT(*) AS stock_count
FROM fact_recommendation_audit
GROUP BY
    recommendation_date,
    profile,
    stage_reached,
    rejection_reason,
    candidate_tier;

CREATE OR REPLACE VIEW vw_latest_recommendation_candidate_funnel AS
SELECT *
FROM vw_recommendation_candidate_funnel
WHERE recommendation_date = (
    SELECT MAX(recommendation_date)
    FROM fact_recommendation_audit
);

COMMIT;
