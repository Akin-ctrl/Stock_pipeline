-- Persist full validation/backtest artifacts for BI dashboards.
--
-- This keeps backtest_runs as the parent execution record and adds
-- chart-friendly child tables for portfolio positions, equity/drawdown,
-- yearly performance, and stock/sector cohorts.

BEGIN;

ALTER TABLE backtest_runs
    ADD COLUMN IF NOT EXISTS run_type VARCHAR(50) NOT NULL DEFAULT 'weekly_validation';

CREATE INDEX IF NOT EXISTS idx_backtest_runs_run_type
    ON backtest_runs (run_type);

ALTER TABLE decision_signals
    ADD COLUMN IF NOT EXISTS run_type VARCHAR(50) NOT NULL DEFAULT 'weekly_validation';

DROP INDEX IF EXISTS ux_decision_signal_date_profile;

CREATE INDEX IF NOT EXISTS idx_decision_signal_run_type
    ON decision_signals (run_type);
CREATE UNIQUE INDEX IF NOT EXISTS ux_decision_signal_date_profile_type
    ON decision_signals (run_date, profile, run_type);

CREATE TABLE IF NOT EXISTS backtest_portfolio_positions (
    position_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    stock_code VARCHAR(20) NOT NULL,
    sector_name VARCHAR(255),
    entry_date DATE NOT NULL,
    exit_date DATE NOT NULL,
    holding_days INTEGER NOT NULL,
    signal_type VARCHAR(20) NOT NULL,
    confidence NUMERIC(5, 4),
    score NUMERIC(6, 2),
    predicted_probability_10d_up NUMERIC(6, 4),
    entry_price NUMERIC(18, 4) NOT NULL,
    exit_price NUMERIC(18, 4) NOT NULL,
    allocated_capital NUMERIC(18, 4) NOT NULL,
    net_return_pct NUMERIC(8, 4) NOT NULL,
    realized_pnl NUMERIC(18, 4) NOT NULL,
    exit_value NUMERIC(18, 4) NOT NULL,
    was_winner BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_backtest_portfolio_position_run
    ON backtest_portfolio_positions (run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_portfolio_position_stock
    ON backtest_portfolio_positions (stock_code);
CREATE INDEX IF NOT EXISTS idx_backtest_portfolio_position_entry
    ON backtest_portfolio_positions (entry_date);

CREATE TABLE IF NOT EXISTS backtest_portfolio_equity_curve (
    equity_point_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    point_index INTEGER NOT NULL,
    event_date DATE NOT NULL,
    cash NUMERIC(18, 4) NOT NULL,
    open_position_capital NUMERIC(18, 4) NOT NULL,
    equity NUMERIC(18, 4) NOT NULL,
    drawdown_pct NUMERIC(8, 4) NOT NULL,
    open_positions INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ux_backtest_portfolio_equity_run_index UNIQUE (run_id, point_index)
);

CREATE INDEX IF NOT EXISTS idx_backtest_portfolio_equity_run
    ON backtest_portfolio_equity_curve (run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_portfolio_equity_date
    ON backtest_portfolio_equity_curve (event_date);

CREATE TABLE IF NOT EXISTS backtest_yearly_performance (
    yearly_metric_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    calendar_year INTEGER NOT NULL,
    trade_count INTEGER NOT NULL,
    win_rate_pct NUMERIC(6, 2) NOT NULL,
    average_return_pct NUMERIC(8, 2) NOT NULL,
    profit_factor NUMERIC(10, 4),
    portfolio_return_pct NUMERIC(8, 2) NOT NULL,
    portfolio_max_drawdown_pct NUMERIC(8, 2) NOT NULL,
    portfolio_win_rate_pct NUMERIC(6, 2) NOT NULL,
    portfolio_profit_factor NUMERIC(10, 4),
    starting_equity NUMERIC(18, 4) NOT NULL,
    ending_equity NUMERIC(18, 4) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ux_backtest_yearly_run_year UNIQUE (run_id, calendar_year)
);

CREATE INDEX IF NOT EXISTS idx_backtest_yearly_run
    ON backtest_yearly_performance (run_id);

CREATE TABLE IF NOT EXISTS backtest_stock_performance (
    stock_metric_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    stock_code VARCHAR(20) NOT NULL,
    sector_name VARCHAR(255),
    trade_count INTEGER NOT NULL,
    win_rate_pct NUMERIC(6, 2) NOT NULL,
    average_return_pct NUMERIC(8, 2) NOT NULL,
    total_realized_pnl NUMERIC(18, 4) NOT NULL,
    best_trade_pct NUMERIC(8, 4) NOT NULL,
    worst_trade_pct NUMERIC(8, 4) NOT NULL,
    profit_factor NUMERIC(10, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ux_backtest_stock_performance_run_stock UNIQUE (run_id, stock_code)
);

CREATE INDEX IF NOT EXISTS idx_backtest_stock_performance_run
    ON backtest_stock_performance (run_id);

CREATE TABLE IF NOT EXISTS backtest_sector_performance (
    sector_metric_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    sector_name VARCHAR(255) NOT NULL,
    trade_count INTEGER NOT NULL,
    win_rate_pct NUMERIC(6, 2) NOT NULL,
    average_return_pct NUMERIC(8, 2) NOT NULL,
    total_realized_pnl NUMERIC(18, 4) NOT NULL,
    profit_factor NUMERIC(10, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ux_backtest_sector_performance_run_sector UNIQUE (run_id, sector_name)
);

CREATE INDEX IF NOT EXISTS idx_backtest_sector_performance_run
    ON backtest_sector_performance (run_id);

CREATE OR REPLACE VIEW vw_latest_model_verdict AS
WITH latest_run AS (
    SELECT br.*
    FROM backtest_runs br
    WHERE br.run_type = 'full_validation'
    ORDER BY br.run_date DESC, br.created_at DESC, br.run_id DESC
    LIMIT 1
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
FROM latest_run lr
LEFT JOIN decision_signals ds
    ON ds.run_date = lr.run_date
    AND ds.profile = lr.profile
    AND ds.run_type = lr.run_type;

CREATE OR REPLACE VIEW vw_model_yearly_performance AS
SELECT
    yp.*,
    br.run_date,
    br.profile,
    br.run_type
FROM backtest_yearly_performance yp
JOIN backtest_runs br ON br.run_id = yp.run_id;

CREATE OR REPLACE VIEW vw_portfolio_equity_curve AS
SELECT
    ep.*,
    br.run_date,
    br.profile,
    br.run_type
FROM backtest_portfolio_equity_curve ep
JOIN backtest_runs br ON br.run_id = ep.run_id;

CREATE OR REPLACE VIEW vw_portfolio_drawdown_curve AS
SELECT
    ep.run_id,
    ep.point_index,
    ep.event_date,
    ep.drawdown_pct,
    br.run_date,
    br.profile,
    br.run_type
FROM backtest_portfolio_equity_curve ep
JOIN backtest_runs br ON br.run_id = ep.run_id;

CREATE OR REPLACE VIEW vw_stock_model_performance AS
SELECT
    sp.*,
    br.run_date,
    br.profile,
    br.run_type
FROM backtest_stock_performance sp
JOIN backtest_runs br ON br.run_id = sp.run_id;

CREATE OR REPLACE VIEW vw_sector_model_performance AS
SELECT
    sep.*,
    br.run_date,
    br.profile,
    br.run_type
FROM backtest_sector_performance sep
JOIN backtest_runs br ON br.run_id = sep.run_id;

CREATE OR REPLACE VIEW vw_trade_distribution AS
SELECT
    pp.run_id,
    br.run_date,
    br.profile,
    br.run_type,
    pp.stock_code,
    pp.sector_name,
    pp.entry_date,
    pp.exit_date,
    pp.holding_days,
    pp.signal_type,
    pp.net_return_pct,
    pp.realized_pnl,
    pp.was_winner,
    CASE
        WHEN pp.net_return_pct >= 10 THEN '>= 10%'
        WHEN pp.net_return_pct >= 5 THEN '5% to 10%'
        WHEN pp.net_return_pct >= 0 THEN '0% to 5%'
        WHEN pp.net_return_pct >= -5 THEN '-5% to 0%'
        WHEN pp.net_return_pct >= -10 THEN '-10% to -5%'
        ELSE '< -10%'
    END AS return_bucket
FROM backtest_portfolio_positions pp
JOIN backtest_runs br ON br.run_id = pp.run_id;

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
    SELECT COUNT(*) AS approved_recommendations
    FROM vw_daily_recommendation_board
) dr ON TRUE;

COMMIT;
