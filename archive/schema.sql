-- ============================================================================
-- Stock Pipeline - Database Schema Definition
-- ============================================================================
-- Purpose: Production-ready schema for Nigerian stock market investment system
-- Version: 1.0
-- Date: 2025-12-06
-- ============================================================================

-- Drop existing tables (for clean setup)
DROP TABLE IF EXISTS alert_history CASCADE;
DROP TABLE IF EXISTS alert_rules CASCADE;
DROP TABLE IF EXISTS fact_technical_indicators CASCADE;
DROP TABLE IF EXISTS fact_daily_prices CASCADE;
DROP TABLE IF EXISTS dim_stocks CASCADE;
DROP TABLE IF EXISTS dim_sectors CASCADE;

-- Drop views
DROP VIEW IF EXISTS vw_investment_dashboard;
DROP VIEW IF EXISTS vw_latest_stock_prices;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Sector Master Data
CREATE TABLE dim_sectors (
    sector_id SERIAL PRIMARY KEY,
    sector_name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sector_name ON dim_sectors(sector_name);

COMMENT ON TABLE dim_sectors IS 'Master list of stock market sectors';
COMMENT ON COLUMN dim_sectors.sector_id IS 'Primary key, auto-incremented';
COMMENT ON COLUMN dim_sectors.sector_name IS 'Sector name (unique)';

-- Insert reference sector data
INSERT INTO dim_sectors (sector_name, description) VALUES
    ('Financials', 'Banks, Insurance, Mortgage, Asset Management'),
    ('Consumer Goods', 'Food, Beverages, Manufacturing'),
    ('Consumer Services', 'Transport, Hospitality, Media'),
    ('Technology', 'IT Services, Software, Telecommunications'),
    ('Basic Materials', 'Chemicals, Construction Materials, Mining'),
    ('Industrials', 'Manufacturing, Engineering, Construction'),
    ('Oil & Gas', 'Exploration, Production, Distribution'),
    ('Healthcare', 'Pharmaceuticals, Hospitals, Medical Equipment'),
    ('Utilities', 'Power Generation, Water, Infrastructure');

-- Stock Master Data
CREATE TABLE dim_stocks (
    stock_id SERIAL PRIMARY KEY,
    stock_code VARCHAR(20) UNIQUE NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    sector_id INTEGER REFERENCES dim_sectors(sector_id) ON DELETE SET NULL,
    exchange VARCHAR(10) NOT NULL,
    listing_date DATE,
    delisting_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_exchange CHECK (exchange IN ('NGX', 'LSE')),
    CONSTRAINT chk_stock_code_format CHECK (stock_code ~ '^[A-Z0-9_]+$')
);

CREATE INDEX idx_stock_code ON dim_stocks(stock_code);
CREATE INDEX idx_exchange ON dim_stocks(exchange);
CREATE INDEX idx_sector ON dim_stocks(sector_id);
CREATE INDEX idx_active_stocks ON dim_stocks(is_active) WHERE is_active = TRUE;

COMMENT ON TABLE dim_stocks IS 'Master list of all stocks in the system';
COMMENT ON COLUMN dim_stocks.stock_code IS 'Ticker symbol (e.g., DANGCEM, GTCO)';
COMMENT ON COLUMN dim_stocks.exchange IS 'Exchange: NGX or LSE';
COMMENT ON COLUMN dim_stocks.is_active IS 'FALSE if delisted';
COMMENT ON COLUMN dim_stocks.metadata IS 'Additional info: website, CEO, market cap category';

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Daily Price Data (Time Series)
CREATE TABLE fact_daily_prices (
    price_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    
    -- OHLCV Data
    open_price NUMERIC(18, 4),
    high_price NUMERIC(18, 4),
    low_price NUMERIC(18, 4),
    close_price NUMERIC(18, 4) NOT NULL,
    volume BIGINT,
    
    -- Calculated Fields
    change_1d_pct NUMERIC(10, 4),
    change_ytd_pct NUMERIC(10, 4),
    market_cap VARCHAR(50),
    
    -- Metadata
    source VARCHAR(50) NOT NULL,
    data_quality_flag VARCHAR(20) DEFAULT 'GOOD',
    has_complete_data BOOLEAN DEFAULT TRUE,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_stock_price_date UNIQUE (stock_id, price_date),
    CONSTRAINT chk_price_positive CHECK (close_price > 0),
    CONSTRAINT chk_ohlc_logic CHECK (
        high_price IS NULL OR low_price IS NULL OR 
        (high_price >= low_price AND 
         high_price >= close_price AND 
         low_price <= close_price)
    ),
    CONSTRAINT chk_data_quality CHECK (
        data_quality_flag IN ('GOOD', 'SUSPICIOUS', 'MISSING', 'STALE')
    )
);

CREATE INDEX idx_price_date ON fact_daily_prices(price_date DESC);
CREATE INDEX idx_stock_date ON fact_daily_prices(stock_id, price_date DESC);
CREATE INDEX idx_recent_prices ON fact_daily_prices(price_date DESC) 
    WHERE price_date >= CURRENT_DATE - INTERVAL '90 days';
CREATE INDEX idx_quality ON fact_daily_prices(data_quality_flag) 
    WHERE data_quality_flag != 'GOOD';

COMMENT ON TABLE fact_daily_prices IS 'Daily stock price history (OHLCV)';
COMMENT ON COLUMN fact_daily_prices.data_quality_flag IS 'GOOD, SUSPICIOUS, MISSING, or STALE';
COMMENT ON COLUMN fact_daily_prices.source IS 'Data source: african-markets.com or yahoo_finance';

-- Technical Indicators (Computed Metrics)
CREATE TABLE fact_technical_indicators (
    indicator_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    calculation_date DATE NOT NULL,
    
    -- Moving Averages
    ma_7 NUMERIC(18, 4),
    ma_30 NUMERIC(18, 4),
    ma_90 NUMERIC(18, 4),
    
    -- Momentum Indicators
    rsi_14 NUMERIC(5, 2),
    macd NUMERIC(18, 4),
    macd_signal NUMERIC(18, 4),
    macd_histogram NUMERIC(18, 4),
    
    -- Volatility Indicators
    volatility_30 NUMERIC(10, 4),
    atr_14 NUMERIC(18, 4),
    
    -- Bollinger Bands
    bollinger_upper NUMERIC(18, 4),
    bollinger_middle NUMERIC(18, 4),
    bollinger_lower NUMERIC(18, 4),
    
    -- Trading Signals
    ma_crossover_signal VARCHAR(10),
    trend_strength NUMERIC(5, 2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_stock_indicator_date UNIQUE (stock_id, calculation_date),
    CONSTRAINT chk_rsi_range CHECK (rsi_14 IS NULL OR rsi_14 BETWEEN 0 AND 100),
    CONSTRAINT chk_trend_range CHECK (trend_strength IS NULL OR trend_strength BETWEEN 0 AND 100),
    CONSTRAINT chk_ma_crossover CHECK (
        ma_crossover_signal IS NULL OR 
        ma_crossover_signal IN ('BULLISH', 'BEARISH', 'NEUTRAL')
    )
);

CREATE INDEX idx_indicator_date ON fact_technical_indicators(calculation_date DESC);
CREATE INDEX idx_stock_calc_date ON fact_technical_indicators(stock_id, calculation_date DESC);
CREATE INDEX idx_ma_crossover ON fact_technical_indicators(ma_crossover_signal) 
    WHERE ma_crossover_signal != 'NEUTRAL';

COMMENT ON TABLE fact_technical_indicators IS 'Computed technical analysis indicators';
COMMENT ON COLUMN fact_technical_indicators.rsi_14 IS 'Relative Strength Index (0-100)';
COMMENT ON COLUMN fact_technical_indicators.ma_crossover_signal IS 'BULLISH, BEARISH, or NEUTRAL';

-- ============================================================================
-- ALERT CONFIGURATION & HISTORY
-- ============================================================================

-- Alert Rules (Configuration)
CREATE TABLE alert_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) UNIQUE NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    condition_sql TEXT,
    threshold_value NUMERIC(10, 4),
    severity VARCHAR(20) DEFAULT 'INFO',
    is_active BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_severity CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    CONSTRAINT chk_rule_type CHECK (
        rule_type IN ('PRICE_MOVEMENT', 'MA_CROSSOVER', 'VOLATILITY', 
                      'VOLUME_SPIKE', 'RSI', 'MACD', 'CUSTOM')
    )
);

COMMENT ON TABLE alert_rules IS 'Configurable investment alert rules';
COMMENT ON COLUMN alert_rules.condition_sql IS 'SQL expression for custom rule evaluation';

-- Insert pre-defined investment alert rules
INSERT INTO alert_rules (rule_name, rule_type, threshold_value, severity, description) VALUES
    ('Daily_Change_Significant', 'PRICE_MOVEMENT', 4.0, 'WARNING', 
     'Daily price change exceeds ±4% - requires attention'),
    ('Daily_Change_Extreme', 'PRICE_MOVEMENT', 8.0, 'CRITICAL', 
     'Daily price change exceeds ±8% - immediate review needed'),
    ('MA_Bullish_Crossover', 'MA_CROSSOVER', 0, 'INFO', 
     '7-day MA crosses above 30-day MA - potential buy signal'),
    ('MA_Bearish_Crossover', 'MA_CROSSOVER', 0, 'WARNING', 
     '7-day MA crosses below 30-day MA - potential sell signal'),
    ('Volatility_Spike', 'VOLATILITY', 2.0, 'WARNING', 
     'Volatility exceeds 2x 30-day average - market uncertainty'),
    ('Volume_Surge', 'VOLUME_SPIKE', 2.5, 'INFO', 
     'Volume exceeds 2.5x average - unusual activity'),
    ('RSI_Oversold', 'RSI', 30, 'INFO', 
     'RSI below 30 - potential undervalued (buy opportunity)'),
    ('RSI_Overbought', 'RSI', 70, 'WARNING', 
     'RSI above 70 - potential overvalued (sell consideration)'),
    ('MACD_Bullish_Cross', 'MACD', 0, 'INFO', 
     'MACD crosses above signal line - bullish momentum'),
    ('MACD_Bearish_Cross', 'MACD', 0, 'WARNING', 
     'MACD crosses below signal line - bearish momentum');

-- Alert History (Generated Alerts)
CREATE TABLE alert_history (
    alert_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES dim_stocks(stock_id) ON DELETE CASCADE,
    rule_id INTEGER NOT NULL REFERENCES alert_rules(rule_id) ON DELETE CASCADE,
    alert_date DATE NOT NULL,
    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    trigger_value NUMERIC(18, 4),
    message TEXT NOT NULL,
    
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolution_notes TEXT,
    
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_channels VARCHAR(100),
    
    CONSTRAINT chk_alert_severity CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL'))
);

CREATE INDEX idx_alert_date ON alert_history(alert_date DESC);
CREATE INDEX idx_stock_alerts ON alert_history(stock_id, alert_date DESC);
CREATE INDEX idx_unresolved ON alert_history(is_resolved, severity) 
    WHERE is_resolved = FALSE;
CREATE INDEX idx_recent_alerts ON alert_history(alert_timestamp DESC) 
    WHERE alert_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days';

COMMENT ON TABLE alert_history IS 'Historical record of all triggered investment alerts';
COMMENT ON COLUMN alert_history.trigger_value IS 'Actual value that triggered the alert';
COMMENT ON COLUMN alert_history.notification_channels IS 'Comma-separated: email,slack,sms';

-- ============================================================================
-- ANALYTICAL VIEWS
-- ============================================================================

-- Latest Stock Prices (Current Market View)
CREATE OR REPLACE VIEW vw_latest_stock_prices AS
SELECT 
    s.stock_id,
    s.stock_code,
    s.company_name,
    sec.sector_name,
    s.exchange,
    p.price_date,
    p.close_price,
    p.change_1d_pct,
    p.change_ytd_pct,
    p.volume,
    p.market_cap,
    p.data_quality_flag
FROM dim_stocks s
JOIN dim_sectors sec ON s.sector_id = sec.sector_id
JOIN LATERAL (
    SELECT *
    FROM fact_daily_prices
    WHERE stock_id = s.stock_id
    ORDER BY price_date DESC
    LIMIT 1
) p ON TRUE
WHERE s.is_active = TRUE
ORDER BY s.stock_code;

COMMENT ON VIEW vw_latest_stock_prices IS 'Most recent price for each active stock';

-- Investment Dashboard (Comprehensive View)
CREATE OR REPLACE VIEW vw_investment_dashboard AS
SELECT 
    s.stock_code,
    s.company_name,
    sec.sector_name,
    p.price_date,
    p.close_price,
    p.change_1d_pct,
    p.change_ytd_pct,
    p.volume,
    i.ma_7,
    i.ma_30,
    i.rsi_14,
    i.volatility_30,
    i.ma_crossover_signal,
    i.trend_strength,
    (SELECT COUNT(*) 
     FROM alert_history ah 
     WHERE ah.stock_id = s.stock_id 
       AND ah.alert_date = p.price_date
       AND ah.is_resolved = FALSE
    ) AS active_alerts_count,
    (SELECT STRING_AGG(ah.severity || ': ' || ah.message, '; ')
     FROM alert_history ah
     WHERE ah.stock_id = s.stock_id
       AND ah.alert_date = p.price_date
       AND ah.is_resolved = FALSE
    ) AS alert_messages
FROM dim_stocks s
JOIN dim_sectors sec ON s.sector_id = sec.sector_id
JOIN LATERAL (
    SELECT * FROM fact_daily_prices
    WHERE stock_id = s.stock_id
    ORDER BY price_date DESC LIMIT 1
) p ON TRUE
LEFT JOIN LATERAL (
    SELECT * FROM fact_technical_indicators
    WHERE stock_id = s.stock_id
    ORDER BY calculation_date DESC LIMIT 1
) i ON TRUE
WHERE s.is_active = TRUE
ORDER BY s.stock_code;

COMMENT ON VIEW vw_investment_dashboard IS 'Complete investment view with prices, indicators, and alerts';

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function: Get stock price history for a date range
CREATE OR REPLACE FUNCTION get_price_history(
    p_stock_code VARCHAR,
    p_start_date DATE,
    p_end_date DATE
)
RETURNS TABLE (
    price_date DATE,
    close_price NUMERIC,
    change_1d_pct NUMERIC,
    volume BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        fdp.price_date,
        fdp.close_price,
        fdp.change_1d_pct,
        fdp.volume
    FROM fact_daily_prices fdp
    JOIN dim_stocks ds ON fdp.stock_id = ds.stock_id
    WHERE ds.stock_code = p_stock_code
      AND fdp.price_date BETWEEN p_start_date AND p_end_date
    ORDER BY fdp.price_date;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_price_history IS 'Retrieve price history for a stock within date range';

-- Function: Get active alerts for a stock
CREATE OR REPLACE FUNCTION get_active_alerts(p_stock_code VARCHAR)
RETURNS TABLE (
    alert_date DATE,
    severity VARCHAR,
    message TEXT,
    alert_timestamp TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ah.alert_date,
        ah.severity,
        ah.message,
        ah.alert_timestamp
    FROM alert_history ah
    JOIN dim_stocks ds ON ah.stock_id = ds.stock_id
    WHERE ds.stock_code = p_stock_code
      AND ah.is_resolved = FALSE
    ORDER BY ah.alert_timestamp DESC;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_active_alerts IS 'Get all unresolved alerts for a specific stock';

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_dim_stocks_modtime
    BEFORE UPDATE ON dim_stocks
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_dim_sectors_modtime
    BEFORE UPDATE ON dim_sectors
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_alert_rules_modtime
    BEFORE UPDATE ON alert_rules
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

-- ============================================================================
-- GRANTS (Security)
-- ============================================================================

-- Create application user (if not exists)
-- CREATE USER stock_app WITH PASSWORD 'secure_password_here';

-- Grant appropriate permissions
-- GRANT CONNECT ON DATABASE your_database TO stock_app;
-- GRANT USAGE ON SCHEMA public TO stock_app;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO stock_app;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO stock_app;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Verify table creation
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
  AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Verify indexes
SELECT tablename, indexname 
FROM pg_indexes 
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

-- Verify foreign keys
SELECT
    tc.table_name, 
    kcu.column_name, 
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM information_schema.table_constraints AS tc 
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name;

-- ============================================================================
-- END OF SCHEMA DEFINITION
-- ============================================================================
