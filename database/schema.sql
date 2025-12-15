-- Create database
CREATE DATABASE stock_dashboard;

-- Create main metrics table (Simplified MVP approach)
CREATE TABLE stock_metrics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    timestamp TIMESTAMPTZ,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    volume BIGINT,
    price_change DECIMAL(10,2),
    percent_change DECIMAL(5,2),
    sma_5 DECIMAL(10,2),
    sma_20 DECIMAL(10,2),
    volume_ratio DECIMAL(5,2),
    rsi DECIMAL(5,2),
    vwap DECIMAL(10,2),
    is_volume_surge BOOLEAN,
    is_price_spike BOOLEAN,
    is_rsi_oversold BOOLEAN,
    is_rsi_overbought BOOLEAN,
    processing_time TIMESTAMPTZ DEFAULT NOW()
);
--     INDEX idx_symbol (symbol),
--     INDEX idx_timestamp (timestamp),
--     INDEX idx_recent (timestamp DESC)
-- );

-- Create alerts table
CREATE TABLE alerts (
    alert_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    alert_type VARCHAR(50),
    timestamp TIMESTAMPTZ,
    message TEXT,
    close_price DECIMAL(10,2),
    volume BIGINT,
    rsi DECIMAL(5,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create materialized views for dashboard

-- 1. Current Market Overview
CREATE MATERIALIZED VIEW market_overview AS
SELECT 
    NOW() as last_updated,
    COUNT(DISTINCT symbol) as total_stocks,
    SUM(CASE WHEN percent_change > 0 THEN 1 ELSE 0 END) as gainers,
    SUM(CASE WHEN percent_change < 0 THEN 1 ELSE 0 END) as losers,
    AVG(percent_change) as avg_market_change,
    MAX(percent_change) as top_gainer,
    MIN(percent_change) as top_loser,
    SUM(volume) as total_volume
FROM stock_metrics
WHERE timestamp >= NOW() - INTERVAL '5 minutes';

-- 2. Active Alerts View
CREATE MATERIALIZED VIEW active_alerts_view AS
SELECT 
    symbol,
    timestamp,
    CASE 
        WHEN is_volume_surge THEN 'VOLUME_SURGE'
        WHEN is_price_spike THEN 'PRICE_SPIKE'
        WHEN is_rsi_oversold THEN 'RSI_OVERSOLD'
        WHEN is_rsi_overbought THEN 'RSI_OVERBOUGHT'
    END as alert_type,
    close_price,
    volume,
    rsi,
    processing_time
FROM stock_metrics
WHERE (is_volume_surge OR is_price_spike OR is_rsi_oversold OR is_rsi_overbought)
  AND timestamp >= NOW() - INTERVAL '15 minutes'
ORDER BY timestamp DESC;

-- 3. Top Movers
CREATE MATERIALIZED VIEW top_movers AS
SELECT 
    symbol,
    close_price,
    percent_change,
    volume,
    rsi,
    timestamp
FROM stock_metrics
WHERE timestamp >= NOW() - INTERVAL '5 minutes'
ORDER BY ABS(percent_change) DESC
LIMIT 20;

-- Create refresh function
CREATE OR REPLACE FUNCTION refresh_dashboard_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW market_overview;
    REFRESH MATERIALIZED VIEW active_alerts_view;
    REFRESH MATERIALIZED VIEW top_movers;
END;
$$ LANGUAGE plpgsql;

-- Create index on alerts for performance
CREATE INDEX idx_alerts_active ON alerts(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_alerts_timestamp ON alerts(timestamp DESC);