-- Facebook Ads Insights Table - Version 1
-- Simplified schema for direct API → PostgreSQL ingestion
-- No foreign keys, just store fb_ad_ad_id as string

CREATE TABLE IF NOT EXISTS core_facebook_ad_ad_insight_v1 (
    id BIGSERIAL PRIMARY KEY,
    
    -- Core fields
    fb_ad_ad_id VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    spend DOUBLE PRECISION NOT NULL DEFAULT 0,
    fb_account_currency VARCHAR(255) DEFAULT 'VND',
    region VARCHAR(255),
    
    -- Metadata
    brand_id BIGINT,
    etl_created TIMESTAMPTZ DEFAULT NOW(),
    etl_updated TIMESTAMPTZ DEFAULT NOW(),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT unique_fb_insight_v1 UNIQUE (fb_ad_ad_id, date, region)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fb_insight_v1_ad_id ON core_facebook_ad_ad_insight_v1(fb_ad_ad_id);
CREATE INDEX IF NOT EXISTS idx_fb_insight_v1_date ON core_facebook_ad_ad_insight_v1(date);
CREATE INDEX IF NOT EXISTS idx_fb_insight_v1_brand ON core_facebook_ad_ad_insight_v1(brand_id);
CREATE INDEX IF NOT EXISTS idx_fb_insight_v1_date_brand ON core_facebook_ad_ad_insight_v1(date, brand_id);

-- Comments
COMMENT ON TABLE core_facebook_ad_ad_insight_v1 IS 'Facebook Ad Insights - Version 1 (Simplified, no foreign keys)';
COMMENT ON COLUMN core_facebook_ad_ad_insight_v1.fb_ad_ad_id IS 'Facebook Ad ID (string, not foreign key)';
COMMENT ON COLUMN core_facebook_ad_ad_insight_v1.spend IS 'Ad spend in account currency';
COMMENT ON COLUMN core_facebook_ad_ad_insight_v1.region IS 'Geographic region breakdown';

-- Verification query
/*
SELECT 
    date,
    COUNT(*) as total_insights,
    COUNT(DISTINCT fb_ad_ad_id) as unique_ads,
    SUM(spend) as total_spend,
    fb_account_currency
FROM core_facebook_ad_ad_insight_v1
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date, fb_account_currency
ORDER BY date DESC;
*/
