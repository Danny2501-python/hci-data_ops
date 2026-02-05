-- Facebook Ads Insights - SIMPLIFIED TABLE
-- Strategy: No foreign keys, just store IDs as strings
-- Much simpler than original Django models

-- ============================================================================
-- Table: core_facebook_ad_ad_insight_simple
-- Stores Facebook Ad Insights (SIMPLIFIED - NO FOREIGN KEYS)
-- ============================================================================

CREATE TABLE IF NOT EXISTS core_facebook_ad_ad_insight_simple (
    id SERIAL PRIMARY KEY,
    
    -- IDs as strings (no foreign keys!)
    ad_id VARCHAR(255) NOT NULL,
    campaign_id VARCHAR(255),
    adset_id VARCHAR(255),
    account_id VARCHAR(255),
    
    -- Metrics
    date DATE NOT NULL,
    spend DOUBLE PRECISION NOT NULL DEFAULT 0,
    currency VARCHAR(10) DEFAULT 'VND',
    region VARCHAR(255),
    
    -- Metadata
    brand_id INTEGER,
    etl_created TIMESTAMP DEFAULT NOW(),
    etl_updated TIMESTAMP DEFAULT NOW(),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT unique_insight_simple UNIQUE (ad_id, date, region)
);

-- Indexes for performance
CREATE INDEX idx_fb_insight_simple_ad ON core_facebook_ad_ad_insight_simple(ad_id);
CREATE INDEX idx_fb_insight_simple_date ON core_facebook_ad_ad_insight_simple(date);
CREATE INDEX idx_fb_insight_simple_campaign ON core_facebook_ad_ad_insight_simple(campaign_id);
CREATE INDEX idx_fb_insight_simple_brand ON core_facebook_ad_ad_insight_simple(brand_id);
CREATE INDEX idx_fb_insight_simple_date_brand ON core_facebook_ad_ad_insight_simple(date, brand_id);

COMMENT ON TABLE core_facebook_ad_ad_insight_simple IS 'Facebook Ad Insights - Simplified version without foreign keys';
COMMENT ON COLUMN core_facebook_ad_ad_insight_simple.ad_id IS 'Facebook Ad ID (string, not foreign key)';
COMMENT ON COLUMN core_facebook_ad_ad_insight_simple.spend IS 'Ad spend in account currency';
COMMENT ON COLUMN core_facebook_ad_ad_insight_simple.region IS 'Geographic region breakdown';

-- ============================================================================
-- Comparison: Original vs Simplified
-- ============================================================================

/*
ORIGINAL (Django with foreign keys):
- core_facebook_ad_account (required)
- core_facebook_ad_campaign (auto-created)
- core_facebook_ad_set (auto-created)
- core_facebook_ad_creative (auto-created)
- core_facebook_ad_ad (auto-created, has FKs to campaign, adset, creative)
- core_facebook_ad_ad_insight (has FK to ad_ad)

SIMPLIFIED (No foreign keys):
- core_facebook_ad_account (required)
- core_facebook_ad_ad_insight_simple (standalone, just IDs as strings)

Benefits:
✅ No dependency creation needed
✅ No foreign key constraints
✅ Faster inserts (no FK checks)
✅ Simpler code (400 lines vs 900 lines)
✅ Same data, just denormalized
*/

-- ============================================================================
-- Migration from Original to Simplified
-- ============================================================================

/*
If you want to migrate data from original tables to simplified:

INSERT INTO core_facebook_ad_ad_insight_simple 
(ad_id, campaign_id, adset_id, account_id, date, spend, currency, region, brand_id, etl_created, etl_updated)
SELECT 
    ad.fb_ad_ad_id as ad_id,
    ad.fb_ad_campaign_id as campaign_id,
    ad.fb_ad_set_id as adset_id,
    campaign.fb_ad_account_id as account_id,
    insight.date,
    insight.spend,
    insight.fb_account_currency as currency,
    insight.region,
    insight.brand_id,
    insight.etl_created,
    insight.etl_updated
FROM core_facebook_ad_ad_insight insight
JOIN core_facebook_ad_ad ad ON insight.fb_ad_ad_id = ad.id
LEFT JOIN core_facebook_ad_campaign campaign ON ad.fb_ad_campaign_id = campaign.fb_ad_campaign_id
ON CONFLICT (ad_id, date, region) DO NOTHING;
*/

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Check data
/*
SELECT 
    date,
    COUNT(*) as total_insights,
    COUNT(DISTINCT ad_id) as unique_ads,
    SUM(spend) as total_spend,
    currency
FROM core_facebook_ad_ad_insight_simple
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date, currency
ORDER BY date DESC;
*/

-- Compare with original
/*
SELECT 
    'original' as source,
    COUNT(*) as count,
    SUM(spend) as total_spend
FROM core_facebook_ad_ad_insight
WHERE date = CURRENT_DATE - INTERVAL '1 day'

UNION ALL

SELECT 
    'simplified' as source,
    COUNT(*) as count,
    SUM(spend) as total_spend
FROM core_facebook_ad_ad_insight_simple
WHERE date = CURRENT_DATE - INTERVAL '1 day';
*/
