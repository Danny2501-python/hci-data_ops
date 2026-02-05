-- Facebook Ads Tables DDL
-- Migrated from etl_marketing Django models
-- Database: dwh_dev (or custom_db_dev)

-- ============================================================================
-- Table: core_facebook_ad_account
-- Stores Facebook Ad Account configurations
-- ============================================================================

CREATE TABLE IF NOT EXISTS core_facebook_ad_account (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    ad_account_id VARCHAR(255) UNIQUE NOT NULL,
    app_id VARCHAR(255) NOT NULL,
    app_secret VARCHAR(255) NOT NULL,
    access_token TEXT NOT NULL,
    access_token_expires TIMESTAMP,
    fb_user_id VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    script_type_to_run VARCHAR(50) DEFAULT 'account' CHECK (script_type_to_run IN ('account', 'campaign')),
    current_brand_id INTEGER,
    created TIMESTAMP DEFAULT NOW(),
    updated TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_fb_ad_account_is_active ON core_facebook_ad_account(is_active);
CREATE INDEX idx_fb_ad_account_brand ON core_facebook_ad_account(current_brand_id);

COMMENT ON TABLE core_facebook_ad_account IS 'Facebook Ad Account configurations';
COMMENT ON COLUMN core_facebook_ad_account.script_type_to_run IS 'account: fetch all ads in account, campaign: fetch by campaign';

-- ============================================================================
-- Table: core_facebook_ad_campaign
-- Stores Facebook Ad Campaigns
-- ============================================================================

CREATE TABLE IF NOT EXISTS core_facebook_ad_campaign (
    id SERIAL PRIMARY KEY,
    fb_ad_campaign_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    fb_ad_account_id VARCHAR(255),
    status VARCHAR(50),
    created_time TIMESTAMP,
    updated_time TIMESTAMP,
    start_time TIMESTAMP,
    stop_time TIMESTAMP,
    brand_id INTEGER,
    etl_created TIMESTAMP DEFAULT NOW(),
    etl_updated TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_fb_campaign_id ON core_facebook_ad_campaign(fb_ad_campaign_id);
CREATE INDEX idx_fb_campaign_account ON core_facebook_ad_campaign(fb_ad_account_id);
CREATE INDEX idx_fb_campaign_brand ON core_facebook_ad_campaign(brand_id);

COMMENT ON TABLE core_facebook_ad_campaign IS 'Facebook Ad Campaigns';

-- ============================================================================
-- Table: core_facebook_ad_set
-- Stores Facebook Ad Sets
-- ============================================================================

CREATE TABLE IF NOT EXISTS core_facebook_ad_set (
    id SERIAL PRIMARY KEY,
    fb_ad_set_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    fb_ad_campaign_id VARCHAR(255),
    status VARCHAR(50),
    optimization_goal VARCHAR(100),
    created_time TIMESTAMP,
    updated_time TIMESTAMP,
    start_time TIMESTAMP,
    brand_id INTEGER,
    etl_created TIMESTAMP DEFAULT NOW(),
    etl_updated TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_fb_adset_id ON core_facebook_ad_set(fb_ad_set_id);
CREATE INDEX idx_fb_adset_campaign ON core_facebook_ad_set(fb_ad_campaign_id);
CREATE INDEX idx_fb_adset_brand ON core_facebook_ad_set(brand_id);

COMMENT ON TABLE core_facebook_ad_set IS 'Facebook Ad Sets';

-- ============================================================================
-- Table: core_facebook_ad_creative
-- Stores Facebook Ad Creatives
-- ============================================================================

CREATE TABLE IF NOT EXISTS core_facebook_ad_creative (
    id SERIAL PRIMARY KEY,
    fb_ad_creative_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    fb_ad_account_id VARCHAR(255),
    object_id VARCHAR(255),
    object_story_id VARCHAR(255),
    link_url TEXT,
    effective_object_story_id VARCHAR(255),
    status VARCHAR(50),
    actor_id VARCHAR(255),
    brand_id INTEGER,
    etl_created TIMESTAMP DEFAULT NOW(),
    etl_updated TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_fb_creative_id ON core_facebook_ad_creative(fb_ad_creative_id);
CREATE INDEX idx_fb_creative_account ON core_facebook_ad_creative(fb_ad_account_id);
CREATE INDEX idx_fb_creative_brand ON core_facebook_ad_creative(brand_id);

COMMENT ON TABLE core_facebook_ad_creative IS 'Facebook Ad Creatives';

-- ============================================================================
-- Table: core_facebook_ad_ad
-- Stores Facebook Ads
-- ============================================================================

CREATE TABLE IF NOT EXISTS core_facebook_ad_ad (
    id SERIAL PRIMARY KEY,
    fb_ad_ad_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    fb_ad_campaign_id VARCHAR(255),
    fb_ad_set_id VARCHAR(255),
    fb_ad_creative_id VARCHAR(255),
    status VARCHAR(50),
    targeting JSONB,
    brand_id INTEGER,
    etl_created TIMESTAMP DEFAULT NOW(),
    etl_updated TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_fb_ad_id ON core_facebook_ad_ad(fb_ad_ad_id);
CREATE INDEX idx_fb_ad_campaign ON core_facebook_ad_ad(fb_ad_campaign_id);
CREATE INDEX idx_fb_ad_adset ON core_facebook_ad_ad(fb_ad_set_id);
CREATE INDEX idx_fb_ad_creative ON core_facebook_ad_ad(fb_ad_creative_id);
CREATE INDEX idx_fb_ad_brand ON core_facebook_ad_ad(brand_id);

COMMENT ON TABLE core_facebook_ad_ad IS 'Facebook Ads';

-- ============================================================================
-- Table: core_facebook_ad_ad_insight
-- Stores Facebook Ad Insights (MAIN TABLE)
-- ============================================================================

CREATE TABLE IF NOT EXISTS core_facebook_ad_ad_insight (
    id SERIAL PRIMARY KEY,
    fb_ad_ad_id INTEGER NOT NULL REFERENCES core_facebook_ad_ad(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    spend DOUBLE PRECISION NOT NULL DEFAULT 0,
    fb_account_currency VARCHAR(10) DEFAULT 'VND',
    region VARCHAR(255),
    brand_id INTEGER,
    etl_created TIMESTAMP DEFAULT NOW(),
    etl_updated TIMESTAMP DEFAULT NOW(),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT unique_insight_per_ad_date_region UNIQUE (fb_ad_ad_id, date, region)
);

CREATE INDEX idx_fb_insight_ad ON core_facebook_ad_ad_insight(fb_ad_ad_id);
CREATE INDEX idx_fb_insight_date ON core_facebook_ad_ad_insight(date);
CREATE INDEX idx_fb_insight_brand ON core_facebook_ad_ad_insight(brand_id);
CREATE INDEX idx_fb_insight_date_brand ON core_facebook_ad_ad_insight(date, brand_id);

COMMENT ON TABLE core_facebook_ad_ad_insight IS 'Facebook Ad Insights - Main data table';
COMMENT ON COLUMN core_facebook_ad_ad_insight.spend IS 'Ad spend in account currency';
COMMENT ON COLUMN core_facebook_ad_ad_insight.region IS 'Geographic region breakdown';

-- ============================================================================
-- Table: core_facebook_ad_ad_insight_message
-- Stores Facebook Ad Insights with message actions
-- ============================================================================

CREATE TABLE IF NOT EXISTS core_facebook_ad_ad_insight_message (
    id SERIAL PRIMARY KEY,
    fb_ad_ad_id INTEGER NOT NULL REFERENCES core_facebook_ad_ad(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    spend DOUBLE PRECISION NOT NULL DEFAULT 0,
    fb_account_currency VARCHAR(10) DEFAULT 'VND',
    region VARCHAR(255),
    unique_actions JSONB,
    brand_id INTEGER,
    etl_created TIMESTAMP DEFAULT NOW(),
    etl_updated TIMESTAMP DEFAULT NOW(),
    
    -- Unique constraint to prevent duplicates
    CONSTRAINT unique_insight_message_per_ad_date_region UNIQUE (fb_ad_ad_id, date, region)
);

CREATE INDEX idx_fb_insight_msg_ad ON core_facebook_ad_ad_insight_message(fb_ad_ad_id);
CREATE INDEX idx_fb_insight_msg_date ON core_facebook_ad_ad_insight_message(date);
CREATE INDEX idx_fb_insight_msg_brand ON core_facebook_ad_ad_insight_message(brand_id);

COMMENT ON TABLE core_facebook_ad_ad_insight_message IS 'Facebook Ad Insights with message actions';

-- ============================================================================
-- Sample Data Insert (for testing)
-- ============================================================================

-- Insert a sample Facebook Ad Account
-- IMPORTANT: Replace with your actual credentials
/*
INSERT INTO core_facebook_ad_account 
(name, ad_account_id, app_id, app_secret, access_token, access_token_expires, script_type_to_run, is_active, current_brand_id)
VALUES 
('Sample Ad Account', '123456789', 'your_app_id', 'your_app_secret', 'your_long_lived_token', '2026-12-31 23:59:59', 'account', TRUE, 1);
*/

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Check table structure
/*
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_name IN (
    'core_facebook_ad_account',
    'core_facebook_ad_campaign',
    'core_facebook_ad_set',
    'core_facebook_ad_creative',
    'core_facebook_ad_ad',
    'core_facebook_ad_ad_insight',
    'core_facebook_ad_ad_insight_message'
)
ORDER BY table_name, ordinal_position;
*/

-- Check indexes
/*
SELECT 
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename LIKE 'core_facebook%'
ORDER BY tablename, indexname;
*/

-- ============================================================================
-- Migration Notes
-- ============================================================================

/*
MIGRATION CHECKLIST:

1. ✅ Create tables in target database (dwh_dev or custom_db_dev)
2. ✅ Verify indexes are created
3. ✅ Insert Facebook Ad Account configurations
4. ✅ Update Airflow connection to point to correct database
5. ✅ Test run DAG with manual trigger
6. ✅ Verify data is being inserted correctly
7. ✅ Compare data with old Django system
8. ✅ Monitor for 3-5 days
9. ✅ Kill Django app when confirmed OK

IMPORTANT:
- If using existing custom_db_dev, tables already exist (created by Django)
- If using new dwh_dev, run this DDL first
- Ensure brand_id references exist if using foreign keys
- Update access tokens regularly (expire after 60 days)
*/
