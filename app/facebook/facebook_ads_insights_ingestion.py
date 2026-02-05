"""
Facebook Ads Insights Ingestion
Migrated from etl_marketing Django app
Strategy: Call API → Write 1:1 to table (no dependencies)
"""

import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List
from loguru import logger

# Facebook Business SDK
from facebook_business.adobjects.adsinsights import AdsInsights

# Common utilities
from common.facebook_api import (
    FacebookRateLimitTracker,
    get_active_facebook_accounts,
    init_facebook_api
)
from common.database import PostgreSQLHelper

# ============================================================================
# CONFIGURATION
# ============================================================================

POSTGRES_URI = os.getenv('POSTGRES_URI')
FB_API_VERSION = os.getenv('FB_API_VERSION', 'v18.0')


# ============================================================================
# FACEBOOK ADS INSIGHTS INGESTION
# ============================================================================

class FacebookAdsInsightsIngestion:
    """
    Facebook Ads Insights Ingestion
    Strategy: Direct write to table (no dependencies)
    """
    
    def __init__(self, db: PostgreSQLHelper, days_for_ago: int = 1):
        self.db = db
        self.days_for_ago = days_for_ago
        self.rate_limit_tracker = FacebookRateLimitTracker()
    
    def upsert_insight(self, insight_data: Dict, brand_id: int):
        """
        Upsert insight to database
        
        Table: core_facebook_ad_ad_insight
        Primary Key: (fb_ad_ad_id, date, region)
        """
        query = """
            INSERT INTO core_facebook_ad_ad_insight (
                date, fb_account_currency, fb_ad_ad_id, spend, region,
                brand_id, etl_created, etl_updated
            ) VALUES (
                %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (fb_ad_ad_id, date, region) 
            DO UPDATE SET
                fb_account_currency = EXCLUDED.fb_account_currency,
                spend = EXCLUDED.spend,
                brand_id = EXCLUDED.brand_id,
                etl_updated = NOW()
        """
        
        region = insight_data.get('region', 'unknown')
        
        params = (
            insight_data.get('date_start'),
            insight_data.get('account_currency'),
            insight_data.get('ad_id'),
            insight_data.get('spend', 0),
            region,
            brand_id
        )
        
        self.db.execute(query, params)
    
    def fetch_insights(self, account: Dict):
        """Fetch insights from Facebook API"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        now = datetime.now()
        this_day = now - timedelta(days=self.days_for_ago)
        this_day_str = this_day.strftime("%Y-%m-%d")
        
        params = {
            'time_range': {"since": this_day_str, "until": this_day_str},
            'level': 'ad',
            'breakdowns': ['region']
        }
        
        fields = [
            AdsInsights.Field.account_id,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.spend,
            AdsInsights.Field.date_start,
            AdsInsights.Field.date_stop,
            AdsInsights.Field.account_currency,
            AdsInsights.Field.region
        ]
        
        try:
            insights = ad_account.get_insights(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get insights: {e}")
            return
        
        self.rate_limit_tracker.reset()
        count = 0
        brand_id = account.get('current_brand_id')
        
        for insight in insights:
            try:
                data = insight._json
                
                # Direct write to table
                self.upsert_insight(data, brand_id)
                count += 1
                
            except Exception as e:
                logger.error(f"Error processing insight: {e}, data: {insight._json}")
            
            # Check rate limit
            try:
                current_headers = insights.headers()
                if self.rate_limit_tracker.should_check_limit(current_headers):
                    logger.debug(f"Headers changed, checking rate limit (processed {count} insights so far)")
                    self.rate_limit_tracker.check_limit(current_headers)
            except Exception as e:
                logger.error(f"Error checking limit: {e}")
        
        logger.success(f"✅ Processed {count} insights for account: {account['name']}")
    
    def run(self):
        """Execute Facebook Ads insights ingestion"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads Insights Ingestion")
        logger.info(f"Days for ago: {self.days_for_ago}")
        logger.info("="*80)
        
        try:
            accounts = get_active_facebook_accounts(self.db)
            logger.info(f"Found {len(accounts)} active Facebook Ad accounts")
            
            for account in accounts:
                try:
                    time.sleep(1)
                    logger.info(f"Processing account: {account['name']} ({account['ad_account_id']})")
                    
                    self.fetch_insights(account)
                    
                    logger.success(f"✅ Completed account: {account['name']}")
                
                except Exception as e:
                    logger.error(f"❌ Failed to process account {account['name']}: {e}")
                    continue
            
            logger.success("✅ Facebook Ads insights ingestion completed successfully")
        
        except Exception as e:
            logger.error(f"❌ Facebook Ads insights ingestion failed: {e}")
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Parse command-line arguments (call_functions pattern)
    # Usage: python facebook_ads_insights_ingestion.py [days_for_ago]
    
    # Argument 1: days_for_ago (integer)
    days_for_ago = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 1
    
    logger.info(f"Raw Arguments: {sys.argv}")
    logger.info(f"Starting Facebook Ads Insights Ingestion")
    logger.info(f"Arguments -> Days for ago: {days_for_ago}")
    
    try:
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        db = PostgreSQLHelper(POSTGRES_URI)
        logger.info("Connected to PostgreSQL")
        
        job = FacebookAdsInsightsIngestion(db, days_for_ago)
        job.run()
        
        db.close()
        logger.info("Closed PostgreSQL connection")
        
        logger.success("✅ Facebook Ads insights ingestion completed successfully")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"❌ Facebook Ads insights ingestion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
