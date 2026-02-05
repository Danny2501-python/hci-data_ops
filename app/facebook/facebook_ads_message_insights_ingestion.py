"""
Facebook Ads Message Insights Ingestion
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
# FACEBOOK ADS MESSAGE INSIGHTS INGESTION
# ============================================================================

class FacebookAdsMessageInsightsIngestion:
    """
    Facebook Ads Message Insights Ingestion
    Strategy: Direct write to table (no dependencies)
    """
    
    def __init__(self, db: PostgreSQLHelper, days_for_ago: int = 1):
        self.db = db
        self.days_for_ago = days_for_ago
        self.rate_limit_tracker = FacebookRateLimitTracker()
    
    def upsert_message_insight(self, insight_data: Dict, brand_id: int):
        """
        Upsert message insight to database
        
        Table: core_facebook_ad_ad_insight_message
        Primary Key: (fb_ad_ad_id, date, region)
        """
        query = """
            INSERT INTO core_facebook_ad_ad_insight_message (
                date, fb_account_currency, fb_ad_ad_id, spend, region,
                brand_id, unique_actions, etl_created, etl_updated
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (fb_ad_ad_id, date, region) 
            DO UPDATE SET
                fb_account_currency = EXCLUDED.fb_account_currency,
                spend = EXCLUDED.spend,
                brand_id = EXCLUDED.brand_id,
                unique_actions = EXCLUDED.unique_actions,
                etl_updated = NOW()
        """
        
        import json
        region = insight_data.get('region', 'unknown')
        unique_actions = json.dumps(insight_data.get('unique_actions', []))
        
        params = (
            insight_data.get('date_start'),
            insight_data.get('account_currency'),
            insight_data.get('ad_id'),
            insight_data.get('spend', 0),
            region,
            brand_id,
            unique_actions
        )
        
        self.db.execute(query, params)
    
    def fetch_message_insights(self, account: Dict):
        """Fetch message insights from Facebook API"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        now = datetime.now()
        this_day = now - timedelta(days=self.days_for_ago)
        this_day_str = this_day.strftime("%Y-%m-%d")
        
        params = {
            'time_range': {"since": this_day_str, "until": this_day_str},
            'level': 'ad',
            'breakdowns': ['region'],
            'action_breakdowns': ['action_type']
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
            AdsInsights.Field.region,
            AdsInsights.Field.unique_actions
        ]
        
        try:
            insights = ad_account.get_insights(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get message insights: {e}")
            return
        
        self.rate_limit_tracker.reset()
        count = 0
        brand_id = account.get('current_brand_id')
        
        for insight in insights:
            try:
                data = insight._json
                
                # Filter for message-related actions
                if 'unique_actions' in data:
                    has_message_action = any(
                        action.get('action_type') in ['onsite_conversion.messaging_conversation_started_7d', 'onsite_conversion.messaging_first_reply']
                        for action in data.get('unique_actions', [])
                    )
                    
                    if has_message_action:
                        # Direct write to table
                        self.upsert_message_insight(data, brand_id)
                        count += 1
                
            except Exception as e:
                logger.error(f"Error processing message insight: {e}, data: {insight._json}")
            
            # Check rate limit
            try:
                current_headers = insights.headers()
                if self.rate_limit_tracker.should_check_limit(current_headers):
                    logger.debug(f"Headers changed, checking rate limit (processed {count} message insights so far)")
                    self.rate_limit_tracker.check_limit(current_headers)
            except Exception as e:
                logger.error(f"Error checking limit: {e}")
        
        logger.success(f"✅ Processed {count} message insights for account: {account['name']}")
    
    def run(self):
        """Execute Facebook Ads message insights ingestion"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads Message Insights Ingestion")
        logger.info(f"Days for ago: {self.days_for_ago}")
        logger.info("="*80)
        
        try:
            accounts = get_active_facebook_accounts(self.db)
            logger.info(f"Found {len(accounts)} active Facebook Ad accounts")
            
            for account in accounts:
                try:
                    time.sleep(1)
                    logger.info(f"Processing account: {account['name']} ({account['ad_account_id']})")
                    
                    self.fetch_message_insights(account)
                    
                    logger.success(f"✅ Completed account: {account['name']}")
                
                except Exception as e:
                    logger.error(f"❌ Failed to process account {account['name']}: {e}")
                    continue
            
            logger.success("✅ Facebook Ads message insights ingestion completed successfully")
        
        except Exception as e:
            logger.error(f"❌ Facebook Ads message insights ingestion failed: {e}")
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Parse command-line arguments (call_functions pattern)
    # Usage: python facebook_ads_message_insights_ingestion.py [days_for_ago]
    
    # Argument 1: days_for_ago (integer)
    days_for_ago = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 1
    
    logger.info(f"Raw Arguments: {sys.argv}")
    logger.info(f"Starting Facebook Ads Message Insights Ingestion")
    logger.info(f"Arguments -> Days for ago: {days_for_ago}")
    
    try:
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        db = PostgreSQLHelper(POSTGRES_URI)
        logger.info("Connected to PostgreSQL")
        
        job = FacebookAdsMessageInsightsIngestion(db, days_for_ago)
        job.run()
        
        db.close()
        logger.info("Closed PostgreSQL connection")
        
        logger.success("✅ Facebook Ads message insights ingestion completed successfully")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"❌ Facebook Ads message insights ingestion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
