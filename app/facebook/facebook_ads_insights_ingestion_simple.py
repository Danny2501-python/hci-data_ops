"""
Facebook Ads Insights Ingestion - SIMPLIFIED VERSION
Migrated from etl_marketing Django app
Strategy: Call API → Write 1:1 to table (no foreign key dependencies)
"""

import os
import sys
import time
import random
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from loguru import logger

# Facebook Business SDK
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
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
# FACEBOOK ADS INSIGHTS INGESTION (SIMPLIFIED)
# ============================================================================

class FacebookAdsInsightsSimplified:
    """
    Simplified Facebook Ads Insights Ingestion
    Strategy: API → Direct write to table (no dependencies)
    """
    
    def __init__(self, db: PostgreSQLHelper, days_for_ago: int = 1):
        self.db = db
        self.days_for_ago = days_for_ago
        self.rate_limit_tracker = FacebookRateLimitTracker()
    

    
    def upsert_insight(self, insight_data: Dict, brand_id: int):
        """
        Upsert insight data to core_facebook_ad_ad_insight_v1
        Matches schema: id, date, fb_account_currency, fb_ad_ad_id, spend, region, brand_id
        """
        fb_ad_ad_id = insight_data.get('ad_id')
        date = insight_data.get('date_start')
        region = insight_data.get('region', '')
        spend = float(insight_data.get('spend', 0))
        fb_account_currency = insight_data.get('account_currency', 'VND')
        
        # Check if exists
        check_query = """
            SELECT id FROM core_facebook_ad_ad_insight_v1
            WHERE fb_ad_ad_id = %s AND date = %s AND region = %s
        """
        result = self.db.fetchone(check_query, (fb_ad_ad_id, date, region))
        
        if result:
            # Update
            update_query = """
                UPDATE core_facebook_ad_ad_insight_v1
                SET spend = %s, fb_account_currency = %s, 
                    etl_updated = NOW()
                WHERE id = %s
            """
            self.db.execute(update_query, (
                spend, fb_account_currency, result[0]
            ))
            logger.debug(f"Updated insight for ad {fb_ad_ad_id}, date {date}")
        else:
            # Insert
            insert_query = """
                INSERT INTO core_facebook_ad_ad_insight_v1
                (fb_ad_ad_id, date, spend, fb_account_currency, region, brand_id, etl_created, etl_updated)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            """
            self.db.execute(insert_query, (
                fb_ad_ad_id,
                date,
                spend,
                fb_account_currency,
                region,
                brand_id
            ))
            logger.debug(f"Created insight for ad {fb_ad_ad_id}, date {date}")
    
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
            'breakdowns': [AdsInsights.Breakdowns.region],
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
        ]
        
        try:
            insights = ad_account.get_insights(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get insights: {e}")
            return
        
        # Reset rate limit tracker for this account
        self.rate_limit_tracker.reset()
        count = 0
        
        for insight in insights:
            # Break if too many rate limit waits
            if self.rate_limit_tracker.time_await_limit >= 5:
                logger.error("Time_await_limit >= 5, breaking loop")
                break
            
            try:
                data = insight._json
                
                # Direct write to table (no dependencies!)
                self.upsert_insight(data, account.get('current_brand_id'))
                count += 1
                
            except Exception as e:
                logger.error(f"Error processing insight: {e}, data: {insight._json}")
            
            # Check rate limit only when headers change (new API call)
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
        logger.info(f"Starting Facebook Ads Insights Ingestion (SIMPLIFIED)")
        logger.info(f"Days for ago: {self.days_for_ago}")
        logger.info("="*80)
        
        try:
            # Get active accounts using shared function
            accounts = get_active_facebook_accounts(self.db)
            logger.info(f"Found {len(accounts)} active Facebook Ad accounts")
            
            # Process each account
            for account in accounts:
                try:
                    time.sleep(1)  # Rate limiting between accounts
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

def main():
    """Main execution function"""
    days_for_ago = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    
    logger.info(f"Arguments → Days for ago: {days_for_ago}")
    
    try:
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        db = PostgreSQLHelper(POSTGRES_URI)
        logger.info("Connected to PostgreSQL")
        
        job = FacebookAdsInsightsSimplified(db, days_for_ago)
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


if __name__ == "__main__":
    main()
