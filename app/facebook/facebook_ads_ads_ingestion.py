"""
Facebook Ads (Ad Ads) Ingestion
Migrated from etl_marketing Django app
Strategy: Call API → Write 1:1 to table (no dependencies)
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional
from loguru import logger

# Facebook Business SDK
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adcreative import AdCreative

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
# FACEBOOK ADS (AD ADS) INGESTION (WITH DEPENDENCIES)
# ============================================================================

class FacebookAdsAdsIngestion:
    """
    Facebook Ads (Ad Ads) Ingestion
    Strategy: Direct write to table (no dependency creation)
    """
    
    def __init__(self, db: PostgreSQLHelper, date_preset: str = 'last_90d'):
        self.db = db
        self.date_preset = date_preset
        self.rate_limit_tracker = FacebookRateLimitTracker()
    
    def upsert_ad(self, ad_data: Dict, brand_id: int):
        """Upsert ad to database"""
        query = """
            INSERT INTO core_facebook_ad_ad (
                fb_ad_ad_id, name, fb_ad_set_id, fb_creative_id,
                created_time, updated_time, status, brand_id,
                etl_created, etl_updated
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (fb_ad_ad_id) 
            DO UPDATE SET
                name = EXCLUDED.name,
                fb_ad_set_id = EXCLUDED.fb_ad_set_id,
                fb_creative_id = EXCLUDED.fb_creative_id,
                updated_time = EXCLUDED.updated_time,
                status = EXCLUDED.status,
                brand_id = EXCLUDED.brand_id,
                etl_updated = NOW()
        """
        
        params = (
            ad_data.get('id'),
            ad_data.get('name'),
            ad_data.get('adset_id'),
            ad_data.get('creative', {}).get('id'),
            ad_data.get('created_time'),
            ad_data.get('updated_time'),
            ad_data.get('status'),
            brand_id
        )
        
        self.db.execute(query, params)
    
    def fetch_ads(self, account: Dict):
        """Fetch ads from Facebook API"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        params = {'date_preset': self.date_preset}
        
        fields = [
            Ad.Field.account_id,
            Ad.Field.name,
            Ad.Field.campaign_id,
            Ad.Field.adset_id,
            Ad.Field.creative,
            Ad.Field.created_time,
            Ad.Field.updated_time,
            Ad.Field.status
        ]
        
        try:
            ad_ads = ad_account.get_ads(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get ads: {e}")
            return
        
        self.rate_limit_tracker.reset()
        count = 0
        brand_id = account.get('current_brand_id')
        
        for ad_ad in ad_ads:
            try:
                data = ad_ad._json
                
                # Direct write to table
                self.upsert_ad(data, brand_id)
                count += 1
                
            except Exception as e:
                logger.error(f"Error processing ad: {e}, data: {ad_ad._json}")
            
            # Check rate limit
            try:
                current_headers = ad_ads.headers()
                if self.rate_limit_tracker.should_check_limit(current_headers):
                    logger.debug(f"Headers changed, checking rate limit (processed {count} ads so far)")
                    self.rate_limit_tracker.check_limit(current_headers)
            except Exception as e:
                logger.error(f"Error checking limit: {e}")
        
        logger.success(f"✅ Processed {count} ads for account: {account['name']}")
    
    def run(self):
        """Execute Facebook Ads (ad ads) ingestion"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads (Ad Ads) Ingestion")
        logger.info(f"Date preset: {self.date_preset}")
        logger.info("="*80)
        
        try:
            accounts = get_active_facebook_accounts(self.db)
            logger.info(f"Found {len(accounts)} active Facebook Ad accounts")
            
            for account in accounts:
                try:
                    time.sleep(1)
                    logger.info(f"Processing account: {account['name']} ({account['ad_account_id']})")
                    
                    self.fetch_ads(account)
                    
                    logger.success(f"✅ Completed account: {account['name']}")
                
                except Exception as e:
                    logger.error(f"❌ Failed to process account {account['name']}: {e}")
                    continue
            
            logger.success("✅ Facebook Ads (ad ads) ingestion completed successfully")
        
        except Exception as e:
            logger.error(f"❌ Facebook Ads (ad ads) ingestion failed: {e}")
            raise



# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Parse command-line arguments (call_functions pattern)
    # Usage: python facebook_ads_ads_ingestion.py [date_preset]
    
    # Argument 1: date_preset (string)
    date_preset = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else 'last_90d'
    
    logger.info(f"Raw Arguments: {sys.argv}")
    logger.info(f"Starting Facebook Ads (Ad Ads) Ingestion")
    logger.info(f"Arguments -> Date preset: {date_preset}")
    
    try:
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        db = PostgreSQLHelper(POSTGRES_URI)
        logger.info("Connected to PostgreSQL")
        
        job = FacebookAdsAdsIngestion(db, date_preset)
        job.run()
        
        db.close()
        logger.info("Closed PostgreSQL connection")
        
        logger.success("✅ Facebook Ads (ad ads) ingestion completed successfully")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"❌ Facebook Ads (ad ads) ingestion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
