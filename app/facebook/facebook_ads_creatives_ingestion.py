"""
Facebook Ads Ad Creatives Ingestion
Migrated from etl_marketing Django app
Strategy: Call API → Write 1:1 to table (no foreign key dependencies)
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, List
from loguru import logger

# Facebook Business SDK
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
# FACEBOOK ADS AD CREATIVES INGESTION
# ============================================================================

class FacebookAdsAdCreativesIngestion:
    """
    Facebook Ads Ad Creatives Ingestion
    Strategy: API → Direct write to table (no dependencies)
    """
    
    def __init__(self, db: PostgreSQLHelper, date_preset: str = 'last_90d'):
        self.db = db
        self.date_preset = date_preset
        self.rate_limit_tracker = FacebookRateLimitTracker()
    
    def upsert_creative(self, creative_data: Dict, brand_id: int):
        """
        Upsert ad creative to database
        
        Table: core_facebook_ad_creative
        Primary Key: fb_ad_creative_id
        """
        query = """
            INSERT INTO core_facebook_ad_creative (
                fb_ad_creative_id,
                name,
                fb_ad_account_id,
                fb_object_id,
                fb_object_story_id,
                fb_link_url,
                fb_effective_object_story_id,
                status,
                brand_id,
                etl_created,
                etl_updated
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (fb_ad_creative_id) 
            DO UPDATE SET
                name = EXCLUDED.name,
                fb_ad_account_id = EXCLUDED.fb_ad_account_id,
                fb_object_id = EXCLUDED.fb_object_id,
                fb_object_story_id = EXCLUDED.fb_object_story_id,
                fb_link_url = EXCLUDED.fb_link_url,
                fb_effective_object_story_id = EXCLUDED.fb_effective_object_story_id,
                status = EXCLUDED.status,
                brand_id = EXCLUDED.brand_id,
                etl_updated = NOW()
        """
        
        params = (
            creative_data.get('id'),
            creative_data.get('name'),
            creative_data.get('account_id'),
            creative_data.get('object_id'),
            creative_data.get('object_story_id'),
            creative_data.get('link_url'),
            creative_data.get('effective_object_story_id'),
            creative_data.get('status'),
            brand_id
        )
        
        self.db.execute(query, params)
    
    def fetch_creatives(self, account: Dict):
        """Fetch ad creatives from Facebook API"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        params = {'date_preset': self.date_preset}
        
        fields = [
            AdCreative.Field.name,
            AdCreative.Field.account_id,
            AdCreative.Field.object_id,
            AdCreative.Field.object_story_id,
            AdCreative.Field.link_url,
            AdCreative.Field.effective_object_story_id,
            AdCreative.Field.status,
            AdCreative.Field.actor_id,
        ]
        
        try:
            ad_creatives = ad_account.get_ad_creatives(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get ad creatives: {e}")
            return
        
        # Reset rate limit tracker for this account
        self.rate_limit_tracker.reset()
        count = 0
        
        for ad_creative in ad_creatives:
            try:
                data = ad_creative._json
                
                # Direct write to table
                self.upsert_creative(data, account.get('current_brand_id'))
                count += 1
                
            except Exception as e:
                logger.error(f"Error processing ad creative: {e}, data: {ad_creative._json}")
            
            # Check rate limit only when headers change (new API call)
            try:
                current_headers = ad_creatives.headers()
                if self.rate_limit_tracker.should_check_limit(current_headers):
                    logger.debug(f"Headers changed, checking rate limit (processed {count} ad creatives so far)")
                    self.rate_limit_tracker.check_limit(current_headers)
            except Exception as e:
                logger.error(f"Error checking limit: {e}")
        
        logger.success(f"✅ Processed {count} ad creatives for account: {account['name']}")
    
    def run(self):
        """Execute Facebook Ads ad creatives ingestion"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads Ad Creatives Ingestion")
        logger.info(f"Date preset: {self.date_preset}")
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
                    
                    self.fetch_creatives(account)
                    
                    logger.success(f"✅ Completed account: {account['name']}")
                
                except Exception as e:
                    logger.error(f"❌ Failed to process account {account['name']}: {e}")
                    continue
            
            logger.success("✅ Facebook Ads ad creatives ingestion completed successfully")
        
        except Exception as e:
            logger.error(f"❌ Facebook Ads ad creatives ingestion failed: {e}")
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Parse command-line arguments (call_functions pattern)
    # Usage: python facebook_ads_creatives_ingestion.py [date_preset]
    
    # Argument 1: date_preset (string)
    date_preset = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else 'last_90d'
    
    logger.info(f"Raw Arguments: {sys.argv}")
    logger.info(f"Starting Facebook Ads Ad Creatives Ingestion")
    logger.info(f"Arguments -> Date preset: {date_preset}")
    
    try:
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        db = PostgreSQLHelper(POSTGRES_URI)
        logger.info("Connected to PostgreSQL")
        
        job = FacebookAdsAdCreativesIngestion(db, date_preset)
        job.run()
        
        db.close()
        logger.info("Closed PostgreSQL connection")
        
        logger.success("✅ Facebook Ads ad creatives ingestion completed successfully")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"❌ Facebook Ads ad creatives ingestion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
