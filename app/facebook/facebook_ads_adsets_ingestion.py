"""
Facebook Ads Ad Sets Ingestion
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
from facebook_business.adobjects.adset import AdSet

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
# FACEBOOK ADS AD SETS INGESTION
# ============================================================================

class FacebookAdsAdSetsIngestion:
    """
    Facebook Ads Ad Sets Ingestion
    Strategy: API → Direct write to table (no dependencies)
    """
    
    def __init__(self, db: PostgreSQLHelper, date_preset: str = 'last_90d'):
        self.db = db
        self.date_preset = date_preset
        self.rate_limit_tracker = FacebookRateLimitTracker()
    
    def upsert_adset(self, adset_data: Dict, brand_id: int):
        """
        Upsert ad set to database
        
        Table: core_facebook_ad_set
        Primary Key: fb_ad_set_id
        """
        query = """
            INSERT INTO core_facebook_ad_set (
                fb_ad_set_id,
                name,
                fb_ad_campaign_id,
                created_time,
                updated_time,
                start_time,
                status,
                etl_created,
                etl_updated
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (fb_ad_set_id) 
            DO UPDATE SET
                name = EXCLUDED.name,
                fb_ad_campaign_id = EXCLUDED.fb_ad_campaign_id,
                updated_time = EXCLUDED.updated_time,
                start_time = EXCLUDED.start_time,
                status = EXCLUDED.status,
                etl_updated = NOW()
        """
        
        params = (
            adset_data.get('id'),
            adset_data.get('name'),
            adset_data.get('campaign_id'),
            adset_data.get('created_time'),
            adset_data.get('updated_time'),
            adset_data.get('start_time'),
            adset_data.get('status')
        )
        
        self.db.execute(query, params)
    
    def fetch_adsets(self, account: Dict):
        """Fetch ad sets from Facebook API"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        params = {'date_preset': self.date_preset}
        
        fields = [
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.created_time,
            AdSet.Field.updated_time,
            AdSet.Field.start_time,
            AdSet.Field.status,
            AdSet.Field.optimization_goal
        ]
        
        try:
            adsets = ad_account.get_ad_sets(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get ad sets: {e}")
            return
        
        # Reset rate limit tracker for this account
        self.rate_limit_tracker.reset()
        count = 0
        
        for adset in adsets:
            try:
                data = adset._json
                
                # Direct write to table
                self.upsert_adset(data, account.get('current_brand_id'))
                count += 1
                
            except Exception as e:
                logger.error(f"Error processing ad set: {e}, data: {adset._json}")
            
            # Check rate limit only when headers change (new API call)
            try:
                current_headers = adsets.headers()
                if self.rate_limit_tracker.should_check_limit(current_headers):
                    logger.debug(f"Headers changed, checking rate limit (processed {count} ad sets so far)")
                    self.rate_limit_tracker.check_limit(current_headers)
            except Exception as e:
                logger.error(f"Error checking limit: {e}")
        
        logger.success(f"✅ Processed {count} ad sets for account: {account['name']}")
    
    def run(self):
        """Execute Facebook Ads ad sets ingestion"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads Ad Sets Ingestion")
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
                    
                    self.fetch_adsets(account)
                    
                    logger.success(f"✅ Completed account: {account['name']}")
                
                except Exception as e:
                    logger.error(f"❌ Failed to process account {account['name']}: {e}")
                    continue
            
            logger.success("✅ Facebook Ads ad sets ingestion completed successfully")
        
        except Exception as e:
            logger.error(f"❌ Facebook Ads ad sets ingestion failed: {e}")
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Parse command-line arguments (call_functions pattern)
    # Usage: python facebook_ads_adsets_ingestion.py [date_preset]
    
    # Argument 1: date_preset (string)
    date_preset = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else 'last_90d'
    
    logger.info(f"Raw Arguments: {sys.argv}")
    logger.info(f"Starting Facebook Ads Ad Sets Ingestion")
    logger.info(f"Arguments -> Date preset: {date_preset}")
    
    try:
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        db = PostgreSQLHelper(POSTGRES_URI)
        logger.info("Connected to PostgreSQL")
        
        job = FacebookAdsAdSetsIngestion(db, date_preset)
        job.run()
        
        db.close()
        logger.info("Closed PostgreSQL connection")
        
        logger.success("✅ Facebook Ads ad sets ingestion completed successfully")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"❌ Facebook Ads ad sets ingestion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
