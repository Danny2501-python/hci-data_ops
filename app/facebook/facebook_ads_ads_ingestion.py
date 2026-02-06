"""
Facebook Ads (Ad Ads) Ingestion
Migrated from etl_marketing Django app
Strategy: Bulk DELETE + COPY (CSV) for maximum performance
"""

import os
import sys
import time
import csv
import tempfile
from datetime import datetime
from typing import Dict, List
from loguru import logger

# Facebook Business SDK
from facebook_business.adobjects.ad import Ad

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
# FACEBOOK ADS (AD ADS) INGESTION
# ============================================================================

class FacebookAdsAdsIngestion:
    """
    Facebook Ads (Ad Ads) Ingestion
    Strategy: Bulk DELETE + COPY from CSV
    """
    
    def __init__(self, db: PostgreSQLHelper, date_preset: str = 'last_90d'):
        self.db = db
        self.date_preset = date_preset
        self.rate_limit_tracker = FacebookRateLimitTracker()
        self.ads_data = []  # Collect all ads
    
    def fetch_ads(self, account: Dict):
        """Fetch ads from Facebook API and collect to list"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        params = {'date_preset': self.date_preset}
        
        # Fetch same fields as original code (even if not all saved to DB)
        fields = [
            Ad.Field.account_id,      # Fetched but not saved (for validation/logging)
            Ad.Field.name,
            Ad.Field.campaign_id,     # Fetched but not saved (for reference)
            Ad.Field.adset_id,
            Ad.Field.creative,
            Ad.Field.created_time,
            Ad.Field.updated_time,
            Ad.Field.status,
            Ad.Field.targeting        # Fetched but not saved (not in DB schema)
        ]
        
        try:
            ads = ad_account.get_ads(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get ads: {e}")
            return
        
        self.rate_limit_tracker.reset()
        count = 0
        brand_id = account.get('current_brand_id')
        
        try:
            for ad in ads:
                try:
                    data = ad._json
                    
                    # Collect ad data
                    self.ads_data.append({
                        'fb_ad_ad_id': data.get('id'),
                        'name': data.get('name'),
                        'fb_ad_set_id': data.get('adset_id'),
                        'fb_creative_id': data.get('creative', {}).get('id') if isinstance(data.get('creative'), dict) else None,
                        'created_time': data.get('created_time'),
                        'updated_time': data.get('updated_time'),
                        'status': data.get('status'),
                        'brand_id': brand_id
                    })
                    count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing ad: {e}")
                    continue
                
                try:
                    current_headers = ads.headers()
                    if self.rate_limit_tracker.should_check_limit(current_headers):
                        logger.debug(f"Headers changed, checking rate limit (processed {count} ads so far)")
                        self.rate_limit_tracker.check_limit(current_headers)
                        
                        if self.rate_limit_tracker.time_await_limit >= 5:
                            logger.warning(f"🛑 Stopping account {account['name']} due to 5x rate limit hits.")
                            break
                            
                except Exception as e:
                    logger.error(f"Error checking limit: {e}")
        
        except Exception as e:
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in [
                'rate limit', 
                'user request limit',
                'code": 17',
                'error_subcode": 2446079'
            ]):
                logger.warning(f"⚠️ Rate limit hit while iterating (after {count} ads)")
                logger.warning(f"Iterator exception: {e}")
            else:
                logger.error(f"Error during iteration: {e}")
                raise
        
        logger.success(f"✅ Collected {count} ads for account: {account['name']}")
    
    def bulk_upsert_ads(self):
        """Bulk upsert ads using DELETE + COPY"""
        if not self.ads_data:
            logger.warning("No ads to upsert")
            return
        
        logger.info(f"Starting bulk upsert for {len(self.ads_data)} ads...")
        
        csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv', encoding='utf-8')
        
        try:
            writer = csv.DictWriter(csv_file, fieldnames=[
                'fb_ad_ad_id', 'name', 'fb_ad_set_id', 'fb_creative_id',
                'created_time', 'updated_time', 'status', 'brand_id'
            ])
            
            for row in self.ads_data:
                # Convert None to empty string for CSV
                csv_row = {k: (v if v is not None else '') for k, v in row.items()}
                writer.writerow(csv_row)
            
            csv_file.close()
            logger.info(f"✅ Wrote {len(self.ads_data)} ads to CSV: {csv_file.name}")
            
            # DELETE existing
            ad_ids = [a['fb_ad_ad_id'] for a in self.ads_data]
            delete_query = "DELETE FROM core_facebook_ad_ad WHERE fb_ad_ad_id = ANY(%s)"
            self.db.execute(delete_query, (ad_ids,))
            logger.info(f"✅ Deleted existing ads")
            
            # COPY from CSV
            copy_query = f"""
                COPY core_facebook_ad_ad (
                    fb_ad_ad_id, name, fb_ad_set_id, fb_creative_id,
                    created_time, updated_time, status, brand_id
                )
                FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')
            """
            
            with open(csv_file.name, 'r', encoding='utf-8') as f:
                cursor = self.db.conn.cursor()
                cursor.copy_expert(copy_query, f)
                self.db.conn.commit()
                cursor.close()
            
            logger.success(f"✅ Bulk inserted {len(self.ads_data)} ads via COPY")
            
        finally:
            if os.path.exists(csv_file.name):
                os.unlink(csv_file.name)
    
    def run(self):
        """Execute Facebook Ads (ad ads) ingestion"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads (Ad Ads) Ingestion (Bulk COPY)")
        logger.info(f"Date preset: {self.date_preset}")
        logger.info("="*80)
        
        try:
            accounts = get_active_facebook_accounts(self.db)
            logger.info(f"Found {len(accounts)} active Facebook Ad accounts")
            
            for account in accounts:
                try:
                    time.sleep(1)
                    logger.info(f"Fetching account: {account['name']} ({account['ad_account_id']})")
                    self.fetch_ads(account)
                except Exception as e:
                    logger.error(f"❌ Failed to fetch account {account['name']}: {e}")
                    continue
            
            self.bulk_upsert_ads()
            logger.success("✅ Facebook Ads (ad ads) ingestion completed successfully")
        
        except Exception as e:
            logger.error(f"❌ Facebook Ads (ad ads) ingestion failed: {e}")
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
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
