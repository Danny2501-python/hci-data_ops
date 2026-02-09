"""
Facebook Ads Ad Sets Ingestion
Migrated from etl_marketing Django app
Strategy: Bulk DELETE + COPY (CSV) for maximum performance
"""

import os
import sys
import time
import csv
import tempfile
import random
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
FB_API_VERSION = os.getenv('FB_API_VERSION')


# ============================================================================
# FACEBOOK ADS AD SETS INGESTION
# ============================================================================

class FacebookAdsAdSetsIngestion:
    """
    Facebook Ads Ad Sets Ingestion
    Strategy: Bulk DELETE + COPY from CSV
    """
    
    def __init__(self, db: PostgreSQLHelper, date_preset: str = 'last_90d'):
        self.db = db
        self.date_preset = date_preset
        self.rate_limit_tracker = FacebookRateLimitTracker()
        self.adsets_data = []  # Collect all ad sets
    
    def fetch_adsets(self, account: Dict):
        """Fetch ad sets from Facebook API and collect to list"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        params = {'date_preset': self.date_preset}
        
        # Fetch same fields as original code (even if not all saved to DB)
        fields = [
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.created_time,
            AdSet.Field.updated_time,
            AdSet.Field.start_time,
            AdSet.Field.status,
            AdSet.Field.optimization_goal  # Fetched but not saved (not in DB schema)
        ]
        
        try:
            adsets = ad_account.get_ad_sets(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get ad sets: {e}")
            return
        
        self.rate_limit_tracker.reset()
        count = 0
        
        try:
            for adset in adsets:
                try:
                    data = adset._json
                    
                    # Collect ad set data
                    self.adsets_data.append({
                        'fb_ad_set_id': data.get('id'),
                        'name': data.get('name'),
                        'fb_ad_campaign_id': data.get('campaign_id'),
                        'created_time': data.get('created_time'),
                        'updated_time': data.get('updated_time'),
                        'start_time': data.get('start_time'),
                        'status': data.get('status')
                    })
                    count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing ad set: {e}")
                    continue
                
                try:
                    current_headers = adsets.headers()
                    if self.rate_limit_tracker.should_check_limit(current_headers):
                        logger.debug(f"Headers changed, checking rate limit (processed {count} ad sets so far)")
                        self.rate_limit_tracker.check_limit(current_headers)
                        
                        # Ad-hoc throttle: Sleep randomly around 1s to avoid burst limits
                        sleep_time = random.uniform(0.8, 1.2)
                        time.sleep(sleep_time)
                        
                        if self.rate_limit_tracker.time_await_limit >= 5:
                            logger.warning(f"🛑 Stopping account {account['name']} due to 5x rate limit hits.")
                            break
                            
                except Exception as e:
                    logger.error(f"Error checking limit: {e}")
        
        except Exception as e:
            # Catch iterator exceptions (e.g., rate limit when fetching next batch)
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in [
                'rate limit', 
                'user request limit',
                'code": 17',
                'error_subcode": 2446079'
            ]):
                logger.warning(f"⚠️ Rate limit hit while iterating (after {count} ad sets)")
                logger.warning(f"Iterator exception: {e}")
            else:
                logger.error(f"Error during iteration: {e}")
                raise  # Re-raise if not rate limit
        
        logger.success(f"✅ Collected {count} ad sets for account: {account['name']}")
    
    def bulk_upsert_adsets(self):
        """Bulk upsert ad sets using DELETE + COPY"""
        if not self.adsets_data:
            logger.warning("No ad sets to upsert")
            return
        
        logger.info(f"Starting bulk upsert for {len(self.adsets_data)} ad sets...")
        
        csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv', encoding='utf-8')
        
        try:
            writer = csv.DictWriter(csv_file, fieldnames=[
                'fb_ad_set_id', 'name', 'fb_ad_campaign_id', 'created_time',
                'updated_time', 'start_time', 'status'
            ])
            
            for row in self.adsets_data:
                writer.writerow(row)
            
            csv_file.close()
            logger.info(f"✅ Wrote {len(self.adsets_data)} ad sets to CSV: {csv_file.name}")
            
            # DELETE existing
            adset_ids = [a['fb_ad_set_id'] for a in self.adsets_data]
            delete_query = "DELETE FROM core_facebook_ad_set WHERE fb_ad_set_id = ANY(%s)"
            self.db.execute(delete_query, (adset_ids,))
            logger.info(f"✅ Deleted existing ad sets")
            
            # COPY from CSV
            copy_query = f"""
                COPY core_facebook_ad_set (
                    fb_ad_set_id, name, fb_ad_campaign_id, created_time,
                    updated_time, start_time, status
                )
                FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')
            """
            
            with open(csv_file.name, 'r', encoding='utf-8') as f:
                cursor = self.db.conn.cursor()
                cursor.copy_expert(copy_query, f)
                self.db.conn.commit()
                cursor.close()
            
            logger.success(f"✅ Bulk inserted {len(self.adsets_data)} ad sets via COPY")
            
        finally:
            if os.path.exists(csv_file.name):
                os.unlink(csv_file.name)
    
    def run(self):
        """Execute Facebook Ads ad sets ingestion"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads Ad Sets Ingestion (Bulk COPY)")
        logger.info(f"Date preset: {self.date_preset}")
        logger.info("="*80)
        
        try:
            accounts = get_active_facebook_accounts(self.db)
            logger.info(f"Found {len(accounts)} active Facebook Ad accounts")
            
            for account in accounts:
                try:
                    time.sleep(1)
                    logger.info(f"Fetching account: {account['name']} ({account['ad_account_id']})")
                    self.fetch_adsets(account)
                except Exception as e:
                    logger.error(f"❌ Failed to fetch account {account['name']}: {e}")
                    continue
            
            self.bulk_upsert_adsets()
            logger.success("✅ Facebook Ads ad sets ingestion completed successfully")
        
        except Exception as e:
            logger.error(f"❌ Facebook Ads ad sets ingestion failed: {e}")
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
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
