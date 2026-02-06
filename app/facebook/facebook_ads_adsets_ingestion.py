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
from datetime import datetime
from typing import Dict, List
from loguru import logger

# Facebook Business SDK
from facebook_business.adobjects.adset import AdSet
from facebook_business.exceptions import FacebookRequestError

import random

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
    Strategy: Bulk DELETE + COPY from CSV
    """
    
    def __init__(self, db: PostgreSQLHelper, date_preset: str = 'last_90d'):
        self.db = db
        self.date_preset = date_preset
        self.rate_limit_tracker = FacebookRateLimitTracker()
        self.adsets_data = []  # Collect all ad sets
    
    def fetch_adsets(self, account: Dict):
        """Fetch ad sets from Facebook API using manual pagination and retry logic"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        # Explicitly define fields requested
        fields = [
            'id',
            'name',
            'campaign_id',
            'created_time',
            'updated_time',
            'start_time',
            'status',
            'optimization_goal'
        ]
        
        # Parameters for the API call
        params = {
            'date_preset': self.date_preset,
            'fields': ",".join(fields),
            'limit': 100, # Explicit batch size
            'summary': True,
        }
        
        self.rate_limit_tracker.reset()
        count = 0
        
        # Build API path: act_<AD_ACCOUNT_ID>/adsets
        # Note: ad_account_id in DB is raw number, so we add prefix
        url_path = f"act_{account['ad_account_id']}/adsets"
        after_cursor = None
        
        logger.info(f"Starting fetch loop for account {account['name']} (ID: {account['ad_account_id']})")
        
        while True:
            # 1. Prepare current batch params
            current_params = params.copy()
            if after_cursor:
                current_params['after'] = after_cursor
            
            batch_data = None
            paging = None
            
            # 2. Retry Loop for Rate Limiting
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    # Call API manually using the SDK's internal call method
                    # This allows full control over handling exceptions and response headers
                    response = api.call(
                        'GET',
                        url_path,
                        params=current_params
                    )
                    
                    # Parse Response
                    json_data = response.json()
                    batch_data = json_data.get('data', [])
                    paging = json_data.get('paging', {})
                    
                    # Check Headers for Rate Limiting (Passive)
                    headers = response.headers()
                    if self.rate_limit_tracker.should_check_limit(headers):
                         self.rate_limit_tracker.check_limit(headers)
                    
                    # If successful, break validation loop
                    break 
                    
                except FacebookRequestError as e:
                    # Capture actual error codes
                    error_code = e.api_error_code()
                    
                    # Code 17: User request limit reached
                    # Code 4: App request limit reached
                    # Code 613: Custom rate limit
                    is_rate_limit = error_code in [17, 4, 613]
                    
                    if is_rate_limit:
                        # Exponential Backoff with Jitter: 60s -> 120s ... -> Max 10 mins
                        # Base wait increases with attempts
                        base_wait = (2 ** attempt) * 60 
                        sleep_time = min(base_wait, 600) + random.randint(5, 30)
                        
                        logger.warning(
                            f"⚠️ Rate limit hit (Code {error_code}). "
                            f"Sleeping {sleep_time}s before retry {attempt+1}/{max_retries}..."
                        )
                        time.sleep(sleep_time)
                        continue # Retry the loop
                    else:
                        logger.error(f"Facebook API Error: {e}")
                        raise e # Non-rate-limit errors break the flow (or we could choose to skip)
                        
                except Exception as e:
                    # Generic network or parsing errors
                    sleep_time = 30
                    logger.warning(f"Unexpected error fetching ad sets: {e}. Retry in {sleep_time}s. Attempt {attempt+1}/{max_retries}")
                    time.sleep(sleep_time)
                    continue

            # 3. Process Batch
            if batch_data is None:
                 logger.error("Failed to fetch batch after max retries. Stopping collection.")
                 break
            
            for item in batch_data:
                try:
                    self.adsets_data.append({
                        'fb_ad_set_id': item.get('id'),
                        'name': item.get('name'),
                        'fb_ad_campaign_id': item.get('campaign_id'),
                        'created_time': item.get('created_time'),
                        'updated_time': item.get('updated_time'),
                        'start_time': item.get('start_time'),
                        'status': item.get('status')
                    })
                    count += 1
                except Exception as e:
                    logger.error(f"Error processing item: {e}")
            
            # 4. Advance Cursor
            cursors = paging.get('cursors', {})
            after_cursor = cursors.get('after')
            
            # Log progress periodically
            if count % 500 == 0:
                logger.info(f"Collected {count} ad sets so far...")
            
            if not after_cursor:
                break
        
        logger.success(f"✅ Collected total {count} ad sets for account: {account['name']}")
    
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
