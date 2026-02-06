"""
Facebook Ads Campaigns Ingestion
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
from facebook_business.adobjects.campaign import Campaign

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
# FACEBOOK ADS CAMPAIGNS INGESTION
# ============================================================================

class FacebookAdsCampaignsIngestion:
    """
    Facebook Ads Campaigns Ingestion
    Strategy: Bulk DELETE + COPY from CSV
    """
    
    def __init__(self, db: PostgreSQLHelper, date_preset: str = 'last_90d'):
        self.db = db
        self.date_preset = date_preset
        self.rate_limit_tracker = FacebookRateLimitTracker()
        self.campaigns_data = []  # Collect all campaigns
    
    def fetch_campaigns(self, account: Dict):
        """Fetch campaigns from Facebook API and collect to list"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        params = {'date_preset': self.date_preset}
        
        fields = [
            Campaign.Field.name,
            Campaign.Field.account_id,
            Campaign.Field.created_time,
            Campaign.Field.updated_time,
            Campaign.Field.start_time,
            Campaign.Field.stop_time,
            Campaign.Field.status
        ]
        
        try:
            campaigns = ad_account.get_campaigns(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get campaigns: {e}")
            return
        
        # Reset rate limit tracker for this account
        self.rate_limit_tracker.reset()
        count = 0
        brand_id = account.get('current_brand_id')
        
        for campaign in campaigns:
            try:
                data = campaign._json
                
                # Collect campaign data
                self.campaigns_data.append({
                    'fb_ad_campaign_id': data.get('id'),
                    'name': data.get('name'),
                    'fb_ad_account_id': data.get('account_id'),
                    'created_time': data.get('created_time'),
                    'updated_time': data.get('updated_time'),
                    'start_time': data.get('start_time'),
                    'stop_time': data.get('stop_time'),
                    'status': data.get('status'),
                    'company_id': brand_id
                })
                count += 1
                
            except Exception as e:
                logger.error(f"Error processing campaign: {e}, data: {campaign._json}")
            
            # Check rate limit only when headers change (new API call)
            try:
                current_headers = campaigns.headers()
                if self.rate_limit_tracker.should_check_limit(current_headers):
                    logger.debug(f"Headers changed, checking rate limit (processed {count} campaigns so far)")
                    self.rate_limit_tracker.check_limit(current_headers)
            except Exception as e:
                logger.error(f"Error checking limit: {e}")
        
        logger.success(f"✅ Collected {count} campaigns for account: {account['name']}")
    
    def bulk_upsert_campaigns(self):
        """Bulk upsert campaigns using DELETE + COPY"""
        if not self.campaigns_data:
            logger.warning("No campaigns to upsert")
            return
        
        logger.info(f"Starting bulk upsert for {len(self.campaigns_data)} campaigns...")
        
        # Step 1: Write data to CSV file
        csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv', encoding='utf-8')
        
        try:
            writer = csv.DictWriter(csv_file, fieldnames=[
                'fb_ad_campaign_id', 'name', 'fb_ad_account_id', 'created_time',
                'updated_time', 'start_time', 'stop_time', 'status', 'company_id'
            ])
            
            for row in self.campaigns_data:
                writer.writerow(row)
            
            csv_file.close()
            logger.info(f"✅ Wrote {len(self.campaigns_data)} campaigns to CSV: {csv_file.name}")
            
            # Step 2: DELETE existing campaigns (by fb_ad_campaign_id)
            campaign_ids = [c['fb_ad_campaign_id'] for c in self.campaigns_data]
            delete_query = """
                DELETE FROM core_facebook_ad_campaign 
                WHERE fb_ad_campaign_id = ANY(%s)
            """
            self.db.execute(delete_query, (campaign_ids,))
            logger.info(f"✅ Deleted existing campaigns")
            
            # Step 3: COPY from CSV to PostgreSQL
            copy_query = f"""
                COPY core_facebook_ad_campaign (
                    fb_ad_campaign_id, name, fb_ad_account_id, created_time,
                    updated_time, start_time, stop_time, status, company_id
                )
                FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')
            """
            
            with open(csv_file.name, 'r', encoding='utf-8') as f:
                cursor = self.db.conn.cursor()
                cursor.copy_expert(copy_query, f)
                self.db.conn.commit()
                cursor.close()
            
            logger.success(f"✅ Bulk inserted {len(self.campaigns_data)} campaigns via COPY")
            
        finally:
            # Cleanup temp file
            if os.path.exists(csv_file.name):
                os.unlink(csv_file.name)
    
    def run(self):
        """Execute Facebook Ads campaigns ingestion"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads Campaigns Ingestion (Bulk COPY)")
        logger.info(f"Date preset: {self.date_preset}")
        logger.info("="*80)
        
        try:
            # Get active accounts using shared function
            accounts = get_active_facebook_accounts(self.db)
            logger.info(f"Found {len(accounts)} active Facebook Ad accounts")
            
            # Step 1: Fetch all campaigns from all accounts
            for account in accounts:
                try:
                    time.sleep(1)  # Rate limiting between accounts
                    logger.info(f"Fetching account: {account['name']} ({account['ad_account_id']})")
                    
                    self.fetch_campaigns(account)
                    
                except Exception as e:
                    logger.error(f"❌ Failed to fetch account {account['name']}: {e}")
                    continue
            
            # Step 2: Bulk upsert all collected campaigns
            self.bulk_upsert_campaigns()
            
            logger.success("✅ Facebook Ads campaigns ingestion completed successfully")
        
        except Exception as e:
            logger.error(f"❌ Facebook Ads campaigns ingestion failed: {e}")
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Parse command-line arguments (call_functions pattern)
    # Usage: python facebook_ads_campaigns_ingestion.py [date_preset]
    
    # Argument 1: date_preset (string)
    date_preset = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else 'last_90d'
    
    logger.info(f"Raw Arguments: {sys.argv}")
    logger.info(f"Starting Facebook Ads Campaigns Ingestion")
    logger.info(f"Arguments -> Date preset: {date_preset}")
    
    try:
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        db = PostgreSQLHelper(POSTGRES_URI)
        logger.info("Connected to PostgreSQL")
        
        job = FacebookAdsCampaignsIngestion(db, date_preset)
        job.run()
        
        db.close()
        logger.info("Closed PostgreSQL connection")
        
        logger.success("✅ Facebook Ads campaigns ingestion completed successfully")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"❌ Facebook Ads campaigns ingestion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
