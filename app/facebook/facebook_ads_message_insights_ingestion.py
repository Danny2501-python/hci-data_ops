"""
Facebook Ads Message Insights Ingestion
Migrated from etl_marketing Django app
Strategy: Bulk DELETE + COPY (CSV) for maximum performance
"""

import os
import sys
import time
import csv
import json
import tempfile
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
    Strategy: Bulk DELETE + COPY from CSV
    """
    
    def __init__(self, db: PostgreSQLHelper, days_for_ago: int = 1):
        self.db = db
        self.days_for_ago = days_for_ago
        self.rate_limit_tracker = FacebookRateLimitTracker()
        self.message_insights_data = []  # Collect all message insights
    
    def fetch_message_insights(self, account: Dict):
        """Fetch message insights from Facebook API and collect to list"""
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
            # NO action_breakdowns - not in original code
        }
        
        # Fetch same fields as original code (even if not all saved to DB)
        fields = [
            AdsInsights.Field.account_id,      # Fetched but not saved (not in DB schema)
            AdsInsights.Field.campaign_id,     # Fetched but not saved (not in DB schema)
            AdsInsights.Field.adset_id,        # Fetched but not saved (not in DB schema)
            AdsInsights.Field.ad_id,
            AdsInsights.Field.spend,
            AdsInsights.Field.date_start,
            AdsInsights.Field.date_stop,       # Fetched but not saved (not in DB schema)
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
        
        try:
            for insight in insights:
                try:
                    data = insight._json
                    
                    # NO FILTERING - Save all insights with unique_actions (same as original)
                    # Collect message insight data
                    self.message_insights_data.append({
                        'date': data.get('date_start'),
                        'fb_account_currency': data.get('account_currency'),
                        'fb_ad_ad_id': data.get('ad_id'),
                        'spend': float(data.get('spend', 0)),
                        'region': data.get('region', ''),
                        'brand_id': brand_id,
                        'unique_actions': json.dumps(data.get('unique_actions', []))
                    })
                    count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing message insight: {e}")
                    continue
                
                try:
                    current_headers = insights.headers()
                    if self.rate_limit_tracker.should_check_limit(current_headers):
                        logger.debug(f"Headers changed, checking rate limit (processed {count} message insights so far)")
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
                logger.warning(f"⚠️ Rate limit hit while iterating (after {count} message insights)")
                logger.warning(f"Iterator exception: {e}")
            else:
                logger.error(f"Error during iteration: {e}")
                raise
        
        logger.success(f"✅ Collected {count} message insights for account: {account['name']}")
    
    def bulk_upsert_message_insights(self):
        """Bulk upsert message insights using DELETE + COPY (composite key)"""
        if not self.message_insights_data:
            logger.warning("No message insights to upsert")
            return
        
        logger.info(f"Starting bulk upsert for {len(self.message_insights_data)} message insights...")
        
        csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv', encoding='utf-8')
        
        try:
            writer = csv.DictWriter(csv_file, fieldnames=[
                'date', 'fb_account_currency', 'fb_ad_ad_id', 'spend', 
                'region', 'brand_id', 'unique_actions'
            ])
            
            for row in self.message_insights_data:
                # Convert None to empty string for CSV
                csv_row = {k: (v if v is not None else '') for k, v in row.items()}
                writer.writerow(csv_row)
            
            csv_file.close()
            logger.info(f"✅ Wrote {len(self.message_insights_data)} message insights to CSV: {csv_file.name}")
            
            # DELETE existing (composite key: fb_ad_ad_id, date, region)
            ad_ids = [i['fb_ad_ad_id'] for i in self.message_insights_data]
            dates = [i['date'] for i in self.message_insights_data]
            regions = [i['region'] for i in self.message_insights_data]
            
            delete_query = """
                DELETE FROM core_facebook_ad_ad_insight_message 
                WHERE (fb_ad_ad_id, date, region) IN (
                    SELECT unnest(%s::varchar[]), 
                           unnest(%s::date[]), 
                           unnest(%s::varchar[])
                )
            """
            self.db.execute(delete_query, (ad_ids, dates, regions))
            logger.info(f"✅ Deleted existing message insights")
            
            # COPY from CSV
            copy_query = f"""
                COPY core_facebook_ad_ad_insight_message (
                    date, fb_account_currency, fb_ad_ad_id, spend, 
                    region, brand_id, unique_actions
                )
                FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')
            """
            
            with open(csv_file.name, 'r', encoding='utf-8') as f:
                cursor = self.db.conn.cursor()
                cursor.copy_expert(copy_query, f)
                self.db.conn.commit()
                cursor.close()
            
            logger.success(f"✅ Bulk inserted {len(self.message_insights_data)} message insights via COPY")
            
        finally:
            if os.path.exists(csv_file.name):
                os.unlink(csv_file.name)
    
    def run(self):
        """Execute Facebook Ads message insights ingestion"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads Message Insights Ingestion (Bulk COPY)")
        logger.info(f"Days for ago: {self.days_for_ago}")
        logger.info("="*80)
        
        try:
            accounts = get_active_facebook_accounts(self.db)
            logger.info(f"Found {len(accounts)} active Facebook Ad accounts")
            
            for account in accounts:
                try:
                    time.sleep(1)
                    logger.info(f"Fetching account: {account['name']} ({account['ad_account_id']})")
                    self.fetch_message_insights(account)
                except Exception as e:
                    logger.error(f"❌ Failed to fetch account {account['name']}: {e}")
                    continue
            
            self.bulk_upsert_message_insights()
            logger.success("✅ Facebook Ads message insights ingestion completed successfully")
        
        except Exception as e:
            logger.error(f"❌ Facebook Ads message insights ingestion failed: {e}")
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
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
