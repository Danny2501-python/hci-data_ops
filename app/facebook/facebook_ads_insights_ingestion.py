"""
Facebook Ads Insights Ingestion
Migrated from etl_marketing Django app
Strategy: Bulk DELETE + COPY (CSV) for maximum performance
"""

import os
import sys
import time
import csv
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List
from loguru import logger

# Facebook Business SDK
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from common.utils import get_current_timestamp_naive

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
# FACEBOOK ADS INSIGHTS INGESTION
# ============================================================================

class FacebookAdsInsightsIngestion:
    """
    Facebook Ads Insights Ingestion
    Strategy: Bulk DELETE + COPY from CSV
    """
    
    def __init__(self, db: PostgreSQLHelper, days_for_ago: int = 1):
        self.db = db
        self.days_for_ago = days_for_ago
        self.account_id = None
        self.rate_limit_tracker = FacebookRateLimitTracker()
        self.insights_data = []  # Collect all insights
        self.processed_ids = {
            'campaigns': set(),
            'adsets': set(),
            'ads': set(),
            'creatives': set()
        }
    
    def fetch_insights(self, account: Dict):
        """Fetch insights from Facebook API and collect to list"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        
        if not ad_account:
            return
        
        now = get_current_timestamp_naive()
        this_day = now - timedelta(days=self.days_for_ago)
        this_day_str = this_day.strftime("%Y-%m-%d")
        
        params = {
            'time_range': {"since": this_day_str, "until": this_day_str},
            'level': 'ad',
            'breakdowns': ['region']
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
            AdsInsights.Field.account_currency
        ]
        
        try:
            insights = ad_account.get_insights(params=params, fields=fields)
        except Exception as e:
            logger.error(f"Failed to get insights: {e}")
            return
        
        self.rate_limit_tracker.reset()
        count = 0
        brand_id = account.get('current_brand_id')
        
        # Dependency Sets
        campaign_ids = set()
        adset_ids = set()
        ad_ids = set()
        
        try:
            for insight in insights:
                try:
                    data = insight._json
                    
                    # Collect Dependencies
                    if data.get('campaign_id'):
                        campaign_ids.add(data.get('campaign_id'))
                    if data.get('adset_id'):
                        adset_ids.add(data.get('adset_id'))
                    if data.get('ad_id'):
                        ad_ids.add(data.get('ad_id'))
                    
                    # Collect insight data
                    self.insights_data.append({
                        'date': data.get('date_start'),
                        'fb_account_currency': data.get('account_currency'),
                        'fb_ad_ad_id': data.get('ad_id'),
                        'spend': float(data.get('spend', 0)),
                        'region': data.get('region', 'unknown'),
                        'brand_id': brand_id,
                        'etl_updated': get_current_timestamp_naive()
                    })
                    count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing insight: {e}")
                    continue
                
                try:
                    current_headers = insights.headers()
                    if self.rate_limit_tracker.should_check_limit(current_headers):
                        logger.debug(f"Headers changed, checking rate limit (processed {count} insights so far)")
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
                'error_subcode": 2446079',
                'error_subcode": 1504044'  # Service temporarily unavailable
            ]):
                logger.warning(f"⚠️ FB API issue while iterating (after {count} insights): {e}")
            else:
                logger.error(f"Error during iteration (partial data will be saved): {e}")
        
        logger.success(f"✅ Collected {count} insights for account: {account['name']}")
        
        # Process Dependencies (Campaigns, AdSets, Ads)
        logger.info(f"Processing dependencies: {len(campaign_ids)} Campaigns, {len(adset_ids)} AdSets, {len(ad_ids)} Ads")
        self.process_dependencies(api, campaign_ids, adset_ids, ad_ids, brand_id)

    def process_dependencies(self, api, campaign_ids, adset_ids, ad_ids, brand_id):
        """Fetch and bulk upsert dependencies (Campaign, AdSet, Ad, Creative)"""
        
        # 1. Campaigns
        if campaign_ids:
            logger.info(f"Fetching {len(campaign_ids)} Campaigns...")
            campaigns_data = self.fetch_campaigns_data(api, campaign_ids, brand_id)
            self.bulk_insert_data(
                'core_facebook_ad_campaign', 
                'fb_ad_campaign_id', 
                campaigns_data,
                ['name', 'fb_ad_campaign_id', 'created_time', 'updated_time', 'start_time', 'stop_time', 'status', 'fb_ad_account_id', 'etl_updated', 'company_id']
            )

        # 2. AdSets
        if adset_ids:
            logger.info(f"Fetching {len(adset_ids)} AdSets...")
            adsets_data = self.fetch_adsets_data(api, adset_ids, brand_id)
            self.bulk_insert_data(
                'core_facebook_ad_set',
                'fb_ad_set_id',
                adsets_data,
                ['name', 'fb_ad_set_id', 'created_time', 'updated_time', 'start_time', 'status', 'fb_ad_campaign_id', 'etl_updated']
            )

        # 3. Ads (and collect Creative IDs)
        creative_ids = set()
        if ad_ids:
            logger.info(f"Fetching {len(ad_ids)} Ads...")
            ads_data = self.fetch_ads_data(api, ad_ids, creative_ids, brand_id)
            self.bulk_insert_data(
                'core_facebook_ad_ad',
                'fb_ad_ad_id',
                ads_data,
                ['name', 'fb_ad_ad_id', 'fb_ad_set_id', 'fb_creative_id', 'created_time', 'updated_time', 'status', 'brand_id', 'etl_updated']
            )

        # 4. Creatives
        if creative_ids:
            logger.info(f"Fetching {len(creative_ids)} Creatives...")
            creatives_data = self.fetch_creatives_data(api, creative_ids, brand_id)
            self.bulk_insert_data(
                'core_facebook_ad_creative',
                'fb_ad_creative_id',
                creatives_data,
                ['name', 'fb_ad_creative_id', 'fb_object_id', 'fb_object_story_id', 'fb_effective_object_story_id', 'fb_link_url', 'status', 'fb_ad_account_id', 'brand_id', 'etl_updated']
            )

    def _check_limit(self, headers):
        """Check rate limit using headers and sleep if necessary"""
        logger.debug("Checking rate limit...")
        try:
            if self.rate_limit_tracker.should_check_limit(headers):
                self.rate_limit_tracker.check_limit(headers)
                if self.rate_limit_tracker.time_await_limit >= 5:
                    logger.warning("🛑 5x Rate Limit Hit. Sleeping 300s...")
                    time.sleep(300)
        except Exception as e:
            logger.error(f"Error checking limit: {e}")

    def fetch_campaigns_data(self, api, ids, brand_id):
        """Fetch Campaign details"""
        data_list = []
        for i, cid in enumerate(ids):
            time.sleep(0.05) # Rate limit: ~20 req/s
            try:
                obj = Campaign(cid, api=api)
                fields = [
                    Campaign.Field.name,
                    Campaign.Field.account_id,
                    Campaign.Field.created_time,
                    Campaign.Field.updated_time,
                    Campaign.Field.start_time,
                    Campaign.Field.stop_time,
                    Campaign.Field.status
                ]
                d = obj.api_get(fields=fields)._json
                if hasattr(obj, 'headers'):
                    self._check_limit(obj.headers())
                
                data_list.append({
                    'id': d.get('id'), # Still needed for business logic if any, but not for COPY
                    'name': d.get('name'),
                    'fb_ad_campaign_id': d.get('id'),
                    'created_time': d.get('created_time'),
                    'updated_time': d.get('updated_time'),
                    'start_time': d.get('start_time'),
                    'stop_time': d.get('stop_time'),
                    'status': d.get('status'),
                    'fb_ad_account_id': d.get('account_id'),
                    'etl_updated': get_current_timestamp_naive(),
                    'company_id': brand_id 
                })
            except Exception as e:
                logger.error(f"Failed to fetch Campaign {cid}: {e}")
        return data_list

    def fetch_adsets_data(self, api, ids, brand_id):
        """Fetch AdSet details"""
        data_list = []
        for i, aid in enumerate(ids):
            time.sleep(0.05) # Rate limit: ~20 req/s
            try:
                obj = AdSet(aid, api=api)
                fields = [
                    AdSet.Field.name,
                    AdSet.Field.campaign_id,
                    AdSet.Field.created_time,
                    AdSet.Field.updated_time,
                    AdSet.Field.start_time,
                    AdSet.Field.status
                ]
                d = obj.api_get(fields=fields)._json
                if hasattr(obj, 'headers'):
                    self._check_limit(obj.headers())
                
                data_list.append({
                    'id': d.get('id'),
                    'name': d.get('name'),
                    'fb_ad_set_id': d.get('id'),
                    'created_time': d.get('created_time'),
                    'updated_time': d.get('updated_time'),
                    'start_time': d.get('start_time'),
                    'status': d.get('status'),
                    'fb_ad_campaign_id': d.get('campaign_id'),
                    'etl_updated': get_current_timestamp_naive()
                })
            except Exception as e:
                logger.error(f"Failed to fetch AdSet {aid}: {e}")
        return data_list

    def fetch_ads_data(self, api, ids, creative_ids_out, brand_id):
        """Fetch Ad details and collect Creative IDs"""
        data_list = []
        for i, aid in enumerate(ids):
            time.sleep(0.05) # Rate limit: ~20 req/s
            try:
                obj = Ad(aid, api=api)
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
                d = obj.api_get(fields=fields)._json
                if hasattr(obj, 'headers'):
                    self._check_limit(obj.headers())
                
                # Extract Creative ID
                creative = d.get('creative', {})
                cid = creative.get('id') if isinstance(creative, dict) else creative
                if cid:
                    creative_ids_out.add(cid)
                
                data_list.append({
                    'id': d.get('id'),
                    'name': d.get('name'),
                    'fb_ad_ad_id': d.get('id'),
                    'fb_ad_set_id': d.get('adset_id'),
                    'fb_creative_id': cid,
                    'created_time': d.get('created_time'),
                    'updated_time': d.get('updated_time'),
                    'status': d.get('status'),
                    'brand_id': brand_id,
                    'etl_updated': get_current_timestamp_naive()
                })
            except Exception as e:
                logger.error(f"Failed to fetch Ad {aid}: {e}")
        return data_list

    def fetch_creatives_data(self, api, ids, brand_id):
        """Fetch Creative details"""
        data_list = []
        for i, cid in enumerate(ids):
            time.sleep(0.05) # Rate limit: ~20 req/s
            try:
                obj = AdCreative(cid, api=api)
                fields = [
                    AdCreative.Field.name,
                    AdCreative.Field.account_id,
                    AdCreative.Field.object_id,
                    AdCreative.Field.object_story_id,
                    AdCreative.Field.effective_object_story_id,
                    AdCreative.Field.link_url, 
                    AdCreative.Field.status,
                    
                ]
                d = obj.api_get(fields=fields)._json
                if hasattr(obj, 'headers'):
                    self._check_limit(obj.headers())
                
                data_list.append({
                    'id': d.get('id'),
                    'name': d.get('name'),
                    'fb_ad_creative_id': d.get('id'),
                    'fb_object_id': d.get('object_id'),
                    'fb_object_story_id': d.get('object_story_id'),
                    'fb_effective_object_story_id': d.get('effective_object_story_id'),
                    'fb_link_url': d.get('link_url'),
                    'status': d.get('status'),
                    'fb_ad_account_id': d.get('account_id'),
                    'brand_id': brand_id,
                    'etl_updated': get_current_timestamp_naive()
                })
            except Exception as e:
                logger.error(f"Failed to fetch Creative {cid}: {e}")
        return data_list

    def bulk_insert_data(self, table_name, pk_col, data, columns):
        """Generic Bulk DELETE + COPY"""
        if not data:
            return

        logger.info(f"Bulk inserting {len(data)} records into {table_name}")
        
        # 1. DELETE existing
        ids = [str(item[pk_col]) for item in data]
        if ids:
            # Note: pk_col (e.g., fb_ad_campaign_id) is varchar in DB, so we cast input to varchar
            delete_sql = f"DELETE FROM {table_name} WHERE {pk_col} IN (SELECT unnest(%s::varchar[]))"
            try:
                self.db.execute(delete_sql, (ids,))
                logger.info(f"Deleted existing records in {table_name}")
            except Exception as e:
                logger.error(f"Failed to delete from {table_name}: {e}")
                # Continue to try insert? risky.
                raise e

        # 2. COPY new data
        csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv', encoding='utf-8')
        try:
            writer = csv.DictWriter(csv_file, fieldnames=columns)
            for row in data:
                # Filter row to only have columns we want
                filtered_row = {k: (row.get(k) if row.get(k) is not None else '') for k in columns}
                writer.writerow(filtered_row)
            
            csv_file.close()
            
            columns_str = ", ".join(columns)
            copy_sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')"
            
            with open(csv_file.name, 'r', encoding='utf-8') as f:
                cursor = self.db.conn.cursor()
                cursor.copy_expert(copy_sql, f)
                self.db.conn.commit()
                cursor.close()
            
            logger.success(f"✅ Upserted {len(data)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to copy to {table_name}: {e}")
            raise e
        finally:
            if os.path.exists(csv_file.name):
                os.unlink(csv_file.name)
    
    def bulk_upsert_insights(self):
        """Bulk upsert insights using DELETE + COPY (composite key)"""
        if not self.insights_data:
            logger.warning("No insights to upsert")
            return
        
        logger.info(f"Starting bulk upsert for {len(self.insights_data)} insights...")
        
        csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv', encoding='utf-8')
        
        try:
            writer = csv.DictWriter(csv_file, fieldnames=[
                'date', 'fb_account_currency', 'fb_ad_ad_id', 'spend', 'region', 'brand_id', 'etl_updated'
            ])
            
            for row in self.insights_data:
                # Convert None to empty string for CSV
                csv_row = {k: (v if v is not None else '') for k, v in row.items()}
                writer.writerow(csv_row)
            
            csv_file.close()
            logger.info(f"✅ Wrote {len(self.insights_data)} insights to CSV: {csv_file.name}")
            
            # DELETE existing (composite key: fb_ad_ad_id, date, region)
            # Build arrays for composite key deletion
            ad_ids = [i['fb_ad_ad_id'] for i in self.insights_data]
            dates = [i['date'] for i in self.insights_data]
            regions = [i['region'] for i in self.insights_data]
            
            delete_query = """
                DELETE FROM core_facebook_ad_ad_insight 
                WHERE (fb_ad_ad_id, date, region) IN (
                    SELECT unnest(%s::varchar[]), 
                           unnest(%s::date[]), 
                           unnest(%s::varchar[])
                )
            """
            self.db.execute(delete_query, (ad_ids, dates, regions))
            logger.info(f"✅ Deleted existing insights")
            
            # COPY from CSV
            copy_query = f"""
                COPY core_facebook_ad_ad_insight (
                    date, fb_account_currency, fb_ad_ad_id, spend, region, brand_id, etl_updated
                )
                FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')
            """
            
            with open(csv_file.name, 'r', encoding='utf-8') as f:
                cursor = self.db.conn.cursor()
                cursor.copy_expert(copy_query, f)
                self.db.conn.commit()
                cursor.close()
            
            logger.success(f"✅ Bulk inserted {len(self.insights_data)} insights via COPY")
            
        finally:
            if os.path.exists(csv_file.name):
                os.unlink(csv_file.name)
    
    def fetch_insights_by_campaign(self, account: Dict):
        """Fetch insights for each campaign in the account (to avoid rate limits)"""
        ad_account, api = init_facebook_api(account, FB_API_VERSION)
        if not ad_account:
            return
        
        now = get_current_timestamp_naive()
        this_day = now - timedelta(days=self.days_for_ago)
        this_day_str = this_day.strftime("%Y-%m-%d")
        brand_id = account.get('current_brand_id')
        
        # Reset processed IDs for this account's persistent run
        self.processed_ids = {'campaigns': set(), 'adsets': set(), 'ads': set(), 'creatives': set()}
        
        logger.info(f"Fetching campaigns for account: {account['name']}")
        try:
            campaigns = ad_account.get_campaigns(
                fields=[Campaign.Field.id, Campaign.Field.name],
                params={'effective_status': ['ACTIVE', 'PAUSED', 'WITH_ISSUES']}
            )
        except Exception as e:
            logger.error(f"Failed to fetch campaigns for account {account['name']}: {e}")
            return

        for campaign in campaigns:
            campaign_id = campaign['id']
            batch_campaign_ids = {campaign_id}
            batch_adset_ids = set()
            batch_ad_ids = set()
            
            logger.info(f"Campaign: {campaign.get('name')} ({campaign_id})")
            
            params = {
                'time_range': {"since": this_day_str, "until": this_day_str},
                'level': 'ad',
                'breakdowns': ['region']
            }
            fields = [AdsInsights.Field.campaign_id, AdsInsights.Field.adset_id, AdsInsights.Field.ad_id, AdsInsights.Field.spend, AdsInsights.Field.date_start, AdsInsights.Field.account_currency]
            
            try:
                campaign_obj = Campaign(campaign_id, api=api)
                insights = campaign_obj.get_insights(params=params, fields=fields)
                
                for insight in insights:
                    data = insight._json
                    aid = data.get('ad_id')
                    sid = data.get('adset_id')
                    if sid: batch_adset_ids.add(sid)
                    if aid: batch_ad_ids.add(aid)
                    
                    self.insights_data.append({
                        'date': data.get('date_start'),
                        'fb_account_currency': data.get('account_currency'),
                        'fb_ad_ad_id': aid,
                        'spend': float(data.get('spend', 0)),
                        'region': data.get('region', 'unknown'),
                        'brand_id': brand_id,
                        'etl_updated': get_current_timestamp_naive()
                    })
                    if self.rate_limit_tracker.should_check_limit(insights.headers()):
                        self.rate_limit_tracker.check_limit(insights.headers())
                
                if self.insights_data:
                    self.process_dependencies_optimized(api, batch_campaign_ids, batch_adset_ids, batch_ad_ids, brand_id, self.processed_ids)
                    self.bulk_upsert_insights()
                    self.insights_data = [] # Clear memory after each campaign
                
            except Exception as e:
                logger.error(f"Error for campaign {campaign_id}: {e}")
                if self.insights_data:
                    self.process_dependencies_optimized(api, batch_campaign_ids, batch_adset_ids, batch_ad_ids, brand_id, self.processed_ids)
                    self.bulk_upsert_insights()
                    self.insights_data = []
                continue

    def process_dependencies_optimized(self, api, campaign_ids, adset_ids, ad_ids, brand_id, processed_ids):
        """Helper to fetch and save dependencies with filtering"""
        new_cids = campaign_ids - processed_ids['campaigns']
        if new_cids:
            data = self.fetch_campaigns_data(api, new_cids, brand_id)
            self.bulk_insert_data('core_facebook_ad_campaign', 'fb_ad_campaign_id', data, ['name', 'fb_ad_campaign_id', 'created_time', 'updated_time', 'start_time', 'stop_time', 'status', 'fb_ad_account_id', 'etl_updated', 'company_id'])
            processed_ids['campaigns'].update(new_cids)

        new_sids = adset_ids - processed_ids['adsets']
        if new_sids:
            data = self.fetch_adsets_data(api, new_sids, brand_id)
            self.bulk_insert_data('core_facebook_ad_set', 'fb_ad_set_id', data, ['name', 'fb_ad_set_id', 'created_time', 'updated_time', 'start_time', 'status', 'fb_ad_campaign_id', 'etl_updated'])
            processed_ids['adsets'].update(new_sids)

        new_aids = ad_ids - processed_ids['ads']
        if new_aids:
            found_creative_ids = set()
            data = self.fetch_ads_data(api, new_aids, found_creative_ids, brand_id)
            self.bulk_insert_data('core_facebook_ad_ad', 'fb_ad_ad_id', data, ['name', 'fb_ad_ad_id', 'fb_ad_set_id', 'fb_creative_id', 'created_time', 'updated_time', 'status', 'brand_id', 'etl_updated'])
            processed_ids['ads'].update(new_aids)
            
            new_crids = found_creative_ids - processed_ids['creatives']
            if new_crids:
                data = self.fetch_creatives_data(api, new_crids, brand_id)
                self.bulk_insert_data('core_facebook_ad_creative', 'fb_ad_creative_id', data, ['name', 'fb_ad_creative_id', 'fb_object_id', 'fb_object_story_id', 'fb_effective_object_story_id', 'fb_link_url', 'status', 'fb_ad_account_id', 'brand_id', 'etl_updated'])
                processed_ids['creatives'].update(new_crids)

    def run(self):
        """Execute Ingestion with dynamic strategy based on account settings"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Ads Insights Ingestion | Days: {self.days_for_ago}")
        logger.info("="*80)
        
        try:
            accounts = get_active_facebook_accounts(self.db)
            if self.account_id:
                accounts = [a for a in accounts if str(a.get('ad_account_id')) == str(self.account_id)]

            for account in accounts:
                try:
                    time.sleep(1)
                    strategy = account.get('script_type_to_run', 'account')
                    logger.info(f">>> Processing: {account['name']} | Strategy: {strategy}")
                    
                    if strategy == 'campaign':
                        self.fetch_insights_by_campaign(account)
                    else:
                        # Clear memory for account-level processing
                        self.insights_data = []
                        self.fetch_insights(account)
                        # Save at the end of account-level fetch
                        self.bulk_upsert_insights()
                        self.insights_data = []
                        
                except Exception as e:
                    logger.error(f"❌ Failed account {account['name']}: {e}")
                    self.insights_data = []
                    continue
                    
            logger.success("✅ Facebook Ads insights ingestion completed successfully")
        except Exception as e:
            logger.error(f"❌ Ingestion failed: {e}")
            raise


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    days_for_ago = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 1
    # Check for optional account_id argument
    account_id = str(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] and sys.argv[2] != 'None' else None
    
    logger.info(f"Raw Arguments: {sys.argv}")
    logger.info(f"Starting Facebook Ads Insights Ingestion")
    logger.info(f"Arguments -> Days for ago: {days_for_ago}")
    if account_id:
        logger.info(f"Arguments -> Account ID: {account_id}")
    
    try:
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        db = PostgreSQLHelper(POSTGRES_URI)
        logger.info("Connected to PostgreSQL")
        
        job = FacebookAdsInsightsIngestion(db, days_for_ago)
        job.account_id = account_id 
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
