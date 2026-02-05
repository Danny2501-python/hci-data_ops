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
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from loguru import logger

# Facebook Business SDK
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

# ============================================================================
# CONFIGURATION
# ============================================================================

POSTGRES_URI = os.getenv('POSTGRES_URI')
FB_API_VERSION = os.getenv('FB_API_VERSION', 'v18.0')


# ============================================================================
# DATABASE HELPER
# ============================================================================

class PostgreSQLHelper:
    """Helper class for PostgreSQL operations"""
    
    def __init__(self, connection_uri: str):
        self.conn = psycopg2.connect(
            connection_uri,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        self.conn.autocommit = False
    
    def execute(self, query: str, params: tuple = None):
        """Execute a query"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            self.conn.commit()
            return cursor
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cursor.close()
    
    def fetchone(self, query: str, params: tuple = None):
        """Fetch one result"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchone()
        finally:
            cursor.close()
    
    def fetchall(self, query: str, params: tuple = None):
        """Fetch all results"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()


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
        self.time_await_limit = 0
    
    def get_active_accounts(self) -> List[Dict]:
        """Get all active Facebook Ad accounts"""
        query = """
            SELECT id, name, ad_account_id, app_id, app_secret, 
                   access_token, access_token_expires, script_type_to_run, current_brand_id
            FROM core_facebook_ad_account_v1
            WHERE is_active = TRUE
        """
        rows = self.db.fetchall(query)
        
        accounts = []
        for row in rows:
            accounts.append({
                'id': row[0],
                'name': row[1],
                'ad_account_id': row[2],
                'app_id': row[3],
                'app_secret': row[4],
                'access_token': row[5],
                'access_token_expires': row[6],
                'script_type_to_run': row[7],
                'current_brand_id': row[8],
            })
        
        return accounts
    
    def init_api(self, account: Dict) -> Optional[AdAccount]:
        """Initialize Facebook API"""
        from datetime import timezone
        now = datetime.now(timezone.utc)  # Use timezone-aware datetime
        
        if (account.get('access_token_expires') and 
            account['access_token_expires'] > now):
            
            api = FacebookAdsApi.init(
                app_id=account['app_id'],
                app_secret=account['app_secret'],
                access_token=account['access_token'],
                api_version=FB_API_VERSION
            )
            
            ad_account = AdAccount("act_" + account['ad_account_id'])
            logger.info(f"Initialized API for account: {account['name']}")
            return ad_account, api
        else:
            logger.error(f"Token expired for account: {account['name']}")
            return None, None
    
    def check_limit(self, headers):
        """
        Check Facebook rate limit
        EXACT COPY from original fb_business.py
        """
        def _lower_keys(d):
            return {k.lower(): v for k, v in d.items()}
        
        try:
            h = _lower_keys(headers or {})
            usage_snapshot = {
                "source": None,
                "call_pct": None,
                "cpu_pct": None,
                "time_pct": None,
                "insights_util": {},
                "tier": None,
                "eta_sec": None,
            }
            
            # 1) Business Use Case header
            if "x-business-use-case-usage" in h:
                usage_snapshot["source"] = "x-business-use-case-usage"
                raw = h["x-business-use-case-usage"]
                data = json.loads(raw)
                
                try:
                    first_key = next(iter(data.keys()))
                    entry = (data[first_key] or [{}])[0]
                except Exception:
                    entry = {}
                
                call = float(entry.get("call_count", 0))
                cpu = float(entry.get("total_cputime", 0))
                total = float(entry.get("total_time", 0))
                usage_snapshot.update({"call_pct": call, "cpu_pct": cpu, "time_pct": total})
                
                eta_min = entry.get("estimated_time_to_regain_access")
                if isinstance(eta_min, (int, float)) and eta_min > 0:
                    usage_snapshot["eta_sec"] = int(eta_min * 60)
            
            # 2) App-level header
            elif "x-app-usage" in h:
                usage_snapshot["source"] = "x-app-usage"
                entry = json.loads(h["x-app-usage"])
                usage_snapshot.update({
                    "call_pct": float(entry.get("call_count", 0)),
                    "cpu_pct": float(entry.get("total_cputime", 0)),
                    "time_pct": float(entry.get("total_time", 0)),
                })
            
            # 3) Insights throttle
            if "x-fb-ads-insights-throttle" in h:
                thr = json.loads(h["x-fb-ads-insights-throttle"])
                usage_snapshot["insights_util"] = {
                    "app_id_util_pct": thr.get("app_id_util_pct"),
                    "acc_id_util_pct": thr.get("acc_id_util_pct"),
                    "business_id_util_pct": thr.get("business_id_util_pct"),
                }
                usage_snapshot["tier"] = thr.get("ads_api_access_tier")
                
                if usage_snapshot["eta_sec"] is None:
                    candidates = [
                        v for v in [
                            usage_snapshot.get("call_pct"),
                            usage_snapshot.get("cpu_pct"),
                            usage_snapshot.get("time_pct"),
                            thr.get("app_id_util_pct"),
                            thr.get("acc_id_util_pct"),
                            thr.get("business_id_util_pct"),
                        ] if isinstance(v, (int, float))
                    ]
                    max_util = max(candidates) if candidates else 0
                    
                    if max_util >= 90:
                        usage_snapshot["eta_sec"] = random.randint(240, 480)
                    elif max_util >= 80:
                        usage_snapshot["eta_sec"] = random.randint(60, 120)
                    elif max_util >= 70:
                        usage_snapshot["eta_sec"] = random.randint(20, 60)
            
            # Decide backoff
            max_pct = max([
                p for p in (usage_snapshot["call_pct"], usage_snapshot["cpu_pct"], usage_snapshot["time_pct"])
                if isinstance(p, (int, float))
            ], default=0)
            
            if (usage_snapshot["eta_sec"] and usage_snapshot["eta_sec"] > 0) or (max_pct >= 80) or any(
                (isinstance(v, (int, float)) and v >= 80) for v in usage_snapshot["insights_util"].values() if v is not None
            ):
                if usage_snapshot["eta_sec"]:
                    wait_time = usage_snapshot["eta_sec"]
                else:
                    self.time_await_limit = getattr(self, "time_await_limit", 0) + 1
                    wait_time = min(300, int(5 * (2 ** self.time_await_limit)))
                
                wait_time = int(wait_time * (1 + random.random() * 0.2))
                
                logger.warning(
                    f"Rate limit high. Cooling {wait_time}s | "
                    f"source={usage_snapshot['source']} | usage={max_pct}% | "
                    f"insights_util={usage_snapshot['insights_util']} | tier={usage_snapshot['tier']}"
                )
                time.sleep(wait_time)
            
            return usage_snapshot
        
        except Exception as e:
            logger.error(f"Error checking rate limit: {e}")
            return {"source": None, "call_pct": 0, "cpu_pct": 0, "time_pct": 0, "insights_util": {}, "tier": None, "eta_sec": None}
    
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
        ad_account, api = self.init_api(account)
        
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
        
        self.time_await_limit = 0
        count = 0
        last_headers = None
        
        for insight in insights:
            # Break if too many rate limit waits
            if self.time_await_limit >= 5:
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
                if current_headers != last_headers:
                    logger.debug(f"Headers changed, checking rate limit (processed {count} insights so far)")
                    self.check_limit(current_headers)
                    last_headers = current_headers
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
            # Get active accounts
            accounts = self.get_active_accounts()
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
