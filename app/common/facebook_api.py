"""
Facebook API Utilities
Shared utilities for Facebook API interactions including rate limit checking and account management
"""

import time
import random
import json
from datetime import datetime, timezone
from typing import Dict, Optional, Any, List, Tuple
from loguru import logger


class FacebookRateLimitTracker:
    """
    Tracks and manages Facebook API rate limits with exponential backoff.
    
    Usage:
        tracker = FacebookRateLimitTracker()
        
        # In your API loop:
        for item in api_results:
            # Process item...
            
            # Check rate limit when headers change
            current_headers = api_results.headers()
            if tracker.should_check_limit(current_headers):
                tracker.check_limit(current_headers)
    """
    
    def __init__(self):
        self.time_await_limit = 0
        self.last_headers = None
    
    def should_check_limit(self, current_headers: Dict) -> bool:
        """
        Determine if rate limit should be checked based on header changes.
        
        Args:
            current_headers: Current API response headers
            
        Returns:
            True if headers have changed (indicating a new API call), False otherwise
        """
        if current_headers != self.last_headers:
            self.last_headers = current_headers
            return True
        return False
    
    def check_limit(self, headers: Dict) -> Dict[str, Any]:
        """
        Check Facebook API rate limit from response headers and sleep if necessary.
        
        Parses rate limit information from Facebook API headers:
        - x-business-use-case-usage: Business-level rate limits
        - x-app-usage: App-level rate limits  
        - x-fb-ads-insights-throttle: Insights API specific throttling
        
        Implements exponential backoff when rate limits are high (>=80%).
        
        Args:
            headers: HTTP response headers from Facebook API
            
        Returns:
            Dict containing rate limit snapshot with keys:
                - source: Which header provided the rate limit info
                - call_pct: API call percentage used
                - cpu_pct: CPU time percentage used
                - time_pct: Total time percentage used
                - insights_util: Insights-specific utilization percentages
                - tier: API access tier
                - eta_sec: Estimated seconds until rate limit resets
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
            
            # Helper to update snapshot with max values
            def update_max(key, val):
                if val is not None:
                    curr = usage_snapshot.get(key)
                    if curr is None or val > curr:
                        usage_snapshot[key] = val

            # 1) Business Use Case header
            if "x-business-use-case-usage" in h:
                usage_snapshot["source"] = "x-business-use-case-usage" # Default source priority
                raw = h["x-business-use-case-usage"]
                data = json.loads(raw)
                
                try:
                    first_key = next(iter(data.keys()))
                    entry = (data[first_key] or [{}])[0]
                except Exception:
                    entry = {}
                
                update_max("call_pct", float(entry.get("call_count", 0)))
                update_max("cpu_pct", float(entry.get("total_cputime", 0)))
                update_max("time_pct", float(entry.get("total_time", 0)))
                
                eta_min = entry.get("estimated_time_to_regain_access")
                if isinstance(eta_min, (int, float)) and eta_min > 0:
                    usage_snapshot["eta_sec"] = int(eta_min * 60)
            
            # 2) App-level header
            if "x-app-usage" in h:
                # Only set source if not set or usage is higher (simplified: just keep BUC as primary if exists, else App)
                if usage_snapshot["source"] is None:
                    usage_snapshot["source"] = "x-app-usage"
                
                entry = json.loads(h["x-app-usage"])
                update_max("call_pct", float(entry.get("call_count", 0)))
                update_max("cpu_pct", float(entry.get("total_cputime", 0)))
                update_max("time_pct", float(entry.get("total_time", 0)))

            # 3) Ad Account Level (Specific for Ad Account Rate Limits)
            if "x-ad-account-usage" in h:
                if usage_snapshot["source"] is None or usage_snapshot["source"] == "x-app-usage":
                     usage_snapshot["source"] = "x-ad-account-usage"

                entry = json.loads(h["x-ad-account-usage"])
                # x-ad-account-usage typically returns {'acc_id_util_pct': float}
                val = float(entry.get("acc_id_util_pct", 0))
                update_max("time_pct", val)
            
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
            
            # Log usage on every check for visibility
            logger.info(
                f"Rate Limit Status: usage={max_pct}% | source={usage_snapshot['source']} | "
                f"insights_util={usage_snapshot['insights_util']}"
            )

            if (usage_snapshot["eta_sec"] and usage_snapshot["eta_sec"] > 0) or (max_pct >= 80) or any(
                (isinstance(v, (int, float)) and v >= 80) for v in usage_snapshot["insights_util"].values() if v is not None
            ):
                if usage_snapshot["eta_sec"]:
                    wait_time = usage_snapshot["eta_sec"]
                else:
                    self.time_await_limit = self.time_await_limit + 1
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
    
    def reset(self):
        """Reset the rate limit tracker state."""
        self.time_await_limit = 0
        self.last_headers = None


def check_facebook_rate_limit(headers: Dict, rate_limit_counter: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Standalone function to check Facebook API rate limit (for backward compatibility).
    
    Note: For new code, prefer using FacebookRateLimitTracker class for better state management.
    
    Args:
        headers: HTTP response headers from Facebook API
        rate_limit_counter: Optional dict to track rate limit state across calls.
                          Should contain 'time_await_limit' key.
    
    Returns:
        Dict containing rate limit snapshot
    """
    if rate_limit_counter is None:
        rate_limit_counter = {'time_await_limit': 0}
    
    tracker = FacebookRateLimitTracker()
    tracker.time_await_limit = rate_limit_counter.get('time_await_limit', 0)
    
    result = tracker.check_limit(headers)
    
    rate_limit_counter['time_await_limit'] = tracker.time_await_limit
    
    return result


# ============================================================================
# Facebook Account Management
# ============================================================================

def get_active_facebook_accounts(db) -> List[Dict]:
    """
    Get all active Facebook Ad accounts from the database.
    
    Args:
        db: PostgreSQLHelper instance or any database connection with fetchall method
        
    Returns:
        List of dictionaries containing account information:
            - id: Account ID
            - name: Account name
            - ad_account_id: Facebook Ad Account ID
            - app_id: Facebook App ID
            - app_secret: Facebook App Secret
            - access_token: Facebook Access Token
            - access_token_expires: Token expiration datetime
            - script_type_to_run: Script type configuration
            - current_brand_id: Associated brand ID
            
    Example:
        from common.database import PostgreSQLHelper
        from common.facebook_api import get_active_facebook_accounts
        
        db = PostgreSQLHelper(connection_uri)
        accounts = get_active_facebook_accounts(db)
        for account in accounts:
            print(f"Account: {account['name']}")
    """
    query = """
        SELECT id, name, ad_account_id, app_id, app_secret, 
               access_token, access_token_expires, script_type_to_run, current_brand_id
        FROM core_facebook_ad_account_v1
        WHERE is_active = TRUE
    """
    rows = db.fetchall(query)
    
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


def init_facebook_api(account: Dict, api_version: str = 'v18.0') -> Tuple[Optional[Any], Optional[Any]]:
    """
    Initialize Facebook API for a given account.
    
    Args:
        account: Account dictionary from get_active_facebook_accounts()
        api_version: Facebook API version (default: 'v18.0')
        
    Returns:
        Tuple of (AdAccount, FacebookAdsApi) if successful, (None, None) if token expired
        
    Example:
        from facebook_business.adobjects.adaccount import AdAccount
        from common.facebook_api import get_active_facebook_accounts, init_facebook_api
        
        accounts = get_active_facebook_accounts(db)
        for account in accounts:
            ad_account, api = init_facebook_api(account)
            if ad_account:
                # Use ad_account to make API calls
                insights = ad_account.get_insights(...)
    """
    # Import here to avoid circular dependency
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    
    now = datetime.now(timezone.utc)  # Use timezone-aware datetime
    
    if (account.get('access_token_expires') and 
        account['access_token_expires'] > now):
        
        api = FacebookAdsApi.init(
            app_id=account['app_id'],
            app_secret=account['app_secret'],
            access_token=account['access_token'],
            api_version=api_version
        )
        
        ad_account = AdAccount("act_" + account['ad_account_id'])
        logger.info(f"Initialized API for account: {account['name']}")
        return ad_account, api
    else:
        logger.error(f"Token expired for account: {account['name']}")
        return None, None

