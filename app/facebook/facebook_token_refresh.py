"""
Facebook Token Refresher
-----------------------
Trao đổi Short-Lived Token lấy Long-Lived Token và cập nhật vào Database.
Strategy: Class-based Consistent Pattern
"""

import os
import sys
import requests
from datetime import datetime, timedelta
from loguru import logger
from common.database import PostgreSQLHelper

# ============================================================================
# CONFIGURATION
# ============================================================================

FB_GRAPH_URL = os.getenv('FB_GRAPH_URL', 'https://graph.facebook.com/v18.0')
POSTGRES_URI = os.getenv('POSTGRES_URI')


# ============================================================================
# FACEBOOK TOKEN REFRESHER
# ============================================================================

class FacebookTokenRefresher:
    """
    Refreshes Facebook Long-Lived Access Token and updates database.
    """
    
    def __init__(self, db: PostgreSQLHelper, ad_account_id: str, short_token: str):
        self.db = db
        self.ad_account_id = ad_account_id
        self.short_token = short_token
        self.app_id = None
        self.app_secret = None
        self.account_name = None

    def get_account_config(self):
        """Lấy App ID và App Secret từ database"""
        sql = """
            SELECT app_id, app_secret, name
            FROM public.core_facebook_ad_account
            WHERE ad_account_id = %s
        """
        result = self.db.fetchall(sql, (self.ad_account_id,))
        if not result:
            return False
            
        row = result[0] # fetchall returns list of tuples
        # Assuming fetchall returns tuples like (app_id, app_secret, name)
        # We need to map them correctly. PostgreSQLHelper usually returns tuples.
        self.app_id = row[0]
        self.app_secret = row[1]
        self.account_name = row[2]
        return True

    def exchange_token(self):
        """Gọi Facebook API để đổi token"""
        params = {
            'grant_type': 'fb_exchange_token',
            'client_id': self.app_id,
            'client_secret': self.app_secret,
            'fb_exchange_token': self.short_token
        }
        
        # Access token endpoint structure
        url = "https://graph.facebook.com/v18.0/oauth/access_token"
        
        logger.info(f"Exchanging token for App ID: {self.app_id}")
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get('access_token'), data.get('expires_in')
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error: {e}")
            logger.error(f"Response: {response.text}")
            raise
        except Exception as e:
            logger.error(f"Error exchanging token: {e}")
            raise

    def update_database(self, new_token, expires_in):
        """Cập nhật token mới vào database"""
        
        # Tính thời gian hết hạn
        if expires_in:
            expires_at = datetime.now() + timedelta(seconds=int(expires_in))
        else:
            # Nếu không có expires_in (token vĩnh viễn?), set xa tít
            expires_at = datetime(9999, 12, 31, 23, 59, 59)
        
        sql = """
            UPDATE public.core_facebook_ad_account
            SET access_token = %s,
                access_token_expires = %s,
                updated = NOW()
            WHERE ad_account_id = %s
        """
        
        try:
            self.db.execute(sql, (new_token, expires_at, self.ad_account_id))
            logger.success(f"Updated database for Account {self.ad_account_id}. Expires at: {expires_at}")
        except Exception as e:
            logger.error(f"Failed to update database: {e}")
            raise

    def run(self):
        """Main execution flow"""
        logger.info("="*80)
        logger.info(f"Starting Facebook Token Refresh for Account: {self.ad_account_id}")
        logger.info("="*80)
        
        try:
            # 1. Get Config
            logger.info("Fetching account configuration...")
            if not self.get_account_config():
                logger.error(f"Account ID {self.ad_account_id} not found in database.")
                sys.exit(1)
            
            logger.info(f"Found account: {self.account_name}")
            
            # 2. Exchange Token
            logger.info("Exchanging token...")
            long_token, expires_in = self.exchange_token()
            
            if not long_token:
                logger.error("Response did not contain access_token")
                sys.exit(1)
                
            # 3. Update DB
            logger.info("Updating database...")
            self.update_database(long_token, expires_in)
            
            logger.success("✅ Token refresh process completed successfully.")
            
        except Exception as e:
            logger.error(f"❌ Token refresh failed: {e}")
            sys.exit(1)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    if len(sys.argv) < 3:
        logger.error("Usage: python facebook_token_refresh.py <ad_account_id> <short_token>")
        sys.exit(1)
        
    ad_account_id = sys.argv[1]
    short_token = sys.argv[2]
    
    logger.info(f"Starting Facebook Token Refresh")
    logger.info(f"Arguments -> Account ID: {ad_account_id}, Token: ***")
    
    if not POSTGRES_URI:
        logger.error("POSTGRES_URI environment variable is not set")
        sys.exit(1)
        
    db = PostgreSQLHelper(POSTGRES_URI)
    
    app = FacebookTokenRefresher(db, ad_account_id, short_token)
    app.run()
