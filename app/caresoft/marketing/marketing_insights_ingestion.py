"""
CareSoft Marketing Insights Ingestion
API -> PostgreSQL (Direct)
"""

import os
import sys
import time
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional

import psycopg2
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from common.brand_config import get_active_brands
from common.caresoft_api import call_caresoft_api_post
from common.database import PostgreSQLHelper
from common.utils import bulk_load

POSTGRES_URI = os.getenv('POSTGRES_URI')
TARGET_TABLE = 'public.cs_facebook_insight'

class MarketingInsightsIngestion:
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        logger.info(f"Initialized MarketingInsightsIngestion for {self.snapshot_date}")

    def fetch_insights(self, brand: dict) -> List[Dict[str, Any]]:
        """
        Fetch marketing insights from CareSoft API
        """
        endpoint = "marketing/insights"
        date_str = self.snapshot_date.strftime("%d-%m-%Y")
        
        # Request body based on user requirements
        payload = {
            "since": date_str,
            "until": date_str,
            "breakdowns": [
                "facebook_page_id", "page_name", "ad_account_id", "ad_account_name", 
                "ad_campaign_id", "ad_campaign_name", "adset_id", "adset_name", 
                "ad_id", "ad_name", "country", "region"
            ],
            "fields": ["spend", "impressions", "clicks", "reach"],
            "page": 1,
            "limit": 200
        }
        
        logger.info(f"Fetching insights for brand {brand['name']} ({brand['caresoft_domain']}) for date {date_str}")
        
        all_data = []
        page = 1
        
        while True:
            payload["page"] = page
            response_data, error = call_caresoft_api_post(brand, endpoint, payload)
            
            if error:
                logger.error(f"API Error for brand {brand['name']}: {error}")
                break
            
            if not response_data or response_data.get("code") != "ok":
                logger.warning(f"Unexpected response format or error code: {response_data}")
                break
            
            data_list = response_data.get("data", [])
            if not data_list:
                break
            
            all_data.extend(data_list)
            logger.info(f"Page {page}: fetched {len(data_list)} records (total: {len(all_data)})")
            
            # If we got fewer records than the limit, it's likely the last page
            if len(data_list) < payload["limit"]:
                break
                
            page += 1
            time.sleep(1) # Simple rate limiting
            
        return all_data

    def transform_record(self, record: Dict[str, Any], brand_etl: int) -> Dict[str, Any]:
        """
        Transform API record to match database schema
        """
        def safe_float(val):
            if val is None or val == "":
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        return {
            "date": self.snapshot_date,
            "facebook_page_id": record.get("facebook_page_id"),
            "page_name": record.get("page_name"),
            "ad_account_id": record.get("ad_account_id"),
            "ad_account_name": record.get("ad_account_name"),
            "ad_campaign_id": record.get("ad_campaign_id"),
            "ad_campaign_name": record.get("ad_campaign_name"),
            "adset_id": record.get("adset_id"),
            "adset_name": record.get("adset_name"),
            "ad_id": record.get("ad_id"),
            "ad_name": record.get("ad_name"),
            "country": record.get("country"),
            "region": record.get("region"),
            "spend": safe_float(record.get("spend")),
            "impressions": safe_float(record.get("impressions")),
            "clicks": safe_float(record.get("clicks")),
            "reach": safe_float(record.get("reach")),
            "brand_etl": brand_etl
        }

    def save_to_postgres(self, records: List[Dict[str, Any]], brand_id: int):
        """
        Save records to PostgreSQL using standardized bulk_load (Delete then Insert)
        """
        if not records:
            logger.warning(f"No records to save for brand {brand_id}")
            return

        cols = [
            "date", "facebook_page_id", "page_name", "ad_account_id", "ad_account_name",
            "ad_campaign_id", "ad_campaign_name", "adset_id", "adset_name",
            "ad_id", "ad_name", "country", "region", "spend", "impressions",
            "clicks", "reach", "brand_etl"
        ]
        
        conflict_cols = [
            "date", "facebook_page_id", "ad_account_id", "ad_campaign_id", 
            "adset_id", "ad_id", "country", "region", "brand_etl"
        ]
        
        try:
            with PostgreSQLHelper(POSTGRES_URI) as db:
                cursor = db.conn.cursor()
                
                # Use bulk_load for high performance
                # Divide into manageable chunks if needed (e.g., 10,000 records per chunk)
                chunk_size = 10000
                total_records = len(records)
                
                for i in range(0, total_records, chunk_size):
                    chunk = records[i:i + chunk_size]
                    logger.info(f"Bulk loading chunk {i//chunk_size + 1} ({len(chunk)} records)...")
                    
                    bulk_load(
                        cursor=cursor,
                        target_table=TARGET_TABLE,
                        records=chunk,
                        columns=cols,
                        conflict_columns=conflict_cols,
                        method='delete_insert'
                    )
                    
                    # Small sleep to avoid temp table name collision in common.utils.bulk_load 
                    # which uses timestamp in seconds
                    if i + chunk_size < total_records:
                        time.sleep(1.1)
                
                db.conn.commit()
                cursor.close()
                logger.success(f"Successfully saved {len(records)} records to {TARGET_TABLE} for brand {brand_id}")
        except Exception as e:
            logger.error(f"Failed to save to PostgreSQL: {e}")
            raise

    def run(self):
        """
        Execute the ingestion process for all active brands
        """
        brands = get_active_brands()
        if not brands:
            logger.error("No active brands found")
            return

        total_records = 0
        for brand in brands:
            try:
                # Optional: Filter for specific brand if needed, for now process all
                # The user's example used kangnam2
                
                raw_data = self.fetch_insights(brand)
                if not raw_data:
                    logger.info(f"No insights found for brand {brand['name']}")
                    continue
                
                transformed_records = [self.transform_record(record, brand["erp_id"]) for record in raw_data]
                self.save_to_postgres(transformed_records, brand["erp_id"])
                total_records += len(transformed_records)
                
            except Exception as e:
                logger.error(f"Error processing brand {brand['name']}: {e}")
                continue
                
        logger.success(f"Ingestion completed. Total records saved: {total_records}")

if __name__ == "__main__":
    # Get snapshot_date from command line arguments
    # Usage: python marketing_insights_ingestion.py 2026-03-22
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    
    if snapshot_date_str:
        try:
            snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format: {snapshot_date_str}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # Default to yesterday
        snapshot_date = date.today() - timedelta(days=1)
        
    job = MarketingInsightsIngestion(snapshot_date)
    job.run()
