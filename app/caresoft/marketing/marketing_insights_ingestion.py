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

# Chủ động đi chậm để không bị nginx của CareSoft trả 429 — xem log lỗi 2026-10-16 lượt 10:30, lúc đó
# code nhảy sang brand kế tiếp chỉ sau 13 mili-giây. Phân trang trong cùng một brand giữ nhịp 1 giây
# như cũ (cho nhanh); chỉ giãn 3 giây ở chỗ hay dính 429 là lúc đổi brand hoặc đổi ngày.
# Backoff khi đã dính 429 nằm ở call_caresoft_api_post.
PAGE_PACING_SECONDS = 1     # giữa 2 trang của cùng brand + ngày
BRAND_PACING_SECONDS = 3    # trước request đầu tiên của mỗi brand / mỗi ngày
_last_request_at = 0.0


def pace_request(min_gap: float):
    """Chờ cho đủ min_gap giây kể từ request trước đó (tính cả thời gian xử lý ở giữa)."""
    global _last_request_at
    wait = min_gap - (time.monotonic() - _last_request_at)
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.monotonic()

class MarketingInsightsIngestion:
    def __init__(self, since_date: date = None, until_date: date = None):
        # Mặc định: until là T-1, since là T-3
        self.until_date = until_date or (date.today() - timedelta(days=1))
        self.since_date = since_date or (date.today() - timedelta(days=3))
        # Các lượt gọi API hỏng hẳn sau khi đã thử lại — tổng kết cuối run(), KHÔNG làm task đỏ.
        self.api_failures: List[str] = []
        
        logger.info(f"Initialized MarketingInsightsIngestion from {self.since_date} to {self.until_date}")

    def fetch_insights(self, brand: dict, target_date: date) -> List[Dict[str, Any]]:
        """
        Fetch marketing insights from CareSoft API for a specific date
        """
        endpoint = "marketing/insights"
        date_str = target_date.strftime("%d-%m-%Y")
        
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
        
        brand_label = brand.get("name") or brand.get("caresoft_domain")
        logger.info(f"Fetching insights for brand {brand_label} ({brand['caresoft_domain']}) for date {date_str}")
        
        all_data = []
        page = 1
        
        while True:
            payload["page"] = page
            pace_request(BRAND_PACING_SECONDS if page == 1 else PAGE_PACING_SECONDS)
            response_data, error = call_caresoft_api_post(brand, endpoint, payload)
            
            if error:
                logger.error(f"API Error for brand {brand_label}: {error}")
                self.api_failures.append(f"{brand['caresoft_domain']} / {target_date} (trang {page}): {error}")
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
            
        return all_data

    def transform_record(self, record: Dict[str, Any], brand_etl: int, target_date: date) -> Dict[str, Any]:
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
            "date": target_date,
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
        Execute the ingestion process for all active brands across the date range
        """
        brands = get_active_brands()
        if not brands:
            logger.error("No active brands found")
            return

        # Tạo danh sách các ngày cần xử lý
        delta = self.until_date - self.since_date
        date_list = [self.since_date + timedelta(days=i) for i in range(delta.days + 1)]
        
        total_records = 0
        
        for target_date in date_list:
            logger.info(f"===> Processing date: {target_date} <===")
            for brand in brands:
                try:
                    raw_data = self.fetch_insights(brand, target_date)
                    if not raw_data:
                        logger.info(f"No insights found for brand {brand['name']} on {target_date}")
                        continue
                    
                    transformed_records = [self.transform_record(record, brand["erp_id"], target_date) for record in raw_data]
                    self.save_to_postgres(transformed_records, brand["erp_id"])
                    total_records += len(transformed_records)
                    
                except Exception as e:
                    logger.error(f"Error processing brand {brand.get('name') or brand['caresoft_domain']} on {target_date}: {e}")
                    continue
                
        self.report_api_failures()
        logger.success(f"Ingestion completed. Total records saved: {total_records}")

    def report_api_failures(self):
        """Cảnh báo cuối log; cố tình KHÔNG exit(1) — lượt chạy 4 tiếng sau tự lấy bù (cửa sổ T-3 → T-0)."""
        if not self.api_failures:
            return
        logger.warning(
            f"⚠️  {len(self.api_failures)} lượt gọi API hỏng hẳn sau khi đã thử lại — brand/ngày dưới đây "
            f"có thể thiếu dữ liệu, lượt chạy sau sẽ lấy bù:"
        )
        for item in self.api_failures:
            logger.warning(f"   - {item}")

if __name__ == "__main__":
    # Nhận tham số từ Airflow hoặc CLI
    # Usage: python marketing_insights_ingestion.py 2026-03-20 2026-03-22
    since_str = sys.argv[1] if len(sys.argv) > 1 else None
    until_str = sys.argv[2] if len(sys.argv) > 2 else None
    
    since_date = None
    until_date = None

    try:
        if since_str:
            since_date = datetime.strptime(since_str, "%Y-%m-%d").date()
        if until_str:
            until_date = datetime.strptime(until_str, "%Y-%m-%d").date()
    except ValueError as e:
        logger.error(f"Invalid date format. Use YYYY-MM-DD. Error: {e}")
        sys.exit(1)
        
    job = MarketingInsightsIngestion(since_date=since_date, until_date=until_date)
    job.run()
