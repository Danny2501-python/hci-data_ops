import os
import sys
import json
import time
import requests
import gc
import pyarrow as pa
from datetime import date, datetime, timedelta
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, TimestampType, DateType, LongType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.io.pyarrow import schema_to_pyarrow

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from common.brand_config import get_active_brands
from common.utils import get_current_timestamp_naive
from common.caresoft_api import get_api_url, get_api_headers

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

# S3 configuration
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE', '500'))
CHAT_BRONZE_TABLE = 'caresoft.cs_chat_api'

class ChatIngestion:
    """Pull Chat data from CareSoft API and store in Bronze Iceberg table"""
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        
        logger.info(f"Initializing ChatIngestion for snapshot_date={self.snapshot_date}")
        
        # Connect to Bronze catalog
        self.bronze_catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            **{
                "uri": ICEBERG_REST_URI_BRONZE,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
                "s3.region": AWS_REGION,
            }
        )
        
        self.ensure_table_exists()

    def ensure_table_exists(self):
        """Create chat bronze table if not exists"""
        try:
            table = self.bronze_catalog.load_table(CHAT_BRONZE_TABLE)
            logger.info(f"Table {CHAT_BRONZE_TABLE} already exists")
        except Exception:
            logger.info(f"Creating table {CHAT_BRONZE_TABLE}")
            
            # Schema definition
            # Based on cs_extract.py usually just extracting raw_data + metadata
            schema = Schema(
                NestedField(1, "ticket_id", LongType(), required=False),
                NestedField(2, "ticket_no", LongType(), required=False),
                NestedField(3, "customer_id", LongType(), required=False),
                NestedField(4, "group_name", StringType(), required=False),
                NestedField(5, "conversation_id", StringType(), required=False),
                NestedField(6, "cus_email", StringType(), required=False),
                NestedField(7, "cus_name", StringType(), required=False),
                NestedField(8, "cus_phone", StringType(), required=False),
                NestedField(9, "start_time", TimestampType(), required=False),
                NestedField(10, "end_time", TimestampType(), required=False),
                NestedField(11, "chat_duration", LongType(), required=False),
                NestedField(12, "agent_email", StringType(), required=False),
                NestedField(13, "ring_time", StringType(), required=False),
                NestedField(14, "meet_time", StringType(), required=False),
                NestedField(15, "waittime", StringType(), required=False),
                NestedField(16, "answer_time", StringType(), required=False),
                NestedField(17, "chat_status", StringType(), required=False),
                NestedField(18, "landing_page", StringType(), required=False),
                NestedField(19, "referrer", StringType(), required=False),
                NestedField(20, "is_trigger", StringType(), required=False),
                NestedField(21, "facebook_page_id", StringType(), required=False),
                NestedField(22, "service_id", LongType(), required=False),
                NestedField(23, "brand_id", IntegerType(), required=False),
                NestedField(24, "raw_data", StringType(), required=False),
                NestedField(25, "agent_name", StringType(), required=False),
                
                # Metadata fields
                NestedField(26, "snapshot_date", DateType(), required=True),
                NestedField(27, "ingestion_timestamp", TimestampType(), required=True),
            )
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=26,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.bronze_catalog.create_table(
                identifier=CHAT_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"Created table {CHAT_BRONZE_TABLE}")

    def fetch_chats(self, brand: dict, days_ago: int = 3) -> list:
        """
        Fetch chats from API
        Logic matches cs_extract.py: 
        - start_time_since = (snapshot_date - 3 days) 00:00:00
        - start_time_to = (snapshot_date + 1 day) 00:00:00
        - conversation_type = '1'
        """
        # Calculate date range
        # Note: snapshot_date is usually "execution date", so we assume "today" logic
        # this_day = datetime.combine(self.snapshot_date, datetime.min.time())
        # To match cs_extract logic precisely:
        # this_day = now
        # this_day_1 = now - 3 days
        # But we use snapshot_date as anchor
        
        anchor_date = datetime.combine(self.snapshot_date, datetime.min.time())
        
        # In cs_extract: 
        # start_time_to = T+1 00:00:00
        # start_time_since = start_time_to - days_ago
        start_time_to = anchor_date + timedelta(days=1)
        start_time_since = start_time_to - timedelta(days=days_ago)
        
        start_time_since_str = start_time_since.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time_to_str = start_time_to.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"Fetching chats for brand {brand['name']} from {start_time_since_str} to {start_time_to_str}")
        
        page = 1
        chat_data = []
        
        while True:
            params = {
                'start_time_since': start_time_since_str,
                'start_time_to': start_time_to_str,
                'conversation_type': '1', # Specific to Chat logic in cs_extract
                'page': page,
                'count': CARESOFT_PAGE_SIZE
            }
            
            try:
                url = get_api_url(brand, "chats")
                headers = get_api_headers(brand)
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_chats = data.get('chats', [])
                    
                    if not new_chats:
                        logger.info(f"No more chats on page {page}")
                        break
                        
                    chat_data.extend(new_chats)
                    logger.info(f"Fetched {len(new_chats)} chats from page {page}")
                    
                    page += 1
                    time.sleep(1) # Rate limiting
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text[:200]}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching chats page {page}: {e}")
                break
                
        return chat_data

    def run(self, brand: dict, days_ago: int = 3):
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info(f"Processing chat ingestion for {brand_name} (id={brand_erp_id})")
        
        chats = self.fetch_chats(brand, days_ago)
        
        if not chats:
            logger.warning(f"No chats found for brand {brand_name}")
            return

        # Transform
        ingestion_timestamp = get_current_timestamp_naive()
        records = []
        
        def safe_ts(val_ms):
            if val_ms:
                # API usually returns strings like "2023-01-01 12:00:00" or raw
                if isinstance(val_ms, str):
                    try:
                        return datetime.strptime(val_ms, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        try:
                            return datetime.fromisoformat(val_ms.replace('Z', '+00:00'))
                        except ValueError:
                            pass
                if isinstance(val_ms, int):
                     return datetime.fromtimestamp(val_ms / 1000.0)
            return None

        for chat in chats:
            record = {
                "ticket_id": chat.get("ticket_id"),
                "ticket_no": chat.get("ticket_no"),
                "customer_id": chat.get("customer_id"),
                "group_name": chat.get("group_name"),
                "conversation_id": str(chat.get("conversation_id")),
                "cus_email": chat.get("cus_email"),
                "cus_name": chat.get("cus_name"),
                "cus_phone": chat.get("cus_phone"),
                "start_time": safe_ts(chat.get("start_time")),
                "end_time": safe_ts(chat.get("end_time")),
                "chat_duration": chat.get("chat_duration"),
                "agent_email": chat.get("agent_email"),
                "ring_time": str(chat.get("ring_time")) if chat.get("ring_time") is not None else None,
                "meet_time": str(chat.get("meet_time")) if chat.get("meet_time") is not None else None,
                "waittime": str(chat.get("waitTime")) if chat.get("waitTime") is not None else None,
                "answer_time": str(chat.get("answer_time")) if chat.get("answer_time") is not None else None,
                "chat_status": str(chat.get("chat_status")),
                "landing_page": chat.get("landing_page"),
                "referrer": chat.get("referrer"),
                "is_trigger": str(chat.get("is_trigger")) if chat.get("is_trigger") is not None else None,
                "facebook_page_id": str(chat.get("facebook_page_id")) if chat.get("facebook_page_id") is not None else None,
                "service_id": chat.get("service_id"),
                "brand_id": brand_erp_id,
                "raw_data": json.dumps(chat, ensure_ascii=False),
                "agent_name": chat.get("agent_name"),
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp
            }
            records.append(record)
            
        # Write to Iceberg
        table = self.bronze_catalog.load_table(CHAT_BRONZE_TABLE)
        
        # Overwrite partition logic
        try:
             logger.info(f"Deleting old data for {brand_name} at {self.snapshot_date}")
             table.delete(
                 And(
                     EqualTo("snapshot_date", self.snapshot_date),
                     EqualTo("brand_id", brand_erp_id)
                 )
             )
        except Exception:
            pass
            
        # Append
        try:
            pa_schema = schema_to_pyarrow(table.schema())
            df = pa.Table.from_pylist(records, schema=pa_schema)
            table.append(df)
            logger.success(f"Successfully wrote {len(records)} chats for {brand_name}")
        except Exception as e:
            logger.error(f"Failed to write chats: {e}")
            
        gc.collect()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Chat Ingestion for {snapshot_date}")
    
    brands = get_active_brands()
    for brand in brands:
        try:
            job = ChatIngestion(snapshot_date)
            job.run(brand, days_ago=3)
        except Exception as e:
            logger.error(f"Failed brand {brand['name']}: {e}")

if __name__ == "__main__":
    main()
