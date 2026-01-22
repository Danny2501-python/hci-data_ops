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
CALL_BRONZE_TABLE = 'caresoft.cs_call_api'

class CallIngestion:
    """Pull Call data from CareSoft API and store in Bronze Iceberg table"""
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        
        logger.info(f"Initializing CallIngestion for snapshot_date={self.snapshot_date}")
        
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
        """Create call bronze table if not exists"""
        try:
            table = self.bronze_catalog.load_table(CALL_BRONZE_TABLE)
            logger.info(f"Table {CALL_BRONZE_TABLE} already exists")
        except Exception:
            logger.info(f"Creating table {CALL_BRONZE_TABLE}")
            
            # Schema definition matching cs_call_api DDL
            schema = Schema(
                NestedField(1, "id", IntegerType(), required=False),
                NestedField(2, "customer_id", LongType(), required=False),
                NestedField(3, "call_id", StringType(), required=False),
                NestedField(4, "caller", StringType(), required=False),
                NestedField(5, "called", StringType(), required=False),
                NestedField(6, "user_id", StringType(), required=False),
                NestedField(7, "agent_id", StringType(), required=False),
                NestedField(8, "group_id", IntegerType(), required=False),
                NestedField(9, "call_type", IntegerType(), required=False),
                NestedField(10, "start_time", TimestampType(), required=False),
                NestedField(11, "call_status", StringType(), required=False),
                NestedField(12, "end_time", TimestampType(), required=False),
                NestedField(13, "wait_time", StringType(), required=False),  # time -> string
                NestedField(14, "hold_time", StringType(), required=False),  # time -> string
                NestedField(15, "talk_time", StringType(), required=False),  # time -> string
                NestedField(16, "end_status", StringType(), required=False),
                NestedField(17, "ticket_id", LongType(), required=False),
                NestedField(18, "last_agent_id", StringType(), required=False),
                NestedField(19, "last_user_id", StringType(), required=False),
                NestedField(20, "call_survey", StringType(), required=False),
                NestedField(21, "call_survey_result", LongType(), required=False),  # numeric -> long
                NestedField(22, "service_id", StringType(), required=False),
                NestedField(23, "missed_reason", StringType(), required=False),
                NestedField(24, "path", StringType(), required=False),
                NestedField(25, "path_download", StringType(), required=False),
                NestedField(26, "raw_data", StringType(), required=False),
                NestedField(27, "brand_id", IntegerType(), required=False),
                NestedField(28, "dtmf", StringType(), required=False),
                
                # Metadata fields
                NestedField(29, "snapshot_date", DateType(), required=True),
                NestedField(30, "ingestion_timestamp", TimestampType(), required=True),
            )
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=29,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.bronze_catalog.create_table(
                identifier=CALL_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"Created table {CALL_BRONZE_TABLE}")

    def fetch_calls(self, brand: dict, days_ago: int = 3) -> list:
        """
        Fetch calls from API
        Logic matches cs_extract.py: 
        - start_time_since = (snapshot_date - 3 days) 00:00:00
        - start_time_to = snapshot_date 00:00:00
        """
        anchor_date = datetime.combine(self.snapshot_date, datetime.min.time())
        
        start_time_to = anchor_date
        start_time_since = anchor_date - timedelta(days=days_ago)
        
        start_time_since_str = start_time_since.strftime('%Y-%m-%dT%H:%M:%SZ')
        start_time_to_str = start_time_to.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"Fetching calls for brand {brand['name']} from {start_time_since_str} to {start_time_to_str}")
        
        page = 1
        call_data = []
        
        while True:
            params = {
                'start_time_since': start_time_since_str,
                'start_time_to': start_time_to_str,
                'page': page,
                'count': CARESOFT_PAGE_SIZE
            }
            
            try:
                url = get_api_url(brand, "calls")
                headers = get_api_headers(brand)
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_calls = data.get('calls', [])
                    
                    if not new_calls:
                        logger.info(f"No more calls on page {page}")
                        break
                        
                    call_data.extend(new_calls)
                    logger.info(f"Fetched {len(new_calls)} calls from page {page}")
                    
                    page += 1
                    time.sleep(2)  # Rate limiting (matching cs_extract.py)
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text[:200]}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching calls page {page}: {e}")
                break
                
        return call_data

    def run(self, brand: dict, days_ago: int = 3):
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info(f"Processing call ingestion for {brand_name} (id={brand_erp_id})")
        
        calls = self.fetch_calls(brand, days_ago)
        
        if not calls:
            logger.warning(f"No calls found for brand {brand_name}")
            return

        # Transform
        ingestion_timestamp = get_current_timestamp_naive()
        records = []
        
        def safe_ts(val_ms):
            if val_ms:
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

        def safe_str(val):
            """Convert to string, handling None and empty string"""
            if val is None or val == "":
                return None
            return str(val)
        
        for call in calls:
            record = {
                "id": call.get("id"),
                "customer_id": call.get("customer_id"),
                "call_id": safe_str(call.get("call_id")),
                "caller": safe_str(call.get("caller")),
                "called": safe_str(call.get("called")),
                "user_id": safe_str(call.get("user_id")),
                "agent_id": safe_str(call.get("agent_id")),
                "group_id": call.get("group_id"),
                "call_type": call.get("call_type"),
                "start_time": safe_ts(call.get("start_time")),
                "call_status": safe_str(call.get("call_status")),
                "end_time": safe_ts(call.get("end_time")),
                "wait_time": safe_str(call.get("wait_time")),
                "hold_time": safe_str(call.get("hold_time")),
                "talk_time": safe_str(call.get("talk_time")),
                "end_status": safe_str(call.get("end_status")),
                "ticket_id": call.get("ticket_id"),
                "last_agent_id": safe_str(call.get("last_agent_id")),
                "last_user_id": safe_str(call.get("last_user_id")),
                "call_survey": safe_str(call.get("call_survey")),
                "call_survey_result": call.get("call_survey_result"),
                "service_id": safe_str(call.get("service_id")),
                "missed_reason": safe_str(call.get("missed_reason")),
                "path": safe_str(call.get("path")),
                "path_download": safe_str(call.get("path_download")),
                "raw_data": json.dumps(call, ensure_ascii=False),
                "brand_id": brand_erp_id,
                "dtmf": safe_str(call.get("dtmf")),
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp
            }
            records.append(record)
            
        # Write to Iceberg
        table = self.bronze_catalog.load_table(CALL_BRONZE_TABLE)
        
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
            logger.success(f"Successfully wrote {len(records)} calls for {brand_name}")
        except Exception as e:
            logger.error(f"Failed to write calls: {e}")
            
        gc.collect()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Call Ingestion for {snapshot_date}")
    
    brands = get_active_brands()
    for brand in brands:
        try:
            job = CallIngestion(snapshot_date)
            job.run(brand, days_ago=3)
        except Exception as e:
            logger.error(f"Failed brand {brand['name']}: {e}")

if __name__ == "__main__":
    main()
