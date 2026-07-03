"""
Group Ingestion: CareSoft API → Iceberg Bronze Layer

Simple API flow - gọi trực tiếp GET /groups và lưu vào Bronze
Chỉ lấy page 1 (tất cả groups)

Flow:
    CareSoft API (/groups) → Bronze (caresoft.cs_groups_api)
"""

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
    NestedField, StringType, IntegerType, TimestampType, DateType
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


class GroupIngestion:
    """Pull Group data from CareSoft API and store in Bronze Iceberg table"""
    GROUP_BRONZE_TABLE = 'caresoft.cs_groups_api'
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        
        logger.info(f"Initializing GroupIngestion for snapshot_date={self.snapshot_date}")
        
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
        """Create group bronze table if not exists"""
        try:
            table = self.bronze_catalog.load_table(self.GROUP_BRONZE_TABLE)
            logger.info(f"Table {self.GROUP_BRONZE_TABLE} already exists")
        except Exception:
            logger.info(f"Creating table {self.GROUP_BRONZE_TABLE}")
            
            # Schema definition matching cs_groups_v1 DDL
            schema = Schema(
                NestedField(1, "group_id", IntegerType(), required=False),
                NestedField(2, "group_name", StringType(), required=False),
                NestedField(3, "created_at", StringType(), required=False),
                NestedField(4, "brand_id", IntegerType(), required=False),
                # NestedField(5, "raw_data", StringType(), required=False), # Removed due to size
                
                # Metadata fields
                NestedField(6, "snapshot_date", DateType(), required=True),
                NestedField(7, "ingestion_timestamp", TimestampType(), required=True),
            )
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=6,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.bronze_catalog.create_table(
                identifier=self.GROUP_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"Created table {self.GROUP_BRONZE_TABLE}")

    def fetch_groups(self, brand: dict, days_ago: int = 3) -> list:
        """
        Fetch groups from API
        Logic matches cs_extract.py: 
        - updated_to = (snapshot_date + 1 day) 00:00:00
        - updated_since = (updated_to - 3 days) 00:00:00
        - Only fetch page 1 (all groups fit in one page)
        """
        anchor_date = datetime.combine(self.snapshot_date, datetime.min.time())
        
        updated_to = anchor_date + timedelta(days=1)
        updated_since = updated_to - timedelta(days=days_ago)
        
        updated_since_str = updated_since.strftime('%Y-%m-%dT%H:%M:%SZ')
        updated_to_str = updated_to.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"Fetching groups for brand {brand['name']} from {updated_since_str} to {updated_to_str}")
        
        page = 1
        group_data = []
        
        # Only fetch page 1 for groups (matching cs_extract.py: while page == 1)
        while page == 1:
            params = {
                'updated_since': updated_since_str,
                'updated_to': updated_to_str,
                'page': page,
                'count': CARESOFT_PAGE_SIZE
            }
            
            try:
                url = get_api_url(brand, "groups")
                headers = get_api_headers(brand)
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_groups = data.get('groups', [])
                    
                    group_data.extend(new_groups)
                    logger.info(f"Fetched {len(new_groups)} groups from page {page}")
                    
                    page += 1
                    time.sleep(2)  # Rate limiting (matching cs_extract.py)
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text[:200]}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching groups page {page}: {e}")
                break
                
        return group_data

    def run(self, brand: dict, days_ago: int = 3):
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info(f"Processing group ingestion for {brand_name} (id={brand_erp_id})")
        
        groups = self.fetch_groups(brand, days_ago)
        
        if not groups:
            logger.warning(f"No groups found for brand {brand_name}")
            return

        # Transform
        ingestion_timestamp = get_current_timestamp_naive()
        records = []
        
        def safe_str(val):
            """Convert to string, handling None and empty string"""
            if val is None or val == "":
                return None
            return str(val)
        
        for group in groups:
            record = {
                "group_id": group.get("group_id"),
                "group_name": safe_str(group.get("group_name")),
                "created_at": safe_str(group.get("created_at")),
                "brand_id": brand_erp_id,
                # "raw_data": json.dumps(group, ensure_ascii=False), # Removed due to size
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp
            }
            records.append(record)
            
        # Write to Iceberg
        table = self.bronze_catalog.load_table(self.GROUP_BRONZE_TABLE)
        
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
            logger.success(f"Successfully wrote {len(records)} groups for {brand_name}")
        except Exception as e:
            logger.error(f"Failed to write groups: {e}")
            
        gc.collect()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Group Ingestion for {snapshot_date}")
    
    brands = get_active_brands()
    for brand in brands:
        try:
            job = GroupIngestion(snapshot_date)
            job.run(brand, days_ago=3)
        except Exception as e:
            logger.error(f"Failed brand {brand['name']}: {e}")

if __name__ == "__main__":
    main()
