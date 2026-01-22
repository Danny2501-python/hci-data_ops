"""
Service Ingestion: CareSoft API → Iceberg Bronze Layer

Flow:
    CareSoft API (/services) → Bronze (caresoft.cs_services_api)
"""

import os
import sys
import json
import time
import requests
import gc
import pyarrow as pa
from datetime import date, datetime
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

SERVICE_BRONZE_TABLE = 'caresoft.cs_services_api'
CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE', '500'))


class ServiceIngestion:
    """Pull Service data from CareSoft API and store in Bronze Iceberg table"""
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        self.bronze_table = None
        
        logger.info(f"Initializing ServiceIngestion for snapshot_date={self.snapshot_date}")
        
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
        
        self.bronze_table = self.ensure_table_exists()

    def ensure_table_exists(self):
        """Create service bronze table if not exists"""
        try:
            table = self.bronze_catalog.load_table(SERVICE_BRONZE_TABLE)
            logger.info(f"Table {SERVICE_BRONZE_TABLE} already exists")
            return table
        except Exception:
            logger.info(f"Creating table {SERVICE_BRONZE_TABLE}")
            
            # Schema definition for services (aligned with DDL & Response)
            schema = Schema(
                NestedField(1, "service_id", LongType(), required=False),
                NestedField(2, "service_name", StringType(), required=False),
                NestedField(3, "service_type", StringType(), required=False),
                NestedField(4, "type", IntegerType(), required=False),
                NestedField(5, "status", IntegerType(), required=False),
                NestedField(6, "detail", StringType(), required=False),
                NestedField(7, "brand_id", IntegerType(), required=False),
                NestedField(8, "raw_data", StringType(), required=False),
                
                # Metadata fields
                NestedField(9, "snapshot_date", DateType(), required=True),
                NestedField(10, "ingestion_timestamp", TimestampType(), required=True),
            )
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=9,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.bronze_catalog.create_table(
                identifier=SERVICE_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"Created table {SERVICE_BRONZE_TABLE}")
            return table

    def fetch_services(self, brand: dict) -> list:
        """
        Fetch services from API
        Page 1 only (Simple API pattern)
        """
        logger.info(f"Fetching services for brand {brand['name']}")
        
        page = 1
        services_data = []
        
        # Only fetch page 1 (matching cs_extract.py logic)
        while page == 1:
            params = {
                'page': page,
                'count': CARESOFT_PAGE_SIZE
            }
            
            try:
                url = get_api_url(brand, "services")
                headers = get_api_headers(brand)
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_services = data.get('services', [])
                    
                    services_data.extend(new_services)
                    logger.info(f"Fetched {len(new_services)} services from page {page}")
                    
                    page += 1
                    time.sleep(2)  # Rate limiting
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text[:200]}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching services page {page}: {e}")
                break
                
        return services_data

    def run(self, brand: dict):
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info(f"Processing service ingestion for {brand_name} (id={brand_erp_id})")
        
        # Fetch services
        services_data = self.fetch_services(brand)
        
        if not services_data:
            logger.warning(f"No services found for brand {brand_name}")
            return

        ingestion_timestamp = get_current_timestamp_naive()
        records = []
        
        def safe_str(val):
            if val is None:
                return None
            return str(val)
        
        for service in services_data:
            # Handle detail field (convert dict to json string)
            detail_obj = service.get("detail")
            detail_str = json.dumps(detail_obj, ensure_ascii=False) if detail_obj else None

            record = {
                "service_id": service.get("service_id"),  # Note: API uses 'service_id' not 'id'
                "service_name": safe_str(service.get("service_name")),
                "service_type": safe_str(service.get("service_type")),
                "type": service.get("type"), # Keep as int
                "status": service.get("status"), # Keep as int
                "detail": detail_str,
                "brand_id": brand_erp_id,
                "raw_data": json.dumps(service, ensure_ascii=False),
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp
            }
            records.append(record)
            
        logger.info(f"Prepared {len(records)} records for ingestion")
            
        # Write to Iceberg
        table = self.bronze_table
        
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
            logger.success(f"Successfully wrote {len(records)} service records for {brand_name}")
        except Exception as e:
            logger.error(f"Failed to write services: {e}")
            
        gc.collect()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Service Ingestion for {snapshot_date}")
    
    brands = get_active_brands()
    for brand in brands:
        try:
            job = ServiceIngestion(snapshot_date)
            job.run(brand)
        except Exception as e:
            logger.error(f"Failed brand {brand['name']}: {e}")

if __name__ == "__main__":
    main()
