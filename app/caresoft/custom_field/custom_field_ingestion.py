"""
Custom Field Ingestion: CareSoft API → Iceberg Bronze Layer

Metadata flow - gọi GET /tickets/custom_fields và flatten nested values

Flow:
    CareSoft API (/tickets/custom_fields) → Bronze (caresoft.cs_custom_fields)
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

CUSTOM_FIELD_BRONZE_TABLE = 'caresoft.cs_custom_fields'


class CustomFieldIngestion:
    """Pull Custom Field metadata from CareSoft API and store in Bronze Iceberg table
    
    Supports two types:
    - 'ticket': Ticket custom fields (/tickets/custom_fields)
    - 'contact': Contact custom fields (/contacts/custom_fields)
    """
    
    def __init__(self, snapshot_date: date = None, field_type: str = 'ticket'):
        """
        Initialize Custom Field Ingestion
        
        Args:
            snapshot_date: Date for snapshot (default: today)
            field_type: Type of custom field - 'ticket' or 'contact' (default: 'ticket')
        """
        self.snapshot_date = snapshot_date or date.today()
        self.field_type = field_type
        self.bronze_catalog = None
        
        logger.info(f"Initializing CustomFieldIngestion for snapshot_date={self.snapshot_date}, field_type={field_type}")
        
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
        """Create custom field bronze table if not exists"""
        try:
            table = self.bronze_catalog.load_table(CUSTOM_FIELD_BRONZE_TABLE)
            logger.info(f"Table {CUSTOM_FIELD_BRONZE_TABLE} already exists")
        except Exception:
            logger.info(f"Creating table {CUSTOM_FIELD_BRONZE_TABLE}")
            
            # Schema definition for custom fields (flattened)
            schema = Schema(
                NestedField(1, "custom_field_id", StringType(), required=False),
                NestedField(2, "custom_field_lable", StringType(), required=False),
                NestedField(3, "type", StringType(), required=False),
                NestedField(4, "value_id", StringType(), required=False),
                NestedField(5, "value_lable", StringType(), required=False),
                NestedField(6, "parent_value_id", StringType(), required=False),
                NestedField(7, "field_type", StringType(), required=False),  # 'ticket' or 'contact'
                NestedField(8, "brand_id", IntegerType(), required=False),
                NestedField(9, "raw_data", StringType(), required=False),
                
                # Metadata fields
                NestedField(10, "snapshot_date", DateType(), required=True),
                NestedField(11, "ingestion_timestamp", TimestampType(), required=True),
            )
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=10,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.bronze_catalog.create_table(
                identifier=CUSTOM_FIELD_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"Created table {CUSTOM_FIELD_BRONZE_TABLE}")
            return table
            
        return table

    def fetch_custom_fields(self, brand: dict) -> list:
        """
        Fetch custom fields from API
        No date filtering - metadata is static
        Only page 1 (matching cs_extract.py: while page == 1)
        
        Endpoint depends on field_type:
        - 'ticket': /tickets/custom_fields
        - 'contact': /contacts/custom_fields
        """
        # Determine endpoint based on field_type
        if self.field_type == 'contact':
            endpoint = "contacts/custom_fields"
            response_key = "custom_fields"
        else:  # default to 'ticket'
            endpoint = "tickets/custom_fields"
            response_key = "custom_fields"
        
        logger.info(f"Fetching {self.field_type} custom fields for brand {brand['name']} from {endpoint}")
        
        page = 1
        custom_fields_data = []
        
        # Only fetch page 1 for custom fields (matching cs_extract.py)
        while page == 1:
            params = {'page': page}
            
            try:
                url = get_api_url(brand, endpoint)
                headers = get_api_headers(brand)
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_fields = data.get(response_key, [])
                    
                    custom_fields_data.extend(new_fields)
                    logger.info(f"Fetched {len(new_fields)} {self.field_type} custom fields from page {page}")
                    
                    page += 1
                    time.sleep(2)  # Rate limiting
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text[:200]}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching custom fields page {page}: {e}")
                break
                
        return custom_fields_data

    def flatten_custom_fields(self, custom_fields_data: list, brand_erp_id: int) -> list:
        """
        Flatten nested custom fields structure
        
        Each custom_field can have multiple values, need to create one record per value
        If no values, create one record with 'None' placeholders
        """
        records = []
        ingestion_timestamp = get_current_timestamp_naive()
        
        def safe_str(val):
            """Convert to string, handling None"""
            if val is None or val == "":
                return None
            return str(val)
        
        for field in custom_fields_data:
            custom_field_id = field.get('custom_field_id')
            custom_field_lable = field.get('custom_field_lable')
            field_type = field.get('type')
            
            # Check if field has values
            if 'values' in field and field['values']:
                # Flatten: create one record per value
                for value in field['values']:
                    record = {
                        "custom_field_id": safe_str(custom_field_id),
                        "custom_field_lable": safe_str(custom_field_lable),
                        "type": safe_str(field_type),
                        "value_id": safe_str(value.get('id')),
                        "value_lable": safe_str(value.get('lable')),
                        "parent_value_id": safe_str(value.get('parent_value_id')),
                        "field_type": self.field_type,  # 'ticket' or 'contact'
                        "brand_id": brand_erp_id,
                        "raw_data": json.dumps(field, ensure_ascii=False),
                        "snapshot_date": self.snapshot_date,
                        "ingestion_timestamp": ingestion_timestamp
                    }
                    records.append(record)
            else:
                # No values: create one record with None placeholders
                record = {
                    "custom_field_id": safe_str(custom_field_id),
                    "custom_field_lable": safe_str(custom_field_lable),
                    "type": safe_str(field_type),
                    "value_id": "None",
                    "value_lable": "None",
                    "parent_value_id": "None",
                    "field_type": self.field_type,  # 'ticket' or 'contact'
                    "brand_id": brand_erp_id,
                    "raw_data": json.dumps(field, ensure_ascii=False),
                    "snapshot_date": self.snapshot_date,
                    "ingestion_timestamp": ingestion_timestamp
                }
                records.append(record)
        
        return records

    def run(self, brand: dict):
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info(f"Processing custom field ingestion for {brand_name} (id={brand_erp_id})")
        
        # Fetch custom fields
        custom_fields_data = self.fetch_custom_fields(brand)
        
        if not custom_fields_data:
            logger.warning(f"No custom fields found for brand {brand_name}")
            return

        # Flatten nested structure
        records = self.flatten_custom_fields(custom_fields_data, brand_erp_id)
        
        logger.info(f"Flattened {len(custom_fields_data)} fields into {len(records)} records")
            
        # Write to Iceberg
        table = self.bronze_table
        
        # Overwrite partition logic (delete by snapshot_date, brand_id, field_type)
        try:
             logger.info(f"Deleting old {self.field_type} data for {brand_name} at {self.snapshot_date}")
             table.delete(
                 And(
                     And(
                         EqualTo("snapshot_date", self.snapshot_date),
                         EqualTo("brand_id", brand_erp_id)
                     ),
                     EqualTo("field_type", self.field_type)
                 )
             )
        except Exception:
            pass
            
        # Append
        try:
            pa_schema = schema_to_pyarrow(table.schema())
            df = pa.Table.from_pylist(records, schema=pa_schema)
            table.append(df)
            logger.success(f"Successfully wrote {len(records)} custom field records for {brand_name}")
        except Exception as e:
            logger.error(f"Failed to write custom fields: {e}")
            
        gc.collect()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    field_type = sys.argv[2] if len(sys.argv) > 2 else 'ticket'  # Default to 'ticket'
    
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    logger.info(f"Starting Custom Field Ingestion for {snapshot_date}, field_type={field_type}")
    
    brands = get_active_brands()
    for brand in brands:
        try:
            job = CustomFieldIngestion(snapshot_date, field_type=field_type)
            job.run(brand)
        except Exception as e:
            logger.error(f"Failed brand {brand['name']}: {e}")

if __name__ == "__main__":
    main()
