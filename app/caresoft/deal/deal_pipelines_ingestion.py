"""
Deal Pipelines Ingestion: CareSoft API → Iceberg Bronze Layer

Metadata flow - gọi GET /deal/pipelines và lưu nguyên trạng cấu trúc

Flow:
    CareSoft API (/deal/pipelines) → Bronze (caresoft.cs_deal_pipelines_api)
"""

import os
import sys
import json
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
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
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


class DealPipelinesIngestion:
    """Pull Deal Pipelines metadata from CareSoft API and store in Bronze Iceberg table"""
    DEAL_PIPELINES_BRONZE_TABLE = 'caresoft.cs_deal_pipelines_api'
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        
        logger.info(f"Initializing DealPipelinesIngestion for snapshot_date={self.snapshot_date}")
        
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
        """Create deal pipelines bronze table if not exists"""
        try:
            table = self.bronze_catalog.load_table(self.DEAL_PIPELINES_BRONZE_TABLE)
            logger.info(f"Table {self.DEAL_PIPELINES_BRONZE_TABLE} already exists")
            return table
        except Exception:
            logger.info(f"Creating table {self.DEAL_PIPELINES_BRONZE_TABLE}")
            
            # Schema definition matching original API keys
            schema = Schema(
                NestedField(1, "id", IntegerType(), required=False),
                NestedField(2, "label", StringType(), required=False),
                NestedField(3, "created_at", StringType(), required=False),
                NestedField(4, "updated_at", StringType(), required=False),
                NestedField(5, "default", IntegerType(), required=False),
                NestedField(6, "stages", StringType(), required=False),  # JSON array as string
                NestedField(7, "brand_id", IntegerType(), required=False),
                
                # Metadata fields
                NestedField(8, "snapshot_date", DateType(), required=True),
                NestedField(9, "ingestion_timestamp", TimestampType(), required=True),
            )
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=8,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.bronze_catalog.create_table(
                identifier=self.DEAL_PIPELINES_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"Created table {self.DEAL_PIPELINES_BRONZE_TABLE}")
            return table

    def fetch_pipelines(self, brand: dict) -> list:
        """
        Fetch deal pipelines from API
        """
        endpoint = "deal/pipelines"
        logger.info(f"Fetching deal pipelines for brand {brand['name']} from {endpoint}")
        
        try:
            url = get_api_url(brand, endpoint)
            headers = get_api_headers(brand)
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                pipelines = data.get('data', [])
                logger.info(f"Successfully fetched {len(pipelines)} deal pipelines")
                return pipelines
            else:
                logger.error(f"API request failed: {response.status_code} - {response.text[:200]}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching deal pipelines: {e}")
            return []

    def transform_pipelines(self, pipelines_data: list, brand_erp_id: int) -> list:
        """
        Transform pipelines data to match Iceberg schema
        """
        records = []
        ingestion_timestamp = get_current_timestamp_naive()
        
        def safe_str(val):
            """Convert to string, handling None"""
            if val is None or val == "":
                return None
            return str(val)
            
        def safe_int(val):
            """Convert to int, handling None"""
            if val is None or val == "":
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        
        for pipeline in pipelines_data:
            stages_list = pipeline.get('stages', [])
            stages_json = json.dumps(stages_list, ensure_ascii=False) if stages_list is not None else None
            
            record = {
                "id": safe_int(pipeline.get('id')),
                "label": safe_str(pipeline.get('label')),
                "created_at": safe_str(pipeline.get('created_at')),
                "updated_at": safe_str(pipeline.get('updated_at')),
                "default": safe_int(pipeline.get('default')),
                "stages": stages_json,
                "brand_id": brand_erp_id,
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp
            }
            records.append(record)
        
        return records

    def run(self, brand: dict):
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info(f"Processing deal pipelines ingestion for {brand_name} (id={brand_erp_id})")
        
        # Fetch pipelines
        pipelines_data = self.fetch_pipelines(brand)
        
        if not pipelines_data:
            logger.warning(f"No deal pipelines found for brand {brand_name}")
            return

        # Transform structures without flattening
        records = self.transform_pipelines(pipelines_data, brand_erp_id)
        
        logger.info(f"Transformed {len(pipelines_data)} pipelines to records")
            
        # Write to Iceberg
        table = self.bronze_table
        
        # Overwrite partition logic (delete by snapshot_date, brand_id)
        try:
             logger.info(f"Deleting old deal pipelines data for {brand_name} at {self.snapshot_date}")
             table.delete(
                 And(
                     EqualTo("snapshot_date", self.snapshot_date),
                     EqualTo("brand_id", brand_erp_id)
                 )
             )
        except Exception as e:
            logger.warning(f"Failed to delete partition (might be first run): {e}")
            
        # Append
        try:
            pa_schema = schema_to_pyarrow(table.schema())
            df = pa.Table.from_pylist(records, schema=pa_schema)
            table.append(df)
            logger.success(f"Successfully wrote {len(records)} deal pipeline records for {brand_name}")
        except Exception as e:
            logger.error(f"Failed to write deal pipelines: {e}")
            raise
            
        gc.collect()


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Deal Pipelines Ingestion for {snapshot_date}")
    
    brands = get_active_brands()
    for brand in brands:
        try:
            job = DealPipelinesIngestion(snapshot_date)
            job.run(brand)
        except Exception as e:
            logger.error(f"Failed brand {brand['name']}: {e}")

if __name__ == "__main__":
    main()
