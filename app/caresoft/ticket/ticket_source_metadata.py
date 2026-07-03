"""
Ticket Source Metadata Ingestion
Architecture: CareSoft API → Bronze (Iceberg)

API: GET /tickets/source/details
Output: Bronze → cs_ticket_source_metadata

This is a METADATA table containing ticket source definitions:
- source_name, channel, utm_source, utm_campaign, utm_medium, etc.
- Used for enrichment and reporting
"""

import os
import sys
from datetime import datetime, date, timedelta
import requests
import time
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And
from pyiceberg.io.pyarrow import schema_to_pyarrow
from loguru import logger
import gc
from common.brand_config import get_active_brands
from common.utils import get_current_timestamp_naive
from common.caresoft_api import get_api_url, get_api_headers

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# CareSoft API configuration
CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE'))

# Stop fetching if data is older than this many days
MAX_DAYS_BACK = 1200


class TicketSourceMetadata:
    TICKET_SOURCE_METADATA_TABLE = 'caresoft.cs_ticket_source_detail_api'
    """
    Pull ticket source metadata from CareSoft API
    This is a metadata table, not transactional data
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        
        # Initialize catalog
        catalog_config = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": AWS_REGION,
        }
        
        self.bronze_catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            uri=ICEBERG_REST_URI_BRONZE,
            **catalog_config
        )
        
        logger.info(f"Initialized TicketSourceMetadata for {self.snapshot_date}")
    
    def ensure_table_exists(self):
        """Create ticket source metadata table if not exists"""
        try:
            return self.bronze_catalog.load_table(self.TICKET_SOURCE_METADATA_TABLE)
        except:
            logger.info(f"Creating table {self.TICKET_SOURCE_METADATA_TABLE}...")
            
            from pyiceberg.schema import Schema
            from pyiceberg.types import (
                NestedField, IntegerType, StringType, TimestampType, 
                DateType, LongType
            )
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform
            
            # Schema for ticket source metadata
            # Fields based on old system's cs_ticket_source_detail_api
            schema = Schema(
                NestedField(1, "id", LongType(), required=False),
                NestedField(2, "source_name", StringType(), required=False),
                NestedField(3, "index", IntegerType(), required=False),
                NestedField(4, "channel", StringType(), required=False),
                NestedField(5, "utm_source", StringType(), required=False),
                NestedField(6, "utm_campaign", StringType(), required=False),
                NestedField(7, "utm_medium", StringType(), required=False),
                NestedField(8, "utm_term", StringType(), required=False),
                NestedField(9, "ad_id", StringType(), required=False),
                NestedField(10, "utm_content", StringType(), required=False),
                NestedField(11, "adgroup_id", StringType(), required=False),
                NestedField(12, "source_key", StringType(), required=False),
                NestedField(13, "updated_at", TimestampType(), required=False),
                NestedField(14, "created_at", TimestampType(), required=False),
                
                # Brand and metadata
                NestedField(15, "brand_id", IntegerType(), required=False),
                # NestedField(16, "raw_data", StringType(), required=False), # Removed due to size
                
                # Metadata
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            table = self.bronze_catalog.create_table(
                identifier=self.TICKET_SOURCE_METADATA_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"✅ Created table: {self.TICKET_SOURCE_METADATA_TABLE}")
            return table
    
    # Note: get_api_url and get_api_headers are now imported from common.caresoft_api
    
    
    def fetch_ticket_sources(self, brand: dict):
        """
        Fetch ticket source metadata from CareSoft API with pagination
        
        Args:
            brand: Brand configuration dict
        
        Returns:
            List of ticket source records
        """
        brand_id = brand["erp_id"]
        
        params = {
            'page': 1,
            'count': CARESOFT_PAGE_SIZE
        }
        
        logger.info(f"Fetching ticket source metadata for brand {brand_id}")
        
        all_sources = []
        page = 1
        found_old_data = False
        
        while not found_old_data:
            params['page'] = page
            
            try:
                response = requests.get(
                    get_api_url(brand, "tickets/source/details"),
                    params=params,
                    headers=get_api_headers(brand),
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    ticket_sources = data.get('ticket_sources', [])
                    
                    if not ticket_sources:
                        break
                    
                    all_sources.extend(ticket_sources)
                    logger.info(f"Page {page}: fetched {len(ticket_sources)} sources (total: {len(all_sources)})")
                    
                    # Check if we should stop (data older than MAX_DAYS_BACK)
                    try:
                        current_date = datetime.now().date()
                        for source in ticket_sources:
                            if source.get('updated_at'):
                                updated_at_date = datetime.strptime(
                                    source['updated_at'], 
                                    "%Y-%m-%d %H:%M:%S"
                                ).date()
                                
                                if updated_at_date <= current_date - timedelta(days=MAX_DAYS_BACK):
                                    logger.info(f"Found data older than {MAX_DAYS_BACK} days, stopping pagination")
                                    found_old_data = True
                                    break
                    except Exception as e:
                        logger.warning(f"Error checking date: {e}")
                    
                    page += 1
                    time.sleep(2)  # Rate limiting
                    
                else:
                    logger.error(f"API Error {response.status_code}: {response.text[:200]}")
                    break
                    
            except Exception as e:
                logger.error(f"Exception fetching page {page}: {e}")
                break
        
        logger.success(f"✅ Fetched {len(all_sources)} ticket sources")
        return all_sources
    
    def run(self, brand: dict):
        """
        Execute ticket source metadata ingestion for a single brand
        
        Args:
            brand: Brand configuration dict
        """
        logger.info("=" * 80)
        logger.info(f"Starting Ticket Source Metadata Ingestion for brand {brand['erp_id']}")
        logger.info("=" * 80)
        
        # Ensure table exists
        metadata_table = self.ensure_table_exists()
        
        # DELETE old partition data (snapshot overwrite pattern)
        try:
            logger.info(f"🗑️  Deleting old data for brand {brand['erp_id']}, snapshot {self.snapshot_date}...")
            metadata_table.delete(
                And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand["erp_id"])
                )
            )
            logger.success(f"✅ Deleted old partition data")
        except Exception as e:
            logger.warning(f"Could not delete old data (table might be empty): {e}")
        
        # Fetch ticket sources
        ticket_sources = self.fetch_ticket_sources(brand)
        
        if not ticket_sources:
            logger.warning("No ticket sources fetched")
            return
        
        # Transform to records
        import json
        
        def safe_int(val):
            if val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        
        def safe_str(val):
            if val is None:
                return None
            return str(val)
        
        def safe_ts(val):
            if val is None:
                return None
            try:
                return datetime.strptime(val, "%Y-%m-%d %H:%M:%S") if isinstance(val, str) else val
            except (ValueError, TypeError):
                return None
        
        transformed_records = []
        ingestion_timestamp = get_current_timestamp_naive()
        
        for source in ticket_sources:
            record = {
                "id": source.get("id"),
                "source_name": safe_str(source.get("source_name")),
                "index": safe_int(source.get("index")),
                "channel": safe_str(source.get("channel")),
                "utm_source": safe_str(source.get("utm_source")),
                "utm_campaign": safe_str(source.get("utm_campaign")),
                "utm_medium": safe_str(source.get("utm_medium")),
                "utm_term": safe_str(source.get("utm_term")),
                "ad_id": safe_str(source.get("ad_id")),
                "utm_content": safe_str(source.get("utm_content")),
                "adgroup_id": safe_str(source.get("adgroup_id")),
                "source_key": safe_str(source.get("source_key")),
                "updated_at": safe_ts(source.get("updated_at")),
                "created_at": safe_ts(source.get("created_at")),
                
                "brand_id": safe_int(brand["erp_id"]),
                # "raw_data": json.dumps(source, ensure_ascii=False), # Removed due to size
                
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp,
            }
            transformed_records.append(record)
        
        # Write to Iceberg in batches to avoid OOM
        BATCH_SIZE = 50000
        total_records = len(transformed_records)
        pyarrow_schema = schema_to_pyarrow(metadata_table.schema())
        
        logger.info(f"Writing {total_records} records in batches of {BATCH_SIZE}...")
        
        for i in range(0, total_records, BATCH_SIZE):
            batch = transformed_records[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info(f"Writing batch {batch_num}/{total_batches} ({len(batch)} records)...")
            
            df = pa.Table.from_pylist(batch, schema=pyarrow_schema)
            metadata_table.append(df)
            
            # Free memory
            del df
            del batch
            gc.collect()
        
        logger.success(f"✅ Wrote {total_records} ticket sources to {self.TICKET_SOURCE_METADATA_TABLE}")
        gc.collect()


def main():
    """
    Main entry point
    Usage: python ticket_source_metadata.py <snapshot_date>
    """
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    # Load all active brands
    brands = get_active_brands()
    
    if not brands:
        logger.error("No active brands found")
        sys.exit(1)
    
    logger.info(f"Found {len(brands)} active brands to process")
    
    # Initialize job once
    job = TicketSourceMetadata(snapshot_date)
    
    # Process each brand sequentially
    total_success = 0
    total_errors = 0
    
    for brand in brands:
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing brand: {brand['name']} (erp_id={brand['erp_id']})")
        logger.info(f"{'='*80}")
        
        try:
            job.run(brand=brand)
            total_success += 1
        except Exception as e:
            logger.error(f"Failed to process brand {brand['erp_id']}: {e}")
            total_errors += 1
    
    logger.info(f"\n{'='*80}")
    logger.success(f"Ticket source metadata ingestion completed. Success: {total_success}/{len(brands)}, Errors: {total_errors}")
    logger.info(f"{'='*80}")


if __name__ == "__main__":
    main()
