import os
import sys
import json
import time
import requests
import gc
import pyarrow as pa
from datetime import date, datetime, timedelta
from loguru import logger
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, TimestampType, BooleanType, DateType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

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

# CareSoft API configuration
CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE'))

# Target table (Bronze) - matching old system naming
CONTACT_BRONZE_TABLE = 'caresoft.cs_contacts_api_ttc'


class ContactIngestion:
    """Pull contacts from CareSoft API and store in Bronze Iceberg table
    Supports incremental updates based on updated_since/updated_to
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        
        logger.info(f"Initializing ContactIngestion for snapshot_date={self.snapshot_date}")
        
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
        
        # Ensure table exists
        self.ensure_contact_bronze_table_exists()
    
    def ensure_contact_bronze_table_exists(self):
        """Create contact bronze table if not exists"""
        try:
            table = self.bronze_catalog.load_table(CONTACT_BRONZE_TABLE)
            logger.info(f"Table {CONTACT_BRONZE_TABLE} already exists")
        except Exception:
            logger.info(f"Creating table {CONTACT_BRONZE_TABLE}")
            
            # Schema matching old system cs_contacts_API_TTC
            schema = Schema(
                NestedField(1, "id", IntegerType(), required=True),
                NestedField(2, "created_at", TimestampType(), required=False),
                NestedField(3, "updated_at", TimestampType(), required=False),
                NestedField(4, "phone_no", StringType(), required=False),
                NestedField(5, "username", StringType(), required=False),
                NestedField(6, "email", StringType(), required=False),
                NestedField(7, "brand_id", IntegerType(), required=True),
                NestedField(8, "raw_data", StringType(), required=False),
                
                # Metadata fields
                NestedField(9, "snapshot_date", DateType(), required=True),
                NestedField(10, "ingestion_timestamp", TimestampType(), required=True),
            )
            
            # Partition by snapshot_date for efficient querying
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=9,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.bronze_catalog.create_table(
                identifier=CONTACT_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            
            logger.success(f"Created table {CONTACT_BRONZE_TABLE}")
    
    # Note: get_api_url and get_api_headers are now imported from common.caresoft_api
    
    
    def fetch_contacts(self, brand: dict, days_ago: int = 3) -> list:
        """
        Fetch contacts from CareSoft API with pagination
        
        Args:
            brand: Brand configuration dict
            days_ago: Number of days to look back (default: 3)
        
        Returns:
            List of contact records
        """
        
        # Calculate date range based on snapshot_date (not current time)
        # This allows backfilling historical data
        this_day = datetime.combine(self.snapshot_date, datetime.min.time())
        this_day_ago = this_day - timedelta(days=days_ago)
        
        updated_since = this_day_ago.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + 'Z'
        updated_to = this_day.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + 'Z'
        
        # Normalize to YYYY-MM-DDTHH:MM:SSZ format
        updated_since = updated_since[:10] + 'T00:00:00Z'
        updated_to = updated_to[:10] + 'T00:00:00Z'
        
        logger.info(f"Fetching contacts for brand {brand['name']} from {updated_since} to {updated_to}")
        
        page = 1
        contacts_data = []
        
        while True:
            params = {
                'updated_since': updated_since,
                'updated_to': updated_to,
                'page': page,
                'count': CARESOFT_PAGE_SIZE
            }
            
            try:
                url = get_api_url(brand, "contacts")
                headers = get_api_headers(brand)
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_contacts = data.get('contacts', [])
                    
                    if not new_contacts:
                        logger.info(f"No more contacts on page {page}")
                        break
                    
                    contacts_data.extend(new_contacts)
                    logger.info(f"Fetched {len(new_contacts)} contacts from page {page}")
                    
                    page += 1
                    time.sleep(2)  # Rate limiting
                else:
                    logger.error(f"API request failed with status {response.status_code}: {response.text[:500]}")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching contacts page {page}: {e}")
                break
        
        logger.success(f"Total contacts fetched: {len(contacts_data)}")
        return contacts_data
    
    def run(self, brand: dict, days_ago: int = 3):
        """
        Execute contact ingestion pipeline for a single brand
        
        Args:
            brand: Brand configuration dict with caresoft_domain, caresoft_key, erp_id
            days_ago: Number of days to look back (default: 3)
        """
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info("=" * 80)
        logger.info(f"Starting contact ingestion for brand: {brand_name} (erp_id={brand_erp_id})")
        logger.info("=" * 80)
        
        # Fetch contacts from API
        contacts = self.fetch_contacts(brand, days_ago)
        
        if not contacts:
            logger.warning(f"No contacts found for brand {brand_name}")
            return
        
        # Transform to Iceberg records
        ingestion_timestamp = get_current_timestamp_naive()
        
        # Helper functions for safe type conversion
        def safe_int(val):
            if val is None or val == '':
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        
        def safe_str(val):
            if val is None:
                return ''
            return str(val)
        
        def safe_ts(val):
            """Convert timestamp string or int to datetime"""
            if val is None or val == '':
                return None
            try:
                # Handle integer milliseconds timestamp
                if isinstance(val, int):
                    return datetime.fromtimestamp(val / 1000.0)
                # Handle string timestamp formats
                if isinstance(val, str):
                    # Try ISO format with 'T': "2026-01-16T23:13:40Z"
                    if 'T' in val:
                        return datetime.fromisoformat(val.replace('Z', '+00:00'))
                    # Try format without 'T': "2026-01-16 23:13:40"
                    else:
                        return datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
                return None
            except (ValueError, TypeError):
                return None
        
        records = []
        for contact in contacts:
            record = {
                "id": safe_int(contact.get("id")),
                "created_at": safe_ts(contact.get("created_at")),
                "updated_at": safe_ts(contact.get("updated_at")),
                "phone_no": safe_str(contact.get("phone_no")),
                "username": safe_str(contact.get("username")),
                "email": safe_str(contact.get("email")),
                "brand_id": brand_erp_id,
                "raw_data": json.dumps(contact, ensure_ascii=False),
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp,
            }
            
            records.append(record)
        
        # Load table
        table = self.bronze_catalog.load_table(CONTACT_BRONZE_TABLE)
        
        # DELETE old partition data (snapshot overwrite pattern)
        # This allows re-running the same snapshot_date without duplicates
        try:
            logger.info(f"🗑️  Deleting old data for brand {brand_name}, snapshot {self.snapshot_date}...")
            table.delete(
                And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand_erp_id)
                )
            )
            logger.success(f"✅ Deleted old partition data")
        except Exception as e:
            logger.warning(f"Could not delete old data (table might be empty): {e}")
        
        # Write to Iceberg Bronze table
        logger.info(f"Writing {len(records)} contact records to {CONTACT_BRONZE_TABLE}")
        
        # Convert to PyArrow Table
        try:
            pa_schema = schema_to_pyarrow(table.schema())
            df = pa.Table.from_pylist(records, schema=pa_schema)
            table.append(df)
        except Exception as e:
            logger.error(f"Failed to convert/append records: {e}")
            raise e
        
        logger.success(f"Successfully wrote {len(records)} contacts for brand {brand_name}")
        
        gc.collect()


def main():
    """
    Main entry point
    Usage: python contact_ingestion.py <snapshot_date>
    """
    # Parse snapshot_date from command line or use today
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    logger.info(f"Starting Contact Ingestion for snapshot_date={snapshot_date}")
    
    # Load all active brands from Gold layer
    brands = get_active_brands()
    
    if not brands:
        logger.error("No active brands found in core_brand table")
        sys.exit(1)
    
    logger.info(f"Found {len(brands)} active brands to process")
    
    # Initialize job once
    job = ContactIngestion(snapshot_date)
    
    # Process each brand sequentially
    total_success = 0
    total_errors = 0
    
    for brand in brands:
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing brand: {brand['name']} (erp_id={brand['erp_id']})")
        logger.info(f"{'='*80}")
        
        try:
            job.run(brand=brand, days_ago=3)
            total_success += 1
        except Exception as e:
            logger.error(f"Failed to process brand {brand['erp_id']}: {e}")
            total_errors += 1
    
    logger.success(f"Completed. Success: {total_success}/{len(brands)}, Errors: {total_errors}")


if __name__ == "__main__":
    main()
