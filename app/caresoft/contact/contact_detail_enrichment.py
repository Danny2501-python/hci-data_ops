import os
import sys
import json
import time
import requests
import gc
from datetime import date, datetime, timedelta
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, TimestampType, BooleanType, DateType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.expressions import EqualTo, And
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from common.brand_config import get_active_brands

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
ICEBERG_REST_URI_SILVER = os.getenv('ICEBERG_REST_URI_SILVER')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')
CATALOG_NAME_SILVER = os.getenv('CATALOG_NAME_SILVER')

# S3 configuration
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Source and target tables - matching old system naming
CONTACT_BRONZE_TABLE = 'caresoft.cs_contacts_api_ttc'
CONTACT_DETAIL_SILVER_TABLE = 'caresoft.cs_contact_api'
CONTACT_DETAIL_ERROR_TABLE = 'caresoft.cs_contact_error'


class ContactDetailEnrichment:
    """Pull full contact details from CareSoft API based on Bronze contact data
    Optimized for Lakehouse architecture
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        self.silver_catalog = None
        
        logger.info(f"Initializing ContactDetailEnrichment for snapshot_date={self.snapshot_date}")
        
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
        
        # Connect to Silver catalog
        self.silver_catalog = load_catalog(
            CATALOG_NAME_SILVER,
            **{
                "uri": ICEBERG_REST_URI_SILVER,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
                "s3.region": AWS_REGION,
            }
        )
        
        # Ensure tables exist
        self.ensure_contact_detail_table_exists()
        self.ensure_contact_detail_error_table_exists()
    
    def ensure_contact_detail_table_exists(self):
        """Create contact detail silver table if not exists"""
        try:
            table = self.silver_catalog.load_table(CONTACT_DETAIL_SILVER_TABLE)
            logger.info(f"Table {CONTACT_DETAIL_SILVER_TABLE} already exists")
        except Exception:
            logger.info(f"Creating table {CONTACT_DETAIL_SILVER_TABLE}")
            
            # Schema matching old system cs_contact_api (DDL: all text except timestamps)
            schema = Schema(
                NestedField(1, "account_id", StringType(), required=False),
                NestedField(2, "id", IntegerType(), required=True),
                NestedField(3, "username", StringType(), required=False),
                NestedField(4, "email", StringType(), required=False),
                NestedField(5, "email2", StringType(), required=False),
                NestedField(6, "phone_no", StringType(), required=False),
                NestedField(7, "phone_no2", StringType(), required=False),
                NestedField(8, "phone_no3", StringType(), required=False),
                NestedField(9, "facebook", StringType(), required=False),
                NestedField(10, "gender", StringType(), required=False),
                NestedField(11, "organization_id", StringType(), required=False),
                NestedField(12, "address", StringType(), required=False),
                NestedField(13, "city_id", StringType(), required=False),
                NestedField(14, "created_from", StringType(), required=False),
                NestedField(15, "district_id", StringType(), required=False),
                NestedField(16, "created_at", TimestampType(), required=False),
                NestedField(17, "updated_at", TimestampType(), required=False),
                NestedField(18, "role_id", StringType(), required=False),
                NestedField(19, "campaign_handler_id", StringType(), required=False),
                NestedField(20, "take_phone_at", StringType(), required=False),
                NestedField(21, "take_email_at", StringType(), required=False),
                NestedField(22, "custom_fields", StringType(), required=False),
                NestedField(23, "organization", StringType(), required=False),
                NestedField(24, "psid", StringType(), required=False),
                NestedField(25, "brand_id", IntegerType(), required=True),
                NestedField(26, "raw_data", StringType(), required=False),
                
                # Metadata fields
                NestedField(27, "snapshot_date", DateType(), required=True),
                NestedField(28, "ingestion_timestamp", TimestampType(), required=True),
            )
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=27,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.silver_catalog.create_table(
                identifier=CONTACT_DETAIL_SILVER_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            
            logger.success(f"Created table {CONTACT_DETAIL_SILVER_TABLE}")
    
    def ensure_contact_detail_error_table_exists(self):
        """Create contact detail error table if not exists"""
        try:
            table = self.silver_catalog.load_table(CONTACT_DETAIL_ERROR_TABLE)
            logger.info(f"Table {CONTACT_DETAIL_ERROR_TABLE} already exists")
        except Exception:
            logger.info(f"Creating table {CONTACT_DETAIL_ERROR_TABLE}")
            
            # Schema matching old system cs_contact_error (DDL: id, brand_id, trangthai)
            schema = Schema(
                NestedField(1, "id", IntegerType(), required=True),
                NestedField(2, "brand_id", IntegerType(), required=True),
                NestedField(3, "trangthai", IntegerType(), required=False),
                
                # Metadata fields for lakehouse
                NestedField(4, "snapshot_date", DateType(), required=True),
                NestedField(5, "error_timestamp", TimestampType(), required=True),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=4,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.silver_catalog.create_table(
                identifier=CONTACT_DETAIL_ERROR_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            
            logger.success(f"Created table {CONTACT_DETAIL_ERROR_TABLE}")
    
    def get_contact_ids_to_enrich(self, brand_erp_id: int) -> list:
        """
        Get contact_ids from Bronze table (today's snapshot only)
        
        Strategy:
        - Scan contacts from TODAY's Bronze snapshot only
        - Deduplicate by contact_id (MAX updated_at)
        - Return deduplicated contact_ids
        
        Args:
            brand_erp_id: The erp_id from core_brand table
        
        Returns:
            List of contact_ids to enrich
        """
        snapshot_date_str = self.snapshot_date.isoformat()
        
        logger.info(f"Scanning Bronze table for brand_id={brand_erp_id}, snapshot_date={snapshot_date_str}")
        
        # Temp directory for DuckDB and Parquet
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"contact_enrich_ids_{brand_erp_id}_{os.getpid()}.parquet")
        
        try:
            # Load Bronze table
            bronze_table = self.bronze_catalog.load_table(CONTACT_BRONZE_TABLE)
            
            # Scan today's partition AND brand_id (optimization)
            scan = bronze_table.scan(
                row_filter=And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand_erp_id)
                )
            )
            
            # Collect batches to Arrow Table
            batches = []
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    batches.append(batch)
            
            if not batches:
                logger.warning(f"No contacts found in Bronze for brand_id={brand_erp_id}")
                return []
            
            # Write to Parquet (avoid large memory footprint in DuckDB registration)
            combined_table = pa.Table.from_batches(batches)
            pq.write_table(combined_table, temp_parquet)
            
            # Use DuckDB for deduplication
            conn = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            
            # Deduplicate: Get latest contact per id
            query = f"""
            SELECT id as contact_id
            FROM (
                SELECT 
                    id,
                    updated_at,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
                FROM read_parquet('{temp_parquet}')
            ) t
            WHERE rn = 1
            ORDER BY updated_at DESC
            """
            
            result = conn.execute(query).fetchall()
            contact_ids = [row[0] for row in result]
            conn.close()
            
            logger.info(f"Found {len(contact_ids)} unique contacts to enrich")
            
            return contact_ids
            
        finally:
            # Cleanup temp parquet
            if os.path.exists(temp_parquet):
                try:
                    os.remove(temp_parquet)
                except:
                    pass
    
    def call_caresoft_api(self, brand: dict, contact_id: int) -> tuple:
        """Call CareSoft API to get contact details"""
        subdomain = brand["caresoft_domain"]
        api_key = brand["caresoft_key"]
        
        url = f"https://api.caresoft.vn/{subdomain}/api/v1/contacts/{contact_id}"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return data.get("contact"), None
            else:
                return None, f"HTTP {response.status_code}: {response.text[:500]}"
                
        except Exception as e:
            return None, str(e)
    
    def run(self, brand: dict):
        """
        Execute contact detail enrichment pipeline
        
        Args:
            brand: Brand configuration dict
        """
        brand_erp_id = brand["erp_id"]
        brand_name = brand["name"]
        
        logger.info("=" * 80)
        logger.info(f"Starting contact detail enrichment for brand: {brand_name} (erp_id={brand_erp_id})")
        logger.info("=" * 80)
        
        # Get contact IDs to enrich from Bronze
        contact_ids = self.get_contact_ids_to_enrich(brand_erp_id)
        
        if not contact_ids:
            logger.warning(f"No contacts to enrich for brand {brand_name}")
            return
        
        snapshot_date_str = self.snapshot_date.isoformat()
        
        # DELETE old partition data (Snapshot Overwrite Pattern)
        # This prevents duplicates if re-running for the same day
        try:
            logger.info(f"🗑️  Cleaning up old data for brand {brand_name} (snapshot={snapshot_date_str})...")
            
            # Delete from Silver table
            silver_table = self.silver_catalog.load_table(CONTACT_DETAIL_SILVER_TABLE)
            silver_table.delete(
                And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand_erp_id)
                )
            )
            
            # Delete from Error table
            error_table = self.silver_catalog.load_table(CONTACT_DETAIL_ERROR_TABLE)
            error_table.delete(
                And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand_erp_id)
                )
            )
            
            logger.success(f"✅ Cleaned up old partition data for brand {brand_name}")
            
        except Exception as e:
            logger.warning(f"Could not clean up old data (tables might be new/empty): {e}")

        logger.info(f"Enriching {len(contact_ids)} contacts for brand {brand_name}")
        
        # Fetch details for each contact
        snapshot_date_str = self.snapshot_date.isoformat()
        ingestion_timestamp = datetime.now()
        
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
        

        
        success_records = []
        error_records = []
        
        for i, contact_id in enumerate(contact_ids, 1):
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(contact_ids)} contacts processed")
            
            contact_data, error = self.call_caresoft_api(brand, contact_id)
            
            if contact_data:
                # Transform contact data (matching DDL: most fields are text)
                record = {
                    "account_id": safe_str(contact_data.get("account_id")),
                    "id": safe_int(contact_data.get("id")),
                    "username": safe_str(contact_data.get("username")),
                    "email": safe_str(contact_data.get("email")),
                    "email2": safe_str(contact_data.get("email2")),
                    "phone_no": safe_str(contact_data.get("phone_no")),
                    "phone_no2": safe_str(contact_data.get("phone_no2")),
                    "phone_no3": safe_str(contact_data.get("phone_no3")),
                    "facebook": safe_str(contact_data.get("facebook")),
                    "gender": safe_str(contact_data.get("gender")),
                    "organization_id": safe_str(contact_data.get("organization_id")),
                    "address": safe_str(contact_data.get("address")),
                    "city_id": safe_str(contact_data.get("city_id")),
                    "created_from": safe_str(contact_data.get("created_from")),
                    "district_id": safe_str(contact_data.get("district_id")),
                    "created_at": safe_ts(contact_data.get("created_at")),
                    "updated_at": safe_ts(contact_data.get("updated_at")),
                    "role_id": safe_str(contact_data.get("role_id")),
                    "campaign_handler_id": safe_str(contact_data.get("campaign_handler_id")),
                    "take_phone_at": safe_str(contact_data.get("take_phone_at")),
                    "take_email_at": safe_str(contact_data.get("take_email_at")),
                    "custom_fields": json.dumps(contact_data.get("custom_fields", []), ensure_ascii=False),
                    "organization": safe_str(contact_data.get("organization")),
                    "psid": json.dumps(contact_data.get("psid", []), ensure_ascii=False),
                    "brand_id": brand_erp_id,
                    "raw_data": json.dumps(contact_data, ensure_ascii=False),
                    "snapshot_date": self.snapshot_date,
                    "ingestion_timestamp": ingestion_timestamp,
                }
                
                success_records.append(record)
                
            else:
                # Log error (matching DDL: id, brand_id, trangthai)
                error_record = {
                    "id": contact_id,
                    "brand_id": brand_erp_id,
                    "trangthai": 1,  # 1 = error status
                    "snapshot_date": self.snapshot_date,
                    "error_timestamp": ingestion_timestamp,
                }
                error_records.append(error_record)
                logger.warning(f"Failed to fetch contact {contact_id}: {error}")
            
            # Rate limiting
            time.sleep(0.05)
            
            # Batch write every 1000 records
            if len(success_records) >= 1000:
                logger.info(f"Writing batch of {len(success_records)} contact details to Silver")
                silver_table = self.silver_catalog.load_table(CONTACT_DETAIL_SILVER_TABLE)
                pa_schema = schema_to_pyarrow(silver_table.schema())
                df = pa.Table.from_pylist(success_records, schema=pa_schema)
                silver_table.append(df)
                success_records = []
        
        # Write remaining records
        if success_records:
            logger.info(f"Writing final batch of {len(success_records)} contact details to Silver")
            silver_table = self.silver_catalog.load_table(CONTACT_DETAIL_SILVER_TABLE)
            pa_schema = schema_to_pyarrow(silver_table.schema())
            df = pa.Table.from_pylist(success_records, schema=pa_schema)
            silver_table.append(df)
        
        # Write errors
        if error_records:
            logger.warning(f"Writing {len(error_records)} errors to error table")
            error_table = self.silver_catalog.load_table(CONTACT_DETAIL_ERROR_TABLE)
            pa_schema = schema_to_pyarrow(error_table.schema())
            df = pa.Table.from_pylist(error_records, schema=pa_schema)
            error_table.append(df)
        
        logger.success(f"Completed enrichment for brand {brand_name}: {len(contact_ids) - len(error_records)} success, {len(error_records)} errors")
        
        gc.collect()


def main():
    """
    Main entry point
    Usage: python contact_detail_enrichment.py <snapshot_date>
    """
    # Parse snapshot_date from command line or use today
    snapshot_date = date.today()
    if len(sys.argv) > 1:
        try:
            snapshot_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid date format: {sys.argv[1]}. Expected YYYY-MM-DD")
            sys.exit(1)
    
    logger.info(f"Starting Contact Detail Enrichment for snapshot_date={snapshot_date}")
    
    # Load all active brands
    brands = get_active_brands()
    
    if not brands:
        logger.error("No active brands found in core_brand table")
        sys.exit(1)
    
    logger.info(f"Found {len(brands)} active brands to process")
    
    # Initialize job once
    job = ContactDetailEnrichment(snapshot_date)
    
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
    
    logger.success(f"Completed. Success: {total_success}/{len(brands)}, Errors: {total_errors}")


if __name__ == "__main__":
    main()
