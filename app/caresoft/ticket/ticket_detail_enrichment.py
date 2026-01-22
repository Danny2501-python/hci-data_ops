"""
Ticket Detail Enrichment: Pull full ticket details from CareSoft API
Architecture: Bronze → Silver enrichment layer

FLOW 2 - STEP 2: Ticket Source Detail API

OLD SYSTEM PROBLEMS:
1. Scan from temporary helper table (cs_ticket_api_new_day)
2. Fixed LIMIT 300 - không linh hoạt
3. Complex temp table operations with COPY
4. No error tracking for failed IDs

NEW LAKEHOUSE APPROACH:
1. Scan from Bronze Iceberg table (cs_ticket_bronze)
2. Incremental processing based on updated_at comparison
3. Stream directly to Silver (no temp tables)
4. Separate error table with proper partitioning
"""

import os
import sys
from datetime import datetime, date, timedelta
import requests
import time
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And, NotNull, GreaterThanOrEqual
from pyiceberg.io.pyarrow import schema_to_pyarrow
from loguru import logger
import gc
from common.brand_config import get_active_brands
from common.utils import get_current_timestamp_naive
from common.caresoft_api import call_caresoft_api

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
ICEBERG_REST_URI_SILVER = os.getenv('ICEBERG_REST_URI_SILVER')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')
CATALOG_NAME_SILVER = os.getenv('CATALOG_NAME_SILVER')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Source and target tables - matching old system naming
TICKET_BRONZE_TABLE = 'caresoft.cs_ticket_api'
TICKET_DETAIL_SILVER_TABLE = 'caresoft.cs_ticket_source_api'
TICKET_DETAIL_ERROR_TABLE = 'caresoft.cs_ticket_source_error_api'


class TicketDetailEnrichment:
    """
    Pull full ticket details from CareSoft API based on Bronze ticket data
    Optimized for Lakehouse architecture
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        
        # Initialize catalogs
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
        
        self.silver_catalog = load_catalog(
            CATALOG_NAME_SILVER,
            uri=ICEBERG_REST_URI_SILVER,
            **catalog_config
        )
        
        logger.info(f"Initialized TicketDetailEnrichment for {self.snapshot_date}")
    
    def ensure_ticket_detail_table_exists(self):
        """Create ticket detail silver table if not exists"""
        try:
            return self.silver_catalog.load_table(TICKET_DETAIL_SILVER_TABLE)
        except:
            logger.info(f"Creating table {TICKET_DETAIL_SILVER_TABLE}...")
            
            from pyiceberg.schema import Schema
            from pyiceberg.types import (
                NestedField, IntegerType, StringType, TimestampType, 
                DateType, LongType, BooleanType
            )
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform
            
            # Extended schema with full ticket details
            schema = Schema(
                # Core IDs
                NestedField(1, "ticket_id", LongType(), required=False),
                NestedField(2, "account_id", IntegerType(), required=False),
                NestedField(3, "sla_id", IntegerType(), required=False),
                NestedField(4, "ticket_no", IntegerType(), required=False),
                NestedField(5, "requester_id", LongType(), required=False),
                NestedField(6, "group_id", IntegerType(), required=False),
                
                # Timestamps
                NestedField(7, "created_at", TimestampType(), required=False),
                NestedField(8, "updated_at", TimestampType(), required=False),
                NestedField(9, "satisfaction_at", TimestampType(), required=False),
                
                # Text fields
                NestedField(10, "duedate", StringType(), required=False),
                NestedField(11, "satisfaction", StringType(), required=False),
                NestedField(12, "satisfaction_send", StringType(), required=False),
                NestedField(13, "satisfaction_content", StringType(), required=False),
                NestedField(14, "automessage_id", StringType(), required=False),
                NestedField(15, "feedback_status", StringType(), required=False),
                
                # Boolean
                NestedField(16, "is_overdue", BooleanType(), required=False),
                
                # Additional IDs
                NestedField(17, "incident_id", IntegerType(), required=False),
                NestedField(18, "service_id", LongType(), required=False),
                NestedField(19, "qa_script_id", StringType(), required=False),
                NestedField(20, "qa_agent", StringType(), required=False),
                NestedField(21, "current_agent", StringType(), required=False),
                NestedField(22, "ticket_source_detail_id", IntegerType(), required=False),
                
                # JSON fields
                NestedField(23, "comments", StringType(), required=False),
                
                # Brand and raw data
                NestedField(24, "brand_id", IntegerType(), required=False),
                NestedField(25, "raw_data", StringType(), required=False),
                
                # Metadata
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            table = self.silver_catalog.create_table(
                identifier=TICKET_DETAIL_SILVER_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"✅ Created table: {TICKET_DETAIL_SILVER_TABLE}")
            return table
    
    def ensure_ticket_detail_error_table_exists(self):
        """Create ticket detail error table if not exists"""
        try:
            return self.silver_catalog.load_table(TICKET_DETAIL_ERROR_TABLE)
        except:
            logger.info(f"Creating table {TICKET_DETAIL_ERROR_TABLE}...")
            
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, LongType, IntegerType, DateType, TimestampType, StringType
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform
            
            schema = Schema(
                NestedField(1, "ticket_id", LongType(), required=False),
                NestedField(2, "brand_id", IntegerType(), required=False),
                NestedField(3, "error_message", StringType(), required=False),
                
                # Metadata
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            table = self.silver_catalog.create_table(
                identifier=TICKET_DETAIL_ERROR_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"✅ Created table: {TICKET_DETAIL_ERROR_TABLE}")
            return table
    
    def get_ticket_ids_to_enrich(self, brand_erp_id: int):
        """
        Get ticket_ids from Bronze table (today's snapshot only)
        Uses DuckDB + Parquet for memory-optimized deduplication.
        
        Strategy:
        - Stream filtered Bronze data to temp Parquet
        - Use DuckDB to deduplicate (MAX updated_at per ticket_id)
        - Return sorted list of ticket_ids
        
        Args:
            brand_erp_id: The erp_id from core_brand table
        
        Returns:
            List of ticket_ids to enrich
        """
        import duckdb
        import pyarrow.parquet as pq
        import tempfile
        import pyarrow as pa
        
        logger.info(f"Scanning Bronze tickets for brand_etl={brand_erp_id}, snapshot={self.snapshot_date}")
        
        bronze_table = self.bronze_catalog.load_table(TICKET_BRONZE_TABLE)
        
        # Temp configuration
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"ticket_enrichment_{brand_erp_id}_{os.getpid()}.parquet")
        
        try:
            # Scan Bronze table - ONLY today's snapshot & specific brand
            scan = bronze_table.scan(
                row_filter=And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand_erp_id),
                    NotNull("ticket_id")
                ),
                selected_fields=("ticket_id", "updated_at")
            )
            
            # Stream to Parquet
            batches = []
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    batches.append(batch)
            
            if not batches:
                logger.info(f"No tickets found in today's snapshot for brand {brand_erp_id}")
                return []
            
            # Write to disk to free RAM
            combined = pa.Table.from_batches(batches)
            pq.write_table(combined, temp_parquet, compression='snappy')
            
            # Free Arrow memory immediately
            del batches
            del combined
            
            # DuckDB Deduplication
            con = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            
            # Deduplicate logic: Group by ticket_id, keep MAX updated_at
            # Order by updated_at DESC to process newest tickets first
            query = f"""
            SELECT ticket_id
            FROM read_parquet('{temp_parquet}')
            WHERE ticket_id IS NOT NULL
            GROUP BY ticket_id
            ORDER BY max(updated_at) DESC NULLS LAST
            """
            
            # Fetch result column
            result_arrow = con.execute(query).fetch_arrow_table()
            ticket_ids = result_arrow.column('ticket_id').to_pylist()
            
            con.close()
            
            logger.info(f"✅ Selected {len(ticket_ids)} unique tickets to enrich from today's snapshot")
            return ticket_ids
            
        finally:
            # Robust cleanup
            if os.path.exists(temp_parquet):
                try:
                    os.remove(temp_parquet)
                except:
                    pass
    
    # Note: call_caresoft_api is now imported from common.caresoft_api
    # Usage: call_caresoft_api(brand, 'tickets', ticket_id, response_key='ticket')
    
    
    def run(self, brand: dict):
        """
        Execute ticket detail enrichment pipeline
        
        Args:
            brand: Brand configuration dict
        """
        logger.info("=" * 80)
        logger.info(f"Starting Ticket Detail Enrichment for brand {brand['erp_id']}")
        logger.info("=" * 80)
        
        # Ensure tables exist
        detail_table = self.ensure_ticket_detail_table_exists()
        error_table = self.ensure_ticket_detail_error_table_exists()
        
        logger.info(f"📊 Writing to table: {TICKET_DETAIL_SILVER_TABLE}")
        logger.info(f"📊 Error table: {TICKET_DETAIL_ERROR_TABLE}")
        
        # Get ticket IDs to enrich
        ticket_ids = self.get_ticket_ids_to_enrich(brand["erp_id"])
        
        if not ticket_ids:
            logger.info(f"No tickets to enrich for brand {brand['erp_id']}")
            return
        
        logger.info(f"Enriching {len(ticket_ids)} tickets...")
        
        # DELETE old partition data (snapshot overwrite pattern)
        try:
            logger.info(f"🗑️  Deleting old data for brand {brand['erp_id']}, snapshot {self.snapshot_date}...")
            
            detail_table.delete(
                And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand["erp_id"])
                )
            )
            
            error_table.delete(
                And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand["erp_id"])
                )
            )
            
            logger.success(f"✅ Deleted old partition data")
        except Exception as e:
            logger.warning(f"Could not delete old data (table might be empty): {e}")
        
        # Process tickets
        batch_records = []
        batch_error_records = []
        total_success = 0
        total_errors = 0
        
        BATCH_SIZE = 10000
        
        for i, ticket_id in enumerate(ticket_ids):
            # Progress logging every 10 tickets
            if (i + 1) % 10 == 0:
                logger.info(f"Progress: {i + 1}/{len(ticket_ids)} tickets ({(i+1)/len(ticket_ids)*100:.1f}%)")
            
            # Call API
            ticket_data, error = call_caresoft_api(brand, 'tickets', ticket_id, response_key='ticket')
            
            if ticket_data:
                # Success - prepare record
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
                        return datetime.fromisoformat(val.replace('Z', '+00:00')) if isinstance(val, str) else val
                    except (ValueError, TypeError):
                        return None
                
                def safe_bool(val):
                    if val is None:
                        return None
                    if isinstance(val, bool):
                        return val
                    return str(val).lower() in ('true', '1', 'yes')
                
                record = {
                    "ticket_id": ticket_data.get("ticket_id"),
                    "account_id": safe_int(ticket_data.get("account_id")),
                    "sla_id": safe_int(ticket_data.get("sla_id")),
                    "ticket_no": safe_int(ticket_data.get("ticket_no")),
                    "requester_id": ticket_data.get("requester_id"),
                    "group_id": safe_int(ticket_data.get("group_id")),
                    
                    "created_at": safe_ts(ticket_data.get("created_at")),
                    "updated_at": safe_ts(ticket_data.get("updated_at")),
                    "satisfaction_at": safe_ts(ticket_data.get("satisfaction_at")),
                    
                    "duedate": safe_str(ticket_data.get("duedate")),
                    "satisfaction": safe_str(ticket_data.get("satisfaction")),
                    "satisfaction_send": safe_str(ticket_data.get("satisfaction_send")),
                    "satisfaction_content": safe_str(ticket_data.get("satisfaction_content")),
                    "automessage_id": safe_str(ticket_data.get("automessage_id")),
                    "feedback_status": safe_str(ticket_data.get("feedback_status")),
                    
                    "is_overdue": safe_bool(ticket_data.get("is_overdue")),
                    
                    "incident_id": safe_int(ticket_data.get("incident_id")),
                    "service_id": ticket_data.get("service_id"),
                    "qa_script_id": safe_str(ticket_data.get("qa_script_id")),
                    "qa_agent": safe_str(ticket_data.get("qa_agent")),
                    "current_agent": safe_str(ticket_data.get("current_agent")),
                    "ticket_source_detail_id": safe_int(ticket_data.get("ticket_source_detail_id")),
                    
                    "comments": json.dumps(ticket_data.get("comments", []), ensure_ascii=False),
                    
                    "brand_id": safe_int(brand["erp_id"]),
                    "raw_data": json.dumps(ticket_data, ensure_ascii=False),
                    
                    "snapshot_date": self.snapshot_date,
                    "ingestion_timestamp": get_current_timestamp_naive(),
                }
                batch_records.append(record)
            
            else:
                error_record = {
                    "ticket_id": ticket_id,
                    "brand_id": brand["erp_id"],
                    "error_message": str(error)[:1000],
                    "snapshot_date": self.snapshot_date,
                    "ingestion_timestamp": get_current_timestamp_naive(),
                }
                batch_error_records.append(error_record)
                total_errors += 1
            
            # Rate limiting
            time.sleep(0.05)
            
            # Flush batch
            if len(batch_records) >= BATCH_SIZE:
                pyarrow_schema = schema_to_pyarrow(detail_table.schema())
                df = pa.Table.from_pylist(batch_records, schema=pyarrow_schema)
                detail_table.append(df)
                total_success += len(batch_records)
                logger.success(f"✅ Flushed {len(batch_records)} records")
                batch_records = []
            
            # Flush error batch
            if len(batch_error_records) >= BATCH_SIZE:
                pyarrow_schema_err = schema_to_pyarrow(error_table.schema())
                df_err = pa.Table.from_pylist(batch_error_records, schema=pyarrow_schema_err)
                error_table.append(df_err)
                logger.info(f"⚠️ Flushed {len(batch_error_records)} error records")
                batch_error_records = []
        
        # Flush remaining
        if batch_records:
            pyarrow_schema = schema_to_pyarrow(detail_table.schema())
            df = pa.Table.from_pylist(batch_records, schema=pyarrow_schema)
            detail_table.append(df)
            total_success += len(batch_records)
            logger.success(f"✅ Flushed final {len(batch_records)} records")
        
        if batch_error_records:
            pyarrow_schema_err = schema_to_pyarrow(error_table.schema())
            df_err = pa.Table.from_pylist(batch_error_records, schema=pyarrow_schema_err)
            error_table.append(df_err)
            logger.info(f"⚠️ Flushed final {len(batch_error_records)} error records")
        
        gc.collect()
        logger.success(f"Ticket enrichment completed. Success: {total_success}, Errors: {total_errors}")


def main():
    """
    Main entry point
    Usage: python ticket_detail_enrichment.py <snapshot_date>
    """
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    # Load all active brands from Gold layer
    brands = get_active_brands()
    
    if not brands:
        logger.error("No active brands found in core_brand table")
        sys.exit(1)
    
    logger.info(f"Found {len(brands)} active brands to process")
    
    # Initialize job once
    job = TicketDetailEnrichment(snapshot_date)
    
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
    logger.success(f"Ticket detail enrichment completed. Success: {total_success}/{len(brands)}, Errors: {total_errors}")
    logger.info(f"{'='*80}")


if __name__ == "__main__":
    main()
