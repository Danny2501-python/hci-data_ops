"""
Deal Detail Enrichment: Pull full deal details from CareSoft API
Architecture: Silver → Gold enrichment layer

OLD SYSTEM PROBLEMS:
1. Loop through core_brand table sequentially (slow)
2. Individual API calls per deal_id (N+1 problem)
3. Temporary tables with COPY operations (overhead)
4. Error handling mixed with success logic
5. No batch processing optimization

NEW LAKEHOUSE APPROACH:
1. Process ALL brands in parallel
2. Batch API requests where possible
3. Stream directly to Iceberg (no temp tables)
4. Separate success/error tables with proper partitioning
5. Incremental processing based on webhook updates
"""

import os
import sys
from datetime import datetime, date
import requests
import time
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And, NotIn
from loguru import logger
import gc

# Iceberg configuration
ICEBERG_REST_URI_SILVER = os.getenv('ICEBERG_REST_URI_SILVER')
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD')
CATALOG_NAME_SILVER = os.getenv('CATALOG_NAME_SILVER')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Source tables (Silver)
WEBHOOK_TABLE = 'caresoft.cs_deal_wehook'

# Target tables (Gold)
DEAL_DETAIL_TABLE = 'caresoft.cs_deal_detail'
DEAL_DETAIL_ERROR_TABLE = 'caresoft.cs_deal_detail_error'
BRAND_TABLE = 'caresoft.core_brand'


class DealDetailEnrichment:
    """
    Pull full deal details from CareSoft API based on webhook events
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
        
        self.silver_catalog = load_catalog(
            CATALOG_NAME_SILVER,
            uri=ICEBERG_REST_URI_SILVER,
            **catalog_config
        )
        
        self.gold_catalog = load_catalog(
            CATALOG_NAME_GOLD,
            uri=ICEBERG_REST_URI_GOLD,
            **catalog_config
        )
        
        logger.info(f"Initialized DealDetailEnrichment for {self.snapshot_date}")
    
    def ensure_brand_table_exists(self):
        """Create core_brand table if not exists"""
        try:
            return self.gold_catalog.load_table(BRAND_TABLE)
        except:
            logger.info(f"Creating table {BRAND_TABLE}...")
            
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, LongType, IntegerType, StringType, BooleanType, TimestampType
            
            # Schema matching legacy core_brand table
            schema = Schema(
                NestedField(1, "id", LongType(), required=True),  # int8 → LongType
                NestedField(2, "name", StringType(), required=False),  # varchar(255)
                NestedField(3, "erp_id", IntegerType(), required=False),  # int4
                NestedField(4, "etl_job_name", StringType(), required=False),  # varchar(255)
                NestedField(5, "is_active", BooleanType(), required=False),  # bool
                NestedField(6, "caresoft_domain", StringType(), required=False),  # varchar(255)
                NestedField(7, "caresoft_key", StringType(), required=False),  # varchar(50)
                NestedField(8, "created", TimestampType(), required=False),  # timestamptz
                NestedField(9, "updated", TimestampType(), required=False),  # timestamptz
            )
            
            table = self.gold_catalog.create_table(
                identifier=BRAND_TABLE,
                schema=schema
            )
            logger.success(f"✅ Created table: {BRAND_TABLE}")
            return table
    
    def ensure_deal_detail_table_exists(self):
        """Create deal detail table if not exists"""
        try:
            return self.gold_catalog.load_table(DEAL_DETAIL_TABLE)
        except:
            logger.info(f"Creating table {DEAL_DETAIL_TABLE}...")
            
            from pyiceberg.schema import Schema
            from pyiceberg.types import (
                NestedField, IntegerType, StringType, TimestampType, 
                DateType, LongType
            )
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform
            
            # Schema matching legacy cs_deal_detail_api table
            schema = Schema(
                # Core fields
                NestedField(1, "id", LongType(), required=True),  # numeric(20) → LongType
                NestedField(2, "account_id", IntegerType(), required=False),
                NestedField(3, "deal_no", IntegerType(), required=False),
                NestedField(4, "requester_id", LongType(), required=False),  # numeric(20)
                NestedField(5, "group_id", IntegerType(), required=False),
                NestedField(6, "ticket_source_end_status", IntegerType(), required=False),
                NestedField(7, "assignee_id", LongType(), required=False),  # numeric(20)
                
                # Text fields
                NestedField(8, "ticket_source", StringType(), required=False),
                NestedField(9, "merge_status", StringType(), required=False),
                NestedField(10, "merge_to", StringType(), required=False),
                NestedField(11, "clone_from", StringType(), required=False),
                NestedField(12, "subject", StringType(), required=False),
                
                # Timestamps
                NestedField(13, "created_at", TimestampType(), required=False),
                NestedField(14, "updated_at", TimestampType(), required=False),
                NestedField(15, "satisfaction_at", TimestampType(), required=False),
                NestedField(16, "convert_at", TimestampType(), required=False),
                NestedField(17, "estimated_closed_date", TimestampType(), required=False),
                NestedField(18, "closed_at", TimestampType(), required=False),
                
                # More text fields
                NestedField(19, "duedate", StringType(), required=False),
                NestedField(20, "satisfaction", StringType(), required=False),
                NestedField(21, "satisfaction_content", StringType(), required=False),
                NestedField(22, "satisfaction_send", StringType(), required=False),
                NestedField(23, "campaign_id", StringType(), required=False),
                NestedField(24, "campaign_action_id", StringType(), required=False),
                NestedField(25, "campaign_status", StringType(), required=False),
                NestedField(26, "automessage_id", StringType(), required=False),
                NestedField(27, "manualmessage_id", StringType(), required=False),
                
                # Integer fields
                NestedField(28, "incident_id", IntegerType(), required=False),
                NestedField(29, "service_id", LongType(), required=False),  # numeric(20)
                
                # More text fields
                NestedField(30, "qa_script_id", StringType(), required=False),
                NestedField(31, "qa_agent", StringType(), required=False),
                NestedField(32, "current_agent", StringType(), required=False),
                
                # Integer fields
                NestedField(33, "ticket_source_detail_id", IntegerType(), required=False),
                NestedField(34, "convert_by", LongType(), required=False),  # numeric(20)
                NestedField(35, "convert_type", IntegerType(), required=False),
                
                # Text fields
                NestedField(36, "unqualified_reasons", StringType(), required=False),
                
                # Integer fields
                NestedField(37, "last_lead_status_id", IntegerType(), required=False),
                NestedField(38, "lead_status_id", IntegerType(), required=False),
                
                # Text fields
                NestedField(39, "closed_by", StringType(), required=False),
                NestedField(40, "closed_type", StringType(), required=False),
                
                # Deal-specific fields
                NestedField(41, "pipeline_id", IntegerType(), required=False),
                NestedField(42, "pipeline_stage_id", IntegerType(), required=False),
                NestedField(43, "value", IntegerType(), required=False),
                NestedField(44, "probability", IntegerType(), required=False),
                NestedField(45, "last_deal_stage_id", IntegerType(), required=False),
                
                # Text fields
                NestedField(46, "lost_reasons", StringType(), required=False),
                NestedField(47, "comments", StringType(), required=False),
                
                # JSON/Array fields (stored as text)
                NestedField(48, "custom_fields", StringType(), required=False),
                NestedField(49, "tags", StringType(), required=False),
                NestedField(50, "ccs", StringType(), required=False),
                NestedField(51, "follows", StringType(), required=False),
                NestedField(52, "labels", StringType(), required=False),
                NestedField(53, "assignee", StringType(), required=False),
                NestedField(54, "requester", StringType(), required=False),
                
                # Brand and raw data
                NestedField(55, "brand_id", IntegerType(), required=False),
                NestedField(56, "raw_data", StringType(), required=False),
                
                # Metadata
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            table = self.gold_catalog.create_table(
                identifier=DEAL_DETAIL_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"✅ Created table: {DEAL_DETAIL_TABLE}")
            return table
    
    def ensure_deal_detail_error_table_exists(self):
        """Create deal detail error table if not exists"""
        try:
            return self.gold_catalog.load_table(DEAL_DETAIL_ERROR_TABLE)
        except:
            logger.info(f"Creating table {DEAL_DETAIL_ERROR_TABLE}...")
            
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, LongType, IntegerType, DateType, TimestampType
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform
            
            # Schema matching legacy cs_deal_detail_error_api table + brand_id
            schema = Schema(
                NestedField(1, "id", LongType(), required=False),  # deal_id (optional in case of errors)
                NestedField(2, "brand_id", LongType(), required=False),  # LongType to match erp_id type
                
                # Metadata (not in legacy, but useful for tracking)
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            table = self.gold_catalog.create_table(
                identifier=DEAL_DETAIL_ERROR_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"✅ Created table: {DEAL_DETAIL_ERROR_TABLE}")
            return table
    
    def get_brands(self):
        """Get active brands from core_brand table"""
        brand_table = self.ensure_brand_table_exists()
        
        # Scan for active brands
        scan = brand_table.scan(
            row_filter=EqualTo("is_active", True),
            selected_fields=("id", "name", "erp_id", "caresoft_domain", "caresoft_key")
        )
        
        brands = []
        for batch in scan.to_arrow_batch_reader():
            for i in range(len(batch)):
                brands.append({
                    "id": batch["id"][i].as_py(),
                    "name": batch["name"][i].as_py(),
                    "erp_id": batch["erp_id"][i].as_py(),
                    "caresoft_domain": batch["caresoft_domain"][i].as_py(),
                    "caresoft_key": batch["caresoft_key"][i].as_py(),
                })
        
        logger.info(f"Found {len(brands)} active brands")
        return brands
    
    def get_deal_ids_to_enrich(self, brand_erp_id: int):
        """
        Get deal_ids that need enrichment by comparing with previous snapshot
        
        Incremental enrichment strategy:
        1. Get all deals from TODAY's snapshot with their updated_at
        2. Get all deals from YESTERDAY's snapshot with their updated_at
        3. Only enrich deals where:
           - New deal (not in yesterday) OR
           - updated_at is newer than yesterday
        
        This dramatically reduces API calls!
        
        Args:
            brand_erp_id: The erp_id from core_brand table (maps to brand_etl in webhook)
        """
        webhook_table = self.silver_catalog.load_table(WEBHOOK_TABLE)
        
        logger.info(f"Scanning webhooks for brand_etl={brand_erp_id}, snapshot_date={self.snapshot_date}")
        
        from pyiceberg.expressions import And, NotNull
        from datetime import timedelta
        import duckdb
        
        con = duckdb.connect()
        
        # Step 1: Get TODAY's deals with MAX(updated_at)
        today_scan = webhook_table.scan(
            row_filter=And(
                EqualTo("snapshot_date", self.snapshot_date),
                EqualTo("brand_etl", brand_erp_id),
                NotNull("deal_id")
            ),
            selected_fields=("deal_id", "updated_at")
        )
        
        total_today = 0
        batches_today = []
        
        for batch in today_scan.to_arrow_batch_reader():
            if len(batch) > 0:
                total_today += len(batch)
                batches_today.append(batch)
        
        if total_today == 0:
            logger.info(f"No deals in today's snapshot for brand {brand_erp_id}")
            con.close()
            return []
        
        # Convert RecordBatches to Table and register with DuckDB
        import pyarrow as pa
        today_table = pa.Table.from_batches(batches_today)  # ✅ from_batches, not concat_tables
        con.register('today_deals', today_table)
        
        logger.info(f"Today's snapshot: {total_today} webhook events")
        
        # Step 2: Get YESTERDAY's deals with MAX(updated_at)
        yesterday = self.snapshot_date - timedelta(days=1)
        
        try:
            yesterday_scan = webhook_table.scan(
                row_filter=And(
                    EqualTo("snapshot_date", yesterday),
                    EqualTo("brand_etl", brand_erp_id),
                    NotNull("deal_id")
                ),
                selected_fields=("deal_id", "updated_at")
            )
            
            total_yesterday = 0
            batches_yesterday = []
            
            for batch in yesterday_scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    total_yesterday += len(batch)
                    batches_yesterday.append(batch)
            
            if total_yesterday > 0:
                # Convert RecordBatches to Table and register with DuckDB
                yesterday_table = pa.Table.from_batches(batches_yesterday)
                con.register('yesterday_deals', yesterday_table)
            
            logger.info(f"Yesterday's snapshot: {total_yesterday} webhook events")
            
        except Exception as e:
            logger.warning(f"No yesterday snapshot found ({yesterday}): {e}")
            logger.info("Will enrich ALL deals (first run or missing yesterday data)")
            total_yesterday = 0
        
        # Step 3: Find deals to enrich (NEW or UPDATED)
        if total_yesterday == 0:
            # No yesterday data - enrich all today's deals
            result = con.execute("""
                SELECT DISTINCT deal_id
                FROM today_deals
                WHERE deal_id IS NOT NULL AND deal_id != 0
                ORDER BY deal_id
            """).fetchall()
        else:
            # Compare with yesterday - only enrich NEW or UPDATED deals
            result = con.execute("""
                WITH today_max AS (
                    SELECT deal_id, MAX(updated_at) as max_updated_at
                    FROM today_deals
                    WHERE deal_id IS NOT NULL AND deal_id != 0
                    GROUP BY deal_id
                ),
                yesterday_max AS (
                    SELECT deal_id, MAX(updated_at) as max_updated_at
                    FROM yesterday_deals
                    WHERE deal_id IS NOT NULL AND deal_id != 0
                    GROUP BY deal_id
                )
                SELECT t.deal_id
                FROM today_max t
                LEFT JOIN yesterday_max y ON t.deal_id = y.deal_id
                WHERE 
                    -- New deal (not in yesterday)
                    y.deal_id IS NULL
                    OR
                    -- Updated deal (newer timestamp)
                    t.max_updated_at > y.max_updated_at
                ORDER BY t.deal_id
            """).fetchall()
        
        con.close()
        
        deal_ids = [row[0] for row in result]
        
        logger.info(f"✅ Found {len(deal_ids)} deals to enrich from {total_today} webhook events")
        if total_yesterday > 0:
            savings = total_today - len(deal_ids)
            logger.success(f"💰 Saved {savings} API calls by incremental comparison ({savings/total_today*100:.1f}%)")
        
        return deal_ids
    
    def call_caresoft_api(self, brand: dict, deal_id: int):
        """Call CareSoft API to get deal details"""
        domain = brand["caresoft_domain"]
        api_key = brand["caresoft_key"]
        
        url = f"https://{domain}/api/deal/{deal_id}"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return response.json().get("deal"), None
            else:
                return None, f"HTTP {response.status_code}"
                
        except Exception as e:
            return None, str(e)
    
    def run(self):
        """Execute deal detail enrichment pipeline"""
        logger.info("=" * 80)
        logger.info(f"Starting Deal Detail Enrichment for {self.snapshot_date}")
        logger.info("=" * 80)
        
        # Ensure tables exist
        detail_table = self.ensure_deal_detail_table_exists()
        error_table = self.ensure_deal_detail_error_table_exists()
        
        logger.info(f"📊 Writing to table: {DEAL_DETAIL_TABLE}")
        logger.info(f"📊 Error table: {DEAL_DETAIL_ERROR_TABLE}")
        
        # Get brands
        brands = self.get_brands()
        
        if not brands:
            logger.warning("No active brands found")
            return
        
        total_success = 0
        total_errors = 0
        
        # Process each brand
        for brand in brands:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing brand: {brand['name']} (ID: {brand['id']})")
            
            # Get deal IDs to enrich
            deal_ids = self.get_deal_ids_to_enrich(brand["erp_id"])
            
            if not deal_ids:
                logger.info(f"No deals to enrich for brand {brand['name']}")
                continue
            
            logger.info(f"Enriching {len(deal_ids)} deals...")
            
            # DELETE old partition data for this brand+snapshot (snapshot overwrite pattern)
            try:
                from pyiceberg.expressions import And
                logger.info(f"🗑️  Deleting old data for brand {brand['erp_id']}, snapshot {self.snapshot_date}...")
                
                # Delete from detail table
                detail_table.delete(
                    And(
                        EqualTo("snapshot_date", self.snapshot_date),
                        EqualTo("brand_id", brand["erp_id"])
                    )
                )
                
                # Delete from error table
                error_table.delete(
                    And(
                        EqualTo("snapshot_date", self.snapshot_date),
                        EqualTo("brand_id", brand["erp_id"])
                    )
                )
                
                logger.success(f"✅ Deleted old partition data")
            except Exception as e:
                logger.warning(f"Could not delete old data (table might be empty): {e}")
            
            success_records = []
            error_records = []
            
            # Process deals in micro-batches
            BATCH_SIZE = 10
            batch_records = []
            
            for i, deal_id in enumerate(deal_ids):
                # Progress logging every 10 deals
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i + 1}/{len(deal_ids)} deals ({(i+1)/len(deal_ids)*100:.1f}%)")
                
                # Call API
                deal_data, error = self.call_caresoft_api(brand, deal_id)
                
                if deal_data:
                    # Success - prepare record
                    import json
                    record = {
                        "id": deal_data.get("id"),
                        "brand_id": brand["erp_id"],
                        "name": deal_data.get("name"),
                        "value": deal_data.get("value"),
                        "probability": deal_data.get("probability"),
                        "pipeline_id": deal_data.get("pipeline_id"),
                        "pipeline_stage_id": deal_data.get("pipeline_stage_id"),
                        "closed_type": deal_data.get("closed_type"),
                        "source": deal_data.get("source"),
                        "source_detail_id": deal_data.get("source_detail_id"),
                        "service_id": deal_data.get("service_id"),
                        "requester_id": deal_data.get("requester_id"),
                        "assignee_id": deal_data.get("assignee_id"),
                        "facebook_page_id": deal_data.get("facebook_page_id"),
                        "ticket_source_end_status": deal_data.get("ticket_source_end_status"),
                        "custom_fields": json.dumps(deal_data.get("custom_fields", {})),
                        "raw_data": json.dumps(deal_data),
                        "snapshot_date": self.snapshot_date,
                        "ingestion_timestamp": datetime.now(),
                    }
                    batch_records.append(record)
                    
                else:
                    # Error - write immediately
                    # Log error for debugging (only first 5 errors to avoid spam)
                    if total_errors < 5:
                        logger.error(f"❌ Failed for deal {deal_id}: {error}")
                    elif total_errors == 5:
                         logger.error(f"❌ ... suppressing further error logs")

                    error_record = {
                        "id": deal_id,
                        "brand_id": brand["erp_id"],
                        "snapshot_date": self.snapshot_date,
                        "ingestion_timestamp": datetime.now(),
                    }
                    df_err = pa.Table.from_pylist([error_record])
                    error_table.append(df_err)
                    total_errors += 1
                
                # Rate limiting
                time.sleep(0.05)  # Faster sleep since we batch writes
                
                # Flush batch
                if len(batch_records) >= BATCH_SIZE:
                    df = pa.Table.from_pylist(batch_records)
                    detail_table.append(df)
                    total_success += len(batch_records)
                    
                    # LOG PROOF OF WRITE
                    try:
                        snapshot_id = detail_table.current_snapshot().snapshot_id
                        logger.success(f"✅ Flushed {len(batch_records)} records. Current Snapshot ID: {snapshot_id}")
                    except:
                        logger.success(f"✅ Flushed {len(batch_records)} records.")
                        
                    batch_records = []
            
            # Flush remaining
            if batch_records:
                df = pa.Table.from_pylist(batch_records)
                detail_table.append(df)
                total_success += len(batch_records)
                logger.success(f"✅ Flushed final {len(batch_records)} records.")
            
            gc.collect()
        
        logger.success(f"Deal enrichment completed. Success: {total_success}, Errors: {total_errors}")


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    job = DealDetailEnrichment(snapshot_date)
    job.run()


if __name__ == "__main__":
    main()
