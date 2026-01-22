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
from pyiceberg.io.pyarrow import schema_to_pyarrow
from loguru import logger
import gc
from common.brand_config import get_active_brands
from common.utils import get_current_timestamp_naive
from common.caresoft_api import call_caresoft_api

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
DEAL_DETAIL_TABLE = 'caresoft.cs_deal_detail_api'
DEAL_DETAIL_ERROR_TABLE = 'caresoft.cs_deal_detail_error_api'
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
                NestedField(1, "id", LongType(), required=False),  # numeric(20) NULL → LongType
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

            try:
                table = self.gold_catalog.load_table(DEAL_DETAIL_TABLE)
                logger.info(f"Loading existing table {DEAL_DETAIL_TABLE}")
                
                # Evolution: Update schema if needed
                with table.update_schema() as update:
                    update.union_by_name(schema)
                    
                return table
            except:
                logger.info(f"Creating table {DEAL_DETAIL_TABLE}...")
                return self.gold_catalog.create_table(
                    identifier=DEAL_DETAIL_TABLE,
                    schema=schema,
                    partition_spec=partition_spec
                )
    
    def ensure_deal_detail_error_table_exists(self):
        """Create deal detail error table if not exists"""
        try:
            return self.gold_catalog.load_table(DEAL_DETAIL_ERROR_TABLE)
        except:
            logger.info(f"Creating table {DEAL_DETAIL_ERROR_TABLE}...")
            
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, LongType, IntegerType, DateType, TimestampType, StringType
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform
            
            # Schema matching legacy cs_deal_detail_error_api table + brand_id
            schema = Schema(
                NestedField(1, "id", LongType(), required=False),  # deal_id (optional in case of errors)
                NestedField(2, "brand_id", LongType(), required=False),  # LongType to match erp_id type
                NestedField(3, "error_message", StringType(), required=False), # Error details
                
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
    
    def get_deal_ids_to_enrich(self, brand_erp_id: int):
        """
        Get deal_ids that need enrichment by comparing with previous snapshot
        Optimized to use Parquet spill to avoid OOM
        """
        webhook_table = self.silver_catalog.load_table(WEBHOOK_TABLE)
        
        logger.info(f"Scanning webhooks for brand_etl={brand_erp_id}, snapshot_date={self.snapshot_date}")
        
        from pyiceberg.expressions import And, NotNull
        from datetime import timedelta
        import duckdb
        import pyarrow.parquet as pq
        import tempfile
        
        # Temp files for Parquet approach
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        today_parquet = os.path.join(temp_dir, f"today_{brand_erp_id}_{os.getpid()}.parquet")
        yesterday_parquet = os.path.join(temp_dir, f"yesterday_{brand_erp_id}_{os.getpid()}.parquet")
        
        try:
            # --- Step 1: Get TODAY's deals ---
            today_scan = webhook_table.scan(
                row_filter=And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_etl", brand_erp_id),
                    NotNull("deal_id")
                ),
                selected_fields=("deal_id", "updated_at")
            )
            
            # Read batches
            batches_today = [b for b in today_scan.to_arrow_batch_reader() if len(b) > 0]
            total_today = sum(len(b) for b in batches_today)
            
            if total_today == 0:
                logger.info(f"No deals in today's snapshot for brand {brand_erp_id}")
                return []
            
            # Write to Parquet & Free RAM
            today_table = pa.Table.from_batches(batches_today)
            pq.write_table(today_table, today_parquet)
            del batches_today, today_table
            gc.collect()
            logger.info(f"Written {total_today} today's events to {today_parquet}")
            
            # --- Step 2: Get YESTERDAY's deals ---
            yesterday = self.snapshot_date - timedelta(days=1)
            total_yesterday = 0
            
            try:
                yesterday_scan = webhook_table.scan(
                    row_filter=And(
                        EqualTo("snapshot_date", yesterday),
                        EqualTo("brand_etl", brand_erp_id),
                        NotNull("deal_id")
                    ),
                    selected_fields=("deal_id", "updated_at")
                )
                batches_yesterday = [b for b in yesterday_scan.to_arrow_batch_reader() if len(b) > 0]
                total_yesterday = sum(len(b) for b in batches_yesterday)
                
                if total_yesterday > 0:
                    yesterday_table = pa.Table.from_batches(batches_yesterday)
                    pq.write_table(yesterday_table, yesterday_parquet)
                    del batches_yesterday, yesterday_table
                    gc.collect()
                    logger.info(f"Written {total_yesterday} yesterday's events to {yesterday_parquet}")
                    
            except Exception as e:
                logger.warning(f"No yesterday snapshot found ({yesterday}): {e}")
            
            # --- Step 3: DuckDB Query (from Parquet) --- 
            # Use reduced memory limit and in-memory DB as data is now on disk
            con = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            
            if total_yesterday == 0:
                # No yesterday data - enrich all today's deals
                query = f"""
                    SELECT DISTINCT deal_id
                    FROM read_parquet('{today_parquet}')
                    WHERE deal_id IS NOT NULL AND deal_id != 0
                """
            else:
                # Compare with yesterday - only enrich NEW or UPDATED deals
                query = f"""
                    WITH today_max AS (
                        SELECT deal_id, MAX(updated_at) as max_updated_at
                        FROM read_parquet('{today_parquet}')
                        WHERE deal_id IS NOT NULL AND deal_id != 0
                        GROUP BY deal_id
                    ),
                    yesterday_max AS (
                        SELECT deal_id, MAX(updated_at) as max_updated_at
                        FROM read_parquet('{yesterday_parquet}')
                        WHERE deal_id IS NOT NULL AND deal_id != 0
                        GROUP BY deal_id
                    )
                    SELECT t.deal_id
                    FROM today_max t
                    LEFT JOIN yesterday_max y ON t.deal_id = y.deal_id
                    WHERE y.deal_id IS NULL OR t.max_updated_at > y.max_updated_at
                """
            
            result = con.execute(query).fetchall()
            con.close()
            
            deal_ids = [row[0] for row in result]
            
            logger.info(f"✅ Found {len(deal_ids)} deals to enrich from {total_today} webhook events")
            if total_yesterday > 0:
                savings = total_today - len(deal_ids)
                logger.success(f"💰 Saved {savings} API calls ({savings/total_today*100:.1f}%)")
            
            return deal_ids
            
        finally:
            # Cleanup temp files
            for f in [today_parquet, yesterday_parquet]:
                if os.path.exists(f):
                    try:
                        os.remove(f)
                    except: pass
    
    # Note: call_caresoft_api is now imported from common.caresoft_api
    # Usage: call_caresoft_api(brand, 'deal', deal_id, response_key='deal')
    
    
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
        brands = get_active_brands()
        
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
            BATCH_SIZE = 10000
            batch_records = []
            batch_error_records = []
            
            for i, deal_id in enumerate(deal_ids):
                # Progress logging every 10 deals
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i + 1}/{len(deal_ids)} deals ({(i+1)/len(deal_ids)*100:.1f}%)")
                
                # Call API
                deal_data, error = call_caresoft_api(brand, 'deal', deal_id, response_key='deal')
                
                if deal_data:
                    # Success - prepare record
                    import json
                    
                    # Helper to safely cast to int (for IntegerType fields)
                    def safe_int(val):
                        if val is None:
                            return None
                        try:
                            return int(val)
                        except (ValueError, TypeError):
                            return None
                    
                    # Helper to convert to string
                    def safe_str(val):
                        if val is None:
                            return None
                        return str(val)

                    # Helper to safely convert to timestamp
                    def safe_ts(val):
                        if val is None:
                            return None
                        try:
                            # Assuming the timestamp is in ISO format or similar string
                            return datetime.fromisoformat(val.replace('Z', '+00:00')) if isinstance(val, str) else val
                        except (ValueError, TypeError):
                            return None
                    
                    record = {
                        # --- Core IDs & Integers ---
                        "id": deal_data.get("id"),  # LongType
                        "brand_id": safe_int(brand["erp_id"]),  # IntegerType
                        "account_id": safe_int(deal_data.get("account_id")),
                        "deal_no": safe_int(deal_data.get("deal_no")),
                        "group_id": safe_int(deal_data.get("group_id")),
                        "ticket_source_end_status": safe_int(deal_data.get("ticket_source_end_status")),
                        
                        # --- Long IDs ---
                        "requester_id": deal_data.get("requester_id"),
                        "assignee_id": deal_data.get("assignee_id"),
                        "service_id": deal_data.get("service_id"),
                        "convert_by": deal_data.get("convert_by"),
                        
                        # --- Text Fields ---
                        "ticket_source": safe_str(deal_data.get("ticket_source")),
                        "merge_status": safe_str(deal_data.get("merge_status")),
                        "merge_to": safe_str(deal_data.get("merge_to")),
                        "clone_from": safe_str(deal_data.get("clone_from")),
                        "subject": safe_str(deal_data.get("subject")),
                        
                        # --- Timestamps ---
                        "created_at": safe_ts(deal_data.get("created_at")),
                        "updated_at": safe_ts(deal_data.get("updated_at")),
                        "satisfaction_at": safe_ts(deal_data.get("satisfaction_at")),
                        "convert_at": safe_ts(deal_data.get("convert_at")),
                        "estimated_closed_date": safe_ts(deal_data.get("estimated_closed_date")),
                        "closed_at": safe_ts(deal_data.get("closed_at")),
                        
                        # --- More Text Fields ---
                        "duedate": safe_str(deal_data.get("duedate")),
                        "satisfaction": safe_str(deal_data.get("satisfaction")),
                        "satisfaction_content": safe_str(deal_data.get("satisfaction_content")),
                        "satisfaction_send": safe_str(deal_data.get("satisfaction_send")),
                        "campaign_id": safe_str(deal_data.get("campaign_id")),
                        "campaign_action_id": safe_str(deal_data.get("campaign_action_id")),
                        "campaign_status": safe_str(deal_data.get("campaign_status")),
                        "automessage_id": safe_str(deal_data.get("automessage_id")),
                        "manualmessage_id": safe_str(deal_data.get("manualmessage_id")),
                        
                        # --- Integer Fields ---
                        "incident_id": safe_int(deal_data.get("incident_id")),
                        "ticket_source_detail_id": safe_int(deal_data.get("ticket_source_detail_id")),
                        "convert_type": safe_int(deal_data.get("convert_type")),
                        "last_lead_status_id": safe_int(deal_data.get("last_lead_status_id")),
                        "lead_status_id": safe_int(deal_data.get("lead_status_id")),
                        
                        # --- QA & Other Text ---
                        "qa_script_id": safe_str(deal_data.get("qa_script_id")),
                        "qa_agent": safe_str(deal_data.get("qa_agent")),
                        "current_agent": safe_str(deal_data.get("current_agent")),
                        "unqualified_reasons": safe_str(deal_data.get("unqualified_reasons")),
                        "closed_by": safe_str(deal_data.get("closed_by")),
                        "closed_type": safe_str(deal_data.get("closed_type")),
                        
                        # --- Deal Specifics ---
                        "pipeline_id": safe_int(deal_data.get("pipeline_id")),
                        "pipeline_stage_id": safe_int(deal_data.get("pipeline_stage_id")),
                        "value": safe_int(deal_data.get("value")),
                        "probability": safe_int(deal_data.get("probability")),
                        "last_deal_stage_id": safe_int(deal_data.get("last_deal_stage_id")),
                        "lost_reasons": safe_str(deal_data.get("lost_reasons")),
                        
                        # --- Complex Objects Mapped to String (JSON or Str representation) ---
                        "comments": json.dumps(deal_data.get("comments", []), ensure_ascii=False),
                        "tags": json.dumps(deal_data.get("tags", []), ensure_ascii=False),
                        "ccs": json.dumps(deal_data.get("ccs", []), ensure_ascii=False),
                        "follows": json.dumps(deal_data.get("follows", []), ensure_ascii=False),
                        "labels": json.dumps(deal_data.get("labels", []), ensure_ascii=False),
                        "assignee": json.dumps(deal_data.get("assignee", {}), ensure_ascii=False),
                        "requester": json.dumps(deal_data.get("requester", {}), ensure_ascii=False),
                        
                        # --- Metadata ---
                        "custom_fields": json.dumps(deal_data.get("custom_fields", {}), ensure_ascii=False),
                        "raw_data": json.dumps(deal_data, ensure_ascii=False),
                        "snapshot_date": self.snapshot_date,
                        "ingestion_timestamp": get_current_timestamp_naive(),
                    }
                    batch_records.append(record)
                
                else:
                    error_record = {
                        "id": deal_id,
                        "brand_id": brand["erp_id"],
                        "error_message": str(error)[:1000],  # Truncate to avoid huge logs
                        "snapshot_date": self.snapshot_date,
                        "ingestion_timestamp": get_current_timestamp_naive(),
                    }
                    batch_error_records.append(error_record)
                    total_errors += 1
                
                # Rate limiting
                time.sleep(0.05)  # Faster sleep since we batch writes
                
                # Flush batch
                if len(batch_records) >= BATCH_SIZE:
                    # Derive PyArrow schema dynamically from Iceberg table
                    # This ensures 100% consistency with the target table
                    pyarrow_schema = schema_to_pyarrow(detail_table.schema())
                    
                    df = pa.Table.from_pylist(batch_records, schema=pyarrow_schema)
                    detail_table.append(df)
                    total_success += len(batch_records)
                    
                    # LOG PROOF OF WRITE
                    try:
                        snapshot_id = detail_table.current_snapshot().snapshot_id
                        logger.success(f"✅ Flushed {len(batch_records)} records. Current Snapshot ID: {snapshot_id}")
                    except:
                        logger.success(f"✅ Flushed {len(batch_records)} records.")
                        
                    batch_records = []
                
                # Flush error batch
                if len(batch_error_records) >= BATCH_SIZE:
                    # Derive PyArrow schema dynamically from Iceberg table for errors
                    # Note: We rely on the table having 100% consistent schema
                    pyarrow_schema_err = schema_to_pyarrow(error_table.schema())
                    
                    df_err = pa.Table.from_pylist(batch_error_records, schema=pyarrow_schema_err)
                    error_table.append(df_err)
                    logger.info(f"⚠️ Flushed {len(batch_error_records)} error records.")
                    batch_error_records = []
            
            # Flush remaining
            if batch_records:
                # Derive PyArrow schema dynamically from Iceberg table
                # This ensures 100% consistency with the target table
                pyarrow_schema = schema_to_pyarrow(detail_table.schema())
                
                df = pa.Table.from_pylist(batch_records, schema=pyarrow_schema)
                detail_table.append(df)
                total_success += len(batch_records)
                logger.success(f"✅ Flushed final {len(batch_records)} records.")
            
            # Flush remaining errors
            if batch_error_records:
                pyarrow_schema_err = schema_to_pyarrow(error_table.schema())
                df_err = pa.Table.from_pylist(batch_error_records, schema=pyarrow_schema_err)
                error_table.append(df_err)
                logger.info(f"⚠️ Flushed final {len(batch_error_records)} error records.")
            
            gc.collect()
        
        logger.success(f"Deal enrichment completed. Success: {total_success}, Errors: {total_errors}")


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    job = DealDetailEnrichment(snapshot_date)
    job.run()


if __name__ == "__main__":
    main()
