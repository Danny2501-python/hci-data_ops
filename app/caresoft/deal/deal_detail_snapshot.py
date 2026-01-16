"""
Deal Detail Snapshot Logic
Creates a '_final' table containing ONLY the latest version of each deal.

Flow:
1. Read ALL snapshots from Gold Incremental (cs_deal_detail_api)
2. Filter for latest snapshot per deal_id
3. Overwrite cs_deal_detail_api_final table

This simplifies the Serving Layer, which can now just read this table directly.
"""

import os
import sys
from datetime import datetime, date
from loguru import logger
import pyarrow as pa
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, IntegerType, StringType, TimestampType, 
    DateType, LongType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.expressions import EqualTo

# Iceberg configuration
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Table names
SOURCE_TABLE = 'caresoft.cs_deal_detail_api'
TARGET_TABLE = 'caresoft.cs_deal_detail_api_final'


class DealDetailSnapshot:
    """Creates a materialized view (snapshot) of the latest deal state"""
    
    def __init__(self, snapshot_date=None):
        self.snapshot_date = snapshot_date or date.today()
        self.gold_catalog = None
        
    def connect_iceberg(self):
        """Connect to Iceberg Gold catalog"""
        logger.info(f"Connecting to Iceberg Gold catalog at {ICEBERG_REST_URI_GOLD}")
        
        catalog_config = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": AWS_REGION,
        }
        
        self.gold_catalog = load_catalog(
            CATALOG_NAME_GOLD,
            uri=ICEBERG_REST_URI_GOLD,
            **catalog_config
        )
        
        logger.success("✅ Connected to Iceberg Gold catalog")

    def ensure_final_table_exists(self):
        """Create final snapshot table with explicit schema if not exists"""
        try:
            return self.gold_catalog.load_table(TARGET_TABLE)
        except:
            logger.info(f"Creating table {TARGET_TABLE}...")
            
            # Explicit Schema Definition (Matching Gold Incremental Table)
            schema = Schema(
                # Core fields
                NestedField(1, "id", LongType(), required=False),
                NestedField(2, "account_id", IntegerType(), required=False),
                NestedField(3, "deal_no", IntegerType(), required=False),
                NestedField(4, "requester_id", LongType(), required=False),
                NestedField(5, "group_id", IntegerType(), required=False),
                NestedField(6, "ticket_source_end_status", IntegerType(), required=False),
                NestedField(7, "assignee_id", LongType(), required=False),
                
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
                NestedField(29, "service_id", LongType(), required=False),
                
                # More text fields
                NestedField(30, "qa_script_id", StringType(), required=False),
                NestedField(31, "qa_agent", StringType(), required=False),
                NestedField(32, "current_agent", StringType(), required=False),
                
                # Integer fields
                NestedField(33, "ticket_source_detail_id", IntegerType(), required=False),
                NestedField(34, "convert_by", LongType(), required=False),
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
            
            # Partition by snapshot_date (Field ID 100)
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )

            return self.gold_catalog.create_table(
                identifier=TARGET_TABLE, 
                schema=schema, 
                partition_spec=partition_spec
            )

    def create_snapshot(self, target_date_obj: date):
        """
        Read increment/snapshot for specific date, find latest state, and overwrite into final table partition
        """
        import pyarrow.parquet as pq
        import gc
        
        target_date = target_date_obj.strftime('%Y-%m-%d')
        logger.info(f"Processing snapshot for date: {target_date}")
        
        logger.info(f"Reading from source table: {SOURCE_TABLE}")
        source_table = self.gold_catalog.load_table(SOURCE_TABLE)
        
        # Temp file for DuckDB processing
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"snapshot_source_{os.getpid()}.parquet")
        
        try:
            # 1. Stream Source Data (Full History Scan)
            logger.info(f"Streaming FULL source data to temp file (for snapshot {target_date})...")
            
            # Scan all data to find true latest state across history
            scan = source_table.scan()
            
            arrow_schema = schema_to_pyarrow(source_table.schema())
            
            count_scanned = 0
            with pq.ParquetWriter(temp_parquet, arrow_schema) as writer:
                for batch in scan.to_arrow_batch_reader():
                    if len(batch) > 0:
                        batch_table = pa.Table.from_batches([batch])
                        writer.write_table(batch_table)
                        count_scanned += len(batch)
                        del batch, batch_table
            
            logger.info(f"Buffered {count_scanned} rows to {temp_parquet}")
            
            if count_scanned == 0:
                logger.warning(f"No source data found. Skipping write.")
                return

            gc.collect()
            
            # 2. Use DuckDB to find latest state per deal (across full history)
            logger.info("Computing latest state using DuckDB...")
            duckdb_config = {
                'temp_directory': temp_dir,
                'memory_limit': '2GB',          # Safe limit for Pod 3GB
                'preserve_insertion_order': 'false', # Save RAM
                'threads': '1'                  # Single thread for stability on small Pods
            }
            
            con = duckdb.connect(config=duckdb_config)
            logger.info(f"🦆 DuckDB Configured: {duckdb_config}")
            
            # Logic:
            # 1. Find latest record per (id, brand_id) based on original snapshot_date
            # 2. Project all columns BUT replace 'snapshot_date' with current 'target_date'
            query = f"""
                WITH ranked_deals AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY id, brand_id 
                            ORDER BY snapshot_date DESC, ingestion_timestamp DESC
                        ) as rn
                    FROM read_parquet('{temp_parquet}')
                )
                SELECT 
                    * EXCLUDE (rn, snapshot_date, ingestion_timestamp),
                    CAST('{target_date}' AS DATE) as snapshot_date,
                    now() as ingestion_timestamp
                FROM ranked_deals
                WHERE rn = 1
            """
            
            final_table = con.execute(query).fetch_arrow_table()
            con.close()
            
            count = len(final_table)
            logger.success(f"✅ Computed latest state: {count} unique deals for {target_date}")
            
            # 3. Overwrite Target Table Partition
            logger.info(f"Writing to target table: {TARGET_TABLE}")
            
            # Ensure table exists with explicit schema
            target_table = self.ensure_final_table_exists()

            # Overwrite Partition Strategy (Delete existing -> Append new)
            
            # 1. Delete existing data for this snapshot_date
            logger.info(f"Deleting existing data for snapshot_date={target_date}")
            target_table.delete(delete_filter=EqualTo("snapshot_date", target_date))
            
            # 2. Append new data
            target_table.append(final_table)
            
            logger.success(f"✅ Successfully updated {TARGET_TABLE} partition {target_date} with {count} records")

        except Exception as e:
            logger.error(f"❌ Snapshot creation failed: {e}")
            raise
        finally:
            if os.path.exists(temp_parquet):
                try: os.remove(temp_parquet)
                except: pass

    def run(self):
        """Execute pipeline"""
        logger.info("=" * 80)
        logger.info(f"Starting Deal Detail Snapshot Creation for {self.snapshot_date}")
        logger.info("=" * 80)
        
        self.connect_iceberg()
        self.create_snapshot(self.snapshot_date)
        
        logger.info("=" * 80)
        logger.info("Snapshot Creation Completed")
        logger.info("=" * 80)


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    
    if snapshot_date_str:
        try:
            snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        snapshot_date = date.today()
        
    start_time = datetime.now()
    job = DealDetailSnapshot(snapshot_date)
    job.run()
    logger.info(f"Total Duration: {(datetime.now() - start_time).total_seconds()}s")


if __name__ == "__main__":
    main()
