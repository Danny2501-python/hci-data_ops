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
    
    def __init__(self):
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

    def create_snapshot(self):
        """
        Read all history, find latest state, truncate and write to final table
        """
        import pyarrow.parquet as pq
        import gc
        
        logger.info(f"Reading from source table: {SOURCE_TABLE}")
        source_table = self.gold_catalog.load_table(SOURCE_TABLE)
        
        # Temp file for DuckDB processing
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"snapshot_source_{os.getpid()}.parquet")
        
        try:
            # 1. Stream Source Data to Parquet (Handle Large Data)
            logger.info("Streaming source data to temp file...")
            arrow_schema = schema_to_pyarrow(source_table.schema())
            
            with pq.ParquetWriter(temp_parquet, arrow_schema) as writer:
                # Scan all data (no filter)
                scan = source_table.scan()
                for batch in scan.to_arrow_batch_reader():
                    if len(batch) > 0:
                        batch_table = pa.Table.from_batches([batch])
                        writer.write_table(batch_table)
                        del batch, batch_table
            
            logger.info(f"Buffered data to {temp_parquet}")
            gc.collect()
            
            # 2. Use DuckDB to find latest state per deal
            logger.info("Computing latest state using DuckDB...")
            con = duckdb.connect(config={'memory_limit': '4GB', 'temp_directory': temp_dir})
            
            # Query: Get record with MAX(snapshot_date) per id
            query = f"""
                WITH ranked_deals AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (PARTITION BY id ORDER BY snapshot_date DESC, ingestion_timestamp DESC) as rn
                    FROM read_parquet('{temp_parquet}')
                )
                SELECT * EXCLUDE (rn)
                FROM ranked_deals
                WHERE rn = 1
            """
            
            final_table = con.execute(query).fetch_arrow_table()
            con.close()
            
            count = len(final_table)
            logger.success(f"✅ Computed latest state: {count} unique deals")
            
            if count == 0:
                logger.warning("No data found! Skipping write.")
                return

            # 3. Overwrite Target Table
            logger.info(f"Writing to target table: {TARGET_TABLE}")
            
            # Create table if not exists
            try:
                target_table = self.gold_catalog.load_table(TARGET_TABLE)
                logger.info("Target table exists, appending (overwrite logic handled via transaction)")
            except Exception:
                logger.info("Creating target table...")
                # Create with same schema as source
                target_table = self.gold_catalog.create_table(
                    TARGET_TABLE,
                    schema=source_table.schema(),
                    # No partitioning needed for final snapshot table usually, 
                    # but we can partition by brand_id if needed. Keeping simple for now.
                )

            # Overwrite Strategy: Replace data
            # PyIceberg doesn't support 'overwrite' directly in one command easily effectively for all cases,
            # but we can use delete + append or overwrite transaction if available.
            # Simple approach: Delete all -> Append (Transaction safe)
            
            # Note: For efficiency in Iceberg, the best 'Snapshot' pattern is usually overwrite_filter(True) 
            # but PyIceberg support varies. We will use the standard 'overwrite' method if available or delete+append.
            
            txn = target_table.transaction()
            
            # Delete all existing data
            txn.delete(filter="id IS NOT NULL") # Effectively truncate
            
            # Append new data
            # Ensure schema matches exactly
            txn.append(final_table)
            
            txn.commit_transaction()
            logger.success(f"✅ Successfully updated {TARGET_TABLE} with {count} records")

        except Exception as e:
            logger.error(f"❌ Snapshot creation failed: {e}")
            raise
        finally:
            # Cleanup
            if os.path.exists(temp_parquet):
                try:
                    os.remove(temp_parquet)
                except: pass

    def run(self):
        """Execute pipeline"""
        logger.info("=" * 80)
        logger.info("Starting Deal Detail Snapshot Creation")
        logger.info("=" * 80)
        
        self.connect_iceberg()
        self.create_snapshot()
        
        logger.info("=" * 80)
        logger.info("Snapshot Creation Completed")
        logger.info("=" * 80)


if __name__ == "__main__":
    DealDetailSnapshot().run()
