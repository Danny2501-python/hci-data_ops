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

    def create_snapshot(self, snapshot_date=None):
        """
        Read increment/snapshot for specific date, find latest state, and overwrite into final table partition
        """
        import pyarrow.parquet as pq
        import gc
        import argparse
        
        target_date = snapshot_date or datetime.now().strftime('%Y-%m-%d')
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
            con = duckdb.connect(config={'memory_limit': '4GB', 'temp_directory': temp_dir})
            
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
                    * EXCLUDE (rn, snapshot_date),
                    CAST('{target_date}' AS DATE) as snapshot_date
                FROM ranked_deals
                WHERE rn = 1
            """
            
            final_table = con.execute(query).fetch_arrow_table()
            con.close()
            
            count = len(final_table)
            logger.success(f"✅ Computed latest state: {count} unique deals for {target_date}")
            
            # 3. Overwrite Target Table Partition
            logger.info(f"Writing to target table: {TARGET_TABLE}")
            
            # Create table if not exists
            try:
                target_table = self.gold_catalog.load_table(TARGET_TABLE)
            except Exception:
                logger.info("Creating target table...")
                target_table = self.gold_catalog.create_table(
                    TARGET_TABLE,
                    schema=source_table.schema(),
                    # Partition by snapshot_date for efficiency
                     partition_spec=source_table.spec() 
                )

            # Transaction: Overwrite Partition (Delete specific date -> Append)
            txn = target_table.transaction()
            
            # Delete data for this snapshot_date
            delete_filter = f"snapshot_date = '{target_date}'"
            logger.info(f"Deleting existing data for: {delete_filter}")
            txn.delete(filter=delete_filter)
            
            # Append new data
            txn.append(final_table)
            
            txn.commit_transaction()
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
        import argparse
        parser = argparse.ArgumentParser(description='Create Deal Snapshot')
        parser.add_argument('snapshot_date', nargs='?', help='YYYY-MM-DD')
        args = parser.parse_args()
        
        # If run from Airflow via PythonOperator/KubernetesPodOperator with extra args
        # sometimes args are passed differently. 
        # But sys.argv processing in argparse usually works.
        # If snapshot_date is passed as loose argument
        
        s_date = args.snapshot_date
        # Fallback to sys.argv if needed or just use None context
        if not s_date and len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
             s_date = sys.argv[1]

        logger.info("=" * 80)
        logger.info("Starting Deal Detail Snapshot Creation (Incremental Mode)")
        logger.info("=" * 80)
        
        self.connect_iceberg()
        self.create_snapshot(s_date)
        
        logger.info("=" * 80)
        logger.info("Snapshot Creation Completed")
        logger.info("=" * 80)


if __name__ == "__main__":
    DealDetailSnapshot().run()
