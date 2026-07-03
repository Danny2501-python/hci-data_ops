"""
Ticket API Serving Layer Refresh (Incremental)
Architecture: Bronze (Iceberg) → Serving (PostgreSQL DWH)

This script reads TODAY's snapshot from Bronze Iceberg table and upserts
into PostgreSQL serving table.

Flow:
1. Read from Bronze Iceberg table (cs_ticket_api) - TODAY's snapshot only
2. Deduplicate by ticket_id (MAX updated_at) using DuckDB - Bronze has duplicates from 3 API calls
3. UPSERT into PostgreSQL (INSERT ... ON CONFLICT UPDATE)

Strategy: Incremental upsert (daily snapshot → upsert by ticket_id)
Note: Bronze keeps raw data with duplicates from 3 API calls (updated, created, created_prev)
"""

import os
import sys
from datetime import datetime, date, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from loguru import logger
import psycopg2
import gc
# from psycopg2.extras import execute_values

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from common.utils import bulk_load

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# PostgreSQL configuration (from Airflow connection)
POSTGRES_URI = os.getenv('POSTGRES_URI')

class TicketApiServingLayer:
    """Incremental refresh serving layer with daily upsert"""
    SOURCE_TABLE = 'caresoft.cs_ticket_api'  # Bronze Iceberg
    TARGET_TABLE = 'cs_ticket_api'        # PostgreSQL serving
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        self.pg_conn = None
    
    def connect_iceberg(self):
        """Connect to Iceberg Bronze catalog"""
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
        logger.success(f"✅ Connected to Iceberg catalog: {CATALOG_NAME_BRONZE}")
    
    def connect_postgres(self):
        """Connect to PostgreSQL"""
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
            
        logger.info("Connecting to PostgreSQL DWH")
        self.pg_conn = psycopg2.connect(POSTGRES_URI)
        self.pg_conn.autocommit = False
        logger.success("✅ Connected to PostgreSQL")
    
    def get_today_tickets(self):
        """
        Read TODAY's snapshot from Bronze, stream to Parquet (avoid OOM),
        then deduplicate using DuckDB.

        Returns:
            (columns, temp_parquet_path) — caller must delete temp file after use
            Returns ([], None) if no data found.
        """
        logger.info(f"Reading from Iceberg table: {self.SOURCE_TABLE}, snapshot_date={self.snapshot_date}")
        table = self.bronze_catalog.load_table(self.SOURCE_TABLE)

        # Temp file for Parquet
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet_raw = os.path.join(temp_dir, f"ticket_api_raw_{os.getpid()}.parquet")
        temp_parquet_dedup = os.path.join(temp_dir, f"ticket_api_dedup_{os.getpid()}.parquet")

        try:
            logger.info(f"Scanning today's snapshot: {self.snapshot_date}")

            scan = table.scan(
                row_filter=EqualTo("snapshot_date", self.snapshot_date),
            )

            # Stream batches directly to Parquet — never accumulate in RAM
            writer = None
            total_raw = 0
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    t = pa.Table.from_batches([batch])
                    if writer is None:
                        writer = pq.ParquetWriter(temp_parquet_raw, t.schema)
                    writer.write_table(t)
                    total_raw += len(batch)
                    del t
            if writer:
                writer.close()

            if total_raw == 0:
                logger.info("No tickets found in today's snapshot")
                return [], None

            logger.info(f"Streamed {total_raw} raw rows to Parquet, deduplicating...")

            # DuckDB deduplication — write result to a second Parquet file (not into RAM)
            con = duckdb.connect(config={
                'memory_limit': '1GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '2'
            })

            query = f"""
                COPY (
                    SELECT * EXCLUDE (rn)
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY ticket_id, brand_id
                                ORDER BY updated_at DESC NULLS LAST
                            ) as rn
                        FROM read_parquet('{temp_parquet_raw}')
                        WHERE ticket_id IS NOT NULL
                    ) sub
                    WHERE rn = 1
                ) TO '{temp_parquet_dedup}' (FORMAT PARQUET)
            """
            con.execute(query)

            # Read just the schema (column names) without loading data
            result_meta = con.execute(f"SELECT * FROM read_parquet('{temp_parquet_dedup}') LIMIT 0").description
            columns = [col[0] for col in result_meta]
            total_dedup = con.execute(f"SELECT COUNT(*) FROM read_parquet('{temp_parquet_dedup}')").fetchone()[0]
            con.close()

            logger.success(f"✅ After deduplication: {total_dedup} unique tickets (from {total_raw} raw rows)")
            return columns, temp_parquet_dedup

        finally:
            # Always clean up raw parquet
            if os.path.exists(temp_parquet_raw):
                try:
                    os.remove(temp_parquet_raw)
                except:
                    pass

    
    def ensure_serving_table_exists(self, columns):
        """Create serving table if not exists with proper schema and constraints"""
        cursor = self.pg_conn.cursor()
        
        try:
            logger.info(f"Ensuring table {self.TARGET_TABLE} exists...")
            
            # Build CREATE TABLE statement
            column_defs = []
            for col in columns:
                column_defs.append(f'"{col}" TEXT')
            
            # Add PRIMARY KEY constraint on ticket_id and brand_id
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.TARGET_TABLE} (
                {', '.join(column_defs)},
                PRIMARY KEY ("ticket_id", "brand_id")
            )
            """
            cursor.execute(create_sql)
            
            # Ensure Constraint Exists (for existing tables created without PK)
            try:
                alter_sql = f"""
                ALTER TABLE {self.TARGET_TABLE} 
                ADD PRIMARY KEY ("ticket_id", "brand_id");
                """
                cursor.execute(alter_sql)
                logger.info(f"Added PRIMARY KEY to {self.TARGET_TABLE}")
            except psycopg2.errors.InvalidTableDefinition:
                # Constraint likely already exists or conflicts
                self.pg_conn.rollback()
                logger.debug("PK constraint likely already exists")
            except psycopg2.errors.MultiplePrimaryKeys:
                # Table already has a PK
                self.pg_conn.rollback()
                logger.debug("Table already has a PK")
            except Exception as e:
                # Log other potential errors (e.g. duplicate data preventing PK)
                self.pg_conn.rollback()
                logger.warning(f"Could not add PK (checking duplicates...): {e}")

            self.pg_conn.commit()
            logger.success(f"✅ Table {self.TARGET_TABLE} ready")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"❌ Error creating/altering table: {e}")
            raise
        finally:
            cursor.close()
    
    def upsert_serving_table(self, columns, parquet_path):
        """
        UPSERT data into serving table in chunks to avoid loading all rows into RAM.
        Reads from the deduplicated Parquet file in batches via DuckDB.
        """
        if not parquet_path:
            logger.warning("No data to upsert")
            return

        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')

        # Ensure table exists (need column list — fetch from Parquet schema)
        self.ensure_serving_table_exists(columns)

        cursor = self.pg_conn.cursor()
        UPSERT_CHUNK_SIZE = 5000

        try:
            con = duckdb.connect(config={
                'memory_limit': '512MB',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            total_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
            logger.info(f"Upserting {total_rows} rows into {TARGET_TABLE} in chunks of {UPSERT_CHUNK_SIZE}...")

            offset = 0
            total_upserted = 0
            while offset < total_rows:
                chunk_arrow = con.execute(
                    f"SELECT * FROM read_parquet('{parquet_path}') LIMIT {UPSERT_CHUNK_SIZE} OFFSET {offset}"
                ).fetch_arrow_table()

                # Convert Arrow chunk to list of dicts for bulk_load
                chunk_records = chunk_arrow.to_pylist()
                del chunk_arrow

                bulk_load(
                    cursor=cursor,
                    target_table=self.TARGET_TABLE,
                    records=chunk_records,
                    columns=columns,
                    conflict_columns=['ticket_id', 'brand_id'],
                    method='upsert',
                    update_condition=f'{self.TARGET_TABLE}."updated_at" < EXCLUDED."updated_at" OR {self.TARGET_TABLE}."updated_at" IS NULL'
                )
                self.pg_conn.commit()

                total_upserted += len(chunk_records)
                offset += UPSERT_CHUNK_SIZE
                del chunk_records
                gc.collect()

                logger.info(f"Upserted {total_upserted}/{total_rows} rows...")

            con.close()
            logger.success(f"✅ Bulk Upserted {total_upserted} rows into {self.TARGET_TABLE}")

        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"❌ Error upserting data: {e}")
            raise
        finally:
            cursor.close()
            # Clean up dedup Parquet file
            if parquet_path and os.path.exists(parquet_path):
                try:
                    os.remove(parquet_path)
                except:
                    pass
    
    def run(self):
        """Execute the incremental serving layer refresh"""
        logger.info("=" * 80)
        logger.info(f"Starting Ticket API Serving Layer Refresh (Incremental)")
        logger.info(f"Snapshot Date: {self.snapshot_date}")
        logger.info("=" * 80)
        
        try:
            # Connect to sources
            self.connect_iceberg()
            self.connect_postgres()
            
            # Get today's tickets from Bronze (deduplicated Parquet path)
            columns, parquet_path = self.get_today_tickets()

            # Upsert into PostgreSQL serving table (chunked, no full RAM load)
            self.upsert_serving_table(columns, parquet_path)
            
            logger.success("=" * 80)
            logger.success("Ticket API Serving Layer Refresh Completed Successfully")
            logger.success("=" * 80)
            
        except Exception as e:
            logger.error(f"Failed to refresh serving layer: {e}")
            raise
        finally:
            if self.pg_conn:
                self.pg_conn.close()
                logger.info("Closed PostgreSQL connection")


def main():
    """
    Main entry point
    Usage: python ticket_api_serving_layer.py <snapshot_date>
    """
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    job = TicketApiServingLayer(snapshot_date)
    job.run()


if __name__ == "__main__":
    main()
