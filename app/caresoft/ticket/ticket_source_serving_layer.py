"""
Ticket Source API Serving Layer Refresh (Incremental - Simplified)
Architecture: Silver (Iceberg) → Serving (PostgreSQL DWH)

This script reads TODAY's snapshot from Silver Iceberg table and upserts
into PostgreSQL serving table.

Flow:
1. Read from Silver Iceberg table (cs_ticket_source_api) - TODAY's snapshot only
2. No deduplication needed - Silver already has unique tickets per snapshot
3. UPSERT into PostgreSQL (INSERT ... ON CONFLICT UPDATE)

Strategy: Incremental upsert (daily snapshot → upsert by ticket_id)
Note: Silver is already unique (enrichment deduplicates Bronze before calling API)
"""

import os
import sys
from datetime import datetime, date
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from loguru import logger
import psycopg2
from psycopg2.extras import execute_values

# Iceberg configuration
ICEBERG_REST_URI_SILVER = os.getenv('ICEBERG_REST_URI_SILVER')
CATALOG_NAME_SILVER = os.getenv('CATALOG_NAME_SILVER')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# PostgreSQL configuration (from Airflow connection)
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Table names
SOURCE_TABLE = 'caresoft.cs_ticket_source_api'  # Silver Iceberg
TARGET_TABLE = 'cs_ticket_source_api_v1'        # PostgreSQL serving


class TicketSourceServingLayer:
    """Incremental refresh serving layer with daily upsert"""
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.silver_catalog = None
        self.pg_conn = None
    
    def connect_iceberg(self):
        """Connect to Iceberg Silver catalog"""
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
        logger.success(f"✅ Connected to Iceberg catalog: {CATALOG_NAME_SILVER}")
    
    def connect_postgres(self):
        """Connect to PostgreSQL"""
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
            
        logger.info("Connecting to PostgreSQL DWH")
        self.pg_conn = psycopg2.connect(POSTGRES_URI)
        self.pg_conn.autocommit = False
        logger.success("✅ Connected to PostgreSQL")
    
    def get_today_ticket_details(self):
        """
        Read TODAY's snapshot from Silver (already unique)
        
        Logic:
        - Scan ONLY today's snapshot_date
        - Write to Parquet (avoid OOM)
        - Read from Parquet using DuckDB
        - Return rows (Silver is already unique)
        """
        logger.info(f"Reading from Iceberg table: {SOURCE_TABLE}, snapshot_date={self.snapshot_date}")
        table = self.silver_catalog.load_table(SOURCE_TABLE)
        
        # Temp file for Parquet
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"ticket_source_serving_{os.getpid()}.parquet")
        
        try:
            logger.info(f"Scanning today's snapshot: {self.snapshot_date}")
            
            # Scan ONLY today's snapshot
            scan = table.scan(
                row_filter=EqualTo("snapshot_date", self.snapshot_date),
            )
            
            # Collect all batches
            batches = []
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    batches.append(batch)
            
            if not batches:
                logger.info(f"No ticket details found in today's snapshot")
                return [], []
            
            # Combine and write to Parquet
            combined = pa.Table.from_batches(batches)
            pq.write_table(combined, temp_parquet)
            
            logger.info(f"Written {len(combined)} rows to Parquet, loading...")
            
            # DuckDB (No Deduplication needed - Silver is unique)
            con = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            
            query = f"""
                SELECT * 
                FROM read_parquet('{temp_parquet}')
            """
            
            result = con.execute(query).fetch_arrow_table()
            con.close()
            
            logger.success(f"✅ Loaded {len(result)} tickets from Silver")
            
            # Convert to columns and rows
            columns = result.schema.names
            rows = []
            for i in range(len(result)):
                row = tuple(result.column(j)[i].as_py() for j in range(len(columns)))
                rows.append(row)
            
            return columns, rows
            
        finally:
            if os.path.exists(temp_parquet):
                try:
                    os.remove(temp_parquet)
                except:
                    pass
    
    def ensure_serving_table_exists(self, columns):
        """Create serving table if not exists with proper schema and constraints"""
        cursor = self.pg_conn.cursor()
        
        try:
            logger.info(f"Ensuring table {TARGET_TABLE} exists...")
            
            # Build CREATE TABLE statement
            column_defs = []
            for col in columns:
                column_defs.append(f'"{col}" TEXT')
            
            # Add PRIMARY KEY constraint on ticket_id and brand_id
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                {', '.join(column_defs)},
                PRIMARY KEY ("ticket_id", "brand_id")
            )
            """
            cursor.execute(create_sql)
            
            # Ensure Constraint Exists (for existing tables created without PK)
            try:
                alter_sql = f"""
                ALTER TABLE {TARGET_TABLE} 
                ADD PRIMARY KEY ("ticket_id", "brand_id");
                """
                cursor.execute(alter_sql)
                logger.info(f"Added PRIMARY KEY to {TARGET_TABLE}")
            except psycopg2.errors.InvalidTableDefinition:
                # Constraint likely already exists or conflicts
                self.pg_conn.rollback()
            except psycopg2.errors.MultiplePrimaryKeys:
                # Table already has a PK
                self.pg_conn.rollback()
            except Exception as e:
                # Log other potential errors (e.g. duplicate data preventing PK)
                self.pg_conn.rollback()
                logger.warning(f"Could not add PK (checking duplicates...): {e}")
            
            self.pg_conn.commit()
            
            logger.success(f"✅ Table {TARGET_TABLE} ready")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"❌ Error creating/altering table: {e}")
            raise
        finally:
            cursor.close()
    
    def upsert_serving_table(self, columns, rows):
        """
        UPSERT data into serving table
        
        Strategy:
        - INSERT new tickets
        - UPDATE existing tickets if updated_at is newer
        
        Args:
            columns: List of column names
            rows: List of tuples containing data
        """
        if not rows:
            logger.warning("No data to upsert")
            return
        
        # Ensure table exists
        self.ensure_serving_table_exists(columns)
        
        cursor = self.pg_conn.cursor()
        
        try:
            logger.info(f"Upserting {len(rows)} rows into {TARGET_TABLE}...")
            
            # Build UPSERT statement (PostgreSQL INSERT ... ON CONFLICT)
            column_names = ', '.join([f'"{col}"' for col in columns])
            
            # Update all columns except PK on conflict
            update_cols = [col for col in columns if col not in ['ticket_id', 'brand_id']]
            update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
            
            upsert_sql = f"""
            INSERT INTO {TARGET_TABLE} ({column_names})
            VALUES %s
            ON CONFLICT ("ticket_id", "brand_id")
            DO UPDATE SET
                {update_set}
            WHERE {TARGET_TABLE}."updated_at" < EXCLUDED."updated_at"
                OR {TARGET_TABLE}."updated_at" IS NULL
            """
            
            # Execute batch upsert using execute_values (faster than executemany)
            execute_values(cursor, upsert_sql, rows, page_size=1000)
            
            # Commit transaction
            self.pg_conn.commit()
            logger.success(f"✅ Upserted {len(rows)} rows into {TARGET_TABLE}")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"❌ Error upserting data: {e}")
            raise
        finally:
            cursor.close()
    
    def run(self):
        """Execute the incremental serving layer refresh"""
        logger.info("=" * 80)
        logger.info(f"Starting Ticket Source API Serving Layer Refresh (Incremental)")
        logger.info(f"Snapshot Date: {self.snapshot_date}")
        logger.info("=" * 80)
        
        try:
            # Connect to sources
            self.connect_iceberg()
            self.connect_postgres()
            
            # Get today's ticket details from Silver (already unique)
            columns, rows = self.get_today_ticket_details()
            
            # Upsert into PostgreSQL serving table
            self.upsert_serving_table(columns, rows)
            
            logger.success("=" * 80)
            logger.success("Ticket Source API Serving Layer Refresh Completed Successfully")
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
    Usage: python ticket_source_serving_layer.py <snapshot_date>
    """
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    job = TicketSourceServingLayer(snapshot_date)
    job.run()


if __name__ == "__main__":
    main()
