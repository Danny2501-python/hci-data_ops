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
from datetime import datetime, date, timedelta
import pyarrow as pa
import gc
import io
import csv
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
TARGET_TABLE = 'cs_ticket_source_api'        # PostgreSQL serving


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
    
    def stream_and_upsert(self):
        """
        Stream TODAY's snapshot from Silver and upsert into PostgreSQL.
        Efficiently handles large datasets by processing in batches.
        """
        logger.info(f"Reading from Iceberg table: {SOURCE_TABLE}, snapshot_date={self.snapshot_date}")
        table = self.silver_catalog.load_table(SOURCE_TABLE)
        
        # Use columns from schema to ensure consistent ordering
        columns = [field.name for field in table.schema().fields]
        
        # Ensure target table exists in PostgreSQL
        self.ensure_serving_table_exists(columns)
        
        # Prepare PostgreSQL UPSERT statement
        column_names = ', '.join([f'"{col}"' for col in columns])
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
        
        cursor = self.pg_conn.cursor()
        total_rows = 0
        
        try:
            logger.info(f"Scanning today's snapshot: {self.snapshot_date}")
            
            # Scan ONLY today's snapshot
            scan = table.scan(
                row_filter=EqualTo("snapshot_date", self.snapshot_date),
            )
            
            # Iterate through batches from Iceberg
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    # OPTIMIZATION: Very Small Sub-batches for OOM prevention
                    # Using 500 to ensure even with large raw_data, we stay under RAM limits.
                    SUB_BATCH_SIZE = 500
                    
                    for start in range(0, len(batch), SUB_BATCH_SIZE):
                        sub_batch = batch.slice(start, SUB_BATCH_SIZE)
                        
                        # Extract data columns
                        data_columns = [sub_batch.column(col).to_pylist() for col in columns]
                        rows_iterator = zip(*data_columns)
                        
                        # OPTIMIZATION: Use COPY via StringIO instead of INSERT
                        # This avoids building a massive INSERT SQL string in memory.
                        output = io.StringIO()
                        writer = csv.writer(output, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
                        
                        # Define a explicit NULL marker to avoid ambiguity with empty strings
                        NULL_MARKER = '__POSTGRES_NULL__'
                        
                        for row in rows_iterator:
                            # Clean newline characters and handle NULL values explicitly
                            processed_row = []
                            for val in row:
                                if val is None:
                                    processed_row.append(NULL_MARKER)
                                else:
                                    # Convert to string and remove newlines to prevent CSV corruption
                                    s_val = str(val).replace('\n', ' ').replace('\r', ' ')
                                    processed_row.append(s_val)
                            writer.writerow(processed_row)
                        
                        output.seek(0)
                        
                        # Prepare temporary table
                        temp_table_name = f"temp_{TARGET_TABLE}_{os.getpid()}"
                        cursor.execute(f"CREATE TEMP TABLE {temp_table_name} (LIKE {TARGET_TABLE} INCLUDING ALL) ON COMMIT DROP")
                        
                        # Build column list for COPY
                        col_list_str = ', '.join([f'"{col}"' for col in columns])
                        
                        # Use the defined NULL_MARKER in the COPY command
                        copy_cmd = f"COPY {temp_table_name} ({col_list_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '{NULL_MARKER}')"
                        cursor.copy_expert(copy_cmd, output)
                        
                        # Perform UPSERT from Temp Table
                        update_cols = [col for col in columns if col not in ['ticket_id', 'brand_id']]
                        update_set = ', '.join([f'"{col}" = {temp_table_name}."{col}"' for col in update_cols])
                        
                        upsert_sql = f"""
                        INSERT INTO {TARGET_TABLE} ({col_list_str})
                        SELECT {col_list_str} FROM {temp_table_name}
                        ON CONFLICT ("ticket_id", "brand_id")
                        DO UPDATE SET
                            {update_set}
                        WHERE {TARGET_TABLE}."updated_at" < EXCLUDED."updated_at"
                            OR {TARGET_TABLE}."updated_at" IS NULL
                        """
                        cursor.execute(upsert_sql)
                        
                        # Cleanup
                        output.close()
                        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                        del data_columns
                        del rows_iterator
                        gc.collect()
                    
                    total_rows += len(batch)
                    logger.info(f"Refreshed Arrow batch of {len(batch)} rows (Total so far: {total_rows})")
            
            # Commit all changes
            self.pg_conn.commit()
            logger.success(f"✅ Successfully completed upsert of {total_rows} tickets to {TARGET_TABLE}")
            return total_rows
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"❌ Error during streaming upsert: {e}")
            raise
        finally:
            cursor.close()

    
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
    
    def run(self):
        """Execute the incremental serving layer refresh using streaming"""
        logger.info("=" * 80)
        logger.info(f"Starting Ticket Source API Serving Layer Refresh (Incremental Streaming)")
        logger.info(f"Snapshot Date: {self.snapshot_date}")
        logger.info("=" * 80)
        
        try:
            # Connect to sources
            self.connect_iceberg()
            self.connect_postgres()
            
            # Stream from Silver Iceberg and upsert to PostgreSQL
            self.stream_and_upsert()
            
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
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    job = TicketSourceServingLayer(snapshot_date)
    job.run()


if __name__ == "__main__":
    main()
