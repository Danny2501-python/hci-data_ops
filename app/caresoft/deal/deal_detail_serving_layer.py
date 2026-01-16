"""
Deal Detail Serving Layer Refresh
Architecture: Gold (Iceberg) → Serving (PostgreSQL DWH)

This script reads the latest snapshot of each deal from the Gold Iceberg table
and writes it to a PostgreSQL serving table for consumption by downstream applications.

Strategy:
1. Read from Gold Iceberg table using PyIceberg
2. Use DuckDB to find latest snapshot per deal_id
3. Drop and recreate the serving table in PostgreSQL
4. Bulk insert the data
"""

import os
import sys
from datetime import datetime, date
from loguru import logger
import psycopg2
from psycopg2.extras import execute_values
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

# PostgreSQL configuration (from Airflow connection)
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Table names
SOURCE_TABLE = 'caresoft.cs_deal_detail_api_final'
TARGET_TABLE = 'cs_deal_detail_api_v1'


class ServingLayerRefresh:
    """Refresh serving layer table with latest deal details"""
    
    def __init__(self):
        self.gold_catalog = None
        self.pg_conn = None
        
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
        
    def connect_postgres(self):
        """Connect to PostgreSQL"""
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
            
        logger.info("Connecting to PostgreSQL DWH")
        self.pg_conn = psycopg2.connect(POSTGRES_URI)
        self.pg_conn.autocommit = False
        
        logger.success("✅ Connected to PostgreSQL")
        
    def get_latest_deals(self, snapshot_date=None):
        """
        Read from Iceberg 'Final Snapshot' table for a specific date.
        """
        target_date = snapshot_date or datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Reading snapshot for date: {target_date} from {SOURCE_TABLE}")
        
        table = self.gold_catalog.load_table(SOURCE_TABLE)
        
        # Filter Scan by snapshot_date to read only the target daily snapshot
        scan = table.scan(row_filter=f"snapshot_date = '{target_date}'")
        result = scan.to_arrow()
        
        # Convert to columns and rows
        columns = result.schema.names
        rows = []
        for i in range(len(result)):
            row = tuple(result.column(j)[i].as_py() for j in range(len(columns)))
            rows.append(row)
        
        logger.success(f"✅ Found {len(rows)} deals for snapshot {target_date}")
        return columns, rows
        
    def ensure_serving_table_exists(self, columns):
        """Create serving table if not exists with proper schema and PK"""
        cursor = self.pg_conn.cursor()
        try:
            logger.info(f"Ensuring table {TARGET_TABLE} exists...")
            
            column_defs = []
            for col in columns:
                column_defs.append(f'"{col}" TEXT')
            
            # Create Table with PK ("id", "brand_id")
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                {', '.join(column_defs)},
                PRIMARY KEY ("id", "brand_id")
            )
            """
            cursor.execute(create_sql)
            
            # Attempt to add PK if missing (for pre-existing tables)
            try:
                alter_sql = f"""
                ALTER TABLE {TARGET_TABLE} 
                ADD PRIMARY KEY ("id", "brand_id");
                """
                cursor.execute(alter_sql)
                logger.info(f"Added PRIMARY KEY to {TARGET_TABLE}")
            except psycopg2.errors.InvalidTableDefinition:
                self.pg_conn.rollback() # PK check constraint failure or similar
            except psycopg2.errors.MultiplePrimaryKeys:
                self.pg_conn.rollback() # Table already has PK
            except Exception as e:
                self.pg_conn.rollback()
                logger.warning(f"Could not add PK to Deal table: {e}")

            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def upsert_serving_table(self, columns, rows):
        """
        UPSERT data into serving table (Insert or Update on Conflict)
        Similar to Ticket Pipeline logic.
        
        Args:
            columns: List of column names
            rows: List of tuples containing data
        """
        if not rows:
            logger.warning("No data to upsert")
            return
            
        cursor = self.pg_conn.cursor()
        
        try:
            # Step 1: Ensure table exists with PK
            self.ensure_serving_table_exists(columns)
            
            logger.info(f"Upserting {len(rows)} rows into {TARGET_TABLE}...")
            
            # Build UPSERT statement (PostgreSQL INSERT ... ON CONFLICT)
            column_names = ', '.join([f'"{col}"' for col in columns])
            
            # Update all columns except PK on conflict
            # PK is (id, brand_id)
            update_cols = [col for col in columns if col not in ('id', 'brand_id')]
            update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
            
            upsert_sql = f"""
            INSERT INTO {TARGET_TABLE} ({column_names})
            VALUES %s
            ON CONFLICT ("id", "brand_id")
            DO UPDATE SET
                {update_set}
            WHERE {TARGET_TABLE}."updated_at" < EXCLUDED."updated_at"
                OR {TARGET_TABLE}."updated_at" IS NULL
            """
                   
            # Execute batch upsert using execute_values
            # Page size 1000 for balance between speed and memory
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
        """Execute the serving layer refresh"""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('snapshot_date', nargs='?')
        args = parser.parse_args()
        
        s_date = args.snapshot_date
        # Fallback to sys.argv if needed (common in some Airflow setups)
        if not s_date and len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
             s_date = sys.argv[1]

        logger.info("=" * 80)
        logger.info("Starting Serving Layer Refresh (Iceberg → PostgreSQL)")
        logger.info("Strategy: UPSERT (Latest Incremental Snapshot)")
        logger.info("=" * 80)
        
        try:
            # Connect to data sources
            self.connect_iceberg()
            self.connect_postgres()
            
            # Get latest deals from Gold layer (Specific Snapshot Date)
            columns, rows = self.get_latest_deals(s_date)
            
            # Upsert serving table
            self.upsert_serving_table(columns, rows)
            
            logger.success("=" * 80)
            logger.success("Serving Layer Refresh Completed Successfully")
            logger.success("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Serving layer refresh failed: {e}")
            raise
        finally:
            # Close connections
            if self.pg_conn:
                self.pg_conn.close()


def main():
    job = ServingLayerRefresh()
    job.run()


if __name__ == "__main__":
    main()
