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
import pyarrow as pa
import duckdb
from pyiceberg.catalog import load_catalog

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
SOURCE_TABLE = 'caresoft.cs_deal_detail'
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
        
    def get_latest_deals(self):
        """
        Read from Iceberg and get the latest snapshot of each deal using DuckDB
        
        Returns:
            Tuple of (columns, rows)
        """
        logger.info(f"Reading from Iceberg table: {SOURCE_TABLE}")
        
        # Load Iceberg table
        table = self.gold_catalog.load_table(SOURCE_TABLE)
        
        # Use DuckDB in-memory with automatic disk spilling
        logger.info("Setting up DuckDB (in-memory with disk spilling)...")
        con = duckdb.connect()
        
        # Configure temp directory for spilling (if RAM is full)
        temp_dir = os.getenv('DUCKDB_TEMP_DIR', '/tmp')
        con.execute(f"SET temp_directory='{temp_dir}'")
        
        # Limit memory to enable automatic spilling to disk
        con.execute("SET max_memory='3GB'")
        
        logger.info(f"DuckDB configured: temp_dir={temp_dir}, max_memory=3GB")
        
        # Scan Iceberg table to Arrow
        logger.info("Reading from Iceberg...")
        scan = table.scan()
        arrow_table = scan.to_arrow()
        
        logger.info(f"Read {len(arrow_table):,} rows from Iceberg")
        
        # Execute aggregation query directly on Arrow table (zero-copy)
        # DuckDB will automatically spill to disk if RAM is insufficient
        logger.info("Finding latest snapshot per deal...")
        result = con.execute("""
            SELECT t.*
            FROM arrow_table t
            JOIN (
                SELECT id, MAX(snapshot_date) as max_date
                FROM arrow_table
                WHERE id IS NOT NULL
                GROUP BY id
            ) latest 
            ON t.id = latest.id 
            AND t.snapshot_date = latest.max_date
            ORDER BY t.id
        """).fetch_arrow_table()
        
        con.close()
        
        # Convert to columns and rows
        columns = result.schema.names
        rows = []
        for i in range(len(result)):
            row = tuple(result.column(j)[i].as_py() for j in range(len(columns)))
            rows.append(row)
        
        logger.success(f"✅ Found {len(rows)} latest deals")
        
        return columns, rows
        
    def refresh_serving_table(self, columns, rows):
        """
        Drop and recreate the serving table, then bulk insert data
        
        Args:
            columns: List of column names
            rows: List of tuples containing data
        """
        if not rows:
            logger.warning("No data to insert")
            return
            
        cursor = self.pg_conn.cursor()
        
        try:
            # Step 1: Drop existing table
            logger.info(f"Dropping table caresoft.{TARGET_TABLE} if exists...")
            cursor.execute(f"DROP TABLE IF EXISTS caresoft.{TARGET_TABLE}")
            
            # Step 2: Create table structure
            logger.info(f"Creating table caresoft.{TARGET_TABLE}...")
            
            # Build CREATE TABLE statement
            # Default to TEXT for safety (you can refine types later)
            column_defs = []
            for col in columns:
                column_defs.append(f'"{col}" TEXT')
            
            create_sql = f"""
            CREATE TABLE caresoft.{TARGET_TABLE} (
                {', '.join(column_defs)}
            )
            """
            cursor.execute(create_sql)
            logger.success(f"✅ Created table caresoft.{TARGET_TABLE}")
            
            # Step 3: Bulk insert data in batches
            BATCH_SIZE = 10000
            total_inserted = 0
            
            logger.info(f"Inserting {len(rows)} rows in batches of {BATCH_SIZE}...")
            
            # Build INSERT statement
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join([f'"{col}"' for col in columns])
            insert_sql = f"""
            INSERT INTO caresoft.{TARGET_TABLE} ({column_names})
            VALUES ({placeholders})
            """
            
            # Execute batch insert
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
                logger.info(f"  Inserted {total_inserted}/{len(rows)} rows ({total_inserted/len(rows)*100:.1f}%)")
            
            # Commit transaction
            self.pg_conn.commit()
            logger.success(f"✅ Inserted {total_inserted} rows into caresoft.{TARGET_TABLE}")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"❌ Error refreshing serving table: {e}")
            raise
        finally:
            cursor.close()
            
    def run(self):
        """Execute the serving layer refresh"""
        logger.info("=" * 80)
        logger.info("Starting Serving Layer Refresh (Iceberg → PostgreSQL)")
        logger.info("=" * 80)
        
        try:
            # Connect to data sources
            self.connect_iceberg()
            self.connect_postgres()
            
            # Get latest deals from Gold layer
            columns, rows = self.get_latest_deals()
            
            # Refresh serving table
            self.refresh_serving_table(columns, rows)
            
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
