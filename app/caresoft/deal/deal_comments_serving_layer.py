"""
Deal Comments Serving Layer
Sync comments from Iceberg Gold to PostgreSQL DWH

Flow:
1. Read all comments from gold.caresoft.cs_deal_api_trans_comments
2. Delete existing records by ticket_id
3. Insert new records
"""

import os
import sys
from datetime import datetime, date, timedelta
from loguru import logger
import psycopg2
from psycopg2.extras import execute_values
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

# Iceberg configuration
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# PostgreSQL configuration
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Table names
SOURCE_TABLE = 'caresoft.cs_deal_api_trans_comments'
TARGET_TABLE = os.getenv('SERVING_TABLE_NAME')


class DealCommentsServingLayer:
    """Sync deal comments from Iceberg Gold to PostgreSQL"""

    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        self.gold_catalog = None
        self.pg_conn = None
        logger.info(f"Initialized DealCommentsServingLayer for {self.snapshot_date}")

    def connect_iceberg(self):
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
        logger.info("Connected to Gold Iceberg")

    def connect_postgres(self):
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI not set")
        self.pg_conn = psycopg2.connect(POSTGRES_URI)
        self.pg_conn.autocommit = False
        logger.info("Connected to PostgreSQL")


    def ensure_serving_table_exists(self, columns):
        """Create serving table with composite index on (brand_id, ticket_id)"""
        cursor = self.pg_conn.cursor()
        try:
            column_defs = [f'"{col}" TEXT' for col in columns]
            
            # Create table
            sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                {', '.join(column_defs)}
            )
            """
            cursor.execute(sql)
            
            # Create composite index on (brand_id, ticket_id) for multi-brand isolation
            table_name_safe = TARGET_TABLE.replace('.', '_')
            index_sql = f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name_safe}_brand_ticket 
            ON {TARGET_TABLE} (brand_id, ticket_id)
            """
            cursor.execute(index_sql)
            
            self.pg_conn.commit()
            logger.info(f"Ensured table {TARGET_TABLE} exists with composite index")
                
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def get_comments_for_date(self):
        """Read all Gold comments for snapshot_date"""
        logger.info(f"Reading comments for {self.snapshot_date}")
        table = self.gold_catalog.load_table(SOURCE_TABLE)
        
        try:
            # Scan Gold for snapshot_date (use date object, not string)
            scan = table.scan(
                row_filter=EqualTo("snapshot_date", self.snapshot_date)
            )
            
            # Convert to Arrow and then to tuples
            arrow_table = scan.to_arrow()
            columns = arrow_table.schema.names
            
            rows = []
            for i in range(len(arrow_table)):
                rows.append(tuple(arrow_table.column(j)[i].as_py() for j in range(len(columns))))
            
            logger.success(f"Loaded {len(rows)} comments from gold")
            return columns, rows
            
        except Exception as e:
            logger.error(f"Error reading from gold: {e}")
            raise

    def sync_comments(self, columns, rows):
        """Delete existing + Insert new comments in single transaction"""
        if not rows:
            logger.warning("No comments to sync")
            return

        # Ensure table exists first
        self.ensure_serving_table_exists(columns)
        
        # Extract and validate ticket_ids and brand_ids
        ticket_id_idx = columns.index('ticket_id')
        brand_id_idx = columns.index('brand_id')
        
        # Build list of (brand_id, ticket_id) tuples for deletion
        delete_pairs = set()
        for row in rows:
            brand_id = row[brand_id_idx]
            ticket_id = row[ticket_id_idx]
            
            # Validate both IDs
            if brand_id is not None and ticket_id is not None:
                try:
                    b = float(brand_id)
                    t = float(ticket_id)
                    if -1e20 < b < 1e20 and -1e20 < t < 1e20:
                        delete_pairs.add((b, t))
                except Exception:
                    pass
        
        # Log brand distribution
        brand_counts = {}
        for brand_id, _ in delete_pairs:
            brand_counts[brand_id] = brand_counts.get(brand_id, 0) + 1
        logger.info(f"Brand distribution: {brand_counts}")

        cursor = self.pg_conn.cursor()
        try:
            # 1. Delete existing records by (brand_id, ticket_id) pairs
            if delete_pairs:
                # Build WHERE clause: (brand_id = %s AND ticket_id = %s) OR ...
                conditions = []
                params = []
                for brand_id, ticket_id in delete_pairs:
                    conditions.append("(brand_id = %s AND ticket_id = %s)")
                    params.extend([brand_id, ticket_id])
                
                where_clause = " OR ".join(conditions)
                delete_sql = f"DELETE FROM {TARGET_TABLE} WHERE {where_clause}"
                
                cursor.execute(delete_sql, params)
                logger.info(f"Deleted records for {len(delete_pairs)} (brand_id, ticket_id) pairs")
            
            # 2. Insert new records
            cols_str = ', '.join([f'"{c}"' for c in columns])
            execute_values(
                cursor,
                f"INSERT INTO {TARGET_TABLE} ({cols_str}) VALUES %s",
                rows,
                page_size=1000
            )
            
            # 3. Commit transaction (atomic)
            self.pg_conn.commit()
            logger.success(f"Synced {len(rows)} comments for {len(delete_pairs)} (brand, ticket) pairs")
            
        except Exception:
            self.pg_conn.rollback()
            raise
        finally:
            cursor.close()


    def run(self):
        try:
            self.connect_iceberg()
            self.connect_postgres()
            
            columns, rows = self.get_comments_for_date()
            
            # Sync comments (delete + insert in single transaction)
            self.sync_comments(columns, rows)
            
            logger.success("Deal comments serving layer completed")
            
        finally:
            if self.pg_conn: 
                self.pg_conn.close()


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    job = DealCommentsServingLayer(snapshot_date)
    job.run()

if __name__ == "__main__":
    main()
