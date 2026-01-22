"""
Deal Comments Serving Layer
Sync comments from Iceberg Gold to PostgreSQL DWH

Flow:
1. Read from gold.caresoft.cs_deal_api_trans_comments
2. Delete existing records by ticket_id
3. Insert new records
"""

import os
import sys
from datetime import datetime, date, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from loguru import logger
import gc

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
TARGET_TABLE = os.getenv('SERVING_TABLE_NAME', 'cs_deal_api_trans_comments_v1')


class DealCommentsServingLayer:
    """
    Sync deal comments from Iceberg to PostgreSQL
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        
        # Initialize Gold catalog
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
        
        # PostgreSQL connection
        self.pg_conn = None
        
        logger.info(f"Initialized DealCommentsServingLayer for {self.snapshot_date}")
    
    def connect_postgres(self):
        """Connect to PostgreSQL DWH"""
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI not set")
        
        self.pg_conn = psycopg2.connect(POSTGRES_URI)
        self.pg_conn.autocommit = False
        logger.info("Connected to PostgreSQL")
    
    def ensure_serving_table_exists(self):
        """
        Create serving table if not exists
        No primary key (as per legacy schema)
        """
        cursor = self.pg_conn.cursor()
        
        try:
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                ticket_comments_id NUMERIC(20),
                ticket_id NUMERIC(20),
                comment TEXT,
                commentator_id NUMERIC(20),
                username TEXT,
                facebook TEXT,
                facebook_name TEXT,
                comment_source TEXT,
                created_at TIMESTAMP,
                attack_file_id TEXT,
                file_name TEXT,
                file_id TEXT,
                download_connection_string TEXT,
                call_id TEXT,
                email_receive_id TEXT,
                addition_details TEXT,
                is_public INTEGER,
                can_hide TEXT,
                can_remove TEXT,
                can_reply_privately TEXT,
                facebook_comment_state INTEGER,
                role_id INTEGER,
                avatar TEXT,
                path TEXT,
                file_path TEXT,
                snapshot_date DATE,
                ingestion_timestamp TIMESTAMP
            )
            """
            
            cursor.execute(create_sql)
            self.pg_conn.commit()
            logger.info(f"Ensured table {TARGET_TABLE} exists")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to create table: {e}")
            raise
        finally:
            cursor.close()
    
    def get_comments_from_iceberg(self) -> pd.DataFrame:
        """
        Read comments from Iceberg for current snapshot_date
        """
        source_table = self.gold_catalog.load_table(SOURCE_TABLE)
        
        logger.info(f"Reading comments for snapshot_date = {self.snapshot_date}...")
        
        # Scan for comments in current snapshot
        scan = source_table.scan(
            row_filter=EqualTo("snapshot_date", self.snapshot_date)
        )
        
        # Convert to pandas
        df = scan.to_pandas()
        
        logger.info(f"Read {len(df)} comments from Iceberg")
        return df
    
    def delete_existing_comments(self, ticket_ids: list):
        """
        Delete existing comments by ticket_id
        No primary key, so delete by ticket_id
        """
        if not ticket_ids:
            logger.info("No ticket_ids to delete")
            return
        
        cursor = self.pg_conn.cursor()
        
        try:
            # Build IN clause
            ticket_ids_str = ','.join([str(int(tid)) for tid in ticket_ids if pd.notna(tid)])
            
            if not ticket_ids_str:
                logger.info("No valid ticket_ids to delete")
                return
            
            delete_sql = f"""
            DELETE FROM {TARGET_TABLE}
            WHERE ticket_id IN ({ticket_ids_str})
            """
            
            cursor.execute(delete_sql)
            deleted_count = cursor.rowcount
            self.pg_conn.commit()
            
            logger.info(f"Deleted {deleted_count} existing comments for {len(set(ticket_ids))} ticket_ids")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to delete existing comments: {e}")
            raise
        finally:
            cursor.close()
    
    def insert_comments(self, df: pd.DataFrame):
        """
        Insert comments to PostgreSQL
        """
        if df.empty:
            logger.warning("No comments to insert")
            return
        
        cursor = self.pg_conn.cursor()
        
        try:
            # Column order
            columns = [
                'ticket_comments_id', 'ticket_id', 'comment', 'commentator_id',
                'username', 'facebook', 'facebook_name', 'comment_source',
                'created_at', 'attack_file_id', 'file_name', 'file_id',
                'download_connection_string', 'call_id', 'email_receive_id',
                'addition_details', 'is_public', 'can_hide', 'can_remove',
                'can_reply_privately', 'facebook_comment_state', 'role_id',
                'avatar', 'path', 'file_path', 'snapshot_date', 'ingestion_timestamp'
            ]
            
            # Ensure all columns exist
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            
            # Select columns in order
            df = df[columns]
            
            # Convert to list of tuples
            records = [tuple(row) for row in df.values]
            
            # Build INSERT query
            cols_str = ', '.join([f'"{c}"' for c in columns])
            insert_sql = f"""
            INSERT INTO {TARGET_TABLE} ({cols_str})
            VALUES %s
            """
            
            # Execute batch insert
            execute_values(cursor, insert_sql, records, page_size=1000)
            self.pg_conn.commit()
            
            logger.success(f"✅ Inserted {len(df)} comments to {TARGET_TABLE}")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to insert comments: {e}")
            raise
        finally:
            cursor.close()
    
    def run(self):
        """
        Execute serving layer sync
        """
        logger.info("="*80)
        logger.info(f"Starting Deal Comments Serving Layer for {self.snapshot_date}")
        logger.info("="*80)
        
        try:
            # 1. Connect to PostgreSQL
            self.connect_postgres()
            
            # 2. Ensure table exists
            self.ensure_serving_table_exists()
            
            # 3. Read comments from Iceberg
            df = self.get_comments_from_iceberg()
            
            if df.empty:
                logger.info("No comments to sync")
                return
            
            # 4. Delete existing comments by ticket_id
            unique_ticket_ids = df['ticket_id'].unique().tolist()
            self.delete_existing_comments(unique_ticket_ids)
            
            # 5. Insert new comments
            self.insert_comments(df)
            
            logger.success("✅ Deal comments serving layer completed")
            
        except Exception as e:
            logger.error(f"❌ Deal comments serving layer failed: {e}")
            raise
        finally:
            if self.pg_conn:
                self.pg_conn.close()
            gc.collect()


def main():
    """Main execution function"""
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    logger.info(f"Starting Deal Comments Serving Layer for {snapshot_date}")
    
    try:
        job = DealCommentsServingLayer(snapshot_date)
        job.run()
        
        logger.success("✅ Deal comments serving layer completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Deal comments serving layer failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
