import os
import sys
from datetime import datetime, date
from loguru import logger
import psycopg2
from psycopg2.extras import execute_values
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# PostgreSQL configuration
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Table names
SOURCE_TABLE = 'caresoft.cs_chat_api'
TARGET_TABLE = 'cs_chat_api_v1'

class ChatServingLayer:
    """Refresh Chat serving layer from Bronze (Simple API Flow)"""
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        self.pg_conn = None
        
    def connect_iceberg(self):
        self.bronze_catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            uri=ICEBERG_REST_URI_BRONZE,
            **{
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
                "s3.region": AWS_REGION,
            }
        )
        logger.info("Connected to Bronze Iceberg")

    def connect_postgres(self):
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI not set")
        self.pg_conn = psycopg2.connect(POSTGRES_URI)
        self.pg_conn.autocommit = False
        logger.info("Connected to PostgreSQL")

    def ensure_serving_table_exists(self, columns):
        """Create serving table with PK (conversation_id, brand_id)"""
        cursor = self.pg_conn.cursor()
        try:
            column_defs = [f'"{col}" TEXT' for col in columns]
            
            sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                {', '.join(column_defs)},
                PRIMARY KEY ("conversation_id", "brand_id")
            )
            """
            cursor.execute(sql)
            self.pg_conn.commit()
            
            # Ensure PK if table existed without it
            try:
                cursor.execute(f'ALTER TABLE {TARGET_TABLE} ADD PRIMARY KEY ("conversation_id", "brand_id");')
                self.pg_conn.commit()
            except:
                self.pg_conn.rollback()
                
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def get_chats_for_date(self):
        """Read Bronze chat data for snapshot_date, dedup with DuckDB"""
        logger.info(f"Reading chats for {self.snapshot_date}")
        table = self.bronze_catalog.load_table(SOURCE_TABLE)
        
        # Temp dir
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"chat_serving_{os.getpid()}.parquet")
        
        try:
            # Scan Bronze for snapshot_date
            scan = table.scan(
                row_filter=EqualTo("snapshot_date", self.snapshot_date.isoformat())
            )
            
            # Stream to Parquet
            batches = []
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    batches.append(batch)
            
            if not batches:
                return [], []
                
            combined = pa.Table.from_batches(batches)
            pq.write_table(combined, temp_parquet)
            del batches, combined
            
            # Reduce fields to flat structure using DuckDB
            # Specifically extracting fields from raw_data if needed, 
            # but for Simple API, we essentially take the Bronze schema.
            # Bronze schema: id, ticket_id, start_time, end_time, customer_id, agent_id, raw_data, brand_id
            
            con = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            
            # Deduplicate by (conversation_id, brand_id). Keep latest ingestion?
            # Bronze might have duplicates if rerun. 
            # We want unique (conversation_id, brand_id).
            query = f"""
            SELECT 
                ticket_id,
                ticket_no,
                customer_id,
                group_name,
                conversation_id,
                cus_email,
                cus_name,
                cus_phone,
                start_time,
                end_time,
                chat_duration,
                agent_email,
                ring_time,
                meet_time,
                waittime,
                answer_time,
                chat_status,
                landing_page,
                referrer,
                is_trigger,
                facebook_page_id,
                service_id,
                brand_id,
                raw_data,
                agent_name,
                snapshot_date,
                ingestion_timestamp
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY conversation_id, brand_id ORDER BY end_time DESC NULLS LAST, ticket_id DESC NULLS LAST, ingestion_timestamp DESC) as rn
                FROM read_parquet('{temp_parquet}')
                WHERE conversation_id IS NOT NULL
            ) WHERE rn=1
            """
            
            result = con.execute(query).fetch_arrow_table()
            con.close()
            
            columns = result.schema.names
            rows = []
            for i in range(len(result)):
                rows.append(tuple(result.column(j)[i].as_py() for j in range(len(columns))))
                
            logger.success(f"Loaded {len(rows)} unique chats")
            return columns, rows
            
        finally:
            if os.path.exists(temp_parquet):
                try: os.remove(temp_parquet)
                except: pass

    def upsert_serving(self, columns, rows):
        if not rows: return
        
        self.ensure_serving_table_exists(columns)
        cursor = self.pg_conn.cursor()
        
        try:
            cols_str = ', '.join([f'"{c}"' for c in columns])
            
            # UPDATE on conflict
            update_cols = [c for c in columns if c not in ('conversation_id', 'brand_id')]
            update_set = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
            
            logger.debug(f"Update columns: {update_cols}")
            
            sql = f"""
            INSERT INTO {TARGET_TABLE} ({cols_str})
            VALUES %s
            ON CONFLICT ("conversation_id", "brand_id")
            DO UPDATE SET
                {update_set}
            """
            
            logger.debug(f"SQL: {sql}")
            
            execute_values(cursor, sql, rows, page_size=1000)
            self.pg_conn.commit()
            logger.success(f"Upserted {len(rows)} chats")
            
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def run(self):
        try:
            self.connect_iceberg()
            self.connect_postgres()
            
            columns, rows = self.get_chats_for_date()
            self.upsert_serving(columns, rows)
            
        finally:
            if self.pg_conn: self.pg_conn.close()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    job = ChatServingLayer(snapshot_date)
    job.run()

if __name__ == "__main__":
    main()
