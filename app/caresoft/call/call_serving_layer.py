import os
import sys
from datetime import datetime, date, timedelta
import psycopg2
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from loguru import logger

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

# PostgreSQL configuration
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Table names
SOURCE_TABLE = 'caresoft.cs_call_api'
TARGET_TABLE = 'cs_call_api'

class CallServingLayer:
    """Refresh Call serving layer from Bronze (Simple API Flow)"""
    
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
        """Create serving table with PK (id, brand_id)"""
        cursor = self.pg_conn.cursor()
        try:
            column_defs = [f'"{col}" TEXT' for col in columns]
            
            sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                {', '.join(column_defs)},
                PRIMARY KEY ("id", "brand_id")
            )
            """
            cursor.execute(sql)
            self.pg_conn.commit()
            
            # Ensure PK if table existed without it
            try:
                cursor.execute(f'ALTER TABLE {TARGET_TABLE} ADD PRIMARY KEY ("id", "brand_id");')
                self.pg_conn.commit()
            except:
                self.pg_conn.rollback()
                
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def get_calls_for_date(self):
        """Read Bronze call data for snapshot_date, dedup with DuckDB"""
        logger.info(f"Reading calls for {self.snapshot_date}")
        table = self.bronze_catalog.load_table(SOURCE_TABLE)
        
        # Temp dir
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"call_serving_{os.getpid()}.parquet")
        
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
            
            # DuckDB connection
            con = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            
            # Deduplicate by (id, brand_id)
            # Priority: end_time (completed calls), then ingestion_timestamp
            query = f"""
            SELECT 
                id,
                customer_id,
                call_id,
                caller,
                called,
                user_id,
                agent_id,
                group_id,
                call_type,
                start_time,
                call_status,
                end_time,
                wait_time,
                hold_time,
                talk_time,
                end_status,
                ticket_id,
                last_agent_id,
                last_user_id,
                call_survey,
                call_survey_result,
                service_id,
                missed_reason,
                path,
                path_download,
                brand_id,
                dtmf,
                snapshot_date,
                ingestion_timestamp
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY id, brand_id ORDER BY ingestion_timestamp DESC) as rn
                FROM read_parquet('{temp_parquet}')
                WHERE id IS NOT NULL
            ) WHERE rn=1
            """
            
            result = con.execute(query).fetch_arrow_table()
            con.close()
            
            columns = result.schema.names
            rows = []
            for i in range(len(result)):
                rows.append(tuple(result.column(j)[i].as_py() for j in range(len(columns))))
                
            logger.success(f"Loaded {len(rows)} unique calls")
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
            logger.info(f"Bulk Upserting {len(rows)} calls into {TARGET_TABLE}...")
            
            bulk_load(
                cursor=cursor,
                target_table=TARGET_TABLE,
                records=rows,
                columns=columns,
                conflict_columns=['id', 'brand_id'],
                method='upsert'
                # Note: call_serving doesn't have a specific updated_at filter in the original upsert logic
            )
            
            self.pg_conn.commit()
            logger.success(f"✅ Bulk Upserted {len(rows)} calls")
            
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def run(self):
        try:
            self.connect_iceberg()
            self.connect_postgres()
            
            columns, rows = self.get_calls_for_date()
            self.upsert_serving(columns, rows)
            
        finally:
            if self.pg_conn: self.pg_conn.close()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    job = CallServingLayer(snapshot_date)
    job.run()

if __name__ == "__main__":
    main()
