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
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# PostgreSQL configuration (from Airflow connection)
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Table names
SOURCE_TABLE = 'caresoft.cs_deal_detail_api'  # Read directly from incremental table
TARGET_TABLE = os.getenv('SERVING_TABLE_NAME')


class ServingLayerRefresh:
    """Refresh serving layer table with latest deal details"""
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
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
                self.pg_conn.rollback()
            except psycopg2.errors.MultiplePrimaryKeys:
                self.pg_conn.rollback()
            except Exception as e:
                self.pg_conn.rollback()
                logger.warning(f"Could not add PK to Deal table: {e}")

            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def get_deals_for_date(self, snapshot_date: date):
        """
        Read deals from Iceberg for a specific snapshot_date (INCREMENTAL).
        Uses DuckDB + Parquet for memory efficiency.
        """
        snapshot_date_str = snapshot_date.strftime('%Y-%m-%d')
        logger.info(f"Reading deals for snapshot_date={snapshot_date_str} from {SOURCE_TABLE}")
        
        table = self.gold_catalog.load_table(SOURCE_TABLE)
        
        # Temp directory configuration
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"deal_serving_{os.getpid()}.parquet")
        
        try:
            # Scan ONLY the partition for this snapshot_date (incremental read)
            scan = table.scan(row_filter=EqualTo("snapshot_date", snapshot_date_str))
            
            # Stream to Parquet
            batches = []
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    batches.append(batch)
            
            if not batches:
                logger.info(f"No deals found for snapshot {snapshot_date_str}")
                return [], []
            
            # Write to Parquet
            combined = pa.Table.from_batches(batches)
            pq.write_table(combined, temp_parquet, compression='snappy')
            
            # Free memory
            del batches
            del combined
            
            logger.info(f"Written deals to temp Parquet: {temp_parquet}")
            
            # Use DuckDB to read back (no dedup needed as Gold is already unique)
            con = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            
            query = f"SELECT * FROM read_parquet('{temp_parquet}')"
            result = con.execute(query).fetch_arrow_table()
            con.close()
            
            # Convert to columns and rows
            columns = result.schema.names
            rows = []
            for i in range(len(result)):
                row = tuple(result.column(j)[i].as_py() for j in range(len(columns)))
                rows.append(row)
            
            logger.success(f"✅ Loaded {len(rows)} deals from Gold")
            return columns, rows
            
        finally:
            # Cleanup
            if os.path.exists(temp_parquet):
                try:
                    os.remove(temp_parquet)
                except:
                    pass
    
    def upsert_serving_table(self, columns, rows):
        """
        UPSERT data into serving table (Insert or Update on Conflict)
        
        Strategy:
        - New deals → INSERT
        - Existing deals with newer updated_at → UPDATE
        - Existing deals with older updated_at → SKIP (keep existing)
        
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
        logger.info("=" * 80)
        logger.info(f"Starting Serving Layer Refresh for {self.snapshot_date}")
        logger.info("Strategy: INCREMENTAL UPSERT (read today's partition only)")
        logger.info("=" * 80)
        
        try:
            # Connect to data sources
            self.connect_iceberg()
            self.connect_postgres()
            
            # Get deals for this snapshot_date (incremental)
            columns, rows = self.get_deals_for_date(self.snapshot_date)
            
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
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    job = ServingLayerRefresh(snapshot_date)
    job.run()


if __name__ == "__main__":
    main()
