"""
Ticket Source Metadata Serving Layer
Architecture: Iceberg Bronze → PostgreSQL Serving Layer

Read from: Bronze → cs_ticket_source_metadata
Write to: PostgreSQL → cs_ticket_source_detail_api

UPSERT Strategy:
- Read ONLY today's partition (all brands)
- UPSERT into PostgreSQL (ON CONFLICT UPDATE)
- PostgreSQL automatically maintains latest state
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
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# PostgreSQL configuration
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Source and target
SOURCE_TABLE = 'caresoft.cs_ticket_source_detail_api'
TARGET_TABLE = 'cs_ticket_source_detail_api'


class TicketSourceMetadataServingLayer:
    """
    Refresh PostgreSQL serving layer with latest ticket source metadata
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        self.pg_conn = None
        
    def connect_iceberg(self):
        """Connect to Iceberg Bronze catalog"""
        logger.info(f"Connecting to Iceberg Bronze catalog at {ICEBERG_REST_URI_BRONZE}")
        
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
        
        logger.success("✅ Connected to Iceberg Bronze catalog")
        
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
                logger.warning(f"Could not add PK: {e}")

            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()
    
    def get_ticket_sources_for_date(self, snapshot_date: date):
        """
        Read ticket source metadata from Iceberg for a specific snapshot_date.
        Uses DuckDB + Parquet for memory-efficient deduplication.
        
        Strategy:
        1. Iceberg scan → Parquet (streaming, no full load)
        2. DuckDB reads Parquet → SQL deduplication (vectorized)
        3. Return only deduplicated results
        
        Args:
            snapshot_date: Date to read
        
        Returns:
            Tuple of (columns, rows)
        """
        import duckdb
        import pyarrow as pa
        import pyarrow.parquet as pq
        import os
        
        snapshot_date_str = snapshot_date.strftime('%Y-%m-%d')
        logger.info(f"Reading ticket sources for snapshot_date={snapshot_date_str} from {SOURCE_TABLE}")
        
        table = self.bronze_catalog.load_table(SOURCE_TABLE)
        
        # Get temp directory from environment or use default
        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"ticket_source_metadata_{os.getpid()}.parquet")
        
        try:
            logger.info(f"Scanning snapshot: {snapshot_date_str}")
            
            # Scan ONLY the partition for this snapshot_date (all brands)
            scan = table.scan(row_filter=EqualTo("snapshot_date", snapshot_date_str))
            
            # Collect all batches and write to Parquet (streaming)
            batches = []
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    batches.append(batch)
            
            if not batches:
                logger.warning(f"No ticket sources found for snapshot {snapshot_date_str}")
                return [], []
            
            # Combine and write to Parquet
            combined = pa.Table.from_batches(batches)
            pq.write_table(combined, temp_parquet, compression='snappy')
            
            logger.info(f"Written {len(combined)} rows to Parquet, deduplicating...")
            
            # DuckDB: Read Parquet and deduplicate with SQL
            # Config optimized for containerized environments
            con = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1'
            })
            
            # Deduplication query: Keep latest record per (id, brand_id)
            # ROW_NUMBER() is more efficient than GROUP BY for this case
            dedup_query = f"""
            SELECT * EXCLUDE(rn)
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY id, brand_id 
                           ORDER BY updated_at DESC NULLS LAST
                       ) as rn
                FROM read_parquet('{temp_parquet}')
                WHERE id IS NOT NULL
            ) WHERE rn = 1
            ORDER BY brand_id, id
            """
            
            # Execute and get deduplicated result
            deduped_result = con.execute(dedup_query).fetch_arrow_table()
            con.close()
            
            logger.success(f"✅ After deduplication: {len(deduped_result)} unique sources")
            
            # Convert to columns and rows (only deduplicated data)
            columns = deduped_result.schema.names
            rows = []
            for i in range(len(deduped_result)):
                row = tuple(deduped_result.column(j)[i].as_py() for j in range(len(columns)))
                rows.append(row)
            
            return columns, rows
            
        finally:
            # Clean up temp Parquet file
            if os.path.exists(temp_parquet):
                try:
                    os.remove(temp_parquet)
                except:
                    pass
    
    def upsert_serving_table(self, columns, rows):
        """
        UPSERT data into serving table (Insert or Update on Conflict)
        
        Strategy:
        - New sources → INSERT
        - Existing sources with newer updated_at → UPDATE
        - Existing sources with older updated_at → SKIP
        
        Args:
            columns: List of column names
            rows: List of tuples containing data
        """
        if not rows:
            logger.warning("No data to upsert")
            return
            
        logger.info(f"Upserting {len(rows)} records to {TARGET_TABLE}...")
        
        cursor = self.pg_conn.cursor()
        try:
            # Build column list (quoted for reserved words like "index")
            quoted_columns = [f'"{col}"' for col in columns]
            columns_str = ', '.join(quoted_columns)
            
            # Build UPDATE SET clause (exclude PK columns)
            pk_columns = {'id', 'brand_id'}
            update_columns = [col for col in columns if col not in pk_columns]
            update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
            
            # UPSERT SQL with conflict resolution
            upsert_sql = f"""
            INSERT INTO {TARGET_TABLE} ({columns_str})
            VALUES %s
            ON CONFLICT (id, brand_id)
            DO UPDATE SET
                {update_set}
            WHERE 
                {TARGET_TABLE}.updated_at < EXCLUDED.updated_at
                OR {TARGET_TABLE}.updated_at IS NULL
            """
            
            # Execute batch insert
            execute_values(cursor, upsert_sql, rows)
            self.pg_conn.commit()
            
            logger.success(f"✅ Upserted to {TARGET_TABLE}")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Error during UPSERT: {e}")
            raise
        finally:
            cursor.close()
    
    def run(self):
        """Execute serving layer refresh"""
        logger.info("=" * 80)
        logger.info(f"Starting Ticket Source Metadata Serving Layer Refresh")
        logger.info(f"Snapshot Date: {self.snapshot_date}")
        logger.info("=" * 80)
        
        try:
            # Connect to data sources
            self.connect_iceberg()
            self.connect_postgres()
            
            # Read from Iceberg (all brands for this date)
            columns, rows = self.get_ticket_sources_for_date(self.snapshot_date)
            
            if not rows:
                logger.warning("No data to process")
                return
            
            # Ensure target table exists (with columns from data)
            self.ensure_serving_table_exists(columns)
            
            # UPSERT to PostgreSQL
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
    """
    Main entry point
    Usage: python ticket_source_metadata_serving_layer.py <snapshot_date>
    """
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    job = TicketSourceMetadataServingLayer(snapshot_date)
    job.run()


if __name__ == "__main__":
    main()
