"""
Ty Gia Techcombank Serving Layer Ingestion
Reads from Bronze Iceberg and upserts to PostgreSQL (dwh_product)
Upsert strategy: DELETE by (itemid, date) then INSERT
"""

import os
import sys
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import pyarrow as pa
import pyarrow.csv as pv
import psycopg2
from loguru import logger
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


class TyGiaTechcombankServing:
    """
    Serving layer ingestion for Ty Gia Techcombank
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        
        # Initialize Bronze catalog
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
        
        self.pg_conn = None
        
        logger.info(f"Initialized TyGiaTechcombankServing for {self.snapshot_date}")
    
    def connect_postgres(self):
        """Connect to PostgreSQL with Keepalive enabled"""
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        try:
            # Enable TCP Keepalive to prevent connection drops
            self.pg_conn = psycopg2.connect(
                POSTGRES_URI,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            self.pg_conn.autocommit = False
            logger.info("Connected to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            raise
    
    def get_iceberg_data(self, table_name: str) -> Optional[pa.RecordBatchReader]:
        """
        Read data from Bronze Iceberg as streaming reader
        Returns RecordBatchReader for memory efficiency
        """
        full_table_name = f'call_functions.{table_name}'
        
        try:
            table = self.bronze_catalog.load_table(full_table_name)
            scan = table.scan(row_filter=EqualTo("snapshot_date", self.snapshot_date))
            reader = scan.to_arrow_batch_reader()
            
            logger.info(f"📖 Reading from {full_table_name} (snapshot_date={self.snapshot_date})")
            return reader
            
        except Exception as e:
            logger.error(f"Failed to read {full_table_name}: {e}")
            return None
    
    def ensure_table_exists(self, target_table: str, schema: pa.Schema):
        """
        Create table if not exists based on Arrow schema
        No primary key constraint
        """
        cursor = self.pg_conn.cursor()
        
        try:
            # Map Arrow types to Postgres types
            type_map = {
                pa.string(): 'TEXT',
                pa.int32(): 'INTEGER',
                pa.int64(): 'BIGINT',
                pa.float32(): 'REAL',
                pa.float64(): 'DOUBLE PRECISION',
                pa.bool_(): 'BOOLEAN',
                pa.date32(): 'DATE',
                pa.timestamp('us'): 'TIMESTAMP',
                pa.timestamp('ms'): 'TIMESTAMP',
                pa.timestamp('ns'): 'TIMESTAMP',
            }
            
            column_defs = []
            for field in schema:
                pg_type = 'TEXT'  # Default
                
                if field.type in type_map:
                    pg_type = type_map[field.type]
                elif isinstance(field.type, pa.TimestampType):
                    pg_type = 'TIMESTAMP'
                elif field.type.equals(pa.date32()) or field.type.equals(pa.date64()):
                    pg_type = 'DATE'
                
                column_defs.append(f'"{field.name}" {pg_type}')
            
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                {', '.join(column_defs)}
            )
            """
            
            cursor.execute(create_sql)
            self.pg_conn.commit()
            logger.info(f"Ensured table {target_table} exists")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to create table {target_table}: {e}")
            raise
        finally:
            cursor.close()
    
    def upsert_data(self, target_table: str, reader: pa.RecordBatchReader):
        """
        Upsert data using DELETE + INSERT strategy
        1. Create staging table
        2. COPY data to staging
        3. DELETE from target WHERE (itemid, date) IN staging
        4. INSERT from staging to target
        """
        if not reader:
            return
        
        # Generate staging table name
        import uuid
        staging_table = f"{target_table}_staging_{uuid.uuid4().hex[:8]}"
        
        cursor = self.pg_conn.cursor()
        
        try:
            # 1. Create staging table (unlogged for speed)
            cursor.execute(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table} INCLUDING DEFAULTS)")
            
            # 2. Stream data to staging using COPY
            csv_write_options = pv.WriteOptions(include_header=False)
            total_rows = 0
            
            import io
            import gc
            
            CHUNK_SIZE = 10000
            
            for batch in reader:
                if len(batch) == 0:
                    continue
                
                columns = batch.schema.names
                cols_str = ', '.join([f'"{c}"' for c in columns])
                
                num_rows = len(batch)
                
                for i in range(0, num_rows, CHUNK_SIZE):
                    chunk = batch.slice(offset=i, length=min(CHUNK_SIZE, num_rows - i))
                    
                    csv_buffer = io.BytesIO()
                    
                    try:
                        pv.write_csv(chunk, csv_buffer, write_options=csv_write_options)
                        csv_buffer.seek(0)
                        
                        sql_copy = f'COPY {staging_table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)'
                        cursor.copy_expert(sql_copy, csv_buffer)
                        
                        total_rows += len(chunk)
                        logger.debug(f"Streamed {len(chunk)} rows to staging (Total: {total_rows})")
                        
                    finally:
                        csv_buffer.close()
                        del csv_buffer
                        del chunk
                
                del batch
                gc.collect()
            
            logger.info(f"Loaded {total_rows} rows into staging table")
            
            # 3. DELETE existing records by (itemid, date)
            logger.info("Deleting existing records matching (itemid, date)")
            delete_sql = f"""
                DELETE FROM {target_table} AS dest
                USING {staging_table} AS src
                WHERE dest.itemid = src.itemid AND dest.date = src.date
            """
            cursor.execute(delete_sql)
            deleted_count = cursor.rowcount
            logger.info(f"Deleted {deleted_count} existing rows")
            
            # 4. INSERT from staging to target
            logger.info("Inserting new records")
            cols_str = ', '.join([f'"{c}"' for c in reader.schema.names])
            insert_sql = f"""
                INSERT INTO {target_table} ({cols_str})
                SELECT {cols_str} FROM {staging_table}
            """
            cursor.execute(insert_sql)
            
            self.pg_conn.commit()
            logger.success(f"✅ Successfully upserted {total_rows} rows to {target_table}")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to upsert data: {e}")
            raise
        finally:
            # Cleanup staging table
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                self.pg_conn.commit()
            except Exception as e:
                logger.warning(f"Failed to drop staging table: {e}")
            
            cursor.close()
    
    def run(self, source_table: str = 'ty_gia_techcombank', target_table: str = 'ty_gia_techcombank_v1'):
        """
        Execute serving layer ingestion
        
        Args:
            source_table: Source Iceberg table name
            target_table: Target PostgreSQL table name
        """
        logger.info("="*80)
        logger.info(f"Starting Ty Gia Techcombank Serving Layer Ingestion")
        logger.info(f"Snapshot Date: {self.snapshot_date}")
        logger.info(f"Source: {source_table} (Iceberg)")
        logger.info(f"Target: {target_table} (PostgreSQL)")
        logger.info("="*80)
        
        try:
            # Read from Iceberg
            reader = self.get_iceberg_data(source_table)
            
            if not reader:
                logger.warning("No data to process")
                return
            
            # Connect to PostgreSQL (lazy)
            if not self.pg_conn:
                self.connect_postgres()
            
            # Ensure table exists
            self.ensure_table_exists(target_table, reader.schema)
            
            # Upsert data
            self.upsert_data(target_table, reader)
            
            # Close connection
            if self.pg_conn:
                self.pg_conn.close()
                logger.info("👋 Closed PostgreSQL connection")
            
            logger.success("✅ Ty Gia Techcombank serving layer ingestion completed")
            
        except Exception as e:
            logger.error(f"❌ Serving layer ingestion failed: {e}")
            raise


def main():
    """Main execution function"""
    # Parse command-line arguments
    # Usage: python ty_gia_techcombank_serving.py [snapshot_date] [source_table] [target_table]
    
    # Argument 1: snapshot_date (YYYY-MM-DD)
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    # Argument 2: source_table (optional)
    source_table = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else 'ty_gia_techcombank'
    
    # Argument 3: target_table (optional)
    target_table = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else 'ty_gia_techcombank'
    
    logger.info(f"Arguments -> Snapshot Date: {snapshot_date}, Source: {source_table}, Target: {target_table}")
    
    try:
        job = TyGiaTechcombankServing(snapshot_date)
        job.run(source_table=source_table, target_table=target_table)
        
        logger.success("✅ Serving layer ingestion completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Serving layer ingestion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
