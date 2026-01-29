"""
Serving Layer Ingestion for Call Functions
Reflects data from Bronze Iceberg to PostgreSQL (dwh_product) 1:1
"""

import os
import sys
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import pyarrow as pa
import psycopg2
import pyarrow.csv as pv
import io
import uuid
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.etl_table_config import get_active_table_configs

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Postgres configuration (dwh_product)
# Assuming POSTGRES_URI points to dwh_product or we use a specific one if available
POSTGRES_URI = os.getenv('POSTGRES_URI')


class ServingIngestion:
    """
    Ingest data from Bronze Iceberg to Postgres Serving Layer
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
        
    def connect_postgres(self):
        """Connect to PostgreSQL"""
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        try:
            self.pg_conn = psycopg2.connect(POSTGRES_URI)
            self.pg_conn.autocommit = False
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            raise

    def get_iceberg_data(self, table_name: str):
        """
        Read data from Iceberg for the current snapshot_date
        Returns pyarrow Table or None
        """
        full_table_name = f'call_functions.{table_name}'
        try:
            table = self.bronze_catalog.load_table(full_table_name)
        except Exception as e:
            logger.warning(f"Iceberg table {full_table_name} not found: {e}")
            return None

        logger.info(f"Scanning {full_table_name} for {self.snapshot_date}")
        
        try:
            scan = table.scan(
                row_filter=EqualTo("snapshot_date", self.snapshot_date)
            )
            
            # Use to_arrow_batch_reader to stream data instead of loading everything to memory
            return scan.to_arrow_batch_reader()
            
        except Exception as e:
            logger.error(f"Failed to read Iceberg data: {e}")
            raise

    def ensure_target_table_exists(self, target_table_name: str, schema: pa.Schema, pk_columns: List[str], drop_table: bool = False):
        """
        Create target Postgres table if not exists based on Arrow schema
        """
        cursor = self.pg_conn.cursor()
        try:
             # Drop table if requested
            if drop_table:
                logger.warning(f"Dropping table {target_table_name} as requested")
                cursor.execute(f"DROP TABLE IF EXISTS {target_table_name}")
                self.pg_conn.commit()

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
                pg_type = 'TEXT' # Default
                
                # Simple type check
                if field.type in type_map:
                    pg_type = type_map[field.type]
                elif isinstance(field.type, pa.TimestampType):
                     pg_type = 'TIMESTAMP'
                elif field.type.equals(pa.date32()) or field.type.equals(pa.date64()):
                     pg_type = 'DATE'
                
                column_defs.append(f'"{field.name}" {pg_type}')
                
            pk_constraint = ""
            if pk_columns:
                pk_str = ", ".join([f'"{c}"' for c in pk_columns])
                pk_constraint = f', PRIMARY KEY ({pk_str})'
                
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table_name} (
                {', '.join(column_defs)}
                {pk_constraint}
            )
            """
            
            cursor.execute(create_sql)
            self.pg_conn.commit()
            
            # If table exists but PK is missing (migration case), we might need to add it?
            # For now assume it's okay or handled manually if schema changes drastically.
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to create table {target_table_name}: {e}")
            raise
        finally:
            cursor.close()

    def upsert_data(self, target_table_name: str, reader: pa.RecordBatchReader, pk_columns: List[str]):
        """
        Upsert data into Postgres using Staging Table + COPY protocol
        This avoids OOM by not creating Python objects for every row.
        """
        if not reader:
            return

        # Generate a unique staging table name
        staging_table = f"{target_table_name}_staging_{uuid.uuid4().hex[:8]}"
        
        cursor = self.pg_conn.cursor()
        
        try:
            # 1. Create Staging Table (Unlogged for speed)
            # clone structure from target_table
            cursor.execute(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table_name} INCLUDING DEFAULTS)")
            
            # 2. Stream Data into Staging using COPY
            # CSV format options for PyArrow
            csv_write_options = pv.WriteOptions(include_header=False)
            # Adjust timestamp format if needed, but ISO8601 usually works for Postgres
            
            total_rows = 0
            
            # Iterate through batches
            # Iterate through batches
            CHUNK_SIZE = 20000 # Process max 20k rows at a time to control RAM peak
            
            import gc # Ensure gc is available

            for batch in reader:
                if len(batch) == 0:
                    continue
                
                # Check types and cast if necessary (e.g. Timestamp to generic string if issues arise)
                # For now rely on PyArrow's default CSV serialization which is ISO8601
                columns = batch.schema.names
                cols_str = ', '.join([f'"{c}"' for c in columns])
                
                # Manual slicing for large batches to avoid huge CSV buffers
                num_rows = len(batch)
                
                for i in range(0, num_rows, CHUNK_SIZE):
                    # Slice the batch (view, zero-copy usually)
                    chunk = batch.slice(offset=i, length=min(CHUNK_SIZE, num_rows - i))
                    
                    # Buffer for CSV data
                    csv_buffer = io.BytesIO()
                    
                    try:
                        # Write chunk to CSV buffer
                        pv.write_csv(chunk, csv_buffer, write_options=csv_write_options)
                        csv_buffer.seek(0)
                        
                        sql_copy = f"COPY {staging_table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)"
                        cursor.copy_expert(sql_copy, csv_buffer)
                        
                        total_rows += len(chunk)
                        logger.debug(f"Streamed chunk {len(chunk)} rows to staging (Total: {total_rows})")
                        
                    finally:
                        # Explicit cleanup
                        csv_buffer.close()
                        del csv_buffer
                        del chunk
                
                # Force GC after each big batch
                del batch
                gc.collect()

            # 3. Perform Merge/Upsert from Staging to Target
            cols_str = ', '.join([f'"{c}"' for c in reader.schema.names])
            
            if pk_columns:
                pk_str = ", ".join([f'"{c}"' for c in pk_columns])
                columns = reader.schema.names
                update_cols = [c for c in columns if c not in pk_columns]

                if update_cols:
                    update_set = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                    sql_merge = f"""
                    INSERT INTO {target_table_name} ({cols_str})
                    SELECT {cols_str} FROM {staging_table}
                    ON CONFLICT ({pk_str}) 
                    DO UPDATE SET {update_set}
                    """
                else:
                    # PK only case
                    sql_merge = f"""
                    INSERT INTO {target_table_name} ({cols_str})
                    SELECT {cols_str} FROM {staging_table}
                    ON CONFLICT ({pk_str}) DO NOTHING
                    """
            else:
                # No PK, just append (though this shouldn't happen based on logic)
                sql_merge = f"""
                INSERT INTO {target_table_name} ({cols_str})
                SELECT {cols_str} FROM {staging_table}
                """
            
            logger.info("Merging data from staging to target table...")
            try:
                cursor.execute(sql_merge)
                self.pg_conn.commit()
                logger.success(f"✅ Successfully loaded {total_rows} rows into {target_table_name}")
            except Exception as e:
                self.pg_conn.rollback()
                logger.warning(f"Standard UPSERT failed: {e}. Attempting fallback (Drop PK -> Delete -> Insert)")
                
                if not pk_columns:
                    # If no PK, we can't really "Delete by key", so re-raise
                    raise e
                
                # Renew cursor
                cursor.close()
                cursor = self.pg_conn.cursor()
                
                try:
                    # 1. Drop PK Constraint
                    # Assumption: Default Postgres naming convention: table_name + _pkey
                    constraint_name = f"{target_table_name}_pkey"
                    logger.info(f"Fallback: Dropping constraint {constraint_name} if exists")
                    cursor.execute(f"ALTER TABLE {target_table_name} DROP CONSTRAINT IF EXISTS {constraint_name}")
                    
                    # 2. Delete existing rows (DELETE ... USING ...)
                    # Use aliases to safely handle table names (even with schemas) inside the WHERE clause
                    pk_match = " AND ".join([f'dest."{c}" = src."{c}"' for c in pk_columns])
                    sql_delete = f'DELETE FROM {target_table_name} AS dest USING {staging_table} AS src WHERE {pk_match}'
                    logger.info("Fallback: Deleting existing rows matching keys")
                    cursor.execute(sql_delete)
                    
                    # 3. Insert new rows
                    sql_insert = f'INSERT INTO {target_table_name} ({cols_str}) SELECT {cols_str} FROM {staging_table}'
                    logger.info("Fallback: Inserting new rows")
                    cursor.execute(sql_insert)
                    
                    self.pg_conn.commit()
                    logger.success(f"✅ Fallback successful: Loaded {total_rows} rows via DELETE+INSERT")
                    
                except Exception as fallback_e:
                    self.pg_conn.rollback()
                    logger.error(f"Fallback failed: {fallback_e}")
                    raise fallback_e  # Raise the secondary error

        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to upsert data via staging: {e}")
            raise
        finally:
            # 4. Cleanup Staging
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                self.pg_conn.commit()
            except Exception as e:
                logger.warning(f"Failed to drop staging table {staging_table}: {e}")
            
            cursor.close()

    def process_table(self, config: Dict):
        """Process a single table"""
        table_name = config['table_name']
        # 1. Get Data Stream
        reader = self.get_iceberg_data(table_name)
        if not reader:
            return

        # 2. Determine Primary Key
        primary_keyword = config.get('primary_keyword')
        
        # Use reader.schema.names instead of arrow_table.column_names
        column_names = reader.schema.names
        
        if not primary_keyword:
            # Fallback if 'id' exists in actual data schema
            if 'id' in column_names:
                 logger.warning(f"No primary_keyword for {table_name}, defaulting to 'id'")
                 primary_keyword = 'id'
            else:
                 logger.warning(f"Skipping {table_name}: No primary_keyword configured and no 'id' column found")
                 return

        pk_columns = [k.strip() for k in primary_keyword.split(',')]
        target_table_name = table_name
        
        logger.info(f"Processing {table_name} -> {target_table_name} (PK: {pk_columns})")
            
        # 3. Connect DB (lazy init)
        if not self.pg_conn:
            self.connect_postgres()
            
        # 4. Ensure Schema (pass schema instead of table)
        drop_table = config.get('drop_table', False)
        self.ensure_target_table_exists(target_table_name, reader.schema, pk_columns, drop_table=drop_table)
        
        # 5. Upsert (pass reader)
        self.upsert_data(target_table_name, reader, pk_columns)

    def get_table_configs(self, table_id: Optional[int] = None, source_type: Optional[str] = None) -> List[Dict]:
        """
        Read table configurations using common module
        """
        # If source_type is standard (None), we use the default logic
        configs = get_active_table_configs(table_id=table_id, source=source_type)
        logger.info(f"Loaded {len(configs)} table configurations")
        return configs

    def run(self, table_id: Optional[int] = None, source_type: Optional[str] = None, drop_table: bool = False):
        """Run serving ingestion"""
        logger.info(f"Starting Serving Ingestion for {self.snapshot_date} (Source: {source_type})")
        
        configs = self.get_table_configs(table_id=table_id, source_type=source_type)
        
        if not configs:
            logger.warning("No active configurations found")
            return
            
        for config in configs:
            try:
                # Override config with runtime flag if provided
                if drop_table:
                    config['drop_table'] = True
                    
                self.process_table(config)
            except Exception as e:
                logger.error(f"Failed table {config['table_name']}: {e}")
                
        if self.pg_conn:
            self.pg_conn.close()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    table_id = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else None
    
    # Arg 3: source_type (e.g. 'ERP')
    source_type = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else None
    
    # Arg 4: drop_table flag (pass 'true' or '1' to enable)
    drop_table_arg = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else 'false'
    drop_table = drop_table_arg.lower() in ('true', '1', 'yes')
    
    try:
        job = ServingIngestion(snapshot_date)
        job.run(table_id, source_type=source_type, drop_table=drop_table)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
