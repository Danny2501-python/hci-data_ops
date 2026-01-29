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
from psycopg2.extras import execute_values
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

    def ensure_target_table_exists(self, target_table_name: str, schema: pa.Schema, pk_columns: List[str]):
        """
        Create target Postgres table if not exists based on Arrow schema
        """
        
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
        
        cursor = self.pg_conn.cursor()
        try:
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
        Upsert data into Postgres using stream processing
        """
        if not reader:
            return

        cursor = self.pg_conn.cursor()
        columns = reader.schema.names
        
        cols_str = ', '.join([f'"{c}"' for c in columns])
        
        sql_template = f"INSERT INTO {target_table_name} ({cols_str}) VALUES %s"
        
        if pk_columns:
            # ON CONFLICT behavior
            pk_str = ", ".join([f'"{c}"' for c in pk_columns])
            
            # Update all columns EXCEPT PK
            update_cols = [c for c in columns if c not in pk_columns]
            
            if update_cols:
                update_set = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                sql_template += f"""
                ON CONFLICT ({pk_str}) 
                DO UPDATE SET {update_set}
                """
            else:
                # If only PK columns exist (edge case), do nothing
                sql_template += f" ON CONFLICT ({pk_str}) DO NOTHING"
        
        try:
            total_rows = 0
            # Iterate through batches from Iceberg
            for batch in reader:
                if len(batch) == 0:
                    continue
                    
                # Convert batch to Python list of tuples
                d = batch.to_pydict()
                # Zip columns
                rows = list(zip(*[d[c] for c in columns]))
                
                # Batch execution for this chunk
                BATCH_SIZE = 50000
                for i in range(0, len(rows), BATCH_SIZE):
                    chunk = rows[i:i + BATCH_SIZE]
                    execute_values(cursor, sql_template, chunk, page_size=BATCH_SIZE)
                    total_rows += len(chunk)
                
                logger.debug(f"Processed batch of {len(rows)} rows (Total so far: {total_rows})")
            
            self.pg_conn.commit()
            logger.success(f"✅ Successfully processed {total_rows} rows for {target_table_name}")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to upsert data: {e}")
            raise
        finally:
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
        target_table_name = f"{table_name}_v1"
        
        logger.info(f"Processing {table_name} -> {target_table_name} (PK: {pk_columns})")
            
        # 3. Connect DB (lazy init)
        if not self.pg_conn:
            self.connect_postgres()
            
        # 4. Ensure Schema (pass schema instead of table)
        self.ensure_target_table_exists(target_table_name, reader.schema, pk_columns)
        
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

    def run(self, table_id: Optional[int] = None, source_type: Optional[str] = None):
        """Run serving ingestion"""
        logger.info(f"Starting Serving Ingestion for {self.snapshot_date} (Source: {source_type})")
        
        configs = self.get_table_configs(table_id=table_id, source_type=source_type)
        
        if not configs:
            logger.warning("No active configurations found")
            return
            
        for config in configs:
            try:
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
    
    try:
        job = ServingIngestion(snapshot_date)
        job.run(table_id, source_type=source_type)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
