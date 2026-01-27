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
from common.etl_table_config import get_active_table_configs, get_table_config_by_id

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
            # check if snapshot_date exists in schema to avoid error
            # It should exist as per bronze design
            
            scan = table.scan(
                row_filter=EqualTo("snapshot_date", self.snapshot_date)
            )
            
            arrow_table = scan.to_arrow()
            if len(arrow_table) == 0:
                logger.info(f"No data found for {self.snapshot_date}")
                return None
                
            return arrow_table
            
        except Exception as e:
            logger.error(f"Failed to read Iceberg data: {e}")
            raise

    def ensure_target_table_exists(self, target_table_name: str, arrow_table: pa.Table, pk_columns: List[str]):
        """
        Create target Postgres table if not exists based on Arrow schema
        """
        schema = arrow_table.schema
        
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

    def upsert_data(self, target_table_name: str, arrow_table: pa.Table, pk_columns: List[str]):
        """
        Upsert data into Postgres
        """
        if not arrow_table or len(arrow_table) == 0:
            return

        cursor = self.pg_conn.cursor()
        columns = arrow_table.column_names
        
        # Prepare data values
        # Convert Arrow table to Python list of tuples for execute_values
        # Need to handle types compatible with psycopg2
        
        data_rows = []
        for batch in arrow_table.to_batches():
            d = batch.to_pydict()
            # Zip columns
            rows = zip(*[d[c] for c in columns])
            data_rows.extend(rows)
            
        # Clean data for Postgres (handle None/NaN/Timestamps)
        # Psycopg2 handles None as NULL. 
        
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
            # Batch execution
            BATCH_SIZE = 5000
            for i in range(0, len(data_rows), BATCH_SIZE):
                batch = data_rows[i:i + BATCH_SIZE]
                execute_values(cursor, sql_template, batch, page_size=BATCH_SIZE)
                logger.info(f"Upserted items {i} to {i+len(batch)}")
            
            self.pg_conn.commit()
            logger.success(f"✅ Successfully processed {len(data_rows)} rows for {target_table_name}")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to upsert data: {e}")
            raise
        finally:
            cursor.close()

    def process_table(self, config: Dict):
        """Process a single table"""
        table_name = config['table_name']
        primary_keyword = config.get('primary_keyword')
        
        if not primary_keyword:
            logger.warning(f"Skipping {table_name}: No primary_keyword configured")
            return

        pk_columns = [k.strip() for k in primary_keyword.split(',')]
        target_table_name = f"{table_name}_v1"
        
        logger.info(f"Processing {table_name} -> {target_table_name} (PK: {pk_columns})")
        
        # 1. Get Data
        arrow_table = self.get_iceberg_data(table_name)
        if not arrow_table:
            return
            
        # 2. Connect DB (lazy init)
        if not self.pg_conn:
            self.connect_postgres()
            
        # 3. Ensure Schema
        self.ensure_target_table_exists(target_table_name, arrow_table, pk_columns)
        
        # 4. Upsert
        self.upsert_data(target_table_name, arrow_table, pk_columns)

    def get_table_configs(self, table_id: Optional[int] = None) -> List[Dict]:
        """
        Read table configurations using common module
        """
        configs = get_active_table_configs(table_id=table_id)
        logger.info(f"Loaded {len(configs)} table configurations")
        return configs

    def run(self, table_id: Optional[int] = None):
        """Run serving ingestion"""
        logger.info(f"Starting Serving Ingestion for {self.snapshot_date}")
        
        configs = self.get_table_configs(table_id=table_id)
        
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
    
    try:
        job = ServingIngestion(snapshot_date)
        job.run(table_id)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
