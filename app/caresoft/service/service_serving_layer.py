"""
Service Serving Layer: Iceberg Bronze → PostgreSQL DWH (cs_services_api)

Flow:
    Bronze (caresoft.cs_services_api) → DuckDB (Deduplicate) → PostgreSQL (cs_services_api)
"""

import os
import sys
import duckdb
from datetime import date, datetime
from loguru import logger
import psycopg2
from psycopg2.extras import execute_values
from pyiceberg.catalog import load_catalog

# Configuration
ICEBERG_REST_URI = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME = os.getenv('CATALOG_NAME_BRONZE')
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Table names
SOURCE_TABLE = 'caresoft.cs_services_api'
TARGET_TABLE = 'cs_services_api'

class ServiceServingLayer:
    """Refresh Service serving layer from Bronze"""
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        self.pg_conn = None
        
        # Connect to Iceberg
        self.bronze_catalog = load_catalog(
            CATALOG_NAME,
            **{
                "uri": ICEBERG_REST_URI,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
                "s3.region": AWS_REGION,
            }
        )
        
        # Connect to Postgres
        self.pg_conn = psycopg2.connect(POSTGRES_URI)

    def ensure_serving_table_exists(self, columns):
        """Create serving table with PK (service_id, brand_id)"""
        cursor = self.pg_conn.cursor()
        try:
            column_defs = [f'"{col}" TEXT' for col in columns]
            
            sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                {', '.join(column_defs)},
                PRIMARY KEY ("service_id", "brand_id")
            )
            """
            cursor.execute(sql)
            self.pg_conn.commit()
            
            # Ensure PK if table existed without it
            try:
                cursor.execute(f'ALTER TABLE {TARGET_TABLE} ADD PRIMARY KEY ("service_id", "brand_id");')
                self.pg_conn.commit()
            except:
                self.pg_conn.rollback()
                
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def get_latest_data(self):
        """
        Read from Bronze Iceberg
        Deduplicate using DuckDB
        Return: (columns, rows)
        """
        table = self.bronze_catalog.load_table(SOURCE_TABLE)
        
        # Scan Iceberg table
        arrow_table = table.scan().to_arrow()
        
        if len(arrow_table) == 0:
            return [], []
            
        # Use DuckDB for efficient deduplication
        con = duckdb.connect(database=':memory:')
        con.register('arrow_table', arrow_table)
        
        # Write to temp parquet to handle large datasets if needed
        # But for services (small dataset), arrow is fine. 
        # Keeping usage consistent with other modules.
        temp_parquet = f"{os.getenv('DUCKDB_TEMP_DIR', '/tmp')}/services_temp.parquet"
        con.execute(f"COPY (SELECT * FROM arrow_table) TO '{temp_parquet}' (FORMAT PARQUET)")
        
        # Deduplicate by (service_id, brand_id)
        # Take latest by ingestion_timestamp
        query = f"""
        SELECT 
            service_id,
            service_name,
            type,
            created_at,
            updated_at,
            status,
            description,
            brand_id,
            raw_data
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY service_id, brand_id 
                    ORDER BY ingestion_timestamp DESC
                ) as rn
            FROM read_parquet('{temp_parquet}')
            WHERE service_id IS NOT NULL
        ) WHERE rn=1
        """
        
        result = con.execute(query).fetchall()
        columns = [desc[0] for desc in con.description]
        
        con.close()
        # Clean up
        if os.path.exists(temp_parquet):
            os.remove(temp_parquet)
            
        return columns, result

    def upsert_serving(self, columns, rows):
        if not rows: return
        
        self.ensure_serving_table_exists(columns)
        cursor = self.pg_conn.cursor()
        
        try:
            cols_str = ', '.join([f'"{c}"' for c in columns])
            
            # UPDATE on conflict
            update_cols = [c for c in columns if c not in ('service_id', 'brand_id')]
            update_set = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
            
            sql = f"""
            INSERT INTO {TARGET_TABLE} ({cols_str})
            VALUES %s
            ON CONFLICT ("service_id", "brand_id")
            DO UPDATE SET
                {update_set}
            """
            
            execute_values(cursor, sql, rows, page_size=1000)
            self.pg_conn.commit()
            logger.success(f"Upserted {len(rows)} service records")
            
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def run(self):
        logger.info(f"Starting Service Serving Layer update for {self.snapshot_date}")
        
        try:
            columns, rows = self.get_latest_data()
            
            if not rows:
                logger.info("No data found in Bronze")
                return

            self.upsert_serving(columns, rows)
            
        except Exception as e:
            logger.error(f"Serving layer failed: {e}")
            raise e
        finally:
            if self.pg_conn:
                self.pg_conn.close()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    job = ServiceServingLayer(snapshot_date)
    job.run()

if __name__ == "__main__":
    main()
