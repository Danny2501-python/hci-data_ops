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
        
    def backup_dependencies(self, table_name: str) -> List[Dict]:
        """
        Find all views/materialized views dependent on the table.
        Returns a list of dicts with 'type', 'name', 'definition'.
        """
        dependencies = []
        cursor = self.pg_conn.cursor()
        try:
            # Recursive query to find direct and indirect dependencies
            query = f"""
            WITH RECURSIVE dependency_tree AS (
                -- Base case: direct dependencies of the table
                SELECT 
                    rw.ev_class AS oid, 
                    1 AS level
                FROM pg_rewrite rw
                JOIN pg_depend d ON d.objid = rw.oid
                WHERE d.refobjid = '{table_name}'::regclass
                AND rw.ev_class != d.refobjid
                
                UNION ALL
                
                -- Recursive case: dependencies of the found views
                SELECT 
                    rw.ev_class AS oid, 
                    dt.level + 1
                FROM pg_rewrite rw
                JOIN pg_depend d ON d.objid = rw.oid
                JOIN dependency_tree dt ON d.refobjid = dt.oid
                WHERE rw.ev_class != d.refobjid
            )
            SELECT 
                n.nspname AS schema_name,
                c.relname AS relation_name,
                c.relkind AS type,
                pg_get_viewdef(c.oid, true) AS definition,
                MAX(dt.level) AS max_level -- Use max level to determine restore order
            FROM dependency_tree dt
            JOIN pg_class c ON c.oid = dt.oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            GROUP BY c.oid, n.nspname, c.relname, c.relkind, c.oid
            ORDER BY max_level ASC; -- Restore Level 1 (Direct) first, then Level 2 (Indirect)
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            for row in rows:
                schema, name, king, definition, level = row
                full_name = f'"{schema}"."{name}"' if schema != 'public' else f'"{name}"'
                dependencies.append({
                    'name': full_name,
                    'type': 'MATERIALIZED VIEW' if king == 'm' else 'VIEW',
                    'definition': definition,
                    'level': level
                })
                logger.info(f"Detected dependency (L{level}): {full_name}")
            
            # Persist to backup table
            if dependencies:
                import json
                deps_json = json.dumps(dependencies)
                cursor.execute("""
                    INSERT INTO public.etl_dependency_backup (table_name, backup_data, created_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (table_name) 
                    DO UPDATE SET backup_data = EXCLUDED.backup_data, created_at = NOW()
                """, (table_name, deps_json))
                self.pg_conn.commit()
                logger.info(f"Persisted dependency backup for {table_name} to DB")
                
        except Exception as e:
            logger.warning(f"Could not check dependencies for {table_name} (might not exist yet): {e}")
            self.pg_conn.rollback()
        finally:
            cursor.close()
            
        return dependencies



    def connect_postgres(self):
        """Connect to PostgreSQL with Keepalive enabled"""
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI environment variable not set")
        
        try:
            # Enable TCP Keepalive to prevent connection drops during long Materialized View restoration
            self.pg_conn = psycopg2.connect(
                POSTGRES_URI,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            self.pg_conn.autocommit = False
            logger.info("Connected to PostgreSQL")
            
            # Ensure backup table exists for crash recovery
            with self.pg_conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS public.etl_dependency_backup (
                        table_name TEXT PRIMARY KEY,
                        backup_data JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                self.pg_conn.commit()

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

    def ensure_target_table_exists(self, target_table_name: str, schema: pa.Schema, pk_columns: List[str], drop_table: bool = False, dependencies_out: List = None):
        """
        Create target Postgres table if not exists based on Arrow schema
        """
        cursor = self.pg_conn.cursor()
        try:
             # Drop table if requested
            if drop_table:
                if dependencies_out is not None:
                    try:
                        found_deps = self.backup_dependencies(target_table_name)
                        if found_deps:
                            dependencies_out.extend(found_deps)
                    except Exception as e:
                         logger.warning(f"Failed to backup dependencies for {target_table_name}: {e}")

                logger.warning(f"Dropping table {target_table_name} AND DEPENDENCIES (CASCADE) as requested")
                cursor.execute(f"DROP TABLE IF EXISTS {target_table_name} CASCADE")
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

    def upsert_data(self, target_table_name: str, reader: pa.RecordBatchReader, pk_columns: List[str], truncate: bool = False):
        """
        Upsert data into Postgres using an Incremental Batch-wise Staging protocol.
        Optimized for minimum Peak RAM.
        """
        if not reader:
            return

        import gc
        import pyarrow.compute as pc

        # 1. Prepare Column Info
        columns = reader.schema.names
        cols_str = ', '.join([f'"{c}"' for c in columns])
        staging_table = f"{target_table_name}_staging_{uuid.uuid4().hex[:8]}"
        
        # 2. Prepare UPSERT/INSERT SQL
        if pk_columns:
            pk_str = ", ".join([f'"{c}"' for c in pk_columns])
            update_cols = [c for c in columns if c not in pk_columns]
            if update_cols:
                update_set = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                exec_sql = f"INSERT INTO {target_table_name} ({cols_str}) SELECT {cols_str} FROM {staging_table} ON CONFLICT ({pk_str}) DO UPDATE SET {update_set}"
            else:
                exec_sql = f"INSERT INTO {target_table_name} ({cols_str}) SELECT {cols_str} FROM {staging_table} ON CONFLICT ({pk_str}) DO NOTHING"
        else:
            exec_sql = f"INSERT INTO {target_table_name} ({cols_str}) SELECT {cols_str} FROM {staging_table}"

        cursor = self.pg_conn.cursor()
        try:
            # 3. Initialize Staging and Truncate target if needed
            cursor.execute(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table_name} INCLUDING DEFAULTS)")
            if truncate:
                logger.warning(f"Truncating table {target_table_name}...")
                cursor.execute(f"TRUNCATE TABLE {target_table_name}")
            self.pg_conn.commit()

            # 4. Stream and process batch-by-batch
            total_rows = 0
            CHUNK_SIZE = 5000 # Reduced chunk size to lower Peak RAM from io.BytesIO
            csv_write_options = pv.WriteOptions(include_header=False)

            import tempfile
            
            while True:
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    break
                
                if len(batch) == 0: continue
                
                num_rows = len(batch)
                logger.debug(f"Source batch fetched: {num_rows} rows. Processing chunks...")

                for i in range(0, num_rows, CHUNK_SIZE):
                    # Zero-copy slice
                    orig_chunk = batch.slice(offset=i, length=min(CHUNK_SIZE, num_rows - i))
                    
                    # 1. Sanitize column by column to avoid keeping multiple copies
                    sanitized_cols = []
                    for col_idx, field in enumerate(orig_chunk.schema):
                        c = orig_chunk.column(col_idx)
                        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                            # pc.replace_substring creates a new buffer
                            c = pc.replace_substring(c, pattern="\x00", replacement="")
                        sanitized_cols.append(c)
                        del c # Clear reference immediately
                    
                    # 2. Convert to RecordBatch for CSV writing
                    tmp_chunk = pa.RecordBatch.from_arrays(sanitized_cols, schema=orig_chunk.schema)
                    
                    # 3. Use DISK-BASED TEMPORARY FILE instead of RAM buffer
                    # Explicitly use configured temp directory if available
                    temp_dir = os.getenv('TEMP_DIR', '/tmp')
                    with tempfile.TemporaryFile(mode='wb+', dir=temp_dir) as tmp_file:
                        pv.write_csv(tmp_chunk, tmp_file, write_options=csv_write_options)
                        tmp_file.seek(0)
                        
                        # Streaming COPY to Postgres from DISK
                        cursor.copy_expert(f"COPY {staging_table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)", tmp_file)
                        cursor.execute(exec_sql)
                        cursor.execute(f"TRUNCATE TABLE {staging_table}")
                    
                    total_rows += len(tmp_chunk)
                    
                    # 4. Aggressive cleanup for EVERY chunk
                    del sanitized_cols
                    del tmp_chunk
                    del orig_chunk
                    gc.collect()
                    pa.default_memory_pool().release_unused() # Return memory to OS
                
                self.pg_conn.commit()
                logger.info(f"Ingested {total_rows} rows total.")
                
                # Final cleanup for the big batch
                del batch
                gc.collect()
                pa.default_memory_pool().release_unused()

            logger.success(f"✅ Successfully loaded {total_rows} rows into {target_table_name}")

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
        
        if primary_keyword:
            pk_columns = [k.strip() for k in primary_keyword.split(',')]
        else:
            logger.warning(f"No primary_keyword for {table_name}. Proceeding with simple INSERT (Append only)")
            pk_columns = []
        target_table_name = table_name
        
        logger.info(f"Processing {table_name} -> {target_table_name} (PK: {pk_columns})")
            
        # 3. Connect DB (lazy init)
        if not self.pg_conn:
            self.connect_postgres()
            
        # 4. Ensure Schema (pass schema instead of table)
        drop_table = config.get('drop_table', False)
        dependencies = []
        self.ensure_target_table_exists(target_table_name, reader.schema, pk_columns, drop_table=drop_table, dependencies_out=dependencies)
        
        # 5. Upsert (pass reader)
        # 5. Upsert (pass reader)
        truncate = config.get('truncate', False)
        self.upsert_data(target_table_name, reader, pk_columns, truncate=truncate)
        
        # 6. Restore Dependencies (SKIPPED AS REQUESTED)
        # User requested manual restoration. Dependencies are preserved in public.etl_dependency_backup
        if drop_table and dependencies:
             logger.warning(f"Dependencies for {target_table_name} have been dropped (CASCADE).")
             logger.warning(f"Backup saved to table 'public.etl_dependency_backup'. Please restore manually.")

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
