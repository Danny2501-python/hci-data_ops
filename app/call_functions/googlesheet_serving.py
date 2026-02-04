"""
Google Sheet Serving Layer Ingestion
Adapted from serving_ingestion.py pattern but using Google Sheet config directly
"""

import os
import sys
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import tempfile
from loguru import logger
import pyarrow as pa
import psycopg2
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

from common.googlesheet_config import get_active_googlesheet_configs

# Environment variables
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE', 'bronze')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
POSTGRES_URI = os.getenv('POSTGRES_URI')


class GoogleSheetServingIngestion:
    """Google Sheet Serving Layer - Simple & Direct"""
    
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
    
    def ensure_target_table_exists(self, target_table_name: str, schema: pa.Schema, 
                                   pk_columns: List[str], drop_table: bool = False, 
                                   dependencies_out: List = None):
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
                
                logger.warning(f"Dropping table {target_table_name} AND DEPENDENCIES (CASCADE)")
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
                pg_type = 'TEXT'  # Default
                
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
            logger.success(f"✅ Ensured table {target_table_name} exists")
            
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Failed to create table {target_table_name}: {e}")
            raise
        finally:
            cursor.close()
    
    def upsert_data(self, target_table_name: str, reader: pa.RecordBatchReader, pk_columns: List[str], truncate: bool = False):
        """
        Upsert data into Postgres using Staging Table + COPY protocol
        This avoids OOM by not creating Python objects for every row.
        """
        if not reader:
            return

        # Generate a unique staging table name
        import uuid
        staging_table = f"{target_table_name}_staging_{uuid.uuid4().hex[:8]}"
        
        cursor = self.pg_conn.cursor()
        
        try:
            # 1. Create Staging Table (Unlogged for speed)
            # clone structure from target_table
            cursor.execute(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table_name} INCLUDING DEFAULTS)")
            
            # 2. Stream Data into Staging using COPY
            # CSV format options for PyArrow
            import pyarrow.csv as pv
            csv_write_options = pv.WriteOptions(include_header=False)
            # Adjust timestamp format if needed, but ISO8601 usually works for Postgres
            
            total_rows = 0
            
            # Iterate through batches
            # Iterate through batches
            CHUNK_SIZE = 10000 # Process max 10k rows at a time to control RAM peak
            
            import gc # Ensure gc is available
            import io

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

            # 2.5. Analyze Staging Table
            # Crucial for Postgres to know the row count/distribution in the temporary staging table
            # so it can choose the optimal join plan for the MERGE step.
            logger.debug("Analyzing staging table...")
            cursor.execute(f"ANALYZE {staging_table}")
            
            # COMMIT here to persist the staging table.
            # If the subsequent MERGE fails and we rollback, we don't want to lose the staging table 
            # because we need it for the Fallback strategy (Delete+Insert).
            self.pg_conn.commit()

            # 3. Perform Merge/Upsert from Staging to Target
            cols_str = ', '.join([f'"{c}"' for c in reader.schema.names])
            
            if truncate:
                logger.warning(f"Truncating table {target_table_name} as requested before loading")
                cursor.execute(f"TRUNCATE TABLE {target_table_name}")
                
                sql_insert = f"""
                INSERT INTO {target_table_name} ({cols_str})
                SELECT {cols_str} FROM {staging_table}
                """
                logger.info("Inserting data from staging to target table (Truncate mode)...")
                cursor.execute(sql_insert)
                self.pg_conn.commit()
                logger.success(f"✅ Successfully loaded {total_rows} rows into {target_table_name} (Truncate & Load)")
                return

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
    
    def process_sheet(self, config: Dict, drop_table: bool = False):
        """Process a single Google Sheet"""
        name = config['name']
        table_name = config['table_data'].lower()  # Bronze table
        target_table = table_name  # Serving table
        
        # Parse primary keys
        pk_columns = []
        if config.get('key'):
            # Split and filter out empty strings
            pk_columns = [k.strip() for k in config['key'].split(',') if k.strip()]
        
        # If no PK configured, log warning but continue (will do simple INSERT)
        if not pk_columns:
            logger.warning(f"No primary key configured for {name}, will use simple INSERT (no upsert)")
        
        truncate = config.get('method_data', False)
        
        logger.info(f"📊 {name} → {target_table} (PK: {pk_columns or 'None'})")
        
        try:
            # 1. Get data stream
            reader = self.get_iceberg_data(table_name)
            if not reader:
                logger.warning(f"No data for {name}")
                return
            
            # 2. Connect DB (lazy)
            if not self.pg_conn:
                self.connect_postgres()
            
            # 3. Ensure table (with dependency tracking)
            dependencies = []
            self.ensure_target_table_exists(target_table, reader.schema, pk_columns, 
                                          drop_table, dependencies_out=dependencies)
            
            # 4. Upsert
            self.upsert_data(target_table, reader, pk_columns, truncate)
            
            # 5. Warn about dependencies (manual restore needed)
            if drop_table and dependencies:
                logger.warning(f"⚠️  Dependencies for {target_table} have been dropped (CASCADE)")
                logger.warning(f"💾 Backup saved to 'public.etl_dependency_backup'")
                logger.warning(f"📝 Please restore manually if needed")
            
            logger.success(f"✅ Completed {name}")
            
        except Exception as e:
            logger.error(f"Failed {name}: {e}")
            raise
    
    def run(self, sheet_id: Optional[int] = None, drop_table: bool = False):
        """Run Google Sheet serving ingestion"""
        logger.info(f"🚀 Starting Google Sheet Serving for {self.snapshot_date}")
        
        # Load configs
        configs = get_active_googlesheet_configs(sheet_id=sheet_id, loai='IN')
        
        if not configs:
            logger.warning("No active Google Sheet configurations")
            return
        
        logger.info(f"📋 Loaded {len(configs)} configurations")
        
        # Process each
        for config in configs:
            try:
                self.process_sheet(config, drop_table)
            except Exception as e:
                logger.error(f"Failed {config['name']}: {e}")
        
        # Cleanup
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("👋 Closed PostgreSQL connection")


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    sheet_id = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else None
    drop_table = sys.argv[3].lower() == 'true' if len(sys.argv) > 3 and sys.argv[3] else False
    
    try:
        job = GoogleSheetServingIngestion(snapshot_date)
        job.run(sheet_id, drop_table)
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
