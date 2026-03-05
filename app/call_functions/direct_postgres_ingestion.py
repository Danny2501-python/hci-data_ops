"""
Direct Ingestion from Source Postgres to Target Postgres (dwh_product)
Architecture: Source PG -> Python (Streaming) -> Target PG (COPY)
Features: 1:1 Schema Parity, Memory Efficient, Bypasses Iceberg/S3.
"""

import os
import sys
import json
import uuid
import gc
import tempfile
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from decimal import Decimal

import pyarrow as pa
import pyarrow.csv as pv
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.etl_table_config import get_active_table_configs
from common.utils import get_current_timestamp_naive

# Source database definitions (from Bronze metadata)
SOURCE_DB_CONNECTIONS = {
    '10.10.10.154': os.getenv('ERP_SCI_CONFIG'),
    '10.10.10.153': os.getenv('GIAOVIEC_CONFIG'),
    '10.10.10.234': os.getenv('SEEDING_CONFIG'),
    '10.10.10.214': os.getenv('CUSTOM_DB_CONFIG'),
    '118.70.128.33': os.getenv('HONGHA_CONFIG'),
    '10.10.10.226': os.getenv('DU_PHONG_CONFIG'),
    '10.10.10.230': os.getenv('PORTAL_GUIDE_CONFIG'),
}

# ERP Sources mapping
ERP_SOURCES = {
    'sci': {'env': 'ERP_SCI_URI', 'brand_etl': 7},
    'kn':  {'env': 'ERP_KN_URI',  'brand_etl': 1},
    'pr':  {'env': 'ERP_PR_URI',  'brand_etl': 3},
    'hh':  {'env': 'ERP_HH_URI',  'brand_etl': 4},
}

# Target Postgres URI
POSTGRES_URI = os.getenv('POSTGRES_URI')

class DirectPostgresToPostgres:
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        self.target_conn = None

    def connect_target(self):
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI (Target) not set")
        self.target_conn = psycopg2.connect(POSTGRES_URI)
        self.target_conn.autocommit = False
        logger.info("Connected to Target PostgreSQL")

    def create_source_engine(self, server: str):
        config_json = SOURCE_DB_CONNECTIONS.get(server)
        if not config_json: return None
        config = json.loads(config_json)
        url = URL.create("postgresql", username=config['user'], password=config['password'],
                         host=config['host'], port=int(config.get('port', 5432)), database=config['dbname'])
        return create_engine(url)

    def check_source_table_exists(self, engine, source_table: str) -> bool:
        """Helper to verify if the source table exists in the given engine"""
        st_parts = source_table.split('.')
        table_name = st_parts[-1]
        schema_name = st_parts[0] if len(st_parts) > 1 else 'public'
        
        query = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = '{schema_name}'
                AND    table_name   = '{table_name}'
            );
        """)
        try:
            with engine.connect() as conn:
                return conn.execute(query).scalar()
        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            return False

    def ensure_target_table_exists_from_source(self, source_engine, source_table: str, target_table: str, rename_map: Dict[str, str] = {}):
        """Fetch schema from source and create exactly the same table in target if not exists"""
        logger.info(f"Ensuring target table {target_table} exists based on source {source_table}")
        
        # 1. Get Column Definitions from Source
        query = text(f"""
            SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns 
            WHERE table_name = '{source_table.split('.')[-1]}' 
            AND table_schema = '{source_table.split('.')[0] if '.' in source_table else 'public'}'
            ORDER BY ordinal_position
        """)
        
        with source_engine.connect() as conn:
            columns = conn.execute(query).fetchall()
        
        if not columns:
            raise ValueError(f"Could not find schema for source table {source_table}")

        source_col_names = [c[0].lower() for c in columns]
        has_brand_etl_in_source = 'brand_etl' in source_col_names

        # 2. Build CREATE TABLE SQL
        col_defs = []
        for col in columns:
            name, dtype, char_len, num_prec, num_scale, is_null = col
            sql_type = dtype.upper()
            
            # Apply rename
            target_name = rename_map.get(name, name)
            
            # Refine types for 1:1 parity
            if dtype == 'character varying' and char_len: sql_type = f"VARCHAR({char_len})"
            elif dtype == 'numeric' and num_prec: sql_type = f"NUMERIC({num_prec}, {num_scale or 0})"
            
            col_def = f'"{target_name}" {sql_type}'
            col_defs.append(col_def)
            
        # Add metadata columns (consistent with our ETL standards)
        metadata_cols = [
            '"snapshot_date" DATE',
            '"ingestion_timestamp" TIMESTAMP',
            '"source_server" VARCHAR(255)'
        ]
        
        # Requirement: brand_etl if not in source, brand_etl_new if in source
        if has_brand_etl_in_source:
            metadata_cols.append('"brand_etl_new" INTEGER')
        else:
            metadata_cols.append('"brand_etl" INTEGER')
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ({', '.join(col_defs + metadata_cols)})"
        
        with self.target_conn.cursor() as cur:
            cur.execute(create_sql)
            self.target_conn.commit()
        logger.success(f"Target table {target_table} is ready.")

    def stream_and_upsert(self, source_engine, config: Dict, target_table: str, brand_etl: int = None, source_name: str = None, load_type: str = 'incremental'):
        # 1. Config & Mapping
        source_table = config['table_name_source']
        pk_columns = [k.strip() for k in config.get('primary_keyword', '').split(',')] if config.get('primary_keyword') else []
        rename_map = json.loads(config.get('rename_column') or '{}')
        keyword, consider = config.get('keyword'), (config.get('consider') or '1=1')
        cutoff_str = (self.snapshot_date - timedelta(days=10)).strftime('%Y-%m-%d')
        
        where = f"({keyword} + interval '7 hour')::date >= '{cutoff_str}' AND {consider}" if load_type == 'incremental' and keyword and keyword.lower() != 'none' else consider
        query = text(f"SELECT * FROM {source_table} WHERE {where}")
        target_schema = target_table.split('.')[0] if '.' in target_table else 'public'
        target_table_name = target_table.split('.')[-1]
        staging_table = f"{target_table_name}_staging_{uuid.uuid4().hex[:8]}"

        with self.target_conn.cursor() as cur:
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table_name}' AND table_schema='{target_schema}' ORDER BY ordinal_position")
            target_cols = [r[0] for r in cur.fetchall()]
            cols_str = ", ".join([f'"{c}"' for c in target_cols])

            cur.execute(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table} INCLUDING DEFAULTS)")
            self.target_conn.commit()

        # 2. Streaming Loop
        BATCH_SIZE, total_rows = 20000, 0
        temp_dir = os.getenv('TEMP_DIR', tempfile.gettempdir())
        
        try:
            with source_engine.connect().execution_options(stream_results=True) as conn:
                try:
                    result = conn.execute(query)
                except Exception as e:
                    raise RuntimeError(f"SOURCE_QUERY_ERROR: {e}")

                source_cols = list(result.keys())
                
                # Pre-calculate constant metadata and schema
                mapping_indices = []
                for col in target_cols:
                    if col in source_cols:
                        mapping_indices.append((col, source_cols.index(col)))
                    else:
                        found_in_rename = False
                        for src, tgt in rename_map.items():
                            if tgt == col and src in source_cols:
                                mapping_indices.append((col, source_cols.index(src)))
                                found_in_rename = True
                                break
                        if not found_in_rename:
                            mapping_indices.append((col, None))
                
                # Pre-calculate labels and Arrow Schema
                snap_date_str = str(self.snapshot_date)
                source_srv_str = str(source_name or config.get('server'))
                b_etl_val = str(brand_etl) if brand_etl is not None else None
                pa_schema = pa.schema([(c, pa.string()) for c in target_cols])

                while True:
                    try:
                        rows = result.fetchmany(BATCH_SIZE)
                    except Exception as e:
                        raise RuntimeError(f"SOURCE_FETCH_ERROR: {e}")

                    if not rows: break
                    
                    row_count = len(rows)
                    batch_data = {col: [] for col in target_cols}
                    
                    # 2.1 Transformation Loop
                    for r in rows:
                        for col, idx in mapping_indices:
                            if idx is not None:
                                val = r[idx]
                                if val is None:
                                    batch_data[col].append(None)
                                elif isinstance(val, (datetime, date)):
                                    if isinstance(val, datetime):
                                        val = val.replace(tzinfo=None)
                                    batch_data[col].append(val.isoformat())
                                elif isinstance(val, Decimal):
                                    batch_data[col].append(str(val))
                                elif isinstance(val, str):
                                    batch_data[col].append(val.replace('\x00', ''))
                                else:
                                    batch_data[col].append(str(val))
                    
                    # Clean up rows immediately to free RAM before Arrow conversion
                    del rows
                    
                    # 2.2 Fill constant metadata columns (Pre-calculated for speed)
                    if 'snapshot_date' in batch_data: batch_data['snapshot_date'] = [snap_date_str] * row_count
                    if 'ingestion_timestamp' in batch_data: batch_data['ingestion_timestamp'] = [str(get_current_timestamp_naive())] * row_count
                    if 'source_server' in batch_data: batch_data['source_server'] = [source_srv_str] * row_count
                    
                    if 'brand_etl' in batch_data and batch_data['brand_etl'] == []:
                        batch_data['brand_etl'] = [b_etl_val] * row_count
                    if 'brand_etl_new' in batch_data and batch_data['brand_etl_new'] == []:
                        batch_data['brand_etl_new'] = [b_etl_val] * row_count

                    # Ensure all columns have data (even for unexpected gaps)
                    for col in target_cols:
                        if not batch_data[col]: batch_data[col] = [None] * row_count
                    
                    pa_table = pa.Table.from_pydict(batch_data, schema=pa_schema)
                    del batch_data
                    with tempfile.TemporaryFile(mode='wb+', dir=temp_dir) as tmp:
                        pv.write_csv(pa_table, tmp, write_options=pv.WriteOptions(include_header=False))
                        tmp.seek(0)
                        try:
                            with self.target_conn.cursor() as cur:
                                cur.copy_expert(f"COPY {staging_table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)", tmp)
                                if pk_columns:
                                    pk_match = " AND ".join([f'dest."{c}" = src."{c}"' for c in pk_columns])
                                    cur.execute(f'DELETE FROM {target_table} AS dest USING {staging_table} AS src WHERE {pk_match}')
                                cur.execute(f"INSERT INTO {target_table} ({cols_str}) SELECT {cols_str} FROM {staging_table}")
                                cur.execute(f"TRUNCATE TABLE {staging_table}")
                                self.target_conn.commit()
                        except Exception as e:
                            raise RuntimeError(f"TARGET_DB_ERROR: {e}")
                    
                    total_rows += row_count
                    logger.info(f"Ingested {row_count} into {target_table} from {source_name or config.get('server')}. Total: {total_rows}")
                    del pa_table
                    gc.collect()
                    pa.default_memory_pool().release_unused()

                    if row_count < BATCH_SIZE:
                        break
            return total_rows
        finally:
            with self.target_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                self.target_conn.commit()
            
    def prepare_target(self, engine, config: Dict, drop_table: bool):
        target_table, source_table = config['table_name'], config['table_name_source']
        rename_map = json.loads(config.get('rename_column') or '{}')
        
        with self.target_conn.cursor() as cur:
            if drop_table:
                logger.warning(f"Dropping target table {target_table} (CASCADE) as requested.")
                cur.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE")
                self.target_conn.commit()
            
            # 1. Ensure table exists with correct schema
            self.ensure_target_table_exists_from_source(engine, source_table, target_table, rename_map=rename_map)

            # 2. Handle Truncate (Wipe existing data for Full Reload)
            if config.get('truncate'):
                logger.info(f"Truncating target table {target_table} as requested.")
                cur.execute(f"TRUNCATE TABLE {target_table} CASCADE")
                self.target_conn.commit()

            # 3. Create Index on PK Columns for performance if not exists
            pk_columns = [k.strip() for k in config.get('primary_keyword', '').split(',')] if config.get('primary_keyword') else []
            if pk_columns:
                target_schema = target_table.split('.')[0] if '.' in target_table else 'public'
                target_table_name = target_table.split('.')[-1]
                
                # Ensure all PK columns actually exist in target (case insensitive check)
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table_name}' AND table_schema='{target_schema}'")
                existing_cols = [r[0].lower() for r in cur.fetchall()]
                valid_pk = [c for c in pk_columns if c.lower() in existing_cols]
                
                if valid_pk:
                    idx_name = f"idx_{target_table.replace('.', '_')}_pk"[:63]
                    pk_cols_str = ", ".join(['"' + c + '"' for c in valid_pk])
                    cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON {target_table} ({pk_cols_str})')
                    self.target_conn.commit()

    def process_erp(self, config: Dict, drop_table: bool, load_type: str):
        target_table = config['table_name']
        source_table = config['table_name_source']
        
        total = 0
        initialized = False
        
        for name, info in ERP_SOURCES.items():
            uri = os.getenv(info['env'])
            if not uri: continue
            if uri.startswith("postgres://"): uri = uri.replace("postgres://", "postgresql://", 1)
            engine = create_engine(uri)
            try:
                # Check if table exists in this source before trying to define schema or stream data
                if not self.check_source_table_exists(engine, source_table):
                    logger.warning(f"Source table {source_table} not found in {name}. Skipping.")
                    continue

                if not initialized:
                    self.prepare_target(engine, config, drop_table)
                    initialized = True
                
                try:
                    total += self.stream_and_upsert(engine, config, target_table, brand_etl=info['brand_etl'], source_name=name, load_type=load_type)
                except Exception as e:
                    err_msg = str(e)
                    db_side = "Source" if "SOURCE_" in err_msg else ("Target" if "TARGET_" in err_msg else "Logic/Unknown")
                    logger.error(f"Failed to ingest {target_table} from {name} ({db_side} side): {e}")
            finally:
                engine.dispose()
        
        if not initialized:
            logger.error(f"Table {source_table} was not found in ANY ERP source database.")
            
        return total

    def run(self, table_id: Optional[int] = None, source_type: Optional[str] = None, drop_table: bool = False, load_type: str = 'incremental'):
        
        # Flexibility: Normalize 'standard' or empty string to None for the config loader
        # This allows 'Standard' ingestion to work by filtering is_transfrom=True
        # If table_id is provided, we ignore the source filter to avoid AND mismatches
        actual_source = None if not source_type or str(source_type).lower() in ('standard', 'none', '') else source_type.strip()
        configs = get_active_table_configs(table_id=table_id, source=None if table_id else actual_source)
        for config in configs:
            try:
                self.connect_target() # Fresh connection for each table
                target_table = config['table_name']
                source_table = config['table_name_source']

                # Use ERP logic if the METADATA says it's an ERP table
                if str(config.get('source')).upper() == 'ERP':
                    self.process_erp(config, drop_table=drop_table, load_type=load_type)
                else:
                    # Single source logic
                    engine = self.create_source_engine(config['server'])
                    if not engine:
                        logger.warning(f"No connection found for server {config['server']} (Table: {target_table})")
                        continue
                    try:
                        self.prepare_target(engine, config, drop_table)
                        self.stream_and_upsert(engine, config, config['table_name'], load_type=load_type)
                    finally:
                        engine.dispose()
            except Exception as e:
                err_msg = str(e)
                db_side = "Source" if "SOURCE_" in err_msg else ("Target" if "TARGET_" in err_msg else "Logic/Unknown")
                logger.error(f"Failed {config['table_name']} ({db_side} side): {e}")
            finally:
                if self.target_conn:
                    self.target_conn.close()
                    self.target_conn = None

if __name__ == "__main__":
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    s_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else None
    t_id_arg = sys.argv[2] if len(sys.argv) > 2 else None
    t_id = None
    if t_id_arg and t_id_arg.strip() and t_id_arg.lower() != 'none':
        # 1. Clean brackets and spaces
        raw_val = str(t_id_arg).strip('[] ')
        
        # 2. Handle comma-separated list
        if ',' in raw_val:
            parts = raw_val.split(',')
            t_id = []
            for p in parts:
                clean_p = p.strip(' "\'') # Remove spaces, double quotes, and single quotes
                if clean_p.isdigit():
                    t_id.append(int(clean_p))
        # 3. Handle single ID
        else:
            clean_p = raw_val.strip(' "\'')
            if clean_p.isdigit():
                t_id = int(clean_p)
        
        # Final safety check: if list is empty, set to None
        if isinstance(t_id, list) and not t_id:
            t_id = None

    s_type = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Arg 4: load_type (incremental/full)
    l_type = sys.argv[4] if len(sys.argv) > 4 else 'incremental'
    
    # Arg 5: drop_table flag
    drop_table_arg = sys.argv[5] if len(sys.argv) > 5 else 'false'
    drop_table = drop_table_arg.lower() in ('true', '1', 'yes')
    
    DirectPostgresToPostgres(s_date).run(table_id=t_id, source_type=s_type, drop_table=drop_table, load_type=l_type)
