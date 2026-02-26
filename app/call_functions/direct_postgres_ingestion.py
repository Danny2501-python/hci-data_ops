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

    def ensure_target_table_exists_from_source(self, source_engine, source_table: str, target_table: str, pk_columns: List[str], rename_map: Dict[str, str] = {}):
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
            if is_null == 'NO': col_def += " NOT NULL"
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
        
        if pk_columns:
            pk_list = [f'"{c}"' for c in pk_columns]
            pk_clause = f", PRIMARY KEY ({', '.join(pk_list)})"
        else:
            pk_clause = ""
            
        create_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ({', '.join(col_defs + metadata_cols)}{pk_clause})"
        
        with self.target_conn.cursor() as cur:
            cur.execute(create_sql)
            self.target_conn.commit()
        logger.success(f"Target table {target_table} is ready.")

    def stream_and_upsert(self, source_engine, config: Dict, target_table: str, brand_etl: int = None, source_name: str = None):
        source_table = config['table_name_source']
        pk_columns = [k.strip() for k in config.get('primary_keyword', '').split(',')] if config.get('primary_keyword') else []
        
        # Build query
        cutoff_date = (self.snapshot_date - timedelta(days=10)).strftime('%Y-%m-%d')
        keyword = config.get('keyword')
        consider = config.get('consider') or '1=1'
        
        if keyword and keyword.lower() != 'none':
            where = f"({keyword} + interval '7 hour')::date >= '{cutoff_date}' AND {consider}"
        else:
            where = consider
        
        query = text(f"SELECT * FROM {source_table} WHERE {where}")
        staging_table = f"{target_table}_staging_{uuid.uuid4().hex[:8]}"
        
        BATCH_SIZE = 5000
        total_rows = 0
        temp_dir = os.getenv('TEMP_DIR', '/tmp')
        
        with self.target_conn.cursor() as cur:
            # 1. Get columns from Target for mapping and cleanup check
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table}' ORDER BY ordinal_position")
            target_cols = [r[0] for r in cur.fetchall()]
            cols_str = ", ".join([f'"{c}"' for c in target_cols])

            # 2. Cleanup target based on config
            if config.get('truncate'):
                logger.info(f"Truncating target table {target_table} (Full Overwrite)")
                cur.execute(f"TRUNCATE TABLE {target_table} CASCADE")
            elif not pk_columns and keyword and keyword.lower() != 'none' and keyword in target_cols:
                # Idempotent incremental load for non-PK tables: Delete matching range first
                logger.info(f"Cleanup {target_table} based on {keyword} >= {cutoff_date} (Delete + Insert mode)")
                # Use the same logic as the source query condition
                delete_where = f"({keyword} + interval '7 hour')::date >= '{cutoff_date}'"
                cur.execute(f"DELETE FROM {target_table} WHERE {delete_where}")
            
            # 3. Create staging table
            cur.execute(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table} INCLUDING DEFAULTS)")
            self.target_conn.commit()

        try:
            with source_engine.connect().execution_options(stream_results=True) as conn:
                result = conn.execute(query)
                source_cols = list(result.keys())
                
                rename_map = json.loads(config.get('rename_column') or '{}')
                mapping = {}
                for col in target_cols:
                    if col in source_cols:
                        mapping[col] = source_cols.index(col)
                    else:
                        # Check if a source column was renamed to this target column
                        for src, tgt in rename_map.items():
                            if tgt == col and src in source_cols:
                                mapping[col] = source_cols.index(src)
                                break
                
                while True:
                    rows = result.fetchmany(BATCH_SIZE)
                    if not rows: break
                    
                    # Convert to processed list for CSV
                    batch_data = {col: [] for col in target_cols}
                    for r in rows:
                        for col in target_cols:
                            if col in mapping:
                                val = r[mapping[col]]
                                if isinstance(val, (datetime, date)): 
                                    if isinstance(val, datetime): val = val.replace(tzinfo=None)
                                    val = val.isoformat()
                                elif isinstance(val, Decimal): val = str(val)
                                elif isinstance(val, str): val = val.replace('\x00', '')
                                else: val = str(val) if val is not None else None
                                batch_data[col].append(val)
                            elif col == 'snapshot_date': batch_data[col].append(str(self.snapshot_date))
                            elif col == 'ingestion_timestamp': batch_data[col].append(str(get_current_timestamp_naive()))
                            elif col == 'source_server': batch_data[col].append(str(source_name or config['server']))
                            elif col in ('brand_etl', 'brand_etl_new'):
                                # Only fill if col is NOT mapping (meaning it was added by us as metadata)
                                if col not in mapping:
                                    batch_data[col].append(str(brand_etl) if brand_etl is not None else None)
                                else:
                                    # This should not happen due to mapping logic, but just in case
                                    val = r[mapping[col]]
                                    batch_data[col].append(str(val) if val is not None else None)
                            else: batch_data[col].append(None)
                    
                    # Convert to Arrow -> CSV -> COPY
                    pa_table = pa.Table.from_pydict(batch_data, schema=pa.schema([(c, pa.string()) for c in target_cols]))
                    
                    with tempfile.TemporaryFile(mode='wb+', dir=temp_dir) as tmp:
                        pv.write_csv(pa_table, tmp, write_options=pv.WriteOptions(include_header=False))
                        tmp.seek(0)
                        with self.target_conn.cursor() as cur:
                            cur.copy_expert(f"COPY {staging_table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)", tmp)
                    
                    # Merge from staging to target
                    with self.target_conn.cursor() as cur:
                        if pk_columns:
                            pk_str = ", ".join([f'"{c}"' for c in pk_columns])
                            update_cols = [c for c in target_cols if c not in pk_columns]
                            if update_cols:
                                update_set = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
                                cur.execute(f"INSERT INTO {target_table} ({cols_str}) SELECT {cols_str} FROM {staging_table} ON CONFLICT ({pk_str}) DO UPDATE SET {update_set}")
                            else:
                                cur.execute(f"INSERT INTO {target_table} ({cols_str}) SELECT {cols_str} FROM {staging_table} ON CONFLICT ({pk_str}) DO NOTHING")
                        else:
                            cur.execute(f"INSERT INTO {target_table} ({cols_str}) SELECT {cols_str} FROM {staging_table}")
                        
                        cur.execute(f"TRUNCATE TABLE {staging_table}")
                        self.target_conn.commit()
                    
                    total_rows += len(rows)
                    logger.info(f"Ingested {len(rows)} rows into {target_table}. Total: {total_rows}")
                    del pa_table, batch_data
                    gc.collect()
                    pa.default_memory_pool().release_unused()

            return total_rows
        finally:
            with self.target_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                self.target_conn.commit()

    def process_erp(self, config: Dict, drop_table: bool = False):
        target_table = config['table_name']
        source_table = config['table_name_source']
        pk_columns = [k.strip() for k in config.get('primary_keyword', '').split(',')] if config.get('primary_keyword') else []
        
        total = 0
        initialized = False
        
        for name, info in ERP_SOURCES.items():
            uri = os.getenv(info['env'])
            if not uri: continue
            if uri.startswith("postgres://"): uri = uri.replace("postgres://", "postgresql://", 1)
            engine = create_engine(uri)
            try:
                # Mirror schema from the FIRST available ERP source
                if not initialized:
                    if drop_table:
                        with self.target_conn.cursor() as cur:
                            cur.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE")
                            self.target_conn.commit()
                    
                    rename_map = json.loads(config.get('rename_column') or '{}')
                    self.ensure_target_table_exists_from_source(engine, source_table, target_table, pk_columns, rename_map=rename_map)
                    initialized = True
                
                total += self.stream_and_upsert(engine, config, target_table, brand_etl=info['brand_etl'], source_name=name)
            finally:
                engine.dispose()
        return total

    def run(self, table_id: Optional[int] = None, source_type: Optional[str] = None, drop_table: bool = False):
        self.connect_target()
        
        # Flexibility: Normalize 'standard' or empty string to None for the config loader
        # This allows 'Standard' ingestion to work by filtering is_transfrom=True
        actual_source = None if not source_type or str(source_type).lower() in ('standard', 'none', '') else source_type
        
        configs = get_active_table_configs(table_id=table_id, source=actual_source)
        for config in configs:
            try:
                target_table = config['table_name']
                source_table = config['table_name_source']
                pk_columns = [k.strip() for k in config.get('primary_keyword', '').split(',')] if config.get('primary_keyword') else []

                # Use ERP logic if the METADATA says it's an ERP table
                if str(config.get('source')).upper() == 'ERP':
                    self.process_erp(config, drop_table=drop_table)
                else:
                    # Single source logic
                    engine = self.create_source_engine(config['server'])
                    if not engine:
                        logger.warning(f"No connection found for server {config['server']} (Table: {target_table})")
                        continue
                    try:
                        if drop_table:
                            with self.target_conn.cursor() as cur:
                                cur.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE")
                                self.target_conn.commit()
                        
                        # Ensure Target mirrors Source (including renames)
                        rename_map = json.loads(config.get('rename_column') or '{}')
                        self.ensure_target_table_exists_from_source(engine, source_table, target_table, pk_columns, rename_map=rename_map)
                        
                        self.stream_and_upsert(engine, config, target_table)
                    finally:
                        engine.dispose()
            except Exception as e:
                logger.error(f"Failed {config['table_name']}: {e}")
        self.target_conn.close()

if __name__ == "__main__":
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    s_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else None
    t_id = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else None
    s_type = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Arg 4: drop_table flag
    drop_table_arg = sys.argv[4] if len(sys.argv) > 4 else 'false'
    drop_table = drop_table_arg.lower() in ('true', '1', 'yes')
    
    DirectPostgresToPostgres(s_date).run(table_id=t_id, source_type=s_type, drop_table=drop_table)
