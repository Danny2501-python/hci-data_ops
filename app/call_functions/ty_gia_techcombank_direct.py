"""
Direct Ingestion from Techcombank API to Target Postgres
Architecture: Techcombank API -> Python -> Target PG (COPY)
Features: Memory Efficient, Bypasses Iceberg/S3.
"""

import os
import sys
import time
import json
import uuid
import gc
import tempfile
import requests
import traceback
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
import psycopg2
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.utils import get_current_timestamp_naive

POSTGRES_URI = os.getenv('POSTGRES_URI')


class DirectTechcombankToPostgres:
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        self.target_conn = None

    def connect_target(self):
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI (Target) not set")
        self.target_conn = psycopg2.connect(POSTGRES_URI)
        self.target_conn.autocommit = False
        logger.info("Connected to Target PostgreSQL")

    def get_api_url(self, ngay: str = '') -> str:
        url = f'https://techcombank.com/content/techcombank/web/vn/vi/cong-cu-tien-ich/ty-gia/_jcr_content.exchange-rates.{ngay}.integration.json'
        return url

    def fetch_exchange_rates(self, start_date: date, end_date: date) -> pd.DataFrame:
        calls_data = []
        current_date = start_date
        
        logger.info(f"Fetching exchange rates from {start_date} to {end_date}")
        
        while current_date <= end_date:
            formatted_date = current_date.strftime('%Y-%m-%d')
            url = self.get_api_url(formatted_date)
            
            try:
                response = requests.get(url, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_calls_data = data.get('exchangeRate', {}).get('data', [])
                    
                    for item in new_calls_data:
                        item['date'] = datetime.strptime(formatted_date, '%Y-%m-%d')
                    
                    calls_data.extend(new_calls_data)
                    logger.info(f"✓ Fetched {len(new_calls_data)} records for {formatted_date}")
                else:
                    logger.warning(f"API request failed for {formatted_date}. Status code: {response.status_code}")
            
            except Exception as e:
                logger.error(f"Error fetching data for {formatted_date}: {e}")
            
            current_date += timedelta(days=1)
            time.sleep(0.5) 
        
        if not calls_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(calls_data)
        
        # Filter for USD only (matching old logic)
        df = df[df['label'].str.contains('USD', case=False, na=False)]
        
        # Normalize column names to lowercase and strip
        df.columns = df.columns.astype(str).str.lower().str.strip()
        
        # Drop columns with empty headers
        df = df.loc[:, df.columns != '']
        df = df.loc[:, ~df.columns.str.startswith('unnamed:')]
        
        # Replace empty strings with None
        df = df.replace({"": None})
        
        return df

    def ensure_target_table_exists_from_df(self, df: pd.DataFrame, target_table: str):
        """Fetch schema from source data and create table in target if not exists"""
        logger.info(f"Ensuring target table {target_table} exists based on Techcombank schema")
        
        col_defs = []
        for col_name, dtype in df.dtypes.items():
            sql_type = 'TEXT' # Default
            if pd.api.types.is_numeric_dtype(dtype):
                sql_type = 'DOUBLE PRECISION' if pd.api.types.is_float_dtype(dtype) else 'BIGINT'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = 'TIMESTAMP'
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = 'BOOLEAN'
                
            col_def = f'"{col_name}" {sql_type}'
            col_defs.append(col_def)
            
        metadata_cols = [
            '"snapshot_date" DATE',
            '"ingestion_timestamp" TIMESTAMP',
            '"source_server" VARCHAR(255)'
        ]
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ({', '.join(col_defs + metadata_cols)})"
        
        with self.target_conn.cursor() as cur:
            cur.execute(create_sql)
            self.target_conn.commit()
        logger.success(f"Target table {target_table} is ready.")

    def stream_and_upsert(self, df: pd.DataFrame, target_table: str, pk_columns: List[str] = ['itemid', 'date']):
        if df.empty:
            logger.warning(f"No data for {target_table}")
            return 0
            
        target_schema = target_table.split('.')[0] if '.' in target_table else 'public'
        target_table_name = target_table.split('.')[-1]
        staging_table = f"{target_table_name}_staging_{uuid.uuid4().hex[:8]}"

        with self.target_conn.cursor() as cur:
            # Get target columns
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table_name}' AND table_schema='{target_schema}' ORDER BY ordinal_position")
            target_cols = [r[0] for r in cur.fetchall()]
            
            if not target_cols:
                self.ensure_target_table_exists_from_df(df, target_table)
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table_name}' AND table_schema='{target_schema}' ORDER BY ordinal_position")
                target_cols = [r[0] for r in cur.fetchall()]
                
            cols_str = ", ".join([f'"{c}"' for c in target_cols])

            cur.execute(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table} INCLUDING DEFAULTS)")
            self.target_conn.commit()

        # 2. Add metadata
        source_cols = df.columns.tolist()
        for col in target_cols:
            if col not in source_cols:
                if col == 'snapshot_date':
                    df['snapshot_date'] = str(self.snapshot_date)
                elif col == 'ingestion_timestamp':
                    df['ingestion_timestamp'] = str(get_current_timestamp_naive())
                elif col == 'source_server':
                    df['source_server'] = 'Techcombank API'
                else:
                    df[col] = None
        
        # Reorder to match target
        df = df[target_cols]
        
        # 3. Stream to DB
        total_rows = 0
        BATCH_SIZE = 20000
        temp_dir = os.getenv('TEMP_DIR', tempfile.gettempdir())
        
        try:
            # Convert to PyArrow chunked table logic
            pa_schema = pa.schema([(c, pa.string()) for c in target_cols])
            
            num_rows = len(df)
            for i in range(0, num_rows, BATCH_SIZE):
                chunk_df = df.iloc[i:i+BATCH_SIZE].copy()
                chunk_df = chunk_df.astype(str).replace({'None': None, 'nan': None})
                
                pa_table = pa.Table.from_pandas(chunk_df, schema=pa_schema, preserve_index=False)
                
                with tempfile.TemporaryFile(mode='wb+', dir=temp_dir) as tmp:
                    pv.write_csv(pa_table, tmp, write_options=pv.WriteOptions(include_header=False))
                    tmp.seek(0)
                    
                    with self.target_conn.cursor() as cur:
                        cur.copy_expert(f"COPY {staging_table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)", tmp)
                        
                        # Direct Upsert Logic (Delete + Insert)
                        if pk_columns:
                            pk_match = " AND ".join([f'dest."{c}" = src."{c}"' for c in pk_columns])
                            cur.execute(f'DELETE FROM {target_table} AS dest USING {staging_table} AS src WHERE {pk_match}')
                        
                        cur.execute(f"INSERT INTO {target_table} ({cols_str}) SELECT {cols_str} FROM {staging_table}")
                        cur.execute(f"TRUNCATE TABLE {staging_table}")
                        self.target_conn.commit()
                
                total_rows += len(chunk_df)
                logger.info(f"Ingested {len(chunk_df)} rows. Total: {total_rows}")
                del pa_table
                del chunk_df
                gc.collect()
                
            return total_rows
        finally:
            with self.target_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                self.target_conn.commit()

    def run(self, start_date: date, end_date: date, target_table: str = 'ty_gia_techcombank'):
        logger.info(f"🚀 Starting Techcombank Direct Ingestion: {start_date} to {end_date}")
        
        try:
            self.connect_target()
            
            # 1. Fetch
            df = self.fetch_exchange_rates(start_date, end_date)
            
            if df.empty:
                logger.warning("No data found to ingest.")
                return
                
            # 2. Upsert
            self.stream_and_upsert(df, target_table)
            
            logger.success(f"✅ Completed Ingestion for {target_table}")
            
        except Exception as e:
            logger.exception(f"Failed Ingestion: {e}")
        finally:
            if self.target_conn:
                self.target_conn.close()

if __name__ == "__main__":
    # snapshot_date start_date end_date target_table
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    s_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    start_date_str = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date() if start_date_str else (date.today() - timedelta(days=30))
    
    end_date_str = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else None
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date() if end_date_str else (date.today() - timedelta(days=1))
    
    target_table = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else 'ty_gia_techcombank'
    
    try:
        DirectTechcombankToPostgres(s_date).run(start_date, end_date, target_table)
    except Exception as e:
        logger.error(str(e))
        sys.exit(1)
