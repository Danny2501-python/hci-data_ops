"""
Direct Ingestion from Google Sheets to Target Postgres
Architecture: Google Sheets -> Python (Streaming) -> Target PG (COPY)
Features: Memory Efficient, Bypasses Iceberg/S3.
"""

import os
import sys
import time
import json
import uuid
import gc
import tempfile
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from decimal import Decimal

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pyarrow as pa
import pyarrow.csv as pv
import psycopg2
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.googlesheet_config import get_active_googlesheet_configs
from common.utils import get_current_timestamp_naive

POSTGRES_URI = os.getenv('POSTGRES_URI')
GOOGLE_CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH')


class DirectGoogleSheetToPostgres:
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        self.target_conn = None
        self.gsheet_client = None

    def connect_target(self):
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI (Target) not set")
        self.target_conn = psycopg2.connect(POSTGRES_URI)
        self.target_conn.autocommit = False
        logger.info("Connected to Target PostgreSQL")

    def connect_google_sheets(self):
        """Connect to Google Sheets API"""
        try:
            scope = ['https://www.googleapis.com/auth/spreadsheets']
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                GOOGLE_CREDENTIALS_PATH, 
                scope
            )
            self.gsheet_client = gspread.authorize(credentials)
            logger.info("Connected to Google Sheets API")
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {e}")
            raise

    def get_google_sheet_data(self, spreadsheet_id: str, sheet_name: str, remove_column: Optional[str] = None, rename_column: Optional[str] = None) -> pd.DataFrame:
        if not self.gsheet_client:
            self.connect_google_sheets()
            
        max_retries = 3
        retry_delay = 30
        
        for attempt in range(max_retries):
            try:
                spreadsheet = self.gsheet_client.open_by_key(spreadsheet_id)
                worksheet = spreadsheet.worksheet(sheet_name)
                data_matrix = worksheet.get_all_values()
                
                if not data_matrix or len(data_matrix) < 2:
                    return pd.DataFrame()
                    
                headers = data_matrix[0]
                rows = data_matrix[1:]
                
                df = pd.DataFrame(rows, columns=headers)
                
                for col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except Exception:
                        pass
                        
                if remove_column and remove_column.strip():
                    cols_to_remove = remove_column.split(',')
                    df.drop(columns=[c for c in cols_to_remove if c in df.columns], inplace=True)
                    
                if rename_column and rename_column.strip():
                    try:
                        rename_map = json.loads(rename_column)
                        df.rename(columns=rename_map, inplace=True)
                    except Exception as e:
                        logger.warning(f"Failed to parse rename column: {e}")
                        
                df.columns = df.columns.astype(str).str.lower().str.strip()
                # Drop columns with empty headers
                df = df.loc[:, df.columns != '']
                df = df.loc[:, ~df.columns.str.startswith('unnamed:')]
                
                df = df.replace({"": None})
                
                return df
            
            except gspread.exceptions.SpreadsheetNotFound as e:
                logger.warning(f"Spreadsheet not found for {spreadsheet_id}: {e}")
                return pd.DataFrame()
            except gspread.exceptions.WorksheetNotFound as e:
                logger.warning(f"Worksheet not found '{sheet_name}': {e}")
                return pd.DataFrame()
            except gspread.exceptions.APIError as e:
                error_msg = str(e)
                if "404" in error_msg:
                    logger.warning(f"Spreadsheet/API not found (404) for {spreadsheet_id}: {e}")
                    return pd.DataFrame()
                if "429" in error_msg or "Quota" in error_msg:
                    if attempt < max_retries - 1:
                        logger.warning(f"API Rate Limit Exceeded (429). Retrying in {retry_delay} seconds... (Attempt {attempt+1}/{max_retries})")
                        time.sleep(retry_delay)
                        continue
                raise
            except Exception as e:
                error_msg = str(e)
                if ("429" in error_msg or "Quota" in error_msg) and attempt < max_retries - 1:
                    logger.warning(f"API Rate Limit Exceeded (429). Retrying in {retry_delay} seconds... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                # Also handle <Response [404]> if it comes as generic Exception
                if "404" in error_msg:
                    logger.warning(f"Not found (404) for {spreadsheet_id}: {e}")
                    return pd.DataFrame()
                # Handle generic WorksheetNotFound exceptions (e.g. error message IS exactly the sheet name)
                if error_msg.strip() == sheet_name.strip() or "WorksheetNotFound" in error_msg:
                    logger.warning(f"Worksheet not found/matching exact exception name: '{sheet_name}'. Source may be deleted.")
                    return pd.DataFrame()
                
                raise

    def ensure_target_table_exists_from_df(self, df: pd.DataFrame, target_table: str):
        """Fetch schema from source data and create table in target if not exists"""
        logger.info(f"Ensuring target table {target_table} exists based on Google Sheet schema")
        
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

    def stream_and_upsert(self, config: Dict, target_table: str, df: pd.DataFrame):
        # 1. Config & Mapping
        pk_columns = [k.strip() for k in config.get('key', '').split(',')] if config.get('key') else []
        
        target_schema = target_table.split('.')[0] if '.' in target_table else 'public'
        target_table_name = target_table.split('.')[-1]
        staging_table = f"{target_table_name}_staging_{uuid.uuid4().hex[:8]}"

        with self.target_conn.cursor() as cur:
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table_name}' AND table_schema='{target_schema}' ORDER BY ordinal_position")
            target_cols = [r[0] for r in cur.fetchall()]
            
            if not target_cols:
                self.ensure_target_table_exists_from_df(df, target_table)
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table_name}' AND table_schema='{target_schema}' ORDER BY ordinal_position")
                target_cols = [r[0] for r in cur.fetchall()]
                
            cols_str = ", ".join([f'"{c}"' for c in target_cols])

            cur.execute(f"CREATE UNLOGGED TABLE {staging_table} (LIKE {target_table} INCLUDING DEFAULTS)")
            self.target_conn.commit()

        # 2. Streaming Loop Logic
        total_rows = 0
        BATCH_SIZE = 20000
        temp_dir = os.getenv('TEMP_DIR', tempfile.gettempdir())
        
        try:
            source_cols = df.columns.tolist()
            
            mapping_cols = []
            for col in target_cols:
                if col in source_cols:
                    mapping_cols.append(col)
                elif col == 'snapshot_date':
                    df['snapshot_date'] = str(self.snapshot_date)
                    mapping_cols.append(col)
                elif col == 'ingestion_timestamp':
                    df['ingestion_timestamp'] = str(get_current_timestamp_naive())
                    mapping_cols.append(col)
                elif col == 'source_server':
                    df['source_server'] = 'Google Sheet'
                    mapping_cols.append(col)
                else:
                    df[col] = None
                    mapping_cols.append(col)
            
            # Reorder df to match target_cols exactly
            df = df[target_cols]
            
            # Convert to PyArrow chunked table logic
            pa_schema = pa.schema([(c, pa.string()) for c in target_cols])
            
            num_rows = len(df)
            for i in range(0, num_rows, BATCH_SIZE):
                chunk_df = df.iloc[i:i+BATCH_SIZE].copy()
                
                # Transform to string representation
                chunk_df = chunk_df.astype(str).replace({'None': None, 'nan': None})
                
                pa_table = pa.Table.from_pandas(chunk_df, schema=pa_schema, preserve_index=False)
                
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
                
                row_count = len(chunk_df)
                total_rows += row_count
                logger.info(f"Ingested {row_count} into {target_table}. Total: {total_rows}")
                del pa_table
                del chunk_df
                gc.collect()
                
            return total_rows
        finally:
            with self.target_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                self.target_conn.commit()
            
    def prepare_target(self, config: Dict, drop_table: bool):
        target_table = config['table_data'].lower()

        # --- Bước 1: DROP (nếu cần) --- dùng cursor riêng
        if drop_table:
            with self.target_conn.cursor() as cur:
                logger.warning(f"Dropping target table {target_table} (CASCADE) as requested.")
                cur.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE")
                self.target_conn.commit()

        # --- Bước 2: TRUNCATE (nếu cần) --- cursor riêng, rollback không ảnh hưởng bước sau
        if config.get('method_data', False):
            logger.info(f"Truncating target table {target_table} as requested.")
            try:
                with self.target_conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {target_table} CASCADE")
                    self.target_conn.commit()
            except Exception as e:
                self.target_conn.rollback()
                logger.warning(f"Could not truncate {target_table} (may not exist yet): {e}")

        # --- Bước 3: Tạo index PK (nếu cần) --- cursor mới, hoàn toàn sạch
        pk_columns = [k.strip() for k in config.get('key', '').split(',')] if config.get('key') else []
        if pk_columns:
            target_schema = target_table.split('.')[0] if '.' in target_table else 'public'
            target_table_name = target_table.split('.')[-1]

            with self.target_conn.cursor() as cur:
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table_name}' AND table_schema='{target_schema}'")
                existing_cols = [r[0].lower() for r in cur.fetchall()]
                valid_pk = [c for c in pk_columns if c.lower() in existing_cols]

                if valid_pk:
                    idx_name = f"idx_{target_table.replace('.', '_')}_pk"[:63]
                    pk_cols_str = ", ".join(['"' + c + '"' for c in valid_pk])
                    cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON {target_table} ({pk_cols_str})')
                    self.target_conn.commit()

    def run(self, sheet_id: Optional[int] = None, drop_table: bool = False):
        logger.info(f"🚀 Starting Google Sheet to Postgres Direct Streaming for {self.snapshot_date}")
        configs = get_active_googlesheet_configs(sheet_id=sheet_id, loai='IN')
        
        for config in configs:
            try:
                self.connect_target()
                target_table = config['table_data'].lower()
                
                # Fetch data first to verify it exists and is valid
                df = self.get_google_sheet_data(
                    config.get('spreadsheet_id'), 
                    config.get('sheet'), 
                    config.get('remove_column'), 
                    config.get('rename_column')
                )
                
                if df is None or len(df) == 0:
                    logger.warning(f"No data for {target_table}, skipping table preparation and upsert.")
                    continue
                
                # If we have data, prepare & stream
                self.prepare_target(config, drop_table)
                self.stream_and_upsert(config, target_table, df)
                
                time.sleep(2)
                logger.success(f"✅ Completed {config['name']}")
                
            except Exception as e:
                logger.error(f"Failed {config['name']}: {e}")
            finally:
                if self.target_conn:
                    self.target_conn.close()
                    self.target_conn = None

if __name__ == "__main__":
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    s_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    s_id_str = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None
    s_id = None
    if s_id_str and s_id_str.strip() and s_id_str.lower() != 'none':
        s_id = int(s_id_str.strip(' "\''))
    
    drop_table_arg = sys.argv[3] if len(sys.argv) > 3 else 'false'
    drop_table = drop_table_arg.lower() in ('true', '1', 'yes')
    
    try:
        DirectGoogleSheetToPostgres(s_date).run(sheet_id=s_id, drop_table=drop_table)
    except Exception as e:
        logger.error(str(e))
        sys.exit(1)
