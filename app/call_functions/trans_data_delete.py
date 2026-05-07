import pandas as pd
import json
import psycopg2
import tempfile
import os
import sys
from datetime import datetime
from time import perf_counter
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.database import PostgreSQLHelper
from common.utils import get_current_timestamp_naive

class TransDataDelete:
    def __init__(self, target_uri: str, brand_name: str, brand_etl: int = None):
        """
        Initialize with target (DWH) connection.
        auditlog_log and ir_model are assumed to be in the target DWH.
        """
        self.target_uri = target_uri
        self.brand_name = brand_name
        self.brand_etl = brand_etl
        self.source_log = f'Etl data delete - {brand_name}'
        
        # All operations happen on the Target DWH
        self.target_db = PostgreSQLHelper(target_uri)

    def check_data_delete(self, query='', date=1):
        """
        Scan ERP auditlog_log for deleted records.
        """
        try:
            if not query:
                # Logic: Find 'unlink' (delete) operations in auditlog_log
                # join with ir_model to get the table name
                query = f"""
                    WITH check_table AS (
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_type = 'BASE TABLE' AND table_schema = 'public'
                    )
                    SELECT 
                        REPLACE(imd2.model, '.', '_') AS model,
                        imd.res_id,
                        imd.create_date,
                        imd.write_date,
                        imd.id, 
                        imd.brand_etl 
                    FROM (
                        SELECT * FROM auditlog_log 
                        WHERE DATE(write_date) >= CURRENT_DATE - INTERVAL '{date} days'
                        AND method = 'unlink'
                        {f"AND brand_etl = {self.brand_etl}" if self.brand_etl else ""}
                    ) imd 
                    LEFT JOIN ir_model imd2 ON imd2.id = imd.model_id AND imd2.brand_etl = imd.brand_etl
                    WHERE imd2.model IS NOT NULL AND imd.res_id IS NOT NULL
                    AND EXISTS (SELECT 1 FROM check_table ck WHERE ck.table_name = REPLACE(imd2.model, '.', '_'))
                    AND NOT EXISTS (
                        SELECT 1 FROM logging_delete ld 
                        WHERE ld.id = imd.id AND ld.res_id = imd.res_id AND ld.brand_etl = imd.brand_etl
                    ) 
                    ORDER BY imd2.model
                """
            
            # Execute on Target DWH (since auditlog_log is synced here)
            cursor = self.target_db.conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data_delete = pd.DataFrame(cursor.fetchall(), columns=columns)
            cursor.close()
            return data_delete
            
        except Exception as e:
            logger.error(f"[{self.source_log}] Error in check_data_delete: {e}")
            raise e

    def delete_data_in_target(self, table, res_id, brand_etl):
        """
        Perform the actual delete in the Target DWH.
        """
        try:
            query = f"DELETE FROM {table} WHERE id = %s AND brand_etl = %s"
            self.target_db.execute(query, (res_id, brand_etl))
            return True
        except Exception as e:
            logger.error(f"[{self.source_log}] Error deleting from {table} (ID: {res_id}): {e}")
            return False

    def insert_data_delete(self, data_delete, table_logging='logging_delete'):
        """
        Backup data, Delete from target, and Log the operation.
        """
        if data_delete.empty:
            logger.info(f"[{self.source_log}] No data to delete.")
            return True

        logger.info(f"[{self.source_log}] Processing {len(data_delete)} delete records...")
        start_time = perf_counter()
        
        # Ensure logging table exists in target
        self._ensure_logging_table(table_logging)

        def convert_to_json(row):
            try:
                return json.dumps(row.to_dict(), default=str)
            except Exception:
                return None

        success_count = 0
        for _, row in data_delete.iterrows():
            try:
                # 1. Fetch current data from Target for backup
                fetch_query = f"SELECT * FROM {row['model']} WHERE id = %s AND brand_etl = %s"
                cursor = self.target_db.conn.cursor()
                cursor.execute(fetch_query, (row['res_id'], row['brand_etl']))
                cols = [desc[0] for desc in cursor.description]
                df_backup = pd.DataFrame(cursor.fetchall(), columns=cols)
                cursor.close()

                if df_backup.empty:
                    # Record already gone or never existed in target
                    df_log = pd.DataFrame([{
                        'id': row['id'],
                        'table_name': row['model'],
                        'create_date': row['create_date'],
                        'write_date': row['write_date'],
                        'res_id': row['res_id'],
                        'raw_data': None,
                        'brand_etl': row['brand_etl'],
                        'has_retry_delete': True
                    }])
                else:
                    success_count += 1
                    # Prepare log entry with backup JSON
                    df_log = df_backup.copy()
                    df_log['raw_data'] = df_log.apply(convert_to_json, axis=1)
                    df_log['table_name'] = row['model']
                    df_log['create_date'] = row['create_date']
                    df_log['write_date'] = row['write_date']
                    df_log['res_id'] = row['res_id']
                    df_log['id'] = row['id']
                    df_log['brand_etl'] = row['brand_etl']
                    
                    # 2. Execute Delete in Target
                    df_log['has_retry_delete'] = self.delete_data_in_target(row['model'], row['res_id'], row['brand_etl'])

                # 3. Save Log to Target logging_delete
                # First remove old log if exists to avoid PK conflict (id is from auditlog_log)
                self.target_db.execute(
                    f"DELETE FROM {table_logging} WHERE id = %s AND res_id = %s AND brand_etl = %s",
                    (row['id'], row['res_id'], row['brand_etl'])
                )

                # Batch insert using COPY for efficiency
                self._copy_to_logging(df_log, table_logging)

            except Exception as e:
                logger.error(f"[{self.source_log}] Failed to process row {row['id']} for table {row['model']}: {e}")

        end_time = perf_counter()
        logger.success(f"[{self.source_log}] Deleted {success_count} records in {end_time - start_time:.2f}s")
        return True

    def _ensure_logging_table(self, table_name):
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id numeric(20) NOT NULL,
                table_name varchar(250) NULL,
                create_date timestamp NULL,
                write_date timestamp NULL,
                res_id numeric(20) NOT NULL,
                raw_data text NULL,
                brand_etl int4 NOT NULL,
                has_retry_delete bool NULL,
                ingestion_timestamp timestamp NULL,
                PRIMARY KEY (id, res_id, brand_etl)
            )
        """
        self.target_db.execute(query)

    def _copy_to_logging(self, df, table_name):
        # Thêm timestamp tại thời điểm thực hiện đổ dữ liệu
        df = df.copy() # Tránh SettingWithCopyWarning
        df['ingestion_timestamp'] = get_current_timestamp_naive()
        
        cols = ['id', 'table_name', 'create_date', 'write_date', 'res_id', 'raw_data', 'brand_etl', 'has_retry_delete', 'ingestion_timestamp']
        df_to_save = df[cols]
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as f:
            df_to_save.to_csv(f, index=False, header=True)
            temp_path = f.name

        try:
            copy_query = f"COPY {table_name} ({', '.join(cols)}) FROM STDIN WITH CSV HEADER"
            with open(temp_path, 'r', encoding='utf-8') as f:
                cursor = self.target_db.conn.cursor()
                cursor.copy_expert(copy_query, f)
                self.target_db.conn.commit()
                cursor.close()
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def run_trans_delete_data(self, date=1):
        logger.info(f"[{self.source_log}] Starting Sync Delete (Date range: {date} days)...")
        data = self.check_data_delete(date=date)
        self.insert_data_delete(data)
        logger.info(f"[{self.source_log}] Sync Delete Finished.")

    def retry_trans_delete(self, date=5):
        logger.info(f"[{self.source_log}] Starting Retry Sync Delete (Date range: {date} days)...")
        query = f"""
            SELECT 
                table_name AS model,
                res_id,
                create_date,
                write_date,
                id, 
                brand_etl 
            FROM 
                logging_delete 
            WHERE 
                (raw_data IS NULL OR has_retry_delete = false)
                AND DATE(write_date) >= CURRENT_DATE - INTERVAL '{date} days'
                {f"AND brand_etl = {self.brand_etl}" if self.brand_etl else ""}
            ORDER BY table_name 
        """
        data = self.check_data_delete(query=query, date=date)
        self.insert_data_delete(data)
        logger.info(f"[{self.source_log}] Retry Sync Delete Finished.")

    def close(self):
        self.target_db.close()

if __name__ == "__main__":
    # Example usage: python trans_data_delete.py <days>
    TARGET_URI = os.getenv('POSTGRES_URI')
    BRAND_NAME = os.getenv('BRAND_NAME', 'Unknown')
    BRAND_ETL = os.getenv('BRAND_ETL')
    
    if not TARGET_URI:
        logger.error("Error: POSTGRES_URI not set")
        sys.exit(1)
        
    b_etl = int(BRAND_ETL) if BRAND_ETL and BRAND_ETL.isdigit() else None
    worker = TransDataDelete(TARGET_URI, BRAND_NAME, b_etl)
    try:
        # Mặc định chạy 15 ngày cho sync mới và 5 ngày cho retry
        days_sync = int(sys.argv[1]) if len(sys.argv) > 1 else 15
        days_retry = 5
        
        # 1. Chạy sync xóa mới
        worker.run_trans_delete_data(date=days_sync)
        
        # 2. Chạy retry các bản ghi lỗi
        worker.retry_trans_delete(date=days_retry)
        
    finally:
        worker.close()
