import os
import sys
import pandas as pd
from datetime import datetime
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.database import PostgreSQLHelper
from common.utils import bulk_load, get_current_timestamp_naive

class SCDProcessor:
    """
    High-performance SCD Type 2 Processor for data_ops platform.
    Uses TEMP tables and set-based SQL operations for maximum speed.
    """
    
    def __init__(self, staging_uri: str, erp_uri: str):
        self.stg_db = PostgreSQLHelper(staging_uri)
        self.erp_db = PostgreSQLHelper(erp_uri)
        self.now = get_current_timestamp_naive()
        self.default_expiry = datetime(2100, 12, 31)

    def run_all(self, id_list=None):
        """Fetch all active configs and process them"""
        # Build filter
        where_clause = 'WHERE "Active" = True'
        
        if id_list:
            if isinstance(id_list, int):
                where_clause += f' AND id = {id_list}'
            elif isinstance(id_list, list):
                if id_list:
                    ids_str = ', '.join(map(str, id_list))
                    where_clause += f" AND id IN ({ids_str})"
        
        # Fetching directly from 'slowly_dimensions' table in Staging
        query = f"""
            SELECT id, "Table_source", "Table_warehouse", "Column_save", "Key", "Khac_biet" 
            FROM slowly_dimensions 
            {where_clause}
        """
        configs = self.stg_db.fetchall(query)
        
        for cfg in configs:
            cfg_id, src, wh, cols, keys, diffs = cfg
            try:
                logger.info(f"🚀 Processing SCD2 for {src} -> {wh}")
                self.process_table_v2(src, wh, cols, keys, diffs)
            except Exception as e:
                logger.error(f"❌ Failed to process {src}: {e}")

    def process_table_v2(self, src, wh, cols, keys, diffs):
        key_list = [k.strip() for k in keys.split(',')]
        diff_list = [d.strip() for d in diffs.split(',')]
        col_list = [c.strip() for c in cols.split(',')]
        
        # 1. Fetch ALL relevant data from ERP (Source)
        erp_sql = f"SELECT {cols} FROM {src}"
        erp_records = self.erp_db.fetchall(erp_sql)
        if not erp_records:
            logger.warning(f"No records found in source table {src}")
            return

        df_erp = pd.DataFrame(erp_records, columns=col_list)
        
        # 2. Load ERP data into a TEMP table in Staging for fast comparison
        temp_table = f"stg_scd_temp_{src.split('.')[-1]}_{os.getpid()}"
        
        with self.stg_db.conn.cursor() as cursor:
            # Create temp table matching warehouse schema (but only for columns we care about)
            cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {wh} INCLUDING ALL) ON COMMIT DROP")
            
            # Clean up metadata columns if they exist in warehouse (start_date, status, etc.)
            for meta_col in ['start_date', 'expiration_date', 'status']:
                cursor.execute(f"ALTER TABLE {temp_table} DROP COLUMN IF EXISTS {meta_col}")
            
            # Load data via COPY (Directly to our temp table)
            from io import StringIO
            import csv
            
            output = StringIO()
            writer = csv.writer(output, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            NULL_MARKER = '__NULL__'
            
            for rec in df_erp.to_dict('records'):
                row = []
                for col in col_list:
                    val = rec.get(col)
                    if val is None: row.append(NULL_MARKER)
                    else: row.append(str(val).replace('\n', ' ').replace('\r', ' '))
                writer.writerow(row)
            
            output.seek(0)
            cursor.copy_expert(f"COPY {temp_table} ({', '.join([f'\"{c}\"' for c in col_list])}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '{NULL_MARKER}')", output)
            
            # 3. IDENTIFY CHANGES & INSERT LOGS (table_change_slowly)
            key_join = " AND ".join([f"w.\"{k}\" = t.\"{k}\"" for k in key_list])
            
            logger.info("Logging changes into table_change_slowly...")
            for col in diff_list:
                if col in col_list:
                    # Using CONCAT logic to generate the name_key string 'k1=v1,k2=v2'
                    name_key_sql = " || ',' || ".join([f"'{k}=' || CAST(w.\"{k}\" AS TEXT)" for k in key_list])
                    
                    log_sql = f"""
                        INSERT INTO table_change_slowly (source_table, warehouse_table, column_name, old_value, new_value, change_date, name_key)
                        SELECT 
                            '{src}', '{wh}', '{col}', 
                            CAST(w."{col}" AS TEXT), CAST(t."{col}" AS TEXT), 
                            %s, 
                            {name_key_sql}
                        FROM {wh} w
                        JOIN {temp_table} t ON {key_join}
                        WHERE w.status = 'Y' AND CAST(w."{col}" AS TEXT) IS DISTINCT FROM CAST(t."{col}" AS TEXT)
                    """
                    cursor.execute(log_sql, (self.now,))

            # 4. CLOSE OLD RECORDS (status='Y' -> 'N')
            diff_cond = " OR ".join([f"w.\"{d}\" IS DISTINCT FROM t.\"{d}\"" for d in diff_list])
            close_sql = f"""
                UPDATE {wh} w
                SET status = 'N', expiration_date = %s
                FROM {temp_table} t
                WHERE {key_join} AND w.status = 'Y' AND ({diff_cond})
            """
            cursor.execute(close_sql, (self.now,))
            
            # 5. INSERT NEW RECORDS ('Y' version for changed data + brand new records)
            # Find columns that exist in both temp and warehouse
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{wh}'")
            wh_all_cols = [r[0] for r in cursor.fetchall()]
            
            # Intersection of col_list and warehouse columns
            valid_cols = [c for c in col_list if c in wh_all_cols]
            
            insert_sql = f"""
                INSERT INTO {wh} ({", ".join([f'"{c}"' for c in valid_cols])}, start_date, expiration_date, status)
                SELECT 
                    {", ".join([f't."{c}"' for c in valid_cols])}, 
                    %s as start_date, 
                    %s as expiration_date, 
                    'Y' as status
                FROM {temp_table} t
                WHERE NOT EXISTS (
                    SELECT 1 FROM {wh} w 
                    WHERE {key_join} AND w.status = 'Y'
                )
            """
            cursor.execute(insert_sql, (self.now, self.default_expiry))
            
            self.stg_db.commit()
            logger.success(f"✅ SCD2 Sync complete for {wh}")

    def close(self):
        self.stg_db.close()
        self.erp_db.close()

if __name__ == "__main__":
    import os
    STG_URI = os.getenv('STAGING_SCI_URI')
    ERP_URI = os.getenv('ERP_URI')
    
    if not STG_URI or not ERP_URI:
        logger.error("STAGING_SCI_URI or ERP_URI not set in environment")
        sys.exit(1)

    # Parse table_ids from CLI (e.g., "[1,2]" or "1,2" or "1")
    t_id_arg = sys.argv[1] if len(sys.argv) > 1 else None
    t_id = None
    if t_id_arg and t_id_arg.strip() and t_id_arg.lower() != 'none':
        # 1. Clean brackets and spaces
        raw_val = str(t_id_arg).strip('[] ')
        
        # 2. Handle comma-separated list
        if ',' in raw_val:
            parts = raw_val.split(',')
            t_id = []
            for p in parts:
                clean_p = p.strip(' "\'')
                if clean_p.isdigit():
                    t_id.append(int(clean_p))
        # 3. Handle single ID
        else:
            clean_p = raw_val.strip(' "\'')
            if clean_p.isdigit():
                t_id = int(clean_p)
    
    logger.info(f"Target IDs for SCD2: {t_id if t_id else 'ALL'}")
        
    proc = SCDProcessor(STG_URI, ERP_URI)
    proc.run_all(id_list=t_id)
    proc.close()
