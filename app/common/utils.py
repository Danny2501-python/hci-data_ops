import io
import csv
import os
from datetime import datetime, date, timezone, timedelta

# Define UTC+7 timezone (Asia/Ho_Chi_Minh)
UTC_PLUS_7 = timezone(timedelta(hours=7))


def get_current_timestamp():
    """Get current timestamp in UTC+7"""
    return datetime.now(UTC_PLUS_7)


def get_current_timestamp_naive():
    """Get current timestamp in UTC+7 as naive datetime"""
    return datetime.now(UTC_PLUS_7).replace(tzinfo=None)


def bulk_load(cursor, target_table, records, columns, conflict_columns, method='upsert', update_condition=None):
    """
    High-performance Bulk Load using TEMP TABLE + COPY + (UPSERT or DELETE-INSERT)
    
    Args:
        cursor: psycopg2 cursor
        target_table: Name of the target table
        records: List of dicts or tuples
        columns: List of column names
        conflict_columns: List of columns for matching/conflict (e.g. ['id', 'brand_id'])
        method: 'upsert' (ON CONFLICT) or 'delete_insert' (Replace by conflict_columns)
        update_condition: Optional condition for update (e.g. 'EXCLUDED.updated_at > target.updated_at')
    """
    if not records:
        return
        
    NULL_MARKER = '__NULL__'
    output = io.StringIO()
    writer = csv.writer(output, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    
    # Prepare data for COPY
    for rec in records:
        row = []
        is_dict = isinstance(rec, dict)
        for i, col in enumerate(columns):
            val = rec.get(col) if is_dict else rec[i]
            if val is None:
                row.append(NULL_MARKER)
            elif isinstance(val, (datetime, date)):
                row.append(val.isoformat())
            else:
                row.append(str(val).replace('\n', ' ').replace('\r', ' '))
        writer.writerow(row)
    
    output.seek(0)
    # Use a unique temp table name to avoid conflicts in parallel tasks
    temp_table = f"temp_stg_{target_table.split('.')[-1]}_{os.getpid()}_{int(datetime.now().timestamp())}"
    
    # 1. Create staging table
    cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {target_table} INCLUDING ALL) ON COMMIT DROP")
    
    # 2. Load via COPY
    col_list = ', '.join([f'"{c}"' for c in columns])
    cursor.copy_expert(f"COPY {temp_table} ({col_list}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '{NULL_MARKER}')", output)
    
    # 3. Perform Sync
    conflict_cols_str = ', '.join([f'"{c}"' for c in conflict_columns])
    
    if method == 'upsert':
        update_cols = [c for c in columns if c not in conflict_columns]
        update_set = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        
        upsert_sql = f"""
            INSERT INTO {target_table} ({col_list}) 
            SELECT {col_list} FROM {temp_table}
            ON CONFLICT ({conflict_cols_str}) 
            DO UPDATE SET {update_set}
        """
        if update_condition:
            upsert_sql += f" WHERE {update_condition}"
        cursor.execute(upsert_sql)
        
    elif method == 'delete_insert':
        # 1. Delete existing records that match the conflict criteria in the staging table
        # We use a subquery to get unique conflict values from temp
        delete_sql = f"""
            DELETE FROM {target_table} as t
            USING (SELECT DISTINCT {conflict_cols_str} FROM {temp_table}) as s
            WHERE {' AND '.join([f't."{c}" = s."{c}"' for c in conflict_columns])}
        """
        cursor.execute(delete_sql)
        
        # 2. Insert all from temp
        insert_sql = f"INSERT INTO {target_table} ({col_list}) SELECT {col_list} FROM {temp_table}"
        cursor.execute(insert_sql)
        
    output.close()
