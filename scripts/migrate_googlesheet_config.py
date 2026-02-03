"""
Migration Script: Google Sheet Configurations from PostgreSQL to Iceberg Gold
Migrate declare_api_googsheet table to call_functions.declare_api_googlesheet (Iceberg)
"""

import os
import sys
from datetime import datetime
import psycopg2
import pyarrow as pa
from loguru import logger

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.googlesheet_config import ensure_googlesheet_config_table_exists

# PostgreSQL connection (old system)
OLD_POSTGRES_URI = os.getenv('OLD_POSTGRES_URI', 'postgresql://user:password@localhost:5432/etl_marketing')


def migrate_googlesheet_configs():
    """
    Migrate Google Sheet configurations from old PostgreSQL to Iceberg Gold
    """
    logger.info("Starting Google Sheet configuration migration...")
    
    # Connect to old PostgreSQL
    try:
        pg_conn = psycopg2.connect(OLD_POSTGRES_URI)
        cursor = pg_conn.cursor()
        logger.info("Connected to old PostgreSQL database")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return
    
    try:
        # Read from old table (all fields, matching schema exactly)
        query = """
        SELECT 
            id,
            name,
            spreadsheet_id,
            sheet,
            table_data,
            method_data,
            remove_column,
            rename_column,
            schedule,
            active_schedule,
            active_daily,
            active,
            key,
            loai,
            query,
            creator_id,
            updater_id,
            ghi_chu,
            result,
            update_time
        FROM declare_api_googsheet
        WHERE active = TRUE
        ORDER BY id
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        logger.info(f"Found {len(rows)} Google Sheet configurations to migrate")
        
        if not rows:
            logger.warning("No configurations found to migrate")
            return
        
        # Prepare data for Iceberg
        data = {
            'id': [],
            'name': [],
            'spreadsheet_id': [],
            'sheet': [],
            'table_data': [],
            'method_data': [],
            'remove_column': [],
            'rename_column': [],
            'schedule': [],
            'active_schedule': [],
            'active_daily': [],
            'active': [],
            'key': [],
            'loai': [],
            'query': [],
            'creator_id': [],
            'updater_id': [],
            'ghi_chu': [],
            'result': [],
            'update_time': [],
        }
        
        for row in rows:
            data['id'].append(row[0])
            data['name'].append(row[1] or '')
            data['spreadsheet_id'].append(row[2] or '')
            data['sheet'].append(row[3] or '')
            data['table_data'].append(row[4] or '')
            data['method_data'].append(row[5] if row[5] is not None else False)
            data['remove_column'].append(row[6] or '')
            data['rename_column'].append(row[7] or '')
            data['schedule'].append(row[8] or '')
            data['active_schedule'].append(row[9] if row[9] is not None else False)
            data['active_daily'].append(row[10] if row[10] is not None else False)
            data['active'].append(row[11] if row[11] is not None else True)
            data['key'].append(row[12] or '')
            data['loai'].append(row[13] or 'IN')
            data['query'].append(row[14] or '')
            data['creator_id'].append(row[15])
            data['updater_id'].append(row[16])
            data['ghi_chu'].append(row[17] or '')
            data['result'].append(row[18] or '')
            data['update_time'].append(row[19])
        
        # Convert to PyArrow Table
        arrow_table = pa.table(data)
        
        # Ensure target table exists
        config_table = ensure_googlesheet_config_table_exists()
        
        # Append data
        logger.info(f"Writing {len(rows)} configurations to Iceberg...")
        config_table.append(arrow_table)
        
        logger.success(f"✅ Successfully migrated {len(rows)} Google Sheet configurations")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        cursor.close()
        pg_conn.close()


if __name__ == "__main__":
    migrate_googlesheet_configs()
