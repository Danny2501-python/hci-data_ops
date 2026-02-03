"""
Google Sheet Configuration Helper
Load Google Sheet configurations from Gold layer call_functions.declare_api_googlesheet
Separate from general ETL config to maintain original logic
"""

import os
from typing import List, Dict, Optional
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, IntegerType, StringType, TimestampType, BooleanType
)
from loguru import logger

# Iceberg configuration
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

GOOGLESHEET_CONFIG_TABLE = 'call_functions.declare_api_googlesheet'


def ensure_googlesheet_config_table_exists():
    """
    Create call_functions.declare_api_googlesheet table if not exists
    Returns the table object
    """
    # Initialize Gold catalog
    catalog_config = {
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": AWS_ACCESS_KEY,
        "s3.secret-access-key": AWS_SECRET_KEY,
        "s3.region": AWS_REGION,
    }
    
    gold_catalog = load_catalog(
        CATALOG_NAME_GOLD,
        uri=ICEBERG_REST_URI_GOLD,
        **catalog_config
    )
    
    try:
        return gold_catalog.load_table(GOOGLESHEET_CONFIG_TABLE)
    except:
        logger.info(f"Creating table {GOOGLESHEET_CONFIG_TABLE}...")
        
        # Schema matching declare_api_googsheet from PostgreSQL
        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "spreadsheet_id", StringType(), required=False),
            NestedField(4, "sheet", StringType(), required=False),
            NestedField(5, "table_data", StringType(), required=False),
            NestedField(6, "method_data", BooleanType(), required=False),
            NestedField(7, "remove_column", StringType(), required=False),
            NestedField(8, "rename_column", StringType(), required=False),
            NestedField(9, "schedule", StringType(), required=False),
            NestedField(10, "active_schedule", BooleanType(), required=False),
            NestedField(11, "active_daily", BooleanType(), required=False),
            NestedField(12, "active", BooleanType(), required=False),
            NestedField(13, "key", StringType(), required=False),
            NestedField(14, "loai", StringType(), required=False),
            NestedField(15, "query", StringType(), required=False),
            NestedField(16, "creator_id", IntegerType(), required=False),
            NestedField(17, "updater_id", IntegerType(), required=False),
            NestedField(18, "ghi_chu", StringType(), required=False),
            NestedField(19, "result", StringType(), required=False),
            NestedField(20, "update_time", TimestampType(), required=False),
        )
        
        table = gold_catalog.create_table(
            identifier=GOOGLESHEET_CONFIG_TABLE,
            schema=schema,
        )
        logger.success(f"✅ Created table: {GOOGLESHEET_CONFIG_TABLE}")
        return table


def get_active_googlesheet_configs(sheet_id: Optional[int] = None, 
                                   loai: str = 'IN') -> List[Dict]:
    """
    Load active Google Sheet configurations from Gold layer
    
    Args:
        sheet_id: Optional specific sheet ID to load
        loai: Direction - 'IN' (Google Sheet -> DB) or 'OUT' (DB -> Google Sheet)
    
    Returns:
        List of Google Sheet config dicts
    """
    # Ensure table exists
    config_table = ensure_googlesheet_config_table_exists()
    
    # Build filter
    filters = [
        EqualTo("active", True),
    ]
    
    if sheet_id:
        filters.append(EqualTo("id", sheet_id))
    
    # Filter by direction (loai)
    if loai:
        filters.append(EqualTo("loai", loai))
    
    # Scan for active configs
    scan = config_table.scan(
        row_filter=And(*filters) if len(filters) > 1 else filters[0]
    )
    
    configs = []
    for batch in scan.to_arrow_batch_reader():
        for i in range(len(batch)):
            config = {
                "id": batch["id"][i].as_py(),
                "name": batch["name"][i].as_py(),
                "spreadsheet_id": batch["spreadsheet_id"][i].as_py(),
                "sheet": batch["sheet"][i].as_py(),
                "table_data": batch["table_data"][i].as_py(),
                "method_data": batch["method_data"][i].as_py(),
                "remove_column": batch["remove_column"][i].as_py(),
                "rename_column": batch["rename_column"][i].as_py(),
                "schedule": batch["schedule"][i].as_py(),
                "active_schedule": batch["active_schedule"][i].as_py(),
                "active_daily": batch["active_daily"][i].as_py(),
                "active": batch["active"][i].as_py(),
                "key": batch["key"][i].as_py(),
                "loai": batch["loai"][i].as_py(),
                "query": batch["query"][i].as_py(),
                "creator_id": batch["creator_id"][i].as_py(),
                "updater_id": batch["updater_id"][i].as_py(),
                "ghi_chu": batch["ghi_chu"][i].as_py(),
                "result": batch["result"][i].as_py(),
                "update_time": batch["update_time"][i].as_py(),
            }
            configs.append(config)
    
    logger.info(f"Loaded {len(configs)} active Google Sheet configurations from {GOOGLESHEET_CONFIG_TABLE}")
    return configs
