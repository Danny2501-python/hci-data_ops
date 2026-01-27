"""
ETL Table Configuration Helper
Load ETL table configurations from Gold layer etl_metadata.table_config
Similar to brand_config.py pattern
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

TABLE_CONFIG_TABLE = 'call_functions.declare_table_etl'


def ensure_table_config_table_exists():
    """
    Create etl_metadata.table_config table if not exists
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
        return gold_catalog.load_table(TABLE_CONFIG_TABLE)
    except:
        logger.info(f"Creating table {TABLE_CONFIG_TABLE}...")
        
        # Schema matching declare_table_etl from PostgreSQL
        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "table_name", StringType(), required=False),
            NestedField(3, "table_name_source", StringType(), required=False),
            NestedField(4, "source", StringType(), required=False),
            NestedField(5, "server", StringType(), required=False),
            NestedField(6, "created_date", TimestampType(), required=False),
            NestedField(7, "updated_date", TimestampType(), required=False),
            NestedField(8, "active", BooleanType(), required=False),
            NestedField(9, "keyword", StringType(), required=False),
            NestedField(10, "rename_column", StringType(), required=False),
            NestedField(11, "ghi_chu", StringType(), required=False),
            NestedField(12, "result", StringType(), required=False),
            NestedField(13, "update_time", TimestampType(), required=False),
            NestedField(14, "primary_keyword", StringType(), required=False),
            NestedField(15, "is_transfrom", BooleanType(), required=False),
            NestedField(16, "consider", StringType(), required=False),
            NestedField(17, "truncate", BooleanType(), required=False),
        )
        
        table = gold_catalog.create_table(
            identifier=TABLE_CONFIG_TABLE,
            schema=schema,
        )
        logger.success(f"✅ Created table: {TABLE_CONFIG_TABLE}")
        return table


def get_active_table_configs(table_id: Optional[int] = None) -> List[Dict]:
    """
    Load active table configurations from Gold layer
    
    Args:
        table_id: Optional specific table ID to load
    
    Returns:
        List of table config dicts with all configuration fields
    """
    # Ensure table exists
    config_table = ensure_table_config_table_exists()
    
    # Build filter
    filters = [
        EqualTo("active", True),
        EqualTo("is_transfrom", True)
    ]
    if table_id:
        filters.append(EqualTo("id", table_id))
    
    # Scan for active configs
    scan = config_table.scan(
        row_filter=And(*filters) if len(filters) > 1 else filters[0]
    )
    
    configs = []
    for batch in scan.to_arrow_batch_reader():
        for i in range(len(batch)):
            config = {
                "id": batch["id"][i].as_py(),
                "table_name": batch["table_name"][i].as_py(),
                "table_name_source": batch["table_name_source"][i].as_py(),
                "source": batch["source"][i].as_py(),
                "server": batch["server"][i].as_py(),
                "created_date": batch["created_date"][i].as_py(),
                "updated_date": batch["updated_date"][i].as_py(),
                "active": batch["active"][i].as_py(),
                "keyword": batch["keyword"][i].as_py(),
                "rename_column": batch["rename_column"][i].as_py(),
                "ghi_chu": batch["ghi_chu"][i].as_py(),
                "result": batch["result"][i].as_py(),
                "update_time": batch["update_time"][i].as_py(),
                "primary_keyword": batch["primary_keyword"][i].as_py(),
                "is_transfrom": batch["is_transfrom"][i].as_py(),
                "consider": batch["consider"][i].as_py(),
                "truncate": batch["truncate"][i].as_py(),
            }
            configs.append(config)
    
    logger.info(f"Loaded {len(configs)} active table configurations from {TABLE_CONFIG_TABLE}")
    return configs


def get_table_config_by_id(table_id: int) -> Optional[Dict]:
    """
    Get a specific table configuration by ID
    
    Args:
        table_id: Table configuration ID
    
    Returns:
        Table config dict or None if not found
    """
    configs = get_active_table_configs(table_id=table_id)
    return configs[0] if configs else None


def get_table_configs_by_source(source: str) -> List[Dict]:
    """
    Get table configurations by source system
    
    Args:
        source: Source system name (e.g., 'ERP', 'GiaoViec')
    
    Returns:
        List of table config dicts for the specified source
    """
    # Ensure table exists
    config_table = ensure_table_config_table_exists()
    
    # Scan for active configs from specific source
    scan = config_table.scan(
        row_filter=And(
            EqualTo("active", True),
            EqualTo("is_transfrom", True),
            EqualTo("source", source)
        )
    )
    
    configs = []
    for batch in scan.to_arrow_batch_reader():
        for i in range(len(batch)):
            config = {
                "id": batch["id"][i].as_py(),
                "table_name": batch["table_name"][i].as_py(),
                "table_name_source": batch["table_name_source"][i].as_py(),
                "source": batch["source"][i].as_py(),
                "server": batch["server"][i].as_py(),
                "created_date": batch["created_date"][i].as_py(),
                "updated_date": batch["updated_date"][i].as_py(),
                "active": batch["active"][i].as_py(),
                "keyword": batch["keyword"][i].as_py(),
                "rename_column": batch["rename_column"][i].as_py(),
                "ghi_chu": batch["ghi_chu"][i].as_py(),
                "result": batch["result"][i].as_py(),
                "update_time": batch["update_time"][i].as_py(),
                "primary_keyword": batch["primary_keyword"][i].as_py(),
                "is_transfrom": batch["is_transfrom"][i].as_py(),
                "consider": batch["consider"][i].as_py(),
                "truncate": batch["truncate"][i].as_py(),
            }
            configs.append(config)
    
    logger.info(f"Loaded {len(configs)} table configurations for source '{source}'")
    return configs
