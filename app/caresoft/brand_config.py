"""
Brand Configuration Helper
Load brand configurations from Gold layer core_brand table
"""

import os
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from loguru import logger

# Iceberg configuration
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

BRAND_TABLE = 'caresoft.core_brand'


def ensure_brand_table_exists():
    """
    Create core_brand table if not exists
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
        return gold_catalog.load_table(BRAND_TABLE)
    except:
        logger.info(f"Creating table {BRAND_TABLE}...")
        
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType, IntegerType, StringType, BooleanType, TimestampType
        
        # Schema matching legacy core_brand table
        schema = Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "erp_id", IntegerType(), required=False),
            NestedField(4, "etl_job_name", StringType(), required=False),
            NestedField(5, "is_active", BooleanType(), required=False),
            NestedField(6, "caresoft_domain", StringType(), required=False),
            NestedField(7, "caresoft_key", StringType(), required=False),
            NestedField(8, "created", TimestampType(), required=False),
            NestedField(9, "updated", TimestampType(), required=False),
        )
        
        table = gold_catalog.create_table(
            identifier=BRAND_TABLE,
            schema=schema
        )
        logger.success(f"✅ Created table: {BRAND_TABLE}")
        return table


def get_active_brands():
    """
    Load active brands from Gold layer core_brand table
    
    Returns:
        List of brand dicts with keys: id, name, erp_id, caresoft_domain, caresoft_key
    """
    # Ensure table exists
    brand_table = ensure_brand_table_exists()
    
    # Scan for active brands
    scan = brand_table.scan(
        row_filter=EqualTo("is_active", True),
        selected_fields=("id", "name", "erp_id", "caresoft_domain", "caresoft_key")
    )
    
    brands = []
    for batch in scan.to_arrow_batch_reader():
        for i in range(len(batch)):
            brand = {
                "id": batch["id"][i].as_py(),
                "name": batch["name"][i].as_py(),
                "erp_id": batch["erp_id"][i].as_py(),
                "caresoft_domain": batch["caresoft_domain"][i].as_py(),
                "caresoft_key": batch["caresoft_key"][i].as_py(),
            }
            brands.append(brand)
    
    logger.info(f"Loaded {len(brands)} active brands from {BRAND_TABLE}")
    return brands
