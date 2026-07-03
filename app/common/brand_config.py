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
            NestedField(10, "custom_field_mappings", StringType(), required=False),  # JSON config for custom field IDs
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


def get_brand_by_erp_id(erp_id: int):
    """
    Load brand by erp_id from Gold layer core_brand table, regardless of is_active status
    
    Args:
        erp_id: The ERP ID of the brand to retrieve
        
    Returns:
        Brand dict with keys: id, name, erp_id, is_active, caresoft_domain, caresoft_key, custom_field_mappings
        Or None if not found
    """
    # Ensure table exists
    brand_table = ensure_brand_table_exists()
    
    # Scan for the brand by erp_id
    scan = brand_table.scan(
        row_filter=EqualTo("erp_id", int(erp_id)),
        selected_fields=("id", "name", "erp_id", "is_active", "caresoft_domain", "caresoft_key", "custom_field_mappings")
    )
    
    for batch in scan.to_arrow_batch_reader():
        for i in range(len(batch)):
            brand = {
                "id": batch["id"][i].as_py() if batch["id"][i] is not None else None,
                "name": batch["name"][i].as_py() if batch["name"][i] is not None else None,
                "erp_id": batch["erp_id"][i].as_py() if batch["erp_id"][i] is not None else None,
                "is_active": batch["is_active"][i].as_py() if batch["is_active"][i] is not None else None,
                "caresoft_domain": batch["caresoft_domain"][i].as_py() if batch["caresoft_domain"][i] is not None else None,
                "caresoft_key": batch["caresoft_key"][i].as_py() if batch["caresoft_key"][i] is not None else None,
                "custom_field_mappings": batch["custom_field_mappings"][i].as_py() if "custom_field_mappings" in batch.schema.names and batch["custom_field_mappings"][i] is not None else None
            }
            logger.info(f"Loaded brand {brand['name']} (erp_id={erp_id}) from {BRAND_TABLE}")
            return brand
            
    logger.warning(f"Brand with erp_id={erp_id} not found in {BRAND_TABLE}")
    return None

