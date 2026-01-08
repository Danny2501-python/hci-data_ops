"""
Test script to verify PyIceberg delete syntax
Run this to test different delete filter formats
"""

import os
from datetime import date
from pyiceberg.catalog import load_catalog
from loguru import logger

# Iceberg configuration
ICEBERG_REST_URI = os.getenv('ICEBERG_REST_URI_BRONZE', 'http://iceberg-rest-bronze-zone:8181')
CATALOG_NAME = os.getenv('CATALOG_NAME_BRONZE', 'bronze')
TABLE_NAME = os.getenv('BRONZE_TABLE', 'caresoft.wehook_api_caresoft')

S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://10.152.183.65:9000')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')

snapshot_date = date(2026, 1, 8)

logger.info(f"Connecting to Iceberg catalog: {CATALOG_NAME}")

# Load catalog
catalog = load_catalog(
    CATALOG_NAME,
    **{
        "uri": ICEBERG_REST_URI,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": AWS_ACCESS_KEY,
        "s3.secret-access-key": AWS_SECRET_KEY,
    }
)

logger.info(f"Loading table: {TABLE_NAME}")
table = catalog.load_table(TABLE_NAME)

# Check current data
logger.info("Querying current data...")
df = table.scan().to_pandas()

if not df.empty:
    logger.info(f"\nCurrent data:")
    logger.info(f"Total records: {len(df):,}")
    
    partition_counts = df.groupby('snapshot_date').size()
    logger.info(f"\nRecords per partition:")
    for partition_date, count in partition_counts.items():
        logger.info(f"  {partition_date}: {count:,} records")
    
    # Check target partition
    target_partition = df[df['snapshot_date'] == snapshot_date]
    logger.info(f"\nTarget partition ({snapshot_date}):")
    logger.info(f"  Total records: {len(target_partition):,}")
    
    if not target_partition.empty:
        brand_counts = target_partition.groupby('brand_etl').size()
        logger.info(f"  Records per brand:")
        for brand, count in brand_counts.items():
            logger.info(f"    brand_etl={brand}: {count:,} records")
else:
    logger.info("Table is empty")

# Test different delete syntaxes
logger.info(f"\n{'='*60}")
logger.info("Testing delete syntaxes...")
logger.info(f"{'='*60}")

test_syntaxes = [
    f"snapshot_date == '{snapshot_date}'",
    f"snapshot_date = '{snapshot_date}'",
    f"snapshot_date == date('{snapshot_date}')",
    f"snapshot_date = date('{snapshot_date}')",
]

for i, syntax in enumerate(test_syntaxes, 1):
    logger.info(f"\nTest {i}: {syntax}")
    try:
        # Don't actually delete, just test the syntax by scanning
        result = table.scan(row_filter=syntax).to_pandas()
        logger.success(f"✅ Syntax valid! Would delete {len(result):,} records")
    except Exception as e:
        logger.error(f"❌ Syntax invalid: {e}")

logger.info(f"\n{'='*60}")
logger.info("Recommendation:")
logger.info("Use the syntax that returned valid results above")
logger.info(f"{'='*60}")
