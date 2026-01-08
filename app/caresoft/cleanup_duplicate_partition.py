"""
Cleanup Script: Remove duplicate data from Bronze table
Use this script to clean up duplicate data caused by running with old code
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


def cleanup_partition(snapshot_date: date):
    """
    Drop partition for specific date
    
    Args:
        snapshot_date: Date of partition to drop
    """
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
    
    logger.info(f"Dropping partition: snapshot_date={snapshot_date}")
    
    try:
        # Delete partition
        table.delete(f"snapshot_date = date('{snapshot_date}')")
        logger.success(f"✅ Successfully dropped partition snapshot_date={snapshot_date}")
        
        # Show remaining partitions
        logger.info("Querying remaining partitions...")
        df = table.scan().to_pandas()
        
        if not df.empty:
            partition_counts = df.groupby('snapshot_date').size()
            logger.info(f"\nRemaining partitions:")
            for partition_date, count in partition_counts.items():
                logger.info(f"  {partition_date}: {count:,} records")
        else:
            logger.info("Table is now empty")
            
    except Exception as e:
        logger.error(f"❌ Failed to drop partition: {e}")
        raise


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        logger.error("Usage: python cleanup_duplicate_partition.py YYYY-MM-DD")
        logger.info("Example: python cleanup_duplicate_partition.py 2026-01-08")
        sys.exit(1)
    
    snapshot_date_str = sys.argv[1]
    
    try:
        snapshot_date = date.fromisoformat(snapshot_date_str)
        logger.info(f"Cleaning up partition for date: {snapshot_date}")
        
        # Confirm before deleting
        confirm = input(f"\n⚠️  This will DELETE all data for partition snapshot_date={snapshot_date}\nAre you sure? (yes/no): ")
        
        if confirm.lower() == 'yes':
            cleanup_partition(snapshot_date)
        else:
            logger.info("Cleanup cancelled")
            
    except ValueError:
        logger.error(f"Invalid date format: {snapshot_date_str}")
        logger.info("Expected format: YYYY-MM-DD (e.g., 2026-01-08)")
        sys.exit(1)
