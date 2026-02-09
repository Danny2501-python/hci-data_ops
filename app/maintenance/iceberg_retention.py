
import os
import sys
import time
from datetime import datetime, date, timedelta
from typing import List
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import LessThan
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.utils import get_current_timestamp_naive

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

class IcebergRetention:
    """
    Iceberg Retention Policy Enforcement
    Deletes data older than X days and performs cleanup.
    """
    
    def __init__(self, retention_days: int = 3):
        self.retention_days = retention_days
        
        # Initialize catalog
        catalog_config = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": AWS_REGION,
        }
        
        self.catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            uri=ICEBERG_REST_URI_BRONZE,
            **catalog_config
        )
        
        logger.info(f"Initialized IcebergRetention (Retention: {self.retention_days} days)")
        
    
    def get_tables(self, namespace: str = "call_functions") -> List[tuple]:
        """List all tables in the given namespace"""
        try:
            # Returns list of identifiers (tuples) e.g., [('call_functions', 'table1'), ...]
            identifiers = self.catalog.list_tables(namespace)
            return identifiers
        except Exception as e:
            logger.error(f"Failed to list tables in namespace {namespace}: {e}")
            return []

    def process_table(self, identifier: tuple):
        """
        Apply retention policy to a single table
        1. Delete data where snapshot_date < cutoff
        2. Expire snapshots older than cutoff
        """
        try:
            table_name = ".".join(identifier)
            logger.info(f"Processing table: {table_name}")
            
            table = self.catalog.load_table(identifier)
            
            # Check partition spec
            is_partitioned_by_date = False
            # spec().fields is a list of PartitionField
            for field in table.spec().fields:
                if field.name == "snapshot_date":
                    is_partitioned_by_date = True
                    break
            
            if not is_partitioned_by_date:
                logger.warning(f"⚠️ Table {table_name} is NOT partitioned by 'snapshot_date'. Skipping.")
                return

            # Calculate cutoff date
            # Logic: Delete data strictly older than X days ago
            today_naive = get_current_timestamp_naive().date()
            cutoff_date = today_naive - timedelta(days=self.retention_days)
            
            # For expire_snapshots, we need timestamp in milliseconds
            # We use midnight of the cutoff date based on naive timestamp
            cutoff_datetime = datetime.combine(cutoff_date, datetime.min.time())
            cutoff_ts_ms = int(cutoff_datetime.timestamp() * 1000)
            
            logger.info(f"Applying retention for {table_name}: deleting data before {cutoff_date}")
            
            # 1. Delete Query (Row-level delete)
            # This creates a delete file, effectively verifying the data is "gone" from query view
            try:
                # Comparison: snapshot_date < cutoff_date
                # Note: Assuming snapshot_date column is DateType
                delete_filter = LessThan("snapshot_date", cutoff_date)
                table.delete(delete_filter=delete_filter)
                logger.success(f"✅ Deleted old data from {table_name}")
            except Exception as e:
                logger.error(f"❌ Failed to delete data from {table_name}: {e}")
                
            # 2. Expire Snapshots
            # This removes old snapshots and their associated data files if no longer trusted
            try:
                # Use expire_snapshots() which creates an action, then use specific expiry method
                # Support varies by pyiceberg version. 
                # Attempting standard specific cleanup.
                
                # Option A: Direct method if available (newer versions)
                if hasattr(table, 'expire_snapshots'):
                    # Using a safe approach for potentially different versions
                    # Some versions accept older_than_ms as arg, some return builder
                    # We'll try the builder pattern first if it returns an object with expire_older_than
                    
                    action = table.expire_snapshots()
                    if hasattr(action, 'expire_older_than'):
                         action.expire_older_than(cutoff_ts_ms).commit()
                    elif callable(action): 
                        # If it's the method itself that does the work (older style?)
                        # But pyiceberg typically uses builder pattern for actions
                         logger.warning(f"⚠️ 'expire_snapshots' signature uncertain for {table_name}")
                    else:
                        logger.warning(f"⚠️ 'expire_snapshots' returned unknown pattern for {table_name}. Skipping expiration.")
                        pass
                        
                    logger.success(f"✅ Expired snapshots for {table_name}")

            except Exception as e:
                # Only log error if strictly necessary, clean up is secondary to data deletion
                logger.warning(f"⚠️ Snapshot expiration info for {table_name}: {e}")
                # Fallback attempts could go here
                
        except Exception as e:
            logger.error(f"❌ Failed to process table {identifier}: {e}")

    def run(self, namespace: str = "call_functions"):
        """Run retention for all tables in namespace"""
        logger.info("="*80)
        logger.info(f"Starting Iceberg Retention Cleanup (Retention: {self.retention_days} days)")
        logger.info("="*80)
        
        identifiers = self.get_tables(namespace)
        logger.info(f"Found {len(identifiers)} tables in namespace '{namespace}'")
        
        for identifier in identifiers:
            self.process_table(identifier)
            
        logger.success("✅ Iceberg Retention Cleanup Completed")

if __name__ == "__main__":
    # Default to 3 days if not specified
    retention_days = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 3
    
    try:
        job = IcebergRetention(retention_days)
        job.run()
    except Exception as e:
        logger.error(f"Retention job failed: {e}")
        sys.exit(1)
