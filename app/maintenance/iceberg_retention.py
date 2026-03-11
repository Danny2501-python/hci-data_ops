
import os
import sys
import requests
import json
import time
from datetime import datetime, date, timedelta
from typing import List
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import LessThan
from loguru import logger

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.utils import get_current_timestamp_naive

# S3 / MinIO config (Required for physical file access via Iceberg)
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Trino Configuration
TRINO_HOST = os.getenv('TRINO_HOST', 'trino.lakehouse.com')
TRINO_PORT = os.getenv('TRINO_PORT', '80')
TRINO_USER = os.getenv('TRINO_USER', 'admin')

class IcebergRetention:
    """
    Iceberg Retention Policy Enforcement
    Deletes data older than X days and performs cleanup.
    """
    
    
    def __init__(self, retention_days: int = 3, 
                 target_uri: str = None, 
                 catalog_name: str = None,
                 namespace: str = None):
        self.retention_days = retention_days
        self.target_uri = target_uri or 'http://iceberg-rest-bronze-zone:8181'
        self.catalog_name = catalog_name or 'bronze'
        self.namespace = namespace or 'caresoft'
        
        if not self.target_uri or not self.catalog_name:
             raise ValueError("Iceberg REST URI and Catalog Name must be provided either via arguments or environment variables.")

        # Initialize catalog
        catalog_config = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": AWS_REGION,
        }
        
        self.catalog = load_catalog(
            self.catalog_name,
            uri=self.target_uri,
            **catalog_config
        )
        
        logger.info(f"Initialized IcebergRetention (Retention: {self.retention_days} days)")
        logger.info(f"Target Catalog: {self.catalog_name} at {self.target_uri}")
        logger.info(f"Trino Connection: http://{TRINO_HOST}:{TRINO_PORT} (User: {TRINO_USER})")
        
    
    def get_tables(self, namespace: str = None) -> List[tuple]:
        """List all tables in the given namespace"""
        target_ns = namespace or self.namespace
        try:
            # Returns list of identifiers (tuples) e.g., [('caresoft', 'table1'), ...]
            identifiers = self.catalog.list_tables(target_ns)
            return identifiers
        except Exception as e:
            logger.error(f"Failed to list tables in namespace {namespace}: {e}")
            return []

    def run_trino_query(self, sql: str, session_props: dict = None):
        """Execute SQL query via Trino REST API"""
        url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/statement"
        headers = {
            "X-Trino-User": TRINO_USER,
        }
        
        # Add session properties if provided
        if session_props:
            # Format: 'key=value,key2=value2'
            session_str = ",".join([f"{k}={v}" for k, v in session_props.items()])
            headers["X-Trino-Session"] = session_str
        
        try:
            # Submit query
            response = requests.post(url, data=sql, headers=headers)
            response.raise_for_status()
            
            # Get query status link
            next_uri = response.json().get('nextUri')
            
            # Poll for completion
            while next_uri:
                res = requests.get(next_uri, headers=headers)
                res.raise_for_status()
                data = res.json()
                
                # Check for errors
                if 'error' in data:
                    raise Exception(f"Trino Query Error: {data['error']['message']}")
                
                next_uri = data.get('nextUri')
                # Wait a bit to avoid hammering during polling
                time.sleep(0.5)
                
            # Rate limiting after successful query
            logger.info("Sleeping 2s to respect Trino API rate limits...")
            time.sleep(2)
            
            return True
        except Exception as e:
            logger.error(f"Trino API Execution Failed: {e}")
            return False

    def process_table(self, identifier: tuple):
        """
        Apply retention policy to a single table
        1. Delete data where snapshot_date < cutoff (via PyIceberg)
        2. Expire snapshots & Remove orphan files (via Trino)
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
            
            logger.info(f"Applying retention for {table_name}: deleting data before {cutoff_date}")
            
            # 1. Delete Query (Row-level delete - Logical)
            # This creates a delete file, effectively verifying the data is "gone" from query view
            try:
                # Comparison: snapshot_date < cutoff_date
                # Note: Assuming snapshot_date column is DateType
                delete_filter = LessThan("snapshot_date", cutoff_date)
                table.delete(delete_filter=delete_filter)
                logger.success(f"✅ Deleted old data (Logical) from {table_name}")
            except Exception as e:
                logger.error(f"❌ Failed to delete data from {table_name}: {e}")
                
            # 2. Expire Snapshots & Optimize (Physical Cleanup via Trino)
            # We use Trino because PyIceberg currently lacks full maintenance support
            try:
                # Construct full table name for Trino (catalog.schema.table)
                # Ensure catalog name matches Trino's catalog name (usually 'iceberg' or same as self.catalog_name)
                # Here we assume self.catalog_name maps correctly to Trino catalog
                trino_table_name = f"{self.catalog_name}.{identifier[0]}.{identifier[1]}"
                
                # A. Expire Snapshots
                retention_threshold = f"'{self.retention_days}d'"
                expire_sql = f"ALTER TABLE {trino_table_name} EXECUTE expire_snapshots(retention_threshold => {retention_threshold})"
                logger.info(f"Executing Trino: {expire_sql}")
                
                if self.run_trino_query(expire_sql, session_props={f'{self.catalog_name}.expire_snapshots_min_retention': '0d'}):
                    logger.success(f"✅ Expired snapshots via Trino for {table_name}")
                
                # B. Remove Orphan Files (Optional but good for cleanup)
                orphan_sql = f"ALTER TABLE {trino_table_name} EXECUTE remove_orphan_files(retention_threshold => '{self.retention_days + 1}d')"
                logger.info(f"Executing Trino: {orphan_sql}")
                
                if self.run_trino_query(orphan_sql, session_props={f'{self.catalog_name}.remove_orphan_files_min_retention': '0d'}):
                    logger.success(f"✅ Removed orphan files via Trino for {table_name}")

            except Exception as e:
                logger.error(f"⚠️ Physical cleanup failed for {table_name}: {e}")
                
        except Exception as e:
            logger.error(f"❌ Failed to process table {identifier}: {e}")

    def run(self, namespace: str = None):
        """Run retention for all tables in namespace"""
        target_ns = namespace or self.namespace
        logger.info("="*80)
        logger.info(f"Starting Iceberg Retention Cleanup")
        logger.info(f"Retention: {self.retention_days} days")
        logger.info(f"Namespace: {target_ns}")
        logger.info(f"Trino Catalog: {self.catalog_name}")
        logger.info("="*80)
        
        identifiers = self.get_tables(target_ns)
        logger.info(f"Found {len(identifiers)} tables in namespace '{target_ns}'")
        
        for identifier in identifiers:
            self.process_table(identifier)
            time.sleep(1) # Extra buffer between tables
            
        logger.success("✅ Iceberg Retention Cleanup Completed")

if __name__ == "__main__":
    # Args: 1=retention_days, 2=target_uri, 3=catalog_name, 4=namespace
    retention_days = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1] else 3
    target_uri = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None
    catalog_name = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else None
    namespace = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else "caresoft"
    
    logger.info(f"Arguments: Days={retention_days}, URI={target_uri}, Catalog={catalog_name}, Namespace={namespace}")
    
    try:
        job = IcebergRetention(retention_days, target_uri, catalog_name, namespace)
        job.run()
    except Exception as e:
        logger.error(f"Retention job failed: {e}")
        sys.exit(1)
