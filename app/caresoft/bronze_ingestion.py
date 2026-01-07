"""
Bronze Snapshot Processor: Iceberg webhook_raw → Iceberg webhook_snapshots
Create daily snapshots from raw webhook data for deal processing
This reads from caresoft.webhook_raw (populated by ERP ingestion)
All operations within Bronze zone catalog
"""

import os
import sys
from datetime import datetime, date
import pandas as pd
from pyiceberg.catalog import load_catalog
from loguru import logger

# Configuration from environment variables
# Bronze zone - Read and write within same catalog
ICEBERG_REST_URI = os.getenv('ICEBERG_REST_URI_BRONZE', 'http://iceberg-rest-bronze-zone:8181')
CATALOG_NAME = os.getenv('CATALOG_NAME_BRONZE', 'bronze')
SOURCE_TABLE = os.getenv('SOURCE_TABLE', 'caresoft.webhook_raw')
TARGET_TABLE = os.getenv('BRONZE_TABLE', 'caresoft.webhook_snapshots')

S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://minio:9000')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')


class BronzeSnapshotProcessor:
    """Process raw webhooks into daily snapshots for deal processing"""
    
    def __init__(self, snapshot_date: date = None):
        """
        Initialize Bronze Snapshot Processor
        
        Args:
            snapshot_date: Date for snapshot (default: today)
        """
        self.snapshot_date = snapshot_date or date.today()
        
        logger.info(f"Initialized BronzeSnapshotProcessor for snapshot_date: {self.snapshot_date}")
        
        # Initialize Iceberg catalog
        self._init_iceberg_catalog()
    
    def _init_iceberg_catalog(self):
        """Initialize Iceberg catalog"""
        self.catalog = load_catalog(
            CATALOG_NAME,
            **{
                "uri": ICEBERG_REST_URI,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
            }
        )
        logger.info(f"Iceberg catalog '{CATALOG_NAME}' initialized")
    
    def read_from_webhook_raw(self) -> pd.DataFrame:
        """
        Read data from Iceberg webhook_raw table
        Filter for deal-related webhooks only
        
        Returns:
            DataFrame with webhook data
        """
        logger.info(f"Reading from Iceberg table: {SOURCE_TABLE}")
        
        try:
            table = self.catalog.load_table(SOURCE_TABLE)
            
            # Read all data (can add filters if needed)
            df = table.scan().to_pandas()
            
            # Filter for deal-related webhooks
            df_deals = df[df['input'].str.contains('deal', case=False, na=False)].copy()
            
            record_count = len(df_deals)
            logger.info(f"Successfully read {record_count} deal-related records from webhook_raw")
            
            return df_deals
            
        except Exception as e:
            logger.error(f"Failed to read from webhook_raw: {str(e)}")
            raise
    
    def add_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add metadata columns for Bronze layer
        
        Args:
            df: Source DataFrame
        
        Returns:
            DataFrame with metadata columns added
        """
        logger.info("Adding metadata columns")
        
        df['snapshot_date'] = self.snapshot_date
        df['ingestion_timestamp'] = datetime.now()
        
        # Ensure correct data types
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.date
        df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'])
        
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])
        
        return df
    
    def write_to_bronze(self, df: pd.DataFrame):
        """
        Write DataFrame to Bronze snapshot table
        
        Args:
            df: DataFrame to write
        """
        logger.info(f"Writing to Bronze snapshot table: {TARGET_TABLE}")
        
        try:
            # Get or create table
            table = self.catalog.load_table(TARGET_TABLE)
            
            record_count = len(df)
            logger.info(f"Writing {record_count} records to Bronze snapshot layer")
            
            # Append data to Iceberg table
            table.append(df)
            
            logger.info(f"Successfully wrote {record_count} records to Bronze snapshot layer")
            
        except Exception as e:
            logger.error(f"Failed to write to Bronze snapshot table: {str(e)}")
            raise
    
    def run(self) -> int:
        """
        Execute full Bronze snapshot processing pipeline
        
        Returns:
            Number of records processed
        """
        logger.info("=" * 80)
        logger.info(f"Starting Bronze Snapshot Processing for {self.snapshot_date}")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        
        try:
            # Step 1: Read from webhook_raw
            df_source = self.read_from_webhook_raw()
            
            if df_source.empty:
                logger.warning("No data to process")
                return 0
            
            # Step 2: Add metadata columns
            df_bronze = self.add_metadata_columns(df_source)
            
            # Step 3: Write to Bronze snapshot table
            self.write_to_bronze(df_bronze)
            
            # Calculate metrics
            record_count = len(df_bronze)
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.info(f"Bronze Snapshot Processing completed successfully")
            logger.info(f"Records processed: {record_count}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("=" * 80)
            
            return record_count
            
        except Exception as e:
            logger.error(f"Bronze Snapshot Processing failed: {str(e)}")
            raise


def main():
    """Main execution function"""
    # Parse command line arguments
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    logger.info(f"Starting Bronze Snapshot Processing for {snapshot_date}")
    
    try:
        # Run Bronze snapshot processing
        processor = BronzeSnapshotProcessor(snapshot_date)
        record_count = processor.run()
        
        logger.success(f"✅ Bronze snapshot processing completed: {record_count} records")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Bronze snapshot processing failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
