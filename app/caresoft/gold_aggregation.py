"""
Gold Layer Aggregation: Silver → Gold
Aggregate deal data into business-ready metrics using PyIceberg
Reads from Silver zone, writes to Gold zone
"""

import os
import sys
from datetime import datetime, date
import pandas as pd
from pyiceberg.catalog import load_catalog
from loguru import logger

# Configuration from environment variables
# Silver zone - Source
ICEBERG_REST_URI_SILVER = os.getenv('ICEBERG_REST_URI_SILVER', 'http://iceberg-rest-silver-zone:8181')
CATALOG_NAME_SILVER = os.getenv('CATALOG_NAME_SILVER', 'silver')
SILVER_TABLE = os.getenv('SILVER_TABLE', 'caresoft.deals')

# Gold zone - Target
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD', 'http://iceberg-rest-gold-zone:8181')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD', 'gold')
GOLD_METRICS_TABLE = os.getenv('GOLD_METRICS_TABLE', 'caresoft.daily_deal_metrics')
GOLD_SUMMARY_TABLE = os.getenv('GOLD_SUMMARY_TABLE', 'caresoft.account_daily_summary')

S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://minio:9000')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')


class GoldAggregation:
    """Aggregate Silver deal data into Gold metrics"""
    
    def __init__(self, snapshot_date: date = None):
        """
        Initialize Gold Aggregation
        
        Args:
            snapshot_date: Date for snapshot (default: today)
        """
        self.snapshot_date = snapshot_date or date.today()
        
        logger.info(f"Initialized GoldAggregation for snapshot_date: {self.snapshot_date}")
        
        # Initialize Iceberg catalogs (Silver for read, Gold for write)
        self._init_iceberg_catalogs()
    
    def _init_iceberg_catalogs(self):
        """Initialize Silver and Gold Iceberg catalogs"""
        # Silver catalog (source)
        self.silver_catalog = load_catalog(
            CATALOG_NAME_SILVER,
            **{
                "uri": ICEBERG_REST_URI_SILVER,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
            }
        )
        logger.info(f"Silver catalog '{CATALOG_NAME_SILVER}' initialized")
        
        # Gold catalog (target)
        self.gold_catalog = load_catalog(
            CATALOG_NAME_GOLD,
            **{
                "uri": ICEBERG_REST_URI_GOLD,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
            }
        )
        logger.info(f"Gold catalog '{CATALOG_NAME_GOLD}' initialized")
    
    def read_from_silver(self) -> pd.DataFrame:
        """
        Read data from Silver Iceberg table for specific snapshot_date
        
        Returns:
            DataFrame with Silver data
        """
        logger.info(f"Reading from Silver table: {SILVER_TABLE}")
        
        try:
            table = self.silver_catalog.load_table(SILVER_TABLE)
            
            # Read data and filter by snapshot_date
            df = table.scan(
                row_filter=f"snapshot_date = '{self.snapshot_date}'"
            ).to_pandas()
            
            record_count = len(df)
            logger.info(f"Read {record_count} records from Silver layer")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from Silver: {str(e)}")
            raise
    
    def aggregate_daily_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate daily deal metrics by dimensions
        
        Args:
            df: Silver DataFrame
        
        Returns:
            DataFrame with aggregated metrics
        """
        logger.info("Aggregating daily deal metrics")
        
        try:
            # Group by dimensions and aggregate
            df_metrics = df.groupby([
                'snapshot_date',
                'account_id',
                'stage',
                'status',
                'activity_source'
            ]).agg({
                'deal_id': 'count',  # total_deals
                'deal_value': ['sum', 'mean', 'min', 'max'],  # value metrics
            }).reset_index()
            
            # Flatten column names
            df_metrics.columns = [
                'snapshot_date', 'account_id', 'stage', 'status', 'activity_source',
                'total_deals', 'total_deal_value', 'avg_deal_value', 
                'min_deal_value', 'max_deal_value'
            ]
            
            # Count new vs updated deals
            new_deals = df[df['action'] == 'create'].groupby([
                'snapshot_date', 'account_id', 'stage', 'status', 'activity_source'
            ]).size().reset_index(name='new_deals')
            
            updated_deals = df[df['action'] == 'update'].groupby([
                'snapshot_date', 'account_id', 'stage', 'status', 'activity_source'
            ]).size().reset_index(name='updated_deals')
            
            # Merge counts
            df_metrics = df_metrics.merge(
                new_deals, 
                on=['snapshot_date', 'account_id', 'stage', 'status', 'activity_source'],
                how='left'
            )
            df_metrics = df_metrics.merge(
                updated_deals,
                on=['snapshot_date', 'account_id', 'stage', 'status', 'activity_source'],
                how='left'
            )
            
            # Fill NaN with 0
            df_metrics['new_deals'] = df_metrics['new_deals'].fillna(0).astype(int)
            df_metrics['updated_deals'] = df_metrics['updated_deals'].fillna(0).astype(int)
            
            # Add metadata
            df_metrics['processing_timestamp'] = datetime.now()
            df_metrics['record_count'] = df_metrics['total_deals']
            
            metrics_count = len(df_metrics)
            logger.info(f"Generated {metrics_count} metric records")
            
            return df_metrics
            
        except Exception as e:
            logger.error(f"Failed to aggregate metrics: {str(e)}")
            raise
    
    def aggregate_account_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate account-level daily summary
        
        Args:
            df: Silver DataFrame
        
        Returns:
            DataFrame with account summaries
        """
        logger.info("Aggregating account daily summary")
        
        try:
            # Basic aggregations
            df_summary = df.groupby([
                'snapshot_date',
                'account_id'
            ]).agg({
                'deal_id': 'count',  # total_deals
                'deal_value': ['sum', 'mean'],  # value metrics
            }).reset_index()
            
            # Flatten column names
            df_summary.columns = [
                'snapshot_date', 'account_id',
                'total_deals', 'total_value', 'avg_value'
            ]
            
            # Add processing timestamp
            df_summary['processing_timestamp'] = datetime.now()
            
            summary_count = len(df_summary)
            logger.info(f"Generated {summary_count} account summary records")
            
            return df_summary
            
        except Exception as e:
            logger.error(f"Failed to aggregate account summary: {str(e)}")
            raise
    
    def write_to_gold_metrics(self, df: pd.DataFrame):
        """
        Write metrics DataFrame to Gold Iceberg table
        
        Args:
            df: Metrics DataFrame
        """
        logger.info(f"Writing to Gold metrics table: {GOLD_METRICS_TABLE}")
        
        try:
            table = self.gold_catalog.load_table(GOLD_METRICS_TABLE)
            
            record_count = len(df)
            logger.info(f"Writing {record_count} metric records to Gold layer")
            
            # Append data to Iceberg table
            table.append(df)
            
            logger.info(f"Successfully wrote {record_count} records to Gold metrics table")
            
        except Exception as e:
            logger.error(f"Failed to write to Gold metrics table: {str(e)}")
            raise
    
    def write_to_gold_summary(self, df: pd.DataFrame):
        """
        Write summary DataFrame to Gold Iceberg table
        
        Args:
            df: Summary DataFrame
        """
        logger.info(f"Writing to Gold summary table: {GOLD_SUMMARY_TABLE}")
        
        try:
            table = self.gold_catalog.load_table(GOLD_SUMMARY_TABLE)
            
            record_count = len(df)
            logger.info(f"Writing {record_count} summary records to Gold layer")
            
            # Append data to Iceberg table
            table.append(df)
            
            logger.info(f"Successfully wrote {record_count} records to Gold summary table")
            
        except Exception as e:
            logger.error(f"Failed to write to Gold summary table: {str(e)}")
            raise
    
    def run(self) -> tuple:
        """
        Execute full Gold aggregation pipeline
        
        Returns:
            Tuple of (metrics_count, summary_count)
        """
        logger.info("=" * 80)
        logger.info(f"Starting Gold Aggregation for {self.snapshot_date}")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        
        try:
            # Step 1: Read from Silver
            df_silver = self.read_from_silver()
            
            if df_silver.empty:
                logger.warning("No data to aggregate")
                return 0, 0
            
            # Step 2: Aggregate daily metrics
            df_metrics = self.aggregate_daily_metrics(df_silver)
            
            # Step 3: Aggregate account summary
            df_summary = self.aggregate_account_summary(df_silver)
            
            # Step 4: Write to Gold tables
            self.write_to_gold_metrics(df_metrics)
            self.write_to_gold_summary(df_summary)
            
            # Calculate metrics
            metrics_count = len(df_metrics)
            summary_count = len(df_summary)
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.info(f"Gold Aggregation completed successfully")
            logger.info(f"Metrics records: {metrics_count}")
            logger.info(f"Summary records: {summary_count}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("=" * 80)
            
            return metrics_count, summary_count
            
        except Exception as e:
            logger.error(f"Gold Aggregation failed: {str(e)}")
            raise


def main():
    """Main execution function"""
    # Parse command line arguments
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    logger.info(f"Starting Gold Aggregation for {snapshot_date}")
    
    try:
        # Run Gold aggregation
        aggregation = GoldAggregation(snapshot_date)
        metrics_count, summary_count = aggregation.run()
        
        logger.success(f"✅ Gold aggregation completed: {metrics_count} metrics, {summary_count} summaries")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Gold aggregation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
