"""
Silver Layer Transformation: Bronze → Silver
Parse JSON webhooks and transform to structured deal data using PyIceberg
Reads from Bronze zone, writes to Silver zone
"""

import os
import sys
from datetime import datetime, date
import pandas as pd
import json
import ast
from pyiceberg.catalog import load_catalog
from loguru import logger

# Configuration from environment variables
# Bronze zone - Source
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE', 'http://iceberg-rest-bronze-zone:8181')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE', 'bronze')
BRONZE_TABLE = os.getenv('BRONZE_TABLE', 'caresoft.webhook_snapshots')

# Silver zone - Target
ICEBERG_REST_URI_SILVER = os.getenv('ICEBERG_REST_URI_SILVER', 'http://iceberg-rest-silver-zone:8181')
CATALOG_NAME_SILVER = os.getenv('CATALOG_NAME_SILVER', 'silver')
SILVER_TABLE = os.getenv('SILVER_TABLE', 'caresoft.deals')

S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://minio:9000')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')


class SilverTransformation:
    """Transform Bronze webhook data to Silver structured deal data"""
    
    def __init__(self, snapshot_date: date = None):
        """
        Initialize Silver Transformation
        
        Args:
            snapshot_date: Date for snapshot (default: today)
        """
        self.snapshot_date = snapshot_date or date.today()
        
        logger.info(f"Initialized SilverTransformation for snapshot_date: {self.snapshot_date}")
        
        # Initialize Iceberg catalogs (Bronze for read, Silver for write)
        self._init_iceberg_catalogs()
    
    def _init_iceberg_catalogs(self):
        """Initialize Bronze and Silver Iceberg catalogs"""
        # Bronze catalog (source)
        self.bronze_catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            **{
                "uri": ICEBERG_REST_URI_BRONZE,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
            }
        )
        logger.info(f"Bronze catalog '{CATALOG_NAME_BRONZE}' initialized")
        
        # Silver catalog (target)
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
    
    def read_from_bronze(self) -> pd.DataFrame:
        """
        Read data from Bronze Iceberg table for specific snapshot_date
        
        Returns:
            DataFrame with Bronze data
        """
        logger.info(f"Reading from Bronze table: {BRONZE_TABLE}")
        
        try:
            table = self.bronze_catalog.load_table(BRONZE_TABLE)
            
            # Read data and filter by snapshot_date
            df = table.scan(
                row_filter=f"snapshot_date = '{self.snapshot_date}'"
            ).to_pandas()
            
            record_count = len(df)
            logger.info(f"Read {record_count} records from Bronze layer")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from Bronze: {str(e)}")
            raise
    
    def parse_json_field(self, json_str: str, field_path: str, default=None):
        """
        Safely extract field from JSON string
        
        Args:
            json_str: JSON string
            field_path: Dot-separated path (e.g., 'deal.deal_id')
            default: Default value if field not found
        
        Returns:
            Extracted value or default
        """
        try:
            data = ast.literal_eval(json_str) if isinstance(json_str, str) else json_str
            json_data = json.loads(json.dumps(data))
            
            # Navigate through nested structure
            value = json_data
            for key in field_path.split('.'):
                value = value.get(key, {})
                if value == {}:
                    return default
            
            return value if value != {} else default
            
        except Exception:
            return default
    
    def parse_json_webhooks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse JSON input field and extract deal information
        
        Args:
            df: Bronze DataFrame with JSON input field
        
        Returns:
            DataFrame with parsed deal fields
        """
        logger.info("Parsing JSON webhooks")
        
        try:
            # Extract fields from JSON
            df_parsed = pd.DataFrame({
                'pk_id': df['pk_id'],
                'webhook_id': df['id'],
                'snapshot_date': df['snapshot_date'],
                'ingestion_timestamp': df['ingestion_timestamp'],
                
                # Extract deal information
                'deal_id': df['input'].apply(lambda x: self.parse_json_field(x, 'deal.deal_id')),
                'account_id': df['input'].apply(lambda x: str(self.parse_json_field(x, 'account_id', ''))),
                'activity_source': df['input'].apply(lambda x: str(self.parse_json_field(x, 'activity_source', ''))),
                'action': df['input'].apply(lambda x: str(self.parse_json_field(x, 'action', ''))),
                'object': df['input'].apply(lambda x: str(self.parse_json_field(x, 'object', ''))),
                'convert_from_object': df['input'].apply(lambda x: str(self.parse_json_field(x, 'convert_from_object', ''))),
                
                # Deal details
                'stage': df['input'].apply(lambda x: str(self.parse_json_field(x, 'deal.stage', ''))),
                'status': df['input'].apply(lambda x: str(self.parse_json_field(x, 'deal.status', ''))),
                'deal_name': df['input'].apply(lambda x: str(self.parse_json_field(x, 'deal.deal_name', ''))),
                'deal_value': df['input'].apply(lambda x: self.parse_json_field(x, 'deal.deal_value', 0)),
                'currency': df['input'].apply(lambda x: str(self.parse_json_field(x, 'deal.currency', ''))),
                
                # Timestamps - Convert from milliseconds
                'created_at': df['input'].apply(
                    lambda x: pd.to_datetime(self.parse_json_field(x, 'deal.created_at', 0), unit='ms', errors='coerce')
                ),
                'updated_at': df['input'].apply(
                    lambda x: pd.to_datetime(self.parse_json_field(x, 'deal.updated_at', 0), unit='ms', errors='coerce')
                ),
                'last_change_status_at': df['input'].apply(
                    lambda x: pd.to_datetime(self.parse_json_field(x, 'deal.last_change_status_at', 0), unit='ms', errors='coerce')
                ),
                'last_change_stage_at': df['input'].apply(
                    lambda x: pd.to_datetime(self.parse_json_field(x, 'deal.last_change_stage_at', 0), unit='ms', errors='coerce')
                ),
                
                # Complex fields - Keep as JSON strings
                'activity': df['input'].apply(lambda x: str(self.parse_json_field(x, 'deal.activity', ''))),
                'addition_fields': df['input'].apply(lambda x: str(self.parse_json_field(x, 'deal.addition_fields', ''))),
                'call_info': df['input'].apply(lambda x: str(self.parse_json_field(x, 'call_info', ''))),
                'agent_submit': df['input'].apply(lambda x: str(self.parse_json_field(x, 'agent_submit', ''))),
                'updated_properties': df['input'].apply(lambda x: str(self.parse_json_field(x, 'updated_properties', ''))),
            })
            
            # Add processing timestamp
            df_parsed['processing_timestamp'] = datetime.now()
            
            # Convert data types
            df_parsed['deal_value'] = pd.to_numeric(df_parsed['deal_value'], errors='coerce')
            
            parsed_count = len(df_parsed)
            logger.info(f"Successfully parsed {parsed_count} records")
            
            return df_parsed
            
        except Exception as e:
            logger.error(f"Failed to parse JSON: {str(e)}")
            raise
    
    def apply_data_quality_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply data quality checks and filters
        
        Args:
            df: Parsed DataFrame
        
        Returns:
            DataFrame with quality checks applied
        """
        logger.info("Applying data quality checks")
        
        initial_count = len(df)
        
        # Filter out records with null deal_id (invalid deals)
        df_clean = df[df['deal_id'].notna()].copy()
        
        final_count = len(df_clean)
        filtered_count = initial_count - final_count
        
        if filtered_count > 0:
            logger.warning(f"Filtered out {filtered_count} invalid records")
        
        logger.info(f"Data quality checks passed: {final_count} valid records")
        
        return df_clean
    
    def write_to_silver(self, df: pd.DataFrame):
        """
        Write DataFrame to Silver Iceberg table
        
        Args:
            df: DataFrame to write
        """
        logger.info(f"Writing to Silver table: {SILVER_TABLE}")
        
        try:
            table = self.silver_catalog.load_table(SILVER_TABLE)
            
            record_count = len(df)
            logger.info(f"Writing {record_count} records to Silver layer")
            
            # Append data to Iceberg table
            table.append(df)
            
            logger.info(f"Successfully wrote {record_count} records to Silver layer")
            
        except Exception as e:
            logger.error(f"Failed to write to Silver table: {str(e)}")
            raise
    
    def run(self) -> int:
        """
        Execute full Silver transformation pipeline
        
        Returns:
            Number of records transformed
        """
        logger.info("=" * 80)
        logger.info(f"Starting Silver Transformation for {self.snapshot_date}")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        
        try:
            # Step 1: Read from Bronze
            df_bronze = self.read_from_bronze()
            
            if df_bronze.empty:
                logger.warning("No data to transform")
                return 0
            
            # Step 2: Parse JSON webhooks
            df_parsed = self.parse_json_webhooks(df_bronze)
            
            # Step 3: Apply data quality checks
            df_clean = self.apply_data_quality_checks(df_parsed)
            
            if df_clean.empty:
                logger.warning("No valid data after quality checks")
                return 0
            
            # Step 4: Write to Silver
            self.write_to_silver(df_clean)
            
            # Calculate metrics
            record_count = len(df_clean)
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.info(f"Silver Transformation completed successfully")
            logger.info(f"Records transformed: {record_count}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("=" * 80)
            
            return record_count
            
        except Exception as e:
            logger.error(f"Silver Transformation failed: {str(e)}")
            raise


def main():
    """Main execution function"""
    # Parse command line arguments
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    logger.info(f"Starting Silver Transformation for {snapshot_date}")
    
    try:
        # Run Silver transformation
        transformation = SilverTransformation(snapshot_date)
        record_count = transformation.run()
        
        logger.success(f"✅ Silver transformation completed: {record_count} records")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Silver transformation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
