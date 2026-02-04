"""
Ty Gia Techcombank Ingestion to Bronze Iceberg Layer
Fetches exchange rate data from Techcombank API and ingests into Iceberg Bronze tables
"""

import os
import sys
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import pandas as pd
import requests
import time
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, FloatType, 
    DoubleType, DateType, TimestampType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.expressions import EqualTo, AlwaysTrue
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


class TyGiaTechcombankIngestion:
    """
    Ingest Techcombank exchange rate data to Iceberg Bronze Layer
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        
        # Initialize Bronze catalog
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
        
        logger.info(f"Initialized TyGiaTechcombankIngestion for {self.snapshot_date}")
    
    def get_api_url(self, ngay: str = '') -> str:
        """
        Generate Techcombank API URL for a specific date
        
        Args:
            ngay: Date string in format YYYY-MM-DD
        
        Returns:
            API URL string
        """
        url = f'https://techcombank.com/content/techcombank/web/vn/vi/cong-cu-tien-ich/ty-gia/_jcr_content.exchange-rates.{ngay}.integration.json'
        return url
    
    def fetch_exchange_rates(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Fetch exchange rate data from Techcombank API for a date range
        
        Args:
            start_date: Start date for fetching data
            end_date: End date for fetching data
        
        Returns:
            DataFrame containing exchange rate data
        """
        calls_data = []
        current_date = start_date
        
        logger.info(f"Fetching exchange rates from {start_date} to {end_date}")
        
        while current_date <= end_date:
            formatted_date = current_date.strftime('%Y-%m-%d')
            url = self.get_api_url(formatted_date)
            
            try:
                response = requests.get(url, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    new_calls_data = data.get('exchangeRate', {}).get('data', [])
                    
                    # Add date to each record
                    for item in new_calls_data:
                        item['date'] = formatted_date
                    
                    calls_data.extend(new_calls_data)
                    logger.info(f"✓ Fetched {len(new_calls_data)} records for {formatted_date}")
                else:
                    logger.warning(f"API request failed for {formatted_date}. Status code: {response.status_code}")
            
            except Exception as e:
                logger.error(f"Error fetching data for {formatted_date}: {e}")
            
            current_date += timedelta(days=1)
            time.sleep(0.5)  # Rate limiting
        
        if not calls_data:
            logger.warning("No data fetched from API")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(calls_data)
        
        # Filter for USD only (matching old logic)
        df = df[df['label'].str.contains('USD', case=False, na=False)]
        
        # Normalize column names to lowercase
        df.columns = df.columns.str.lower()
        
        # Log initial dtypes
        logger.info(f"Initial DataFrame dtypes:\n{df.dtypes}")
        
        # Convert numeric string columns to proper numeric types
        # API might return numbers as strings, need to convert them
        for col in df.columns:
            if col == 'date':  # Skip date column
                continue
            if col in ['label', 'code']:  # Skip known string columns
                continue
            
            # Try to convert to numeric
            try:
                # First try integer
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception:
                pass
        
        # Convert float columns to int (matching old logic)
        # Only convert if all values are whole numbers
        float_columns = df.select_dtypes(include=['float']).columns
        for col in float_columns:
            # Check if all non-null values are whole numbers
            if df[col].notna().any():
                if (df[col].dropna() % 1 == 0).all():
                    df[col] = df[col].fillna(0).astype(int)
        
        logger.info(f"Final DataFrame dtypes:\n{df.dtypes}")
        logger.info(f"Sample data (first row):\n{df.head(1).to_dict('records')}")
        logger.info(f"Processed {len(df)} USD exchange rate records")
        return df
    
    def infer_iceberg_schema(self, df: pd.DataFrame) -> Schema:
        """
        Infer Iceberg schema from pandas DataFrame
        """
        def get_iceberg_type(dtype):
            """Map pandas dtype to Iceberg type"""
            if pd.api.types.is_integer_dtype(dtype):
                # Use LongType for int64 to match PyArrow's conversion
                # pandas int64 -> PyArrow long (64-bit), not int (32-bit)
                return LongType()
            elif pd.api.types.is_float_dtype(dtype):
                return DoubleType()
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                return TimestampType()
            else:
                return StringType()
        
        # Infer fields from DataFrame columns
        fields = []
        logger.info("Schema inference - Column type mapping:")
        for i, (col_name, dtype) in enumerate(df.dtypes.items()):
            iceberg_type = get_iceberg_type(dtype)
            logger.info(f"  {col_name}: {dtype} -> {iceberg_type}")
            fields.append(NestedField(
                field_id=i + 1,
                name=col_name,
                field_type=iceberg_type,
                required=False
            ))
        
        # Add metadata columns
        next_id = len(fields) + 1
        fields.extend([
            NestedField(
                field_id=next_id,
                name="snapshot_date",
                field_type=DateType(),
                required=False
            ),
            NestedField(
                field_id=next_id + 1,
                name="ingestion_timestamp",
                field_type=TimestampType(),
                required=False
            )
        ])
        
        return Schema(*fields)
    
    def ensure_table_exists(self, table_name: str, schema: Schema):
        """
        Create Iceberg table if it doesn't exist
        """
        full_table_name = f'call_functions.{table_name}'
        
        try:
            table = self.catalog.load_table(full_table_name)
            logger.info(f"Table {full_table_name} already exists")
            return table
        except Exception:
            logger.info(f"Creating table {full_table_name}")
            
            # Create partition spec (partition by snapshot_date)
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=schema.find_field("snapshot_date").field_id,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.catalog.create_table(
                identifier=full_table_name,
                schema=schema,
                partition_spec=partition_spec
            )
            
            logger.success(f"✅ Created table {full_table_name}")
            return table
    
    def write_to_iceberg(self, table_name: str, df: pd.DataFrame, truncate: bool = False):
        """
        Write DataFrame to Iceberg table
        
        Args:
            table_name: Target table name
            df: DataFrame to write (WITHOUT metadata columns)
            truncate: If True, delete all existing data before writing
        """
        if df.empty:
            logger.warning(f"No data to write for {table_name}")
            return
        
        # Infer schema from data columns BEFORE adding metadata
        schema = self.infer_iceberg_schema(df)
        table = self.ensure_table_exists(table_name, schema)
        
        # Add metadata columns AFTER schema inference
        df['snapshot_date'] = self.snapshot_date
        df['ingestion_timestamp'] = get_current_timestamp_naive()
        
        # Convert to PyArrow Table
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        
        try:
            if truncate:
                logger.warning(f"Truncating table {table_name}")
                table.delete(delete_filter=AlwaysTrue())
            else:
                # Delete existing data for this snapshot_date only
                logger.info(f"Deleting existing data for {self.snapshot_date}")
                table.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
            
            # Append new data
            logger.info(f"Writing {len(df)} rows to {table_name}")
            table.append(arrow_table)
            
            logger.success(f"✅ Successfully wrote {len(df)} rows to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to write to Iceberg: {e}")
            raise
    
    def run(self, start_date: date, end_date: date, table_name: str = 'ty_gia_techcombank'):
        """
        Execute Ty Gia Techcombank ingestion
        
        Args:
            start_date: Start date for fetching data
            end_date: End date for fetching data
            table_name: Target Iceberg table name (default: ty_gia_techcombank)
        """
        logger.info("="*80)
        logger.info(f"Starting Ty Gia Techcombank Ingestion")
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Target Table: {table_name}")
        logger.info("="*80)
        
        try:
            # Fetch data from API
            df = self.fetch_exchange_rates(start_date, end_date)
            
            if df.empty:
                logger.warning("No data to ingest")
                return
            
            # Write to Iceberg
            self.write_to_iceberg(table_name=table_name, df=df, truncate=False)
            
            logger.success("✅ Ty Gia Techcombank ingestion completed")
            
        except Exception as e:
            logger.error(f"❌ Ty Gia Techcombank ingestion failed: {e}")
            raise


def main():
    """Main execution function"""
    # Parse command-line arguments
    # Usage: python ty_gia_techcombank_ingestion.py [snapshot_date] [start_date] [end_date] [table_name]
    
    # Argument 1: snapshot_date (YYYY-MM-DD) - for partitioning
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    # Argument 2: start_date (YYYY-MM-DD) - for API fetching
    if len(sys.argv) > 2 and sys.argv[2]:
        start_date_str = sys.argv[2]
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    else:
        # Default: 30 days ago
        start_date = date.today() - timedelta(days=30)
    
    # Argument 3: end_date (YYYY-MM-DD) - for API fetching
    if len(sys.argv) > 3 and sys.argv[3]:
        end_date_str = sys.argv[3]
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    else:
        # Default: yesterday
        end_date = date.today() - timedelta(days=1)
    
    # Argument 4: table_name (optional)
    table_name = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else 'ty_gia_techcombank'
    
    logger.info(f"Raw Arguments: {sys.argv}")
    logger.info(f"Starting Ty Gia Techcombank Ingestion")
    logger.info(f"Arguments -> Snapshot Date: {snapshot_date}, Start Date: {start_date}, End Date: {end_date}, Table: {table_name}")
    
    try:
        job = TyGiaTechcombankIngestion(snapshot_date)
        job.run(start_date=start_date, end_date=end_date, table_name=table_name)
        
        logger.success("✅ Ty Gia Techcombank ingestion completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Ty Gia Techcombank ingestion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
