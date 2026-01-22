"""
Bronze Layer Ingestion for ETL Staging SCI
Replaces: etl_staging_sci.py run_etl_staging_sci()

Architecture:
- Read metadata from Iceberg Gold (etl_metadata.table_config)
- Pull data from source DBs (ERP, GiaoViec, etc.)
- Write to Iceberg Bronze layer
- Support incremental and full load
"""

import os
import sys
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, TimestampType, DateType, LongType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from sqlalchemy import create_engine, text
from loguru import logger
import hashlib
import gc

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.etl_table_config import get_active_table_configs

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Source database connections (from environment)
SOURCE_DB_CONNECTIONS = {
    '10.10.10.154': os.getenv('ERP_SCI_URI'),      # ERP
    '10.10.10.153': os.getenv('GIAOVIEC_URI'),     # GiaoViec
    '10.10.10.234': os.getenv('SEEDING_URI'),      # Seeding
    '10.10.10.214': os.getenv('CUSTOM_DB_URI'),    # Custom DB
    '118.70.128.33': os.getenv('HONGHA_URI'),      # HongHa
    '10.10.10.226': os.getenv('DU_PHONG_URI'),     # Du Phong
    '10.10.10.152': os.getenv('DEV_SCI_URI'),      # Dev SCI
    '10.10.10.249': os.getenv('TEST_SCI_URI'),     # Test SCI
    '10.10.10.230': os.getenv('PORTAL_GUIDE_URI'), # Portal Guide
}


class BronzeIngestion:
    """
    Bronze layer ingestion from source databases
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))
        
        # Initialize catalogs
        catalog_config = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": AWS_REGION,
        }
        
        self.bronze_catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            uri=ICEBERG_REST_URI_BRONZE,
            **catalog_config
        )
        
        self.gold_catalog = load_catalog(
            CATALOG_NAME_GOLD,
            uri=ICEBERG_REST_URI_GOLD,
            **catalog_config
        )
        
        logger.info(f"Initialized BronzeIngestion for {self.snapshot_date}")
    
    def get_table_configs(self, table_id: Optional[int] = None) -> List[Dict]:
        """
        Read table configurations using common module
        """
        configs = get_active_table_configs(table_id=table_id)
        logger.info(f"Loaded {len(configs)} table configurations")
        return configs
    
    def create_source_connection(self, server: str):
        """
        Create SQLAlchemy engine for source database
        """
        uri = SOURCE_DB_CONNECTIONS.get(server)
        
        if not uri:
            logger.warning(f"No connection configured for server {server}")
            return None
        
        # Fix postgres:// to postgresql://
        if uri.startswith('postgres://'):
            uri = uri.replace('postgres://', 'postgresql://', 1)
        
        engine = create_engine(uri)
        logger.info(f"Connected to {server}")
        return engine
    
    def ensure_bronze_table_exists(self, table_name: str, source_df: pd.DataFrame):
        """
        Create Bronze table if not exists
        Dynamic schema based on source data
        """
        bronze_table_name = f'staging_sci.{table_name}_bronze'
        
        try:
            table = self.bronze_catalog.load_table(bronze_table_name)
            logger.info(f"Table {bronze_table_name} exists")
            return table
        except:
            logger.info(f"Creating table {bronze_table_name}...")
            
            # Build schema from source DataFrame
            fields = []
            field_id = 1
            
            # Add source columns
            for col in source_df.columns:
                dtype = source_df[col].dtype
                
                # Map pandas dtype to Iceberg type
                if pd.api.types.is_integer_dtype(dtype):
                    iceberg_type = LongType()
                elif pd.api.types.is_float_dtype(dtype):
                    iceberg_type = StringType()  # Store as string to avoid precision issues
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    iceberg_type = TimestampType()
                else:
                    iceberg_type = StringType()
                
                fields.append(NestedField(field_id, col, iceberg_type, required=False))
                field_id += 1
            
            # Add metadata columns
            fields.extend([
                NestedField(field_id, "snapshot_date", DateType(), required=True),
                NestedField(field_id + 1, "ingestion_timestamp", TimestampType(), required=True),
                NestedField(field_id + 2, "source_server", StringType(), required=False),
            ])
            
            schema = Schema(*fields)
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=field_id,  # snapshot_date field
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            table = self.bronze_catalog.create_table(
                identifier=bronze_table_name,
                schema=schema,
                partition_spec=partition_spec
            )
            
            logger.success(f"✅ Created table {bronze_table_name}")
            return table
    
    def fetch_data_from_source(
        self, 
        engine, 
        config: Dict,
        days_ago: int = 10
    ) -> pd.DataFrame:
        """
        Fetch data from source database
        Supports incremental and full load
        """
        table_source = config['table_name_source']
        keyword = config['keyword']
        consider = config['consider'] or '1=1'
        
        # Build WHERE clause
        if keyword and keyword.strip():
            # Incremental load
            cutoff_date = self.snapshot_date - timedelta(days=days_ago)
            cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
            
            where_clause = f"""
                date({keyword}) + interval '7 hours' >= '{cutoff_date_str}'
                AND {consider}
            """
        else:
            # Full load
            where_clause = consider
        
        # Build query
        query = text(f"""
            SELECT * 
            FROM {table_source}
            WHERE {where_clause}
            ORDER BY {config['primary_keyword'] or 'id'}
        """)
        
        logger.info(f"Fetching data from {table_source}...")
        
        try:
            df = pd.read_sql(query, engine)
            logger.info(f"Fetched {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            raise
    
    def transform_data(self, df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """
        Apply transformations:
        - Column renaming
        - Type conversions
        """
        # Rename columns if configured
        if config['rename_column']:
            try:
                column_mapping = json.loads(config['rename_column'])
                df = df.rename(columns=column_mapping)
                logger.info(f"Renamed columns: {column_mapping}")
            except Exception as e:
                logger.warning(f"Failed to rename columns: {e}")
        
        # Convert float to int where appropriate
        for col in df.select_dtypes(include=['float']).columns:
            has_decimal = df[col].dropna().apply(
                lambda x: isinstance(x, float) and not x.is_integer()
            ).any()
            
            if not has_decimal:
                df[col] = df[col].astype(pd.Int64Dtype())
        
        return df
    
    def write_to_bronze(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        config: Dict
    ):
        """
        Write data to Bronze Iceberg table
        """
        if df.empty:
            logger.warning(f"No data to write for {table_name}")
            return
        
        # Add metadata columns
        df['snapshot_date'] = self.snapshot_date
        df['ingestion_timestamp'] = datetime.now()
        df['source_server'] = config['server']
        
        # Ensure table exists
        table = self.ensure_bronze_table_exists(table_name, df)
        
        # Delete old partition if truncate mode
        if config['truncate']:
            try:
                logger.info(f"Truncating partition for {self.snapshot_date}...")
                table.delete(
                    delete_filter=EqualTo("snapshot_date", self.snapshot_date)
                )
            except Exception as e:
                logger.warning(f"Could not delete partition: {e}")
        
        # Convert to PyArrow and write
        try:
            pyarrow_schema = schema_to_pyarrow(table.schema())
            arrow_table = pa.Table.from_pandas(df, schema=pyarrow_schema)
            
            table.append(arrow_table)
            logger.success(f"✅ Wrote {len(df)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to write to Bronze: {e}")
            raise
    
    def process_table(self, config: Dict, days_ago: int = 10):
        """
        Process a single table configuration
        """
        table_name = config['table_name']
        server = config['server']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing table: {table_name}")
        logger.info(f"Source: {config['source']} ({server})")
        
        try:
            # Create source connection
            engine = self.create_source_connection(server)
            if not engine:
                logger.error(f"Skipping {table_name} - no connection")
                return
            
            # Fetch data
            df = self.fetch_data_from_source(engine, config, days_ago)
            
            # Transform
            df = self.transform_data(df, config)
            
            # Write to Bronze
            self.write_to_bronze(df, table_name, config)
            
            # Cleanup
            engine.dispose()
            del df
            gc.collect()
            
            logger.success(f"✅ Completed {table_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed {table_name}: {e}")
            raise
    
    def run(self, table_id: Optional[int] = None, days_ago: int = 10):
        """
        Execute Bronze ingestion for all configured tables
        """
        logger.info("="*80)
        logger.info(f"Starting Bronze Ingestion for {self.snapshot_date}")
        logger.info("="*80)
        
        # Get table configurations
        configs = self.get_table_configs(table_id)
        
        if not configs:
            logger.warning("No table configurations found")
            return
        
        # Process each table
        for config in configs:
            try:
                self.process_table(config, days_ago)
            except Exception as e:
                logger.error(f"Failed to process table {config['table_name']}: {e}")
                # Continue with next table
        
        logger.success("✅ Bronze ingestion completed")


def main():
    """Main execution function"""
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    # Optional: table_id for single table processing
    table_id = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    logger.info(f"Starting Bronze Ingestion for {snapshot_date}")
    
    try:
        job = BronzeIngestion(snapshot_date)
        job.run(table_id=table_id, days_ago=10)
        
        logger.success("✅ Bronze ingestion completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Bronze ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
