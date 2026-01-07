"""
ERP Direct Ingestion: ERP api_log → Iceberg Bronze
Read directly from 4 ERP systems (SCI, KN, HH, PR) and ingest to Iceberg
Connections are passed from Airflow DAG as environment variables
"""

import os
import sys
from datetime import datetime, date
import pandas as pd
from pyiceberg.catalog import load_catalog
from sqlalchemy import create_engine
from loguru import logger
import hashlib
import json

# Iceberg configuration - Read from common_env_vars
ICEBERG_REST_URI = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME = os.getenv('CATALOG_NAME_BRONZE')
TABLE_NAME = os.getenv('BRONZE_TABLE', 'caresoft.wehook_api_caresoft')  # Match PostgreSQL naming

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# ERP connection configs (passed from Airflow as URIs)
ERP_CONFIGS = {
    'sci': {
        'uri': os.getenv('ERP_SCI_URI'),
        'brand_etl': 7  # SCI brand ID
    },
    'kn': {
        'uri': os.getenv('ERP_KN_URI'),
        'brand_etl': 1  # KN brand ID
    },
    # TEMPORARILY DISABLED: Network connectivity issue from Kubernetes to 10.10.10.237
    # 'pr': {
    #     'uri': os.getenv('ERP_PR_URI'),
    #     'brand_etl': 3  # PR brand ID
    # },
    'hh': {
        'uri': os.getenv('ERP_HH_URI'),
        'brand_etl': 4  # HH brand ID
    }
}


class ERPDirectIngestion:
    """Ingest data directly from ERP api_log tables to Bronze Iceberg layer"""
    
    def __init__(self, snapshot_date: date = None, incremental: bool = True):
        """
        Initialize ERP Direct Ingestion
        
        Args:
            snapshot_date: Date for snapshot (default: today)
            incremental: If True, only load new/updated records since last run
        """
        self.snapshot_date = snapshot_date or date.today()
        self.incremental = incremental
        self.source_table = "api_log"
        
        logger.info(f"Initialized ERPDirectIngestion for snapshot_date: {self.snapshot_date}")
        logger.info(f"Incremental mode: {self.incremental}")
        
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
    
    def create_erp_connection(self, erp_name: str):
        """
        Create SQLAlchemy engine for ERP connection
        
        Args:
            erp_name: Name of ERP system (sci, kn, pr, hh)
        
        Returns:
            SQLAlchemy engine
        """
        config = ERP_CONFIGS.get(erp_name)
        
        if not config or not config.get('uri'):
            logger.warning(f"ERP {erp_name} not configured or URI missing, skipping")
            return None
        
        try:
            # Get URI and fix for SQLAlchemy 2.x compatibility
            uri = config['uri']
            
            # SQLAlchemy 2.x requires 'postgresql://' instead of 'postgres://'
            if uri and uri.startswith('postgres://'):
                uri = uri.replace('postgres://', 'postgresql://', 1)
                logger.debug(f"Fixed URI for SQLAlchemy 2.x: postgres:// → postgresql://")
            
            # Create engine from URI
            engine = create_engine(uri)
            logger.info(f"Created connection to ERP {erp_name.upper()}")
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create connection to ERP {erp_name.upper()}: {str(e)}")
            return None
    
    def generate_pk_id(self, row):
        """
        Generate unique PK_ID from row data
        Uses hash of key fields to ensure uniqueness
        
        Args:
            row: DataFrame row
        
        Returns:
            Integer PK_ID
        """
        # Combine key fields to create unique identifier
        key_string = f"{row['id']}_{row['brand_etl']}_{row.get('create_date', '')}_{row.get('write_date', '')}"
        
        # Generate hash and convert to integer
        hash_object = hashlib.md5(key_string.encode())
        pk_id = int(hash_object.hexdigest()[:15], 16)  # Use first 15 hex chars
        
        return pk_id
    
    def get_last_write_date(self, brand_etl: int):
        """
        Get the last write_date from Bronze table for specific brand
        Used for incremental loading
        
        Args:
            brand_etl: Brand ETL ID
        
        Returns:
            Last write_date or None
        """
        if not self.incremental:
            return None
        
        try:
            table = self.catalog.load_table(TABLE_NAME)
            
            # Query for max write_date for this brand
            df = table.scan(
                row_filter=f"brand_etl = {brand_etl}"
            ).to_pandas()
            
            if df.empty:
                logger.info(f"No previous data found for brand {brand_etl}, will do full load")
                return None
            
            last_date = df['write_date'].max()
            logger.info(f"Last write_date for brand {brand_etl}: {last_date}")
            
            return last_date
            
        except Exception as e:
            logger.warning(f"Could not get last write_date for brand {brand_etl}: {str(e)}")
            return None
    
    def read_from_erp(self, erp_name: str, engine) -> pd.DataFrame:
        """
        Read data from ERP api_log table
        
        Args:
            erp_name: Name of ERP system
            engine: SQLAlchemy engine
        
        Returns:
            DataFrame with api_log data
        """
        brand_etl = ERP_CONFIGS[erp_name]['brand_etl']
        
        logger.info(f"Reading from ERP {erp_name.upper()} api_log table (brand_etl={brand_etl})")
        
        # Build query based on incremental mode
        if self.incremental:
            last_date = self.get_last_write_date(brand_etl)
            
            if last_date:
                # Incremental: only new/updated records
                query = f"""
                    SELECT 
                        id,
                        name,
                        create_uid,
                        create_date,
                        write_uid,
                        write_date,
                        type_log,
                        model_id,
                        id_record,
                        input,
                        response,
                        status_code,
                        url,
                        header
                    FROM {self.source_table}
                    WHERE write_date > '{last_date}'
                    ORDER BY write_date ASC
                """
                logger.info(f"Incremental load: fetching records after {last_date}")
            else:
                # First run: load all
                query = f"""
                    SELECT 
                        id,
                        name,
                        create_uid,
                        create_date,
                        write_uid,
                        write_date,
                        type_log,
                        model_id,
                        id_record,
                        input,
                        response,
                        status_code,
                        url,
                        header
                    FROM {self.source_table}
                    ORDER BY write_date ASC
                """
                logger.info("First run: loading all records")
        else:
            # Full load: all records
            query = f"""
                SELECT 
                    id,
                    name,
                    create_uid,
                    create_date,
                    write_uid,
                    write_date,
                    type_log,
                    model_id,
                    id_record,
                    input,
                    response,
                    status_code,
                    url,
                    header
                FROM {self.source_table}
                ORDER BY write_date ASC
            """
            logger.info("Full load mode: loading all records")
        
        try:
            df = pd.read_sql(query, engine)
            
            # Add brand_etl column
            df['brand_etl'] = brand_etl
            
            record_count = len(df)
            logger.info(f"Successfully read {record_count} records from ERP {erp_name.upper()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from ERP {erp_name.upper()}: {str(e)}")
            raise
    
    def add_metadata_and_pk(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add PK_ID and metadata columns
        
        Args:
            df: Source DataFrame
        
        Returns:
            DataFrame with PK_ID and metadata columns added
        """
        if df.empty:
            return df
        
        logger.info("Adding PK_ID and metadata columns")
        
        # Generate PK_ID for each row
        df['pk_id'] = df.apply(self.generate_pk_id, axis=1)
        
        # Add metadata
        df['snapshot_date'] = self.snapshot_date
        df['ingestion_timestamp'] = datetime.now()
        
        # Ensure correct data types
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.date
        df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'])
        
        # Convert timestamp columns
        if 'create_date' in df.columns:
            df['create_date'] = pd.to_datetime(df['create_date'], errors='coerce')
        if 'write_date' in df.columns:
            df['write_date'] = pd.to_datetime(df['write_date'], errors='coerce')
        
        logger.info(f"Generated PK_IDs for {len(df)} records")
        
        return df
    
    def write_to_bronze(self, df: pd.DataFrame):
        """
        Write DataFrame to Bronze Iceberg table
        Auto-creates table if it doesn't exist
        
        Args:
            df: DataFrame to write
        """
        if df.empty:
            logger.warning("No data to write")
            return
        
        logger.info(f"Writing to Bronze table: {TABLE_NAME}")
        
        try:
            # Try to load existing table
            try:
                table = self.catalog.load_table(TABLE_NAME)
                logger.info(f"Table {TABLE_NAME} exists, appending data")
            except Exception as e:
                # Table doesn't exist, create it
                logger.warning(f"Table {TABLE_NAME} not found, creating new table")
                logger.info(f"Error was: {str(e)}")
                
                from pyiceberg.schema import Schema
                from pyiceberg.types import (
                    NestedField, LongType, IntegerType, StringType,
                    TimestampType, DateType
                )
                from pyiceberg.partitioning import PartitionSpec, PartitionField
                from pyiceberg.transforms import IdentityTransform
                
                # Define schema matching DataFrame
                schema = Schema(
                    NestedField(1, "pk_id", LongType(), required=False),
                    NestedField(2, "id", IntegerType(), required=False),
                    NestedField(3, "name", StringType(), required=False),
                    NestedField(4, "create_uid", IntegerType(), required=False),
                    NestedField(5, "create_date", TimestampType(), required=False),
                    NestedField(6, "write_uid", IntegerType(), required=False),
                    NestedField(7, "write_date", TimestampType(), required=False),
                    NestedField(8, "type_log", StringType(), required=False),
                    NestedField(9, "model_id", IntegerType(), required=False),
                    NestedField(10, "id_record", IntegerType(), required=False),
                    NestedField(11, "input", StringType(), required=False),
                    NestedField(12, "response", StringType(), required=False),
                    NestedField(13, "status_code", IntegerType(), required=False),
                    NestedField(14, "url", StringType(), required=False),
                    NestedField(15, "header", StringType(), required=False),
                    NestedField(16, "brand_etl", IntegerType(), required=False),
                    NestedField(17, "snapshot_date", DateType(), required=False),
                    NestedField(18, "ingestion_timestamp", TimestampType(), required=False),
                )
                
                # NOTE: PyIceberg doesn't support append/overwrite to partitioned tables
                # Partitioning is commented out until we migrate to Spark for writes
                # partition_spec = PartitionSpec(
                #     PartitionField(
                #         source_id=17,  # snapshot_date
                #         field_id=1000,
                #         transform=IdentityTransform(),
                #         name="snapshot_date"
                #     ),
                #     PartitionField(
                #         source_id=16,  # brand_etl
                #         field_id=1001,
                #         transform=IdentityTransform(),
                #         name="brand_etl"
                #     )
                # )
                
                # Create table WITHOUT partitioning
                table = self.catalog.create_table(
                    identifier=TABLE_NAME,
                    schema=schema,
                    # partition_spec=partition_spec,  # Commented out
                )
                logger.info(f"✅ Created new table: {TABLE_NAME} (unpartitioned)")
            
            record_count = len(df)
            logger.info(f"Writing {record_count} records to Bronze layer")
            
            # Convert Pandas DataFrame to PyArrow table
            import pyarrow as pa
            arrow_table = pa.Table.from_pandas(df)
            logger.debug(f"Converted DataFrame to PyArrow table: {arrow_table.schema}")
            
            # Table is now unpartitioned, so we can use append()
            # For full load, we should clear the table first
            if not self.incremental:
                logger.info("Full load mode: using overwrite() to replace all data")
                table.overwrite(arrow_table)
            else:
                logger.info("Incremental mode: using append()")
                table.append(arrow_table)
            
            logger.info(f"✅ Successfully wrote {record_count} records to Bronze layer")
            
        except Exception as e:
            logger.error(f"❌ Failed to write to Bronze table: {str(e)}")
            raise
    
    def run(self) -> dict:
        """
        Execute full ERP direct ingestion pipeline for all ERPs
        
        Returns:
            Dictionary with ingestion stats per ERP
        """
        logger.info("=" * 80)
        logger.info(f"Starting ERP Direct Ingestion for {self.snapshot_date}")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        stats = {}
        all_data = []
        
        # Process each ERP system
        for erp_name in ['sci', 'kn', 'pr', 'hh']:
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing ERP: {erp_name.upper()}")
                logger.info(f"{'='*60}")
                
                # Create connection
                engine = self.create_erp_connection(erp_name)
                
                if not engine:
                    stats[erp_name] = {'status': 'skipped', 'records': 0}
                    continue
                
                # Read from ERP
                df_erp = self.read_from_erp(erp_name, engine)
                
                if df_erp.empty:
                    logger.info(f"No data from ERP {erp_name.upper()}")
                    stats[erp_name] = {'status': 'success', 'records': 0}
                    continue
                
                # Add to combined data
                all_data.append(df_erp)
                
                stats[erp_name] = {
                    'status': 'success',
                    'records': len(df_erp)
                }
                
                logger.info(f"✅ ERP {erp_name.upper()}: {len(df_erp)} records")
                
                # Close connection
                engine.dispose()
                
            except Exception as e:
                logger.error(f"❌ Failed to process ERP {erp_name.upper()}: {str(e)}")
                stats[erp_name] = {'status': 'failed', 'records': 0, 'error': str(e)}
        
        # Combine all data
        if all_data:
            logger.info(f"\n{'='*60}")
            logger.info("Combining data from all ERPs")
            logger.info(f"{'='*60}")
            
            df_combined = pd.concat(all_data, ignore_index=True)
            
            # Add metadata and PK
            df_bronze = self.add_metadata_and_pk(df_combined)
            
            # Write to Bronze
            self.write_to_bronze(df_bronze)
            
            total_records = len(df_bronze)
        else:
            logger.warning("No data from any ERP system")
            total_records = 0
        
        # Calculate metrics
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"ERP Direct Ingestion completed")
        logger.info(f"Total records ingested: {total_records}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"\nStats by ERP:")
        for erp, stat in stats.items():
            logger.info(f"  {erp.upper()}: {stat}")
        logger.info("=" * 80)
        
        return stats


def main():
    """Main execution function"""
    # Parse command line arguments
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    incremental = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else True
    
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    logger.info(f"Starting ERP Direct Ingestion for {snapshot_date}")
    logger.info(f"Incremental mode: {incremental}")
    
    try:
        # Run ERP direct ingestion
        ingestion = ERPDirectIngestion(snapshot_date, incremental)
        stats = ingestion.run()
        
        # Check if any ERP failed
        failed_erps = [erp for erp, stat in stats.items() if stat['status'] == 'failed']
        
        if failed_erps:
            logger.warning(f"⚠️  Some ERPs failed: {failed_erps}")
            logger.success(f"✅ ERP direct ingestion completed with warnings")
            sys.exit(0)  # Still exit 0 if at least one ERP succeeded
        else:
            total_records = sum(stat['records'] for stat in stats.values())
            logger.success(f"✅ ERP direct ingestion completed: {total_records} total records")
            sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ ERP direct ingestion failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
