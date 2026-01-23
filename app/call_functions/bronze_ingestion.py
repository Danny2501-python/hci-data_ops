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
from sqlalchemy.engine.url import URL
from loguru import logger
import hashlib
import gc

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.etl_table_config import get_active_table_configs
from common.utils import get_current_timestamp_naive

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
    '10.10.10.154': os.getenv('ERP_SCI_CONFIG'),
    '10.10.10.153': os.getenv('GIAOVIEC_CONFIG'),
    '10.10.10.234': os.getenv('SEEDING_CONFIG'),
    '10.10.10.214': os.getenv('CUSTOM_DB_CONFIG'),
    '118.70.128.33': os.getenv('HONGHA_CONFIG'),
    '10.10.10.226': os.getenv('DU_PHONG_CONFIG'),
    '10.10.10.152': os.getenv('DEV_SCI_CONFIG'),
    '10.10.10.249': os.getenv('TEST_SCI_CONFIG'),
    '10.10.10.230': os.getenv('PORTAL_GUIDE_CONFIG'),
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
        Expected config format: {"user": "", "password": "", "host": "", "port": "", "dbname": ""}
        """
        config_json = SOURCE_DB_CONNECTIONS.get(server)
        
        if not config_json:
            logger.warning(f"No connection configured for server {server}")
            return None
        
        try:
            config = json.loads(config_json)
            
            # Construct URL safely using SQLAlchemy
            url_obj = URL.create(
                "postgresql",
                username=config.get('user'),
                password=config.get('password'),
                host=config.get('host'),
                port=int(config.get('port', 5432)),
                database=config.get('dbname')
            )
            
            engine = create_engine(url_obj)
            logger.info(f"Connected to {server} ({config.get('dbname')})")
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create connection for {server}: {e}")
            return None
    
    def ensure_bronze_table_exists(self, table_name: str, source_df: pd.DataFrame):
        """
        Create Bronze table if not exists
        Dynamic schema based on source data
        """
        bronze_table_name = f'call_functions.{table_name}'
        
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
    
    def stream_and_ingest(
        self, 
        engine, 
        table,
        config: Dict,
        days_ago: int = 10,
        load_type: str = 'incremental'
    ) -> int:
        """
        Stream data from SQL -> PyArrow -> Iceberg (Bypassing Pandas)
        Memory efficient processing for large datasets.
        """
        table_source = config['table_name_source']
        consider = config.get('consider') or '1=1'
        
        # Build WHERE clause (reused logic)
        keyword = config.get('keyword')
        if load_type == 'incremental':
            if keyword and str(keyword).strip() and str(keyword).lower() != 'none':
                cutoff_date = self.snapshot_date - timedelta(days=days_ago)
                cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
                # Use interval addition for robust timezone handling (UTC+7)
                # This avoids 'timezone not found' errors if the DB lacks full timezone data
                where_clause = f"({keyword} + interval '7 hour')::date >= '{cutoff_date_str}' AND {consider}"
            else:
                where_clause = consider
        else:
            where_clause = consider
            
        # Build query
        query_str = f"SELECT * FROM {table_source} WHERE {where_clause}"
        logger.info(f"Generated Query: {query_str}")
        query = text(query_str)
        
        BATCH_SIZE = 50000
        total_ingested = 0
        
        logger.info(f"Starting STREAM ingestion for {config['table_name']}")
        
        try:
             # Get columns first to handle mapping and types
            with engine.connect() as conn:
                # Get one row to determine schema and columns
                limit_query = text(f"SELECT * FROM {table_source} WHERE {where_clause} LIMIT 0")
                result_proxy = conn.execute(limit_query)
                source_columns = result_proxy.keys()
                
            # Prepare column mapping
            rename_map = {}
            if config['rename_column']:
                try:
                    rename_map = json.loads(config['rename_column'])
                except Exception as e:
                    logger.warning(f"Failed to parse rename_column: {e}")

            # Define target schema based on Iceberg table
            target_schema = schema_to_pyarrow(table.schema())
            
            with engine.connect().execution_options(stream_results=True) as conn:
                result = conn.execute(query)
                
                while True:
                    rows = result.fetchmany(BATCH_SIZE)
                    if not rows:
                        break
                        
                    batch_size = len(rows)
                    logger.info(f"Processing batch of {batch_size} records...")
                    
                    # Convert to PyArrow Table directly from rows
                    # We create a dictionary of arrays
                    columns_data = list(zip(*rows))
                    arrow_arrays = []
                    
                    # Iterate through target schema fields to build arrays
                    # This logic aligns source columns with target schema
                    
                    # Create a map of source col name -> data index
                    col_to_idx = {name: i for i, name in enumerate(source_columns)}
                    
                    for field in target_schema:
                        field_name = field.name
                        
                        # Handle metadata columns
                        if field_name == 'snapshot_date':
                            arrow_arrays.append(pa.array([self.snapshot_date] * batch_size, type=pa.date32()))
                            continue
                        if field_name == 'ingestion_timestamp':
                            arrow_arrays.append(pa.array([get_current_timestamp_naive()] * batch_size, type=pa.timestamp('us')))
                            continue
                        if field_name == 'source_server':
                            arrow_arrays.append(pa.array([config['server']] * batch_size, type=pa.string()))
                            continue
                            
                        # Find source column name (reverse default mapping)
                        # We need to find which source column maps to this target field
                        # If renamed: target = rename_map[source] -> source?
                        # Inefficient to search every time, but ok for now. 
                        # Better: compute source_field_name for each target field once outside loop
                        
                        source_field_name = None
                        
                        # Check direct match
                        if field_name in source_columns:
                            source_field_name = field_name
                        
                        # Check reverse mapping
                        # rename_map is {source: target}
                        if not source_field_name:
                            for s, t in rename_map.items():
                                if t == field_name and s in source_columns:
                                    source_field_name = s
                                    break
                                    
                        if source_field_name and source_field_name in col_to_idx:
                            idx = col_to_idx[source_field_name]
                            data = columns_data[idx]
                            
                            # Handle type conversion (float -> int if needed)
                            # PyArrow handles most, but we might need explicit checks if logic requires
                            # For simplicity getting PyArrow to infer or casting
                            
                            try:
                                arrow_arrays.append(pa.array(data, type=field.type))
                            except Exception:
                                # Fallback: let PyArrow infer then cast
                                arrow_arrays.append(pa.array(data).cast(field.type))
                        else:
                            # Configured 100% dynamic schema? 
                            # If ensuring table exists created the table based on source_df, then names usually match.
                            # If manual override, might be null.
                            arrow_arrays.append(pa.array([None] * batch_size, type=field.type))

                    batch_table = pa.Table.from_arrays(arrow_arrays, schema=target_schema)
                    
                    # Append to Iceberg
                    table.append(batch_table)
                    total_ingested += batch_size
                    
                    del rows
                    del columns_data
                    del batch_table
                    gc.collect()
                    
            return total_ingested
            
        except Exception as e:
            logger.error(f"Stream ingestion failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def process_table(self, config: Dict, days_ago: int = 10, load_type: str = 'incremental'):
        """
        Process a single table configuration
        """
        table_name = config['table_name']
        server = config['server']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing table: {table_name}")
        logger.info(f"Source: {config['source']} ({server}) - Mode: {load_type}")
        
        try:
            # Create source connection
            engine = self.create_source_connection(server)
            if not engine:
                logger.error(f"Skipping {table_name} - no connection")
                return
            
            # 1. Ensure Table Exists & Get Schema (using empty query)
            # Fetch 0 rows to infer schema efficiently
            query_schema = text(f"SELECT * FROM {config['table_name_source']} WHERE 1=0")
            df_schema = pd.read_sql(query_schema, engine)
            
            # Apply renaming to schema-df
            if config['rename_column']:
                try:
                    mapping = json.loads(config['rename_column'])
                    df_schema = df_schema.rename(columns=mapping)
                except Exception as e:
                    logger.warning(f"Failed to prepare schema renaming: {e}")

            table = self.ensure_bronze_table_exists(table_name, df_schema)

            # 2. Truncate Partition if needed (Logic moved from write_to_bronze)
            if config['truncate']:
                try:
                    logger.info(f"Truncating partition for {self.snapshot_date}...")
                    table.delete(
                        delete_filter=EqualTo("snapshot_date", self.snapshot_date)
                    )
                except Exception as e:
                    logger.warning(f"Could not delete partition: {e}")

            # 3. Stream Data (replaces fetch -> transform -> write)
            total_records = self.stream_and_ingest(
                engine, 
                table, 
                config, 
                days_ago, 
                load_type
            )
            
            # Cleanup
            engine.dispose()
            del df_schema
            gc.collect()
            
            logger.success(f"✅ Completed {table_name} - {total_records} records")
            
        except Exception as e:
            logger.error(f"❌ Failed {table_name}: {e}")
            raise
    
    def run(self, table_id: Optional[int] = None, days_ago: int = 10, load_type: str = 'incremental'):
        """
        Execute Bronze ingestion for all configured tables
        """
        logger.info("="*80)
        logger.info(f"Starting Bronze Ingestion for {self.snapshot_date} (Mode: {load_type})")
        logger.info("="*80)
        
        # Get table configurations
        configs = self.get_table_configs(table_id)
        
        if not configs:
            logger.warning("No table configurations found")
            return
        
        # Process each table
        for config in configs:
            try:
                self.process_table(config, days_ago, load_type)
            except Exception as e:
                logger.error(f"Failed to process table {config['table_name']}: {e}")
                # Continue with next table
        
        logger.success("✅ Bronze ingestion completed")


def main():
    """Main execution function"""
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))
    
    # Optional: table_id for single table processing
    table_id = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else None

    logger.info(f"Raw Arguments: {sys.argv}")
    
    # Optional: load_type
    load_type = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else 'incremental'
    load_type = load_type.lower().strip()
    
    logger.info(f"Starting Bronze Ingestion")
    logger.info(f"Arguments -> Snapshot Date: {snapshot_date}, Table ID: {table_id}, Load Type: {load_type}")
    
    try:
        job = BronzeIngestion(snapshot_date)
        job.run(table_id=table_id, days_ago=10, load_type=load_type)
        
        logger.success("✅ Bronze ingestion completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Bronze ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
