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
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And, AlwaysTrue
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, TimestampType, DateType, LongType, 
    BooleanType, DoubleType, IntegerType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine.url import URL
from sqlalchemy import types as sa_types
from loguru import logger
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
    '10.10.10.230': os.getenv('PORTAL_GUIDE_CONFIG'),
}

# ERP Sources: map to Environment Variables containing credentials (URIs)
ERP_SOURCES = {
    'sci': {'env': 'ERP_SCI_URI', 'brand_etl': 7},
    'kn':  {'env': 'ERP_KN_URI',  'brand_etl': 1},
    'pr':  {'env': 'ERP_PR_URI',  'brand_etl': 3},
    'hh':  {'env': 'ERP_HH_URI',  'brand_etl': 4},
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
    
    def get_table_configs(self, table_id: Optional[int] = None, source_type: Optional[str] = None) -> List[Dict]:
        """
        Read table configurations using common module
        """
        configs = get_active_table_configs(table_id=table_id, source=source_type)
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
    
    def _map_type_to_iceberg(self, sa_type):
        """Map SQLAlchemy generic types to Iceberg types"""
        # Handle standard types
        if isinstance(sa_type, (sa_types.Boolean, sa_types.Boolean)):
            return BooleanType()
        
        if isinstance(sa_type, (sa_types.Integer, sa_types.SmallInteger)):
            return IntegerType()
            
        if isinstance(sa_type, sa_types.BigInteger):
            return LongType()
            
        if isinstance(sa_type, (sa_types.DateTime, sa_types.TIMESTAMP)):
            return TimestampType()
            
        if isinstance(sa_type, sa_types.Date):
            return DateType()
            
        if isinstance(sa_type, (sa_types.Float, sa_types.Numeric, sa_types.DECIMAL, sa_types.REAL)):
            # Warning: Using DoubleType for floats/numeric. 
            # Previous logic used String for precision, but user wants matching types.
            return DoubleType()
            
        # Default fallback
        return StringType()

    def ensure_bronze_table_exists(self, table_name: str, engine, config: Dict):
        """
        Create Bronze table if not exists
        Dynamic schema based on source database inspection
        """
        bronze_table_name = f'call_functions.{table_name}'
        
        try:
            table = self.bronze_catalog.load_table(bronze_table_name)
            logger.info(f"Table {bronze_table_name} exists")
            self._check_schema_evolution(table, engine, config) # Optional: Check for new columns
            return table
        except:
            logger.info(f"Creating table {bronze_table_name}...")
            
            # Inspect source schema
            insp = inspect(engine)
            source_table = config['table_name_source']
            schema_name = None
            t_name = source_table
            
            if '.' in source_table:
                schema_name, t_name = source_table.split('.', 1)
                
            try:
                columns = insp.get_columns(t_name, schema=schema_name)
            except Exception as e:
                logger.error(f"Failed to inspect table {source_table}: {e}")
                raise

            # Get rename mapping
            rename_map = {}
            if config.get('rename_column'):
                try:
                    rename_map = json.loads(config['rename_column'])
                except:
                    pass
            
            # Build fields
            fields = []
            field_id = 1
            
            for col in columns:
                col_name = col['name']
                col_type = col['type']
                
                # Apply rename
                final_name = rename_map.get(col_name, col_name)
                
                # Map type
                try:
                    iceberg_type = self._map_type_to_iceberg(col_type)
                except Exception as e:
                    logger.warning(f"Type mapping failed for {col_name} ({col_type}): {e}. Defaulting to String.")
                    iceberg_type = StringType()
                
                fields.append(NestedField(field_id, final_name, iceberg_type, required=False))
                field_id += 1
            
            # Add metadata columns
            fields.extend([
                NestedField(field_id, "snapshot_date", DateType(), required=True),
                NestedField(field_id + 1, "ingestion_timestamp", TimestampType(), required=True),
                NestedField(field_id + 2, "source_server", StringType(), required=False),
                NestedField(field_id + 3, "brand_etl", IntegerType(), required=False),
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
            
    def _check_schema_evolution(self, table, engine, config):
        """Basic check if we need to add columns (optional implementation)"""
        pass
    
    def stream_and_ingest(
        self, 
        engine, 
        table,
        config: Dict,
        days_ago: int = 10,
        load_type: str = 'incremental',
        brand_etl: int = None,
        source_name: str = None
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
                            srv_val = source_name if source_name else config['server']
                            arrow_arrays.append(pa.array([srv_val] * batch_size, type=pa.string()))
                            continue
                        
                        if field_name == 'brand_etl':
                            arrow_arrays.append(pa.array([brand_etl] * batch_size, type=pa.int32()))
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
                            
                            # Sanitize data to fix PyArrow timezone issues and type mismatches
                            # Problem 1: Postgres returns "+07:00" offset which PyArrow doesn't recognize as a valid timezone name
                            # Problem 2: Target schema might be String but data is Datetime
                            
                            cleaned_data = None
                            first_val = next((x for x in data if x is not None), None)
                            
                            if isinstance(first_val, (datetime, date)):
                                cleaned_data = []
                                # Determine if we need string conversion
                                target_is_string = pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
                                
                                for item in data:
                                    if item is None:
                                        cleaned_data.append(None)
                                        continue
                                        
                                    val = item
                                    # Handle datetime/date objects
                                    if isinstance(val, datetime):
                                        # Remove timezone info (make naive) to satisfy PyArrow
                                        val = val.replace(tzinfo=None)
                                    
                                    # Convert to string if target requires it
                                    if target_is_string:
                                        cleaned_data.append(str(val))
                                    else:
                                        cleaned_data.append(val)
                                        
                                data = cleaned_data

                            try:
                                arrow_arrays.append(pa.array(data, type=field.type))
                            except Exception:
                                # Fallback: let PyArrow infer then cast
                                try:
                                    arrow_arrays.append(pa.array(data).cast(field.type))
                                except Exception as e:
                                    logger.error(f"Failed to convert column {field_name} (Source: {source_field_name}): {e}")
                                    # Last resort: append nulls to avoid crushing the whole batch
                                    arrow_arrays.append(pa.array([None] * batch_size, type=field.type))
                        else:
                            # Column missing in source or not mapped
                            # logger.warning(f"Column {field_name} not found in source")
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
    
        except Exception as e:
            logger.error(f"❌ Failed {table_name}: {e}")
            raise

    def process_table_standard(self, config: Dict, table, days_ago: int, load_type: str):
         """Process standard 1-to-1 table"""
         server = config['server']
         engine = self.create_source_connection(server)
         if not engine:
             logger.error(f"Skipping {config['table_name']} - no connection to {server}")
             return 0
         
         try:
             total = self.stream_and_ingest(engine, table, config, days_ago, load_type)
             return total
         finally:
             engine.dispose()
             gc.collect()

    def process_table_erp(self, config: Dict, table, days_ago: int, load_type: str):
        """Process ERP table (1-to-4 sources) using direct URI connections"""
        total_all = 0
        from sqlalchemy.engine.url import make_url

        for name, info in ERP_SOURCES.items():
            env_var = info['env']
            brand_etl = info['brand_etl']
            uri = os.getenv(env_var)
            
            if not uri:
                logger.warning(f"Skipping {name}: Environment variable {env_var} not set")
                continue

            logger.info(f"--- Processing ERP source: {name.upper()} ---")
            
            # Fix SQLAlchemy dialect if needed
            if uri.startswith("postgres://"):
                uri = uri.replace("postgres://", "postgresql://", 1)
                
            try:
                engine = create_engine(uri)
                recs = self.stream_and_ingest(
                    engine, table, config, days_ago, load_type, 
                    brand_etl=brand_etl, source_name=name
                )
                total_all += recs
                logger.info(f"Ingested {recs} from {name}")
                
            except Exception as e:
                logger.error(f"Failed source {name}: {e}")
            finally:
                try:
                    engine.dispose()
                except:
                    pass
                
        return total_all


    def process_table(self, config: Dict, days_ago: int = 10, load_type: str = 'incremental'):
        """
        Process a single table configuration
        """
        table_name = config['table_name']
        source_type = config.get('source')
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing table: {table_name}")
        logger.info(f"Source: {source_type} - Mode: {load_type}")
        
        try:
            # 1. Determine Sample Engine for Schema
            # For ERP, we force use 'sci' (ERP_SCI_URI) as reference schema
            if source_type == 'ERP':
                 # Create temp engine just for schema inspection
                 sample_uri = os.getenv('ERP_SCI_URI')
                 if sample_uri and sample_uri.startswith('postgres://'):
                     sample_uri = sample_uri.replace('postgres://', 'postgresql://', 1)
                 engine = create_engine(sample_uri) if sample_uri else None
                 if not engine:
                     logger.error("Missing ERP_SCI_URI for schema inspection")
                     return
            else:
                 engine = self.create_source_connection(config['server'])
            if not engine:
                logger.error(f"Skipping {table_name} - no connection to sample server {sample_server}")
                return

            # 2. Ensure Table Exists
            table = self.ensure_bronze_table_exists(table_name, engine, config)
            engine.dispose() # Close sample connection
            
            # 3. Cleanup Data
            try:
                if config.get('truncate'):
                    logger.info(f"Truncating ENTIRE table data...")
                    table.delete(delete_filter=AlwaysTrue())
                else:
                    logger.info(f"Deleting data for snapshot_date {self.snapshot_date}...")
                    table.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
            except Exception as e:
                logger.warning(f"Could not delete data: {e}")

            # 4. Ingest Data (Fork logic)
            if source_type == 'ERP':
                total_records = self.process_table_erp(config, table, days_ago, load_type)
            else:
                total_records = self.process_table_standard(config, table, days_ago, load_type)
            
            logger.success(f"✅ Completed {table_name} - {total_records} records")
            
        except Exception as e:
            logger.error(f"❌ Failed {table_name}: {e}")
            raise
    
    def run(self, table_id: Optional[int] = None, days_ago: int = 10, load_type: str = 'incremental', source_type: str = None):
        """
        Execute Bronze ingestion for all configured tables
        """
        logger.info("="*80)
        logger.info(f"Starting Bronze Ingestion (Source: {source_type})")
        logger.info("="*80)
        
        # Get table configurations with filter
        configs = self.get_table_configs(table_id, source_type)
        
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
    
    # Optional: source_type (e.g. 'ERP')
    source_type = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else None
    
    logger.info(f"Starting Bronze Ingestion")
    logger.info(f"Arguments -> Date: {snapshot_date}, ID: {table_id}, Type: {load_type}, Source: {source_type}")
    
    try:
        job = BronzeIngestion(snapshot_date)
        job.run(table_id=table_id, days_ago=10, load_type=load_type, source_type=source_type)
        
        logger.success("✅ Bronze ingestion completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ Bronze ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
