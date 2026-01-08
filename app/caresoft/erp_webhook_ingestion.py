"""
ERP Direct Ingestion: ERP api_log → Iceberg Bronze
Read directly from 4 ERP systems (SCI, KN, HH, PR) and ingest to Iceberg
Simplified version: Always full load, drop partition if exists, then append
"""

import os
import sys
from datetime import datetime, date
import pandas as pd
from pyiceberg.catalog import load_catalog
from sqlalchemy import create_engine
from loguru import logger
import hashlib

# Iceberg configuration
ICEBERG_REST_URI = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME = os.getenv('CATALOG_NAME_BRONZE')
TABLE_NAME = os.getenv('BRONZE_TABLE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# ERP connection configs
ERP_CONFIGS = {
    'sci': {'uri': os.getenv('ERP_SCI_URI'), 'brand_etl': 7},
    'kn':  {'uri': os.getenv('ERP_KN_URI'),  'brand_etl': 1},
    'pr':  {'uri': os.getenv('ERP_PR_URI'),  'brand_etl': 3},
    'hh':  {'uri': os.getenv('ERP_HH_URI'),  'brand_etl': 4}
}


class ERPDirectIngestion:
    """Ingest data directly from ERP api_log tables to Bronze Iceberg layer"""
    
    def __init__(self, snapshot_date: date = None):
        """
        Initialize ERP Direct Ingestion
        
        Args:
            snapshot_date: Date for snapshot (default: today)
        """
        self.snapshot_date = snapshot_date or date.today()
        self.source_table = "api_log"
        
        logger.info(f"Initialized ERPDirectIngestion for snapshot_date: {self.snapshot_date}")
        
        # Initialize Iceberg catalog
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
        """Create SQLAlchemy engine for ERP connection"""
        config = ERP_CONFIGS.get(erp_name)
        
        if not config or not config.get('uri'):
            logger.warning(f"ERP {erp_name} not configured or URI missing, skipping")
            return None
        
        try:
            uri = config['uri']
            
            # SQLAlchemy 2.x requires 'postgresql://' instead of 'postgres://'
            if uri and uri.startswith('postgres://'):
                uri = uri.replace('postgres://', 'postgresql://', 1)
            
            engine = create_engine(uri)
            logger.info(f"Created connection to ERP {erp_name.upper()}")
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create connection to ERP {erp_name.upper()}: {str(e)}")
            return None

    def ensure_table_exists(self):
        """Create table if it doesn't exist"""
        try:
            table = self.catalog.load_table(TABLE_NAME)
            logger.info(f"Table {TABLE_NAME} exists")
            return table
        except:
            logger.info(f"Table {TABLE_NAME} not found, creating new table")
            
            from pyiceberg.schema import Schema
            from pyiceberg.types import (
                NestedField, LongType, IntegerType, StringType,
                TimestampType, DateType
            )
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform
            
            # Define schema
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
            
            # Partition by snapshot_date
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=17,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )
            
            # Create table
            table = self.catalog.create_table(
                identifier=TABLE_NAME,
                schema=schema,
                partition_spec=partition_spec,
            )
            logger.info(f"✅ Created new table: {TABLE_NAME} (partitioned by snapshot_date)")
            
            return table
    
    def stream_and_ingest(self, erp_name: str, engine, table) -> int:
        """
        Stream data from SQL -> PyArrow -> Iceberg (Bypassing Pandas)
        Memory efficient processing for large datasets.
        """
        import pyarrow as pa
        import gc
        from sqlalchemy import text
        
        brand_etl = ERP_CONFIGS[erp_name]['brand_etl']
        total_ingested = 0
        BATCH_SIZE = 100000  # Increased to 100k for better throughput & file size
        
        logger.info(f"Starting STREAM ingestion for ERP {erp_name.upper()} (brand_etl={brand_etl})")
        
        # 1. Define SQL Query
        query = text(f"""
            SELECT 
                id, name, create_uid, create_date, write_uid, write_date,
                type_log, model_id, id_record, input, response, 
                status_code, url, header
            FROM {self.source_table}
            ORDER BY write_date ASC
        """)
        
        # 2. Define PyArrow Schema (Explicit types)
        target_schema = pa.schema([
            ('pk_id', pa.int64()),
            ('id', pa.int32()),
            ('name', pa.string()),
            ('create_uid', pa.int32()),
            ('create_date', pa.timestamp('us')),
            ('write_uid', pa.int32()),
            ('write_date', pa.timestamp('us')),
            ('type_log', pa.string()),
            ('model_id', pa.int32()),
            ('id_record', pa.int32()),
            ('input', pa.string()),
            ('response', pa.string()),
            ('status_code', pa.int32()),
            ('url', pa.string()),
            ('header', pa.string()),
            ('brand_etl', pa.int32()),
            ('snapshot_date', pa.date32()),
            ('ingestion_timestamp', pa.timestamp('us'))
        ])
        
        try:
            # 3. Connect and Stream
            with engine.connect().execution_options(stream_results=True) as conn:
                result = conn.execute(query)
                
                while True:
                    # Fetch batch of raw tuples
                    rows = result.fetchmany(BATCH_SIZE)
                    if not rows:
                        break
                        
                    batch_size = len(rows)
                    logger.info(f"Processing batch of {batch_size} records...")
                    
                    # 4. Prepare Column Arrays (Data Oriented Design)
                    # Unzip rows into columns for vectorization (faster than row processing)
                    # rows is list of tuples: [(id, name, ...), (id, name, ...)]
                    # zip(*rows) becomes: [(id, id...), (name, name...), ...]
                    columns = list(zip(*rows))
                    
                    # Mapping SQL columns to variables based on query order
                    # id(0), name(1), create_uid(2), create_date(3), write_uid(4), write_date(5)...
                    ids = columns[0]
                    create_dates = columns[3]
                    write_dates = columns[5]
                    
                    # 5. Generate PKs (Python Optimization)
                    # Still need python loop but on simple primitives, faster than Pandas Series apply
                    pk_ids = []
                    for i in range(batch_size):
                        # Safe string conversion handling None
                        c_date = str(create_dates[i]) if create_dates[i] else ''
                        w_date = str(write_dates[i]) if write_dates[i] else ''
                        
                        key_string = f"{ids[i]}_{brand_etl}_{c_date}_{w_date}"
                        # MD5 hash
                        hash_obj = hashlib.md5(key_string.encode())
                        # Hex to Int (15 chars)
                        pk_ids.append(int(hash_obj.hexdigest()[:15], 16))
                    
                    # 6. Build PyArrow Arrays
                    # Constant arrays
                    brand_etl_arr = pa.array([brand_etl] * batch_size, type=pa.int32())
                    snapshot_date_arr = pa.array([self.snapshot_date] * batch_size, type=pa.date32())
                    ingest_time_arr = pa.array([datetime.now()] * batch_size, type=pa.timestamp('us'))
                    
                    # Construct Table directly from Arrays
                    # Be careful to match target_schema order EXACTLY
                    batch_table = pa.Table.from_arrays([
                        pa.array(pk_ids, type=pa.int64()),                      # pk_id
                        pa.array(columns[0], type=pa.int32()),                  # id
                        pa.array(columns[1], type=pa.string()),                 # name
                        pa.array(columns[2], type=pa.int32()),                  # create_uid
                        pa.array(columns[3], type=pa.timestamp('us')),          # create_date
                        pa.array(columns[4], type=pa.int32()),                  # write_uid
                        pa.array(columns[5], type=pa.timestamp('us')),          # write_date
                        pa.array(columns[6], type=pa.string()),                 # type_log
                        pa.array(columns[7], type=pa.int32()),                  # model_id
                        pa.array(columns[8], type=pa.int32()),                  # id_record
                        pa.array(columns[9], type=pa.string()),                 # input
                        pa.array(columns[10], type=pa.string()),                # response
                        pa.array(columns[11], type=pa.int32()),                 # status_code
                        pa.array(columns[12], type=pa.string()),                # url
                        pa.array(columns[13], type=pa.string()),                # header
                        brand_etl_arr,                                          # brand_etl
                        snapshot_date_arr,                                      # snapshot_date
                        ingest_time_arr                                         # ingestion_timestamp
                    ], schema=target_schema)
                    
                    # 7. Append to Iceberg
                    table.append(batch_table)
                    
                    total_ingested += batch_size
                    logger.info(f"✅ Appended {batch_size} records. Total: {total_ingested}")
                    
                    # Cleanup
                    del rows
                    del columns
                    del batch_table
                    del pk_ids
                    gc.collect()

            return total_ingested

        except Exception as e:
            logger.error(f"Stream ingestion failed for {erp_name}: {e}")
            raise

    def run(self) -> dict:
        """
        Execute full ERP direct ingestion pipeline for all ERPs
        """
        logger.info("=" * 80)
        logger.info(f"Starting ERP Direct Ingestion (Streaming Mode) for {self.snapshot_date}")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        stats = {}
        total_records = 0
        
        # Ensure table exists
        table = self.ensure_table_exists()
        
        # Drop partition ONCE
        try:
            from pyiceberg.expressions import EqualTo
            import gc
            logger.info(f"Deleting partition snapshot_date={self.snapshot_date}...")
            table.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
            logger.success(f"✅ Deleted partition snapshot_date={self.snapshot_date}")
            gc.collect()
        except Exception as e:
            logger.warning(f"Could not delete partition: {e}")
        
        # Process each ERP system SEQUENTIALLY
        for erp_name in ['sci', 'kn', 'pr', 'hh']:
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing ERP: {erp_name.upper()}")
                
                # Create connection
                engine = self.create_erp_connection(erp_name)
                
                if not engine:
                    stats[erp_name] = {'status': 'skipped', 'records': 0}
                    continue
                
                # STREAM Data -> PyArrow -> Iceberg
                record_count = self.stream_and_ingest(erp_name, engine, table)
                
                total_records += record_count
                stats[erp_name] = {'status': 'success', 'records': record_count}
                
                # Close connection
                engine.dispose()
                gc.collect()
                
            except Exception as e:
                logger.error(f"❌ Failed to process ERP {erp_name.upper()}: {str(e)}")
                stats[erp_name] = {'status': 'failed', 'records': 0, 'error': str(e)}
        
        # Warning if no data
        if total_records == 0:
            logger.warning("No data from any ERP system")
        
        # Calculate metrics
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"Ingestion completed. Total: {total_records} records. Duration: {duration:.2f}s")
        return stats


def main():
    """Main execution function"""
    # Parse command line arguments
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    logger.info(f"Starting ERP Direct Ingestion for {snapshot_date}")
    
    try:
        # Run ERP direct ingestion
        ingestion = ERPDirectIngestion(snapshot_date)
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
