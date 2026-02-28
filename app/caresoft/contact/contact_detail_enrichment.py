import os
import sys
import json
import time
import io
import csv
import gc
from datetime import date, datetime, timedelta
from loguru import logger
import psycopg2
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, TimestampType, DateType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.expressions import EqualTo, And, NotNull
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from common.brand_config import get_active_brands
from common.utils import get_current_timestamp_naive
from common.caresoft_api import call_caresoft_api

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
ICEBERG_REST_URI_SILVER = os.getenv('ICEBERG_REST_URI_SILVER')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')
CATALOG_NAME_SILVER = os.getenv('CATALOG_NAME_SILVER')

# S3 configuration
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# PostgreSQL configuration
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Source and target tables
CONTACT_BRONZE_TABLE = 'caresoft.cs_contacts_api_ttc'
CONTACT_DETAIL_ERROR_TABLE = 'caresoft.cs_contact_error'
TARGET_PG_TABLE = 'cs_contact_api'


class ContactDetailEnrichment:
    """Pull full contact details from CareSoft API based on Bronze contact data
    Architecture: Bronze (Iceberg) → Serving (PostgreSQL) - BYPASSING ICEBERG SILVER
    """
    
    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        if isinstance(self.snapshot_date, str):
            self.snapshot_date = datetime.strptime(self.snapshot_date, "%Y-%m-%d").date()
            
        # Catalog configuration
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
        self.silver_catalog = load_catalog(
            CATALOG_NAME_SILVER,
            uri=ICEBERG_REST_URI_SILVER,
            **catalog_config
        )
        
        self.pg_uri = POSTGRES_URI
        self.target_pg_table = TARGET_PG_TABLE
        
        logger.info(f"Initialized ContactDetailEnrichment (Postgres-Direct) for snapshot_date={self.snapshot_date}")
        
        # Ensure error table exists in Iceberg
        self.ensure_contact_detail_error_table_exists()
    
    def ensure_contact_detail_error_table_exists(self):
        """Create contact detail error table in Iceberg if not exists"""
        try:
            return self.silver_catalog.load_table(CONTACT_DETAIL_ERROR_TABLE)
        except Exception:
            logger.info(f"Creating error table {CONTACT_DETAIL_ERROR_TABLE} in Iceberg...")
            schema = Schema(
                NestedField(1, "id", IntegerType(), required=True),
                NestedField(2, "brand_id", IntegerType(), required=True),
                NestedField(3, "trangthai", IntegerType(), required=False),
                NestedField(4, "snapshot_date", DateType(), required=True),
                NestedField(5, "error_timestamp", TimestampType(), required=True),
            )
            partition_spec = PartitionSpec(
                PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            return self.silver_catalog.create_table(
                identifier=CONTACT_DETAIL_ERROR_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )

    def ensure_target_pg_table_exists(self, cursor, columns):
        """Ensure the target PostgreSQL table exists with the correct primary key"""
        logger.info(f"Ensuring PostgreSQL target table {self.target_pg_table} exists...")
        column_defs = [f'"{col}" TEXT' for col in columns]
        # Note: We use TEXT for all columns to match legacy schema, but id/brand_id could be INT.
        # However, to maintain parity with contact_detail_serving_layer.py, we use TEXT.
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.target_pg_table} (
            {', '.join(column_defs)},
            PRIMARY KEY ("id", "brand_id")
        )
        """
        cursor.execute(create_sql)

    def get_contact_ids_to_enrich(self, brand_erp_id: int) -> list:
        """Scan Bronze Iceberg for contacts to enrich using DuckDB"""
        try:
            bronze_table = self.bronze_catalog.load_table(CONTACT_BRONZE_TABLE)
        except Exception as e:
            logger.error(f"Error loading Bronze table: {e}")
            return []
            
        import tempfile
        temp_parquet = os.path.join(tempfile.gettempdir(), f"contact_ids_{brand_erp_id}_{os.getpid()}.parquet")
        
        try:
            scan = bronze_table.scan(
                row_filter=And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand_erp_id),
                    NotNull("id")
                ),
                selected_fields=("id", "updated_at")
            )
            
            writer = None
            total_rows = 0
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    t = pa.Table.from_batches([batch])
                    if writer is None:
                        writer = pq.ParquetWriter(temp_parquet, t.schema)
                    writer.write_table(t)
                    total_rows += len(batch)
                    del t
                    gc.collect()
            
            if writer:
                writer.close()
            
            if total_rows == 0:
                return []
            
            con = duckdb.connect()
            query = f"""
            SELECT id
            FROM (
                SELECT 
                    id,
                    updated_at,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
                FROM read_parquet('{temp_parquet}')
            ) t
            WHERE rn = 1
            ORDER BY updated_at DESC
            """
            contact_ids = con.execute(query).fetch_arrow_table().column('id').to_pylist()
            con.close()
            return contact_ids
        finally:
            if os.path.exists(temp_parquet):
                try:
                    os.remove(temp_parquet)
                except:
                    pass

    def run(self, brand: dict):
        """Standard pipeline logic with Peak RAM optimizations and Direct Postgres Upsert"""
        if not self.pg_uri:
            logger.critical("❌ POSTGRES_URI environment variable missing")
            return

        contact_ids = self.get_contact_ids_to_enrich(brand["erp_id"])
        if not contact_ids:
            logger.warning(f"No contacts to enrich for brand {brand['name']}")
            return
            
        logger.info(f"Enriching {len(contact_ids)} contacts directly to PostgreSQL for brand {brand['name']}...")
        error_table = self.ensure_contact_detail_error_table_exists()
        
        # Delete old error records for this partition
        try:
            error_table.delete(
                And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand["erp_id"])
                )
            )
        except Exception:
            pass

        try:
            pg_conn = psycopg2.connect(self.pg_uri)
            cursor = pg_conn.cursor()
        except Exception as e:
            logger.critical(f"❌ PostgreSQL Connection Error: {e}")
            return
        
        try:
            columns = [
                "account_id", "id", "username", "email", "email2", "phone_no", 
                "phone_no2", "phone_no3", "facebook", "gender", "organization_id", 
                "address", "city_id", "created_from", "district_id", "created_at", 
                "updated_at", "role_id", "campaign_handler_id", "take_phone_at", 
                "take_email_at", "custom_fields", "organization", "psid", 
                "brand_id", "raw_data", "snapshot_date", "ingestion_timestamp"
            ]
            
            # Ensure target table exists once
            self.ensure_target_pg_table_exists(cursor, columns)
            pg_conn.commit()

            batch_records = []
            batch_error_records = []
            total_success = 0
            total_errors = 0
            MICRO_BATCH_SIZE = 100
            
            for i, contact_id in enumerate(contact_ids):
                contact_data, error = call_caresoft_api(brand, 'contacts', contact_id, response_key='contact')
                
                if contact_data:
                    record = {
                        "account_id": self._safe_str(contact_data.get("account_id")),
                        "id": self._safe_int(contact_data.get("id")),
                        "username": self._safe_str(contact_data.get("username")),
                        "email": self._safe_str(contact_data.get("email")),
                        "email2": self._safe_str(contact_data.get("email2")),
                        "phone_no": self._safe_str(contact_data.get("phone_no")),
                        "phone_no2": self._safe_str(contact_data.get("phone_no2")),
                        "phone_no3": self._safe_str(contact_data.get("phone_no3")),
                        "facebook": self._safe_str(contact_data.get("facebook")),
                        "gender": self._safe_str(contact_data.get("gender")),
                        "organization_id": self._safe_str(contact_data.get("organization_id")),
                        "address": self._safe_str(contact_data.get("address")),
                        "city_id": self._safe_str(contact_data.get("city_id")),
                        "created_from": self._safe_str(contact_data.get("created_from")),
                        "district_id": self._safe_str(contact_data.get("district_id")),
                        "created_at": self._safe_ts(contact_data.get("created_at")),
                        "updated_at": self._safe_ts(contact_data.get("updated_at")),
                        "role_id": self._safe_str(contact_data.get("role_id")),
                        "campaign_handler_id": self._safe_str(contact_data.get("campaign_handler_id")),
                        "take_phone_at": self._safe_str(contact_data.get("take_phone_at")),
                        "take_email_at": self._safe_str(contact_data.get("take_email_at")),
                        "custom_fields": json.dumps(contact_data.get("custom_fields", []), ensure_ascii=False),
                        "organization": self._safe_str(contact_data.get("organization")),
                        "psid": json.dumps(contact_data.get("psid", []), ensure_ascii=False),
                        "brand_id": int(brand["erp_id"]),
                        "raw_data": json.dumps(contact_data, ensure_ascii=False),
                        "snapshot_date": self.snapshot_date,
                        "ingestion_timestamp": get_current_timestamp_naive()
                    }
                    batch_records.append(record)
                    del contact_data
                else:
                    batch_error_records.append({
                        "id": contact_id, 
                        "brand_id": int(brand["erp_id"]), 
                        "trangthai": 1,
                        "snapshot_date": self.snapshot_date, 
                        "error_timestamp": get_current_timestamp_naive()
                    })
                    total_errors += 1
                
                # Flush Cycle
                if len(batch_records) >= MICRO_BATCH_SIZE:
                    self._upsert_to_postgres(cursor, batch_records, columns)
                    pg_conn.commit()
                    total_success += len(batch_records)
                    batch_records = []
                    gc.collect()

                if len(batch_error_records) >= MICRO_BATCH_SIZE:
                    error_table.append(pa.Table.from_pylist(batch_error_records))
                    batch_error_records = []
                    gc.collect()
                
                if (i+1) % 50 == 0:
                    logger.info(f"Progress: {i+1}/{len(contact_ids)} contacts processed for {brand['name']}")
                
                # Rate limit protection
                time.sleep(0.01)
                
            # Final remaining
            if batch_records:
                self._upsert_to_postgres(cursor, batch_records, columns)
                pg_conn.commit()
                total_success += len(batch_records)
            
            if batch_error_records:
                error_table.append(pa.Table.from_pylist(batch_error_records))
                
            logger.success(f"Enrichment complete for {brand['name']}. Success: {total_success}, Errors: {total_errors}")
            
        except Exception as e:
            logger.error(f"Failed Run for brand {brand['erp_id']}: {e}")
            pg_conn.rollback()
        finally:
            cursor.close()
            pg_conn.close()

    def _safe_int(self, v):
        try:
            return int(v) if v is not None else None
        except:
            return None
    
    def _safe_str(self, v):
        return str(v) if v is not None else ''

    def _safe_ts(self, v):
        try:
            if v is None or v == '': return None
            # Handle integer milliseconds timestamp
            if isinstance(v, int):
                return datetime.fromtimestamp(v / 1000.0)
            # Handle string ISO format
            if isinstance(v, str):
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            return None
        except:
            return None

    def _upsert_to_postgres(self, cursor, records, columns):
        """Streaming COPY + UPSERT with automatic staging cleanup"""
        NULL_MARKER = '__NULL__'
        output = io.StringIO()
        writer = csv.writer(output, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        
        for rec in records:
            row = []
            for col in columns:
                val = rec.get(col)
                if val is None:
                    row.append(NULL_MARKER)
                elif isinstance(val, (datetime, date)):
                    row.append(val.isoformat())
                else:
                    # Clean newline characters to prevent CSV corruption
                    row.append(str(val).replace('\n', ' ').replace('\r', ' '))
            writer.writerow(row)
        
        output.seek(0)
        temp_table = f"temp_stg_contact_{os.getpid()}"
        
        # Create staging table
        cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {self.target_pg_table} INCLUDING ALL) ON COMMIT DROP")
        
        # Load via COPY
        col_list = ', '.join([f'"{c}"' for c in columns])
        cursor.copy_expert(f"COPY {temp_table} ({col_list}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '{NULL_MARKER}')", output)
        
        # Perform UPSERT
        upd_list = [f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in ['id', 'brand_id']]
        cursor.execute(f"""
            INSERT INTO {self.target_pg_table} ({col_list}) 
            SELECT {col_list} FROM {temp_table}
            ON CONFLICT ("id", "brand_id") 
            DO UPDATE SET 
                {', '.join(upd_list)}
            WHERE {self.target_pg_table}."updated_at" < EXCLUDED."updated_at" 
               OR {self.target_pg_table}."updated_at" IS NULL
        """)
        
        output.close()
        del output


def main():
    """Main entry point"""
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = snapshot_date_str if snapshot_date_str else (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    brands = get_active_brands()
    if not brands:
        logger.error("No active brands found")
        return
        
    job = ContactDetailEnrichment(snapshot_date)
    for brand in brands:
        try:
            job.run(brand)
        except Exception as e:
            logger.error(f"Global Failure for brand {brand['erp_id']}: {e}")


if __name__ == "__main__":
    main()
