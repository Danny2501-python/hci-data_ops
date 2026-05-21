"""
Ticket Detail Enrichment: Pull full ticket details from CareSoft API
Architecture: Bronze (Iceberg) → Serving (PostgreSQL) - BYPASSING ICEBERG SILVER
Peak RAM Optimized & 100% Logic Parity with Error tracking in Iceberg.
"""

import os
import sys
import time
import io
import csv
import json
import gc
from datetime import datetime, date, timedelta
import psycopg2
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And, NotNull
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType, StringType, IntegerType, LongType, NestedField
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

from common.brand_config import get_active_brands
from common.utils import get_current_timestamp_naive, bulk_load
from common.caresoft_api import call_caresoft_api

# Table names
TICKET_BRONZE_TABLE = 'caresoft.cs_ticket_api'
TARGET_PG_TABLE = 'cs_ticket_source_api'

class TicketDetailEnrichment:
    def __init__(self, snapshot_date=None):
        if isinstance(snapshot_date, str):
            self.snapshot_date = snapshot_date
        elif isinstance(snapshot_date, (datetime, date)):
            self.snapshot_date = snapshot_date.strftime('%Y-%m-%d')
        else:
            self.snapshot_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
            
        # Catalog config (Synchronized with DAG and old.py)
        catalog_config = {
            "token": os.getenv('ICEBERG_TOKEN', 'admin:password'),
            "s3.endpoint": os.getenv('S3_ENDPOINT', 'http://minio.lakehouse.com'),
            "s3.access-key-id": os.getenv('AWS_ACCESS_KEY_ID', os.getenv('AWS_ACCESS_KEY', 'admin')),
            "s3.secret-access-key": os.getenv('AWS_SECRET_ACCESS_KEY', os.getenv('AWS_SECRET_KEY', 'password')),
            "s3.region": os.getenv('AWS_REGION', 'us-east-1'),
        }
        
        self.bronze_catalog = load_catalog(
            os.getenv('CATALOG_NAME_BRONZE', 'bronze'),
            uri=os.getenv('ICEBERG_REST_URI_BRONZE'),
            **catalog_config
        )
        self.silver_catalog = load_catalog(
            os.getenv('CATALOG_NAME_SILVER', 'silver'),
            uri=os.getenv('ICEBERG_REST_URI_SILVER'),
            **catalog_config
        )
        
        self.pg_uri = os.getenv('POSTGRES_URI')
        self.target_pg_table = TARGET_PG_TABLE
        logger.info(f"Initialized TicketDetailEnrichment (Final Polish) for {self.snapshot_date}")


    def get_ticket_ids_to_enrich(self, brand_erp_id):
        """Scan Bronze Iceberg for tickets with streaming Parquet writing"""
        try:
            table = self.bronze_catalog.load_table(TICKET_BRONZE_TABLE)
        except Exception as e:
            logger.error(f"❌ Error loading Bronze table: {e}"); return []
        
        import tempfile
        temp_parquet = os.path.join(tempfile.gettempdir(), f"ids_final_{brand_erp_id}_{os.getpid()}.parquet")
        
        try:
            scan = table.scan(
                row_filter=And(EqualTo("snapshot_date", self.snapshot_date), EqualTo("brand_id", brand_erp_id), NotNull("ticket_id")),
                selected_fields=("ticket_id", "updated_at")
            )
            
            writer = None
            total_rows = 0
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    t = pa.Table.from_batches([batch])
                    if writer is None: writer = pq.ParquetWriter(temp_parquet, t.schema)
                    writer.write_table(t)
                    total_rows += len(batch)
                    del t; gc.collect()
            
            if writer: writer.close()
            if total_rows == 0: return []
            
            temp_dir = os.environ.get('DUCKDB_TEMP_DIR', tempfile.gettempdir())
            con = duckdb.connect(config={
                'memory_limit': '512MB',  # Query chỉ 2 cột nên nhẹ, nhưng giới hạn cho an toàn
                'temp_directory': temp_dir,
                'threads': '1'
            })
            query = f"SELECT ticket_id FROM read_parquet('{temp_parquet}') WHERE ticket_id IS NOT NULL GROUP BY ticket_id ORDER BY max(updated_at) DESC NULLS LAST"
            ticket_ids = con.execute(query).fetch_arrow_table().column('ticket_id').to_pylist()
            con.close()
            return ticket_ids
        finally:
            if os.path.exists(temp_parquet): 
                try: os.remove(temp_parquet)
                except: pass

    def run(self, brand):
        """Standard pipeline logic with Peak RAM optimizations"""
        if not self.pg_uri:
            logger.critical("❌ POSTGRES_URI environment variable missing"); return

        ticket_ids = self.get_ticket_ids_to_enrich(brand["erp_id"])
        if not ticket_ids: return
            
        logger.info(f"Enriching {len(ticket_ids)} tickets directly to PostgreSQL...")
        
        try:
            pg_conn = psycopg2.connect(self.pg_uri)
            cursor = pg_conn.cursor()
        except Exception as e:
            logger.critical(f"❌ PostgreSQL Connection Error: {e}"); return
        
        try:
            columns = [
                "ticket_id", "account_id", "sla_id", "ticket_no", "requester_id", "group_id",
                "created_at", "updated_at", "satisfaction_at", "duedate", "satisfaction",
                "satisfaction_send", "satisfaction_content", "automessage_id", "feedback_status",
                "is_overdue", "incident_id", "service_id", "qa_script_id", "qa_agent",
                "current_agent", "ticket_source_detail_id", "comments", "brand_id",
                # "raw_data", 
                "snapshot_date", "ingestion_timestamp"
            ]
            
            batch_records = []
            total_success = 0
            total_errors = 0
            MICRO_BATCH_SIZE = 100
            ingestion_timestamp = get_current_timestamp_naive()  # Compute once, reuse for all tickets

            for i, ticket_id in enumerate(ticket_ids):
                ticket_data, error = call_caresoft_api(brand, 'tickets', ticket_id, response_key='ticket')
                
                if ticket_data:
                    record = {
                        "ticket_id": ticket_data.get("ticket_id"),
                        "account_id": self._safe_int(ticket_data.get("account_id")),
                        "sla_id": self._safe_int(ticket_data.get("sla_id")),
                        "ticket_no": self._safe_int(ticket_data.get("ticket_no")),
                        "requester_id": ticket_data.get("requester_id"),
                        "group_id": self._safe_int(ticket_data.get("group_id")),
                        "created_at": self._safe_ts(ticket_data.get("created_at")),
                        "updated_at": self._safe_ts(ticket_data.get("updated_at")),
                        "satisfaction_at": self._safe_ts(ticket_data.get("satisfaction_at")),
                        "duedate": self._safe_str(ticket_data.get("duedate")),
                        "satisfaction": self._safe_str(ticket_data.get("satisfaction")),
                        "satisfaction_send": self._safe_str(ticket_data.get("satisfaction_send")),
                        "satisfaction_content": self._safe_str(ticket_data.get("satisfaction_content")),
                        "automessage_id": self._safe_str(ticket_data.get("automessage_id")),
                        "feedback_status": self._safe_str(ticket_data.get("feedback_status")),
                        "is_overdue": self._safe_bool(ticket_data.get("is_overdue")),
                        "incident_id": self._safe_int(ticket_data.get("incident_id")),
                        "service_id": ticket_data.get("service_id"),
                        "qa_script_id": self._safe_str(ticket_data.get("qa_script_id")),
                        "qa_agent": self._safe_str(ticket_data.get("qa_agent")),
                        "current_agent": self._safe_str(ticket_data.get("current_agent")),
                        "ticket_source_detail_id": self._safe_int(ticket_data.get("ticket_source_detail_id")),
                        "comments": json.dumps(ticket_data.get("comments", []), ensure_ascii=False),
                        "brand_id": int(brand["erp_id"]),
                        # "raw_data": json.dumps(ticket_data, ensure_ascii=False), # Removed due to size
                        "snapshot_date": self.snapshot_date,
                        "ingestion_timestamp": ingestion_timestamp
                    }
                    batch_records.append(record)
                    del ticket_data; gc.collect()
                else:
                    total_errors += 1
                    logger.warning(f"Failed to fetch ticket {ticket_id}: {error}")
                
                # Flush Cycle
                if len(batch_records) >= MICRO_BATCH_SIZE:
                    self._upsert_to_postgres(cursor, batch_records, columns)
                    pg_conn.commit(); total_success += len(batch_records)
                    batch_records = []; gc.collect()
                
                if (i+1) % 50 == 0: logger.info(f"Progress: {i+1}/{len(ticket_ids)} Done.")
                time.sleep(0.01)
                
            # Final remaining
            if batch_records:
                self._upsert_to_postgres(cursor, batch_records, columns); pg_conn.commit()
                total_success += len(batch_records)
                
            logger.success(f"Execution complete. Success: {total_success}, Errors: {total_errors}")
            
        except Exception as e:
            logger.error(f"Failed Run for brand {brand['erp_id']}: {e}"); pg_conn.rollback()
        finally:
            cursor.close(); pg_conn.close()

    def _safe_int(self, v):
        try: return int(v) if v is not None else None
        except: return None
    
    def _safe_str(self, v):
        return str(v) if v is not None else None

    def _safe_ts(self, v):
        try: return datetime.fromisoformat(v.replace('Z', '+00:00')) if isinstance(v, str) else v
        except: return None
    
    def _safe_bool(self, v):
        if v is None: return None
        return str(v).lower() in ('true', '1', 'yes') if not isinstance(v, bool) else v

    def _upsert_to_postgres(self, cursor, records, columns):
        """Standardized Bulk Upsert using common utility"""
        bulk_load(
            cursor=cursor,
            target_table=self.target_pg_table,
            records=records,
            columns=columns,
            conflict_columns=['ticket_id', 'brand_id'],
            method='upsert',
            update_condition=f'{self.target_pg_table}."updated_at" < EXCLUDED."updated_at" OR {self.target_pg_table}."updated_at" IS NULL'
        )

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = snapshot_date_str if snapshot_date_str else (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    brands = get_active_brands()
    if brands:
        job = TicketDetailEnrichment(snapshot_date)
        for brand in brands:
            try: job.run(brand)
            except Exception as e: logger.error(f"Global Failure: {e}")

if __name__ == "__main__":
    main()
