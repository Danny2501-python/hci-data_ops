"""
Ticket Detail Enrichment: Pull full ticket details from CareSoft API
Architecture: Bronze (Iceberg) → Serving (PostgreSQL) - BYPASSING ICEBERG SILVER

This script reads ticket IDs from Bronze Iceberg and writes details directly to PostgreSQL.
"""

import os
import sys
import time
from datetime import datetime, date, timedelta
import io
import csv
import json
import gc
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And, NotNull

from common.brand_config import get_active_brands
from common.utils import get_current_timestamp_naive
from common.caresoft_api import call_caresoft_api

# Table names
TICKET_BRONZE_TABLE = 'caresoft.cs_ticket_api'
TARGET_PG_TABLE = 'cs_ticket_source_api'

class TicketDetailEnrichment:
    """
    Pull full ticket details from CareSoft API and write directly to PostgreSQL.
    Bypasses the Iceberg Silver layer.
    """
    
    def __init__(self, snapshot_date=None):
        if isinstance(snapshot_date, str):
            self.snapshot_date = snapshot_date
        elif isinstance(snapshot_date, (datetime, date)):
            self.snapshot_date = snapshot_date.strftime('%Y-%m-%d')
        else:
            self.snapshot_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
            
        # Initialize Bronze Catalog (to read IDs)
        catalog_config = {
            "token": os.getenv('ICEBERG_TOKEN', 'admin:password'),
            "s3.endpoint": os.getenv('S3_ENDPOINT', 'http://minio.lakehouse.com'),
            "s3.access-key-id": os.getenv('AWS_ACCESS_KEY', 'admin'),
            "s3.secret-access-key": os.getenv('AWS_SECRET_KEY', 'password'),
            "s3.region": os.getenv('AWS_REGION', 'us-east-1'),
        }
        
        self.bronze_catalog = load_catalog(
            os.getenv('CATALOG_NAME_BRONZE', 'bronze'),
            uri=os.getenv('ICEBERG_REST_URI_BRONZE'),
            **catalog_config
        )
        
        # PostgreSQL connection
        self.pg_uri = os.getenv('POSTGRES_URI')
        self.target_pg_table = TARGET_PG_TABLE
        
        logger.info(f"Initialized TicketDetailEnrichment (Direct Postgres Mode) for {self.snapshot_date}")

    def get_ticket_ids_to_enrich(self, brand_erp_id):
        """Scan Bronze Iceberg for tickets in today's snapshot"""
        logger.info(f"Scanning Bronze tickets for brand={brand_erp_id}, snapshot={self.snapshot_date}")
        
        table = self.bronze_catalog.load_table(TICKET_BRONZE_TABLE)
        
        # Use DuckDB for efficient deduplication if needed
        import tempfile
        temp_dir = tempfile.gettempdir()
        temp_parquet = os.path.join(temp_dir, f"enrich_ids_{brand_erp_id}_{os.getpid()}.parquet")
        
        try:
            scan = table.scan(
                row_filter=And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("brand_id", brand_erp_id),
                    NotNull("ticket_id")
                ),
                selected_fields=("ticket_id", "updated_at")
            )
            
            total_rows = 0
            writer = None
            for batch in scan.to_arrow_batch_reader():
                if len(batch) > 0:
                    t = pa.Table.from_batches([batch])
                    if writer is None:
                        writer = pq.ParquetWriter(temp_parquet, t.schema)
                    writer.write_table(t)
                    total_rows += len(batch)
            
            if writer: writer.close()
            if total_rows == 0: return []
            
            con = duckdb.connect()
            query = f"SELECT ticket_id FROM read_parquet('{temp_parquet}') GROUP BY ticket_id ORDER BY max(updated_at) DESC"
            ticket_ids = con.execute(query).fetch_arrow_table().column('ticket_id').to_pylist()
            con.close()
            
            return ticket_ids
        finally:
            if os.path.exists(temp_parquet): os.remove(temp_parquet)

    def run(self, brand):
        """Execute enrichment and write directly to PostgreSQL"""
        ticket_ids = self.get_ticket_ids_to_enrich(brand["erp_id"])
        if not ticket_ids:
            logger.info(f"No tickets for brand {brand['erp_id']}")
            return
            
        logger.info(f"Enriching {len(ticket_ids)} tickets directly to PostgreSQL...")
        
        pg_conn = psycopg2.connect(self.pg_uri)
        cursor = pg_conn.cursor()
        
        try:
            batch_records = []
            total_success = 0
            BATCH_SIZE = 100
            
            columns = [
                "ticket_id", "account_id", "sla_id", "ticket_no", "requester_id", "group_id",
                "created_at", "updated_at", "satisfaction_at", "duedate", "satisfaction",
                "satisfaction_send", "satisfaction_content", "automessage_id", "feedback_status",
                "is_overdue", "incident_id", "service_id", "qa_script_id", "qa_agent",
                "current_agent", "ticket_source_detail_id", "comments", "brand_id",
                "raw_data", "snapshot_date", "ingestion_timestamp"
            ]
            
            for i, ticket_id in enumerate(ticket_ids):
                if (i+1) % 50 == 0:
                    logger.info(f"Progress: {i+1}/{len(ticket_ids)}...")
                
                ticket_data, error = call_caresoft_api(brand, 'tickets', ticket_id, response_key='ticket')
                
                if ticket_data:
                    def safe_int(v): return int(v) if v is not None else None
                    def safe_ts(v):
                        try: return datetime.fromisoformat(v.replace('Z', '+00:00')) if isinstance(v, str) else v
                        except: return None
                        
                    record = {
                        "ticket_id": ticket_data.get("ticket_id"),
                        "account_id": safe_int(ticket_data.get("account_id")),
                        "sla_id": safe_int(ticket_data.get("sla_id")),
                        "ticket_no": safe_int(ticket_data.get("ticket_no")),
                        "requester_id": ticket_data.get("requester_id"),
                        "group_id": safe_int(ticket_data.get("group_id")),
                        "created_at": safe_ts(ticket_data.get("created_at")),
                        "updated_at": safe_ts(ticket_data.get("updated_at")),
                        "satisfaction_at": safe_ts(ticket_data.get("satisfaction_at")),
                        "duedate": str(ticket_data.get("duedate")) if ticket_data.get("duedate") else None,
                        "satisfaction": ticket_data.get("satisfaction"),
                        "satisfaction_send": ticket_data.get("satisfaction_send"),
                        "satisfaction_content": ticket_data.get("satisfaction_content"),
                        "automessage_id": ticket_data.get("automessage_id"),
                        "feedback_status": ticket_data.get("feedback_status"),
                        "is_overdue": bool(ticket_data.get("is_overdue")),
                        "incident_id": safe_int(ticket_data.get("incident_id")),
                        "service_id": ticket_data.get("service_id"),
                        "qa_script_id": ticket_data.get("qa_script_id"),
                        "qa_agent": ticket_data.get("qa_agent"),
                        "current_agent": ticket_data.get("current_agent"),
                        "ticket_source_detail_id": safe_int(ticket_data.get("ticket_source_detail_id")),
                        "comments": json.dumps(ticket_data.get("comments", []), ensure_ascii=False),
                        "brand_id": int(brand["erp_id"]),
                        "raw_data": json.dumps(ticket_data, ensure_ascii=False),
                        "snapshot_date": self.snapshot_date,
                        "ingestion_timestamp": datetime.now()
                    }
                    batch_records.append(record)
                
                if len(batch_records) >= BATCH_SIZE:
                    self._upsert_to_postgres(cursor, batch_records, columns)
                    total_success += len(batch_records)
                    pg_conn.commit()
                    batch_records = []
                    gc.collect()
                
                time.sleep(0.01) # Small delay for API fairness
                
            if batch_records:
                self._upsert_to_postgres(cursor, batch_records, columns)
                total_success += len(batch_records)
                pg_conn.commit()
                
            logger.success(f"Brand {brand['erp_id']} direct enrichment complete: {total_success} tickets.")
            
        except Exception as e:
            logger.error(f"Error for brand {brand['erp_id']}: {e}")
            pg_conn.rollback()
            raise
        finally:
            cursor.close()
            pg_conn.close()

    def _upsert_to_postgres(self, cursor, records, columns):
        """Internal helper for high-performance UPSERT via COPY"""
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
                    row.append(str(val).replace('\n', ' ').replace('\r', ' '))
            writer.writerow(row)
            
        output.seek(0)
        temp_table = f"temp_enrich_{os.getpid()}"
        cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {self.target_pg_table} INCLUDING ALL) ON COMMIT DROP")
        
        col_list = ', '.join([f'"{c}"' for c in columns])
        cursor.copy_expert(f"COPY {temp_table} ({col_list}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '{NULL_MARKER}')", output)
        
        update_cols = [c for c in columns if c not in ['ticket_id', 'brand_id']]
        update_set = ', '.join([f'"{c}" = {temp_table}."{c}"' for c in update_cols])
        
        upsert_sql = f"""
            INSERT INTO {self.target_pg_table} ({col_list})
            SELECT {col_list} FROM {temp_table}
            ON CONFLICT ("ticket_id", "brand_id")
            DO UPDATE SET {update_set}
            WHERE {self.target_pg_table}."updated_at" < EXCLUDED."updated_at"
               OR {self.target_pg_table}."updated_at" IS NULL
        """
        cursor.execute(upsert_sql)
        output.close()

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = snapshot_date_str if snapshot_date_str else (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    brands = get_active_brands()
    if not brands: return
    
    job = TicketDetailEnrichment(snapshot_date)
    for brand in brands:
        try:
            job.run(brand)
        except Exception as e:
            logger.error(f"Brand {brand['erp_id']} failed: {e}")

if __name__ == "__main__":
    main()
