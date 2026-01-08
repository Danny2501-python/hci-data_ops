
"""
Silver Transformation: Parse JSON Webhook Data
Refactored from: trans_json_wehook_caresoft_p2.py
Target Tables (Iceberg):
- caresoft.cs_deal_wehook
- caresoft.cs_deal_wehook_error
"""

import os
import sys
import json
import ast
import hashlib
from datetime import datetime, date
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from loguru import logger
import gc

# Configs
ICEBERG_REST_URI = os.getenv('ICEBERG_REST_URI_SILVER')
CATALOG_NAME = os.getenv('CATALOG_NAME_SILVER')
SOURCE_TABLE = os.getenv('BRONZE_TABLE', 'caresoft.wehook_api_caresoft')
TARGET_TABLE = os.getenv('SILVER_TABLE_DEAL', 'caresoft.cs_deal_wehook')
TARGET_TABLE_ERROR = os.getenv('SILVER_TABLE_DEAL_ERROR', 'caresoft.cs_deal_wehook_error')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


class WebhookTransformation:
    def __init__(self, snapshot_date: date):
        self.snapshot_date = snapshot_date
        
        # Init Catalog
        self.catalog = load_catalog(
            CATALOG_NAME,
            **{
                "uri": ICEBERG_REST_URI,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
            }
        )
        logger.info(f"Initialized WebhookTransformation for {self.snapshot_date}")

    def ensure_table_error_exists(self):
        """Create Error table if not exists"""
        try:
            return self.catalog.load_table(TARGET_TABLE_ERROR)
        except:
            logger.info(f"Creating table {TARGET_TABLE_ERROR}...")
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, LongType, DateType, TimestampType, StringType
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform

            schema = Schema(
                NestedField(1, "pk_id", StringType(), required=True), # Primary Key (source + id)
                NestedField(2, "id", LongType(), required=False),     # Original ID
                NestedField(3, "snapshot_date", DateType(), required=False),
                NestedField(4, "ingestion_timestamp", TimestampType(), required=False),
            )
            partition_spec = PartitionSpec(
                PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            return self.catalog.create_table(TARGET_TABLE_ERROR, schema=schema, partition_spec=partition_spec)

    def ensure_table_exists(self):
        """Create Silver Deal table if not exists"""
        try:
            return self.catalog.load_table(TARGET_TABLE)
        except:
            logger.info(f"Creating table {TARGET_TABLE}...")
            from pyiceberg.schema import Schema
            from pyiceberg.types import (
                NestedField, IntegerType, StringType, TimestampType, DoubleType, DateType, LongType
            )
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform

            # Schema matching DDL provided
            schema = Schema(
                NestedField(1, "pk_id", StringType(), required=True), # Primary Key (unique global)
                NestedField(2, "id", LongType(), required=False),     # Original ID (integer)
                
                NestedField(3, "last_change_status_at", TimestampType(), required=False),
                NestedField(4, "probability", IntegerType(), required=False),
                NestedField(5, "closed_type", IntegerType(), required=False),
                NestedField(6, "created_at", TimestampType(), required=False),
                NestedField(7, "source", StringType(), required=False),
                NestedField(8, "facebook_page_id", StringType(), required=False),
                NestedField(9, "updated_at", TimestampType(), required=False),
                NestedField(10, "ticket_source_end_status", IntegerType(), required=False),
                NestedField(11, "source_detail_id", IntegerType(), required=False),
                NestedField(12, "service_id", IntegerType(), required=False),
                NestedField(13, "name", StringType(), required=False),
                NestedField(14, "pipeline_id", IntegerType(), required=False),
                NestedField(15, "deal_id", IntegerType(), required=False),
                NestedField(16, "value", IntegerType(), required=False),
                NestedField(17, "last_change_stage_at", TimestampType(), required=False),
                NestedField(18, "requester_id", IntegerType(), required=False),
                NestedField(19, "assignee_id", IntegerType(), required=False),
                NestedField(20, "pipeline_stage_id", IntegerType(), required=False),
                NestedField(21, "activity", StringType(), required=False),
                NestedField(22, "addition_fields", StringType(), required=False),
                NestedField(23, "call_info", StringType(), required=False),
                NestedField(24, "agent_submit", StringType(), required=False),
                NestedField(25, "updated_properties", StringType(), required=False),
                NestedField(26, "account_id", StringType(), required=False),
                NestedField(27, "activity_source", StringType(), required=False),
                NestedField(28, "action", StringType(), required=False),
                NestedField(29, "convert_from_object", StringType(), required=False),
                NestedField(30, "object", StringType(), required=False),
                NestedField(31, "fb_psid", DoubleType(), required=False), # numeric -> double
                NestedField(32, "gclid", StringType(), required=False),
                NestedField(33, "convert_by", DoubleType(), required=False),
                NestedField(34, "csa_acc", DoubleType(), required=False),
                NestedField(35, "addition_details", StringType(), required=False),
                NestedField(36, "commentator_id", DoubleType(), required=False),
                NestedField(37, "comment", StringType(), required=False),
                NestedField(38, "convert_type", StringType(), required=False),
                NestedField(39, "convert_at", StringType(), required=False),
                NestedField(40, "closed_by", StringType(), required=False),
                NestedField(41, "zalo_page_id", DoubleType(), required=False),
                NestedField(42, "closed_at", StringType(), required=False),
                NestedField(43, "unqualified_reasons", StringType(), required=False),
                NestedField(44, "lost_reasons", StringType(), required=False),
                NestedField(45, "estimated_closed_date", StringType(), required=False),
                NestedField(46, "utm_campaign", StringType(), required=False),
                
                # Partition fields
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            return self.catalog.create_table(TARGET_TABLE, schema=schema, partition_spec=partition_spec)

    def parse_and_transform_batch(self, bronze_table):
        """
        Generator function that yields batches of (success_rows, error_ids)
        Uses batch streaming to prevent OOM
        """
        logger.info(f"Scanning Bronze table {SOURCE_TABLE} for {self.snapshot_date}...")
        
        # Use batch reader (Streaming)
        scan = bronze_table.scan(
            row_filter=EqualTo("snapshot_date", self.snapshot_date),
            selected_fields=("pk_id", "id", "input") # Select pk_id
        )
        
        # Helper for safer parsing
        def safe_eval(s):
            try:
                if not s: return None
                return json.loads(json.dumps(ast.literal_eval(s)))
            except:
                return None

        # Helper for timestamp conversion
        def parse_ts(val):
            if not val: return None
            try:
                return datetime.fromtimestamp(float(val) / 1000.0)
            except:
                return None

        total_processed = 0
        
        # Iterate through batches
        for arrow_batch in scan.to_arrow_batch_reader():
            batch_success = []
            batch_errors = []
            
            # Convert batch to python list for processing
            pk_ids = arrow_batch["pk_id"].to_pylist()
            ids = arrow_batch["id"].to_pylist()
            inputs = arrow_batch["input"].to_pylist()
            
            for i, input_str in enumerate(inputs):
                pk = pk_ids[i]
                row_id = ids[i]
                
                # 1. Check content
                if not input_str or not input_str.strip().startswith("{'deal'"):
                     continue
                
                # 2. Parse
                json_data = safe_eval(input_str)
                if not json_data:
                    batch_errors.append((pk, row_id)) # Store tuple for error
                    continue
                
                # 3. Extract
                try:
                    deal_info = json_data.get('deal', {})
                    record = {
                        "pk_id": pk, # Use PK_ID
                        "id": row_id,
                        "deal_id": deal_info.get("deal_id"),
                        "name": deal_info.get("name"),
                        "value": deal_info.get("value"),
                        "probability": deal_info.get("probability"),
                        "pipeline_id": deal_info.get("pipeline_id"),
                        "pipeline_stage_id": deal_info.get("pipeline_stage_id"),
                        "closed_type": deal_info.get("closed_type"),
                        "source": deal_info.get("source"),
                        "source_detail_id": deal_info.get("source_detail_id"),
                        "service_id": deal_info.get("service_id"),
                        "requester_id": deal_info.get("requester_id"),
                        "assignee_id": deal_info.get("assignee_id"),
                        "facebook_page_id": str(deal_info.get("facebook_page_id")) if deal_info.get("facebook_page_id") else None,
                        "ticket_source_end_status": deal_info.get("ticket_source_end_status"),
                        
                        "created_at": parse_ts(deal_info.get("created_at")),
                        "updated_at": parse_ts(deal_info.get("updated_at")),
                        "last_change_status_at": parse_ts(deal_info.get("last_change_status_at")),
                        "last_change_stage_at": parse_ts(deal_info.get("last_change_stage_at")),
                        
                        "activity": str(deal_info.get("activity")),
                        "addition_fields": str(deal_info.get("addition_fields")),
                        "call_info": str(json_data.get("call_info", {})),
                        "agent_submit": str(json_data.get("agent_submit", {})),
                        "updated_properties": str(json_data.get("updated_properties", {})),
                        "account_id": str(json_data.get("account_id", {})),
                        "activity_source": str(json_data.get("activity_source", {})),
                        "action": str(json_data.get("action", {})),
                        "convert_from_object": str(json_data.get("convert_from_object", {})),
                        "object": str(json_data.get("object", {})),
                        
                        "fb_psid": float(deal_info.get("fb_psid")) if deal_info.get("fb_psid") else None,
                        "csa_acc": float(deal_info.get("csa_acc")) if deal_info.get("csa_acc") else None,
                        "commentator_id": float(deal_info.get("commentator_id")) if deal_info.get("commentator_id") else None,
                        "zalo_page_id": float(deal_info.get("zalo_page_id")) if deal_info.get("zalo_page_id") else None,
                        "convert_by": float(deal_info.get("convert_by")) if deal_info.get("convert_by") else None,

                        "gclid": str(deal_info.get("gclid")) if deal_info.get("gclid") else None,
                        "addition_details": str(deal_info.get("addition_details")) if deal_info.get("addition_details") else None,
                        "comment": str(deal_info.get("comment")) if deal_info.get("comment") else None,
                        "convert_type": str(deal_info.get("convert_type")) if deal_info.get("convert_type") else None,
                        "convert_at": str(deal_info.get("convert_at")) if deal_info.get("convert_at") else None,
                        "closed_by": str(deal_info.get("closed_by")) if deal_info.get("closed_by") else None,
                        "closed_at": str(deal_info.get("closed_at")) if deal_info.get("closed_at") else None,
                        "unqualified_reasons": str(deal_info.get("unqualified_reasons")) if deal_info.get("unqualified_reasons") else None,
                        "lost_reasons": str(deal_info.get("lost_reasons")) if deal_info.get("lost_reasons") else None,
                        "estimated_closed_date": str(deal_info.get("estimated_closed_date")) if deal_info.get("estimated_closed_date") else None,
                        "utm_campaign": str(deal_info.get("utm_campaign")) if deal_info.get("utm_campaign") else None,

                        "snapshot_date": self.snapshot_date,
                        "ingestion_timestamp": datetime.now()
                    }
                    batch_success.append(record)
                except:
                    batch_errors.append((pk, row_id))
            
            total_processed += len(inputs)
            logger.info(f"Processed batch of {len(inputs)}. Total scanned: {total_processed}")
            
            # Yield results for this batch
            yield batch_success, batch_errors
            
            # Explicit cleanup
            del ids, inputs, pk_ids, arrow_batch
            gc.collect()

    def run(self):
        # 1. Load Tables
        tbl_error = self.ensure_table_error_exists()
        tbl_target = self.ensure_table_exists()
        
        try:
            bronze_catalog = load_catalog(
                os.getenv('CATALOG_NAME_BRONZE', 'bronze'),
                **{
                     "uri": os.getenv('ICEBERG_REST_URI_BRONZE'),
                     "s3.endpoint": S3_ENDPOINT,
                     "s3.access-key-id": AWS_ACCESS_KEY,
                     "s3.secret-access-key": AWS_SECRET_KEY,
                }
            )
            tbl_source = bronze_catalog.load_table(SOURCE_TABLE)
        except Exception as e:
            logger.error(f"Failed to load Bronze table: {e}")
            sys.exit(1)

        # 2. DELETE OLD DATA (Overwrite logic manually implemented)
        # Because we append batch by batch, we cannot use 'overwrite' mode in loop (it would overwrite previous batches).
        # Strategy: Delete existing partition FIRST, then Append batches.
        
        logger.info(f"Cleaning existing partition {self.snapshot_date}...")
        try:
            tbl_target.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
            tbl_error.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
        except Exception as e:
            logger.warning(f"Delete partition warning (first run?): {e}")

        # 3. Transform & Append Batch by Batch
        total_success = 0
        total_error = 0
        
        # Pre-fetch schemas as Arrow schema for strict typing
        target_schema = tbl_target.schema().as_arrow()
        error_schema = tbl_error.schema().as_arrow()

        for success_batch, error_batch in self.parse_and_transform_batch(tbl_source):
            if success_batch:
                # Use explicit schema to prevent "tried to convert to boolean" errors
                try:
                    df = pa.Table.from_pylist(success_batch, schema=target_schema)
                    tbl_target.append(df)
                    total_success += len(success_batch)
                except Exception as e:
                    logger.error(f"Error appending success batch: {e}")
                    # fallback or re-raise? Let's log specifically which column might fail if possible, 
                    # but for now printing error is critical.
                    # We might want to move these to error_batch if they fail schema validation?
                    # For now, let's Raise to not lose data silently.
                    raise e
            
            if error_batch:
                error_records = [
                    {"pk_id": pk, "id": row_id, "snapshot_date": self.snapshot_date, "ingestion_timestamp": datetime.now()} 
                    for (pk, row_id) in error_batch
                ]
                try:
                    df_err = pa.Table.from_pylist(error_records, schema=error_schema)
                    tbl_error.append(df_err)
                    total_error += len(error_batch)
                except Exception as e:
                    logger.error(f"Error appending error batch: {e}")
                    raise e

        logger.success(f"Silver Transformation Completed. Success: {total_success}, Errors: {total_error}")

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    start = datetime.now()
    job = WebhookTransformation(snapshot_date)
    job.run()
    logger.info(f"Total Duration: {(datetime.now() - start).total_seconds()}s")

if __name__ == "__main__":
    main()
