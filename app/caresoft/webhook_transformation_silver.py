
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
            from pyiceberg.types import NestedField, LongType, DateType, TimestampType
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform

            schema = Schema(
                NestedField(1, "id", LongType(), required=False), # Original ID from Bronze
                NestedField(2, "snapshot_date", DateType(), required=False),
                NestedField(3, "ingestion_timestamp", TimestampType(), required=False),
            )
            partition_spec = PartitionSpec(
                PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
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
                NestedField(1, "id", IntegerType(), required=False),
                NestedField(2, "last_change_status_at", TimestampType(), required=False),
                NestedField(3, "probability", IntegerType(), required=False),
                NestedField(4, "closed_type", IntegerType(), required=False),
                NestedField(5, "created_at", TimestampType(), required=False),
                NestedField(6, "source", StringType(), required=False),
                NestedField(7, "facebook_page_id", StringType(), required=False),
                NestedField(8, "updated_at", TimestampType(), required=False),
                NestedField(9, "ticket_source_end_status", IntegerType(), required=False),
                NestedField(10, "source_detail_id", IntegerType(), required=False),
                NestedField(11, "service_id", IntegerType(), required=False),
                NestedField(12, "name", StringType(), required=False),
                NestedField(13, "pipeline_id", IntegerType(), required=False),
                NestedField(14, "deal_id", IntegerType(), required=False),
                NestedField(15, "value", IntegerType(), required=False),
                NestedField(16, "last_change_stage_at", TimestampType(), required=False),
                NestedField(17, "requester_id", IntegerType(), required=False),
                NestedField(18, "assignee_id", IntegerType(), required=False),
                NestedField(19, "pipeline_stage_id", IntegerType(), required=False),
                NestedField(20, "activity", StringType(), required=False),
                NestedField(21, "addition_fields", StringType(), required=False),
                NestedField(22, "call_info", StringType(), required=False),
                NestedField(23, "agent_submit", StringType(), required=False),
                NestedField(24, "updated_properties", StringType(), required=False),
                NestedField(25, "account_id", StringType(), required=False),
                NestedField(26, "activity_source", StringType(), required=False),
                NestedField(27, "action", StringType(), required=False),
                NestedField(28, "convert_from_object", StringType(), required=False),
                NestedField(29, "object", StringType(), required=False),
                NestedField(30, "fb_psid", DoubleType(), required=False), # numeric -> double
                NestedField(31, "gclid", StringType(), required=False),
                NestedField(32, "convert_by", DoubleType(), required=False),
                NestedField(33, "csa_acc", DoubleType(), required=False),
                NestedField(34, "addition_details", StringType(), required=False),
                NestedField(35, "commentator_id", DoubleType(), required=False),
                NestedField(36, "comment", StringType(), required=False),
                NestedField(37, "convert_type", StringType(), required=False),
                NestedField(38, "convert_at", StringType(), required=False),
                NestedField(39, "closed_by", StringType(), required=False),
                NestedField(40, "zalo_page_id", DoubleType(), required=False),
                NestedField(41, "closed_at", StringType(), required=False),
                NestedField(42, "unqualified_reasons", StringType(), required=False),
                NestedField(43, "lost_reasons", StringType(), required=False),
                NestedField(44, "estimated_closed_date", StringType(), required=False),
                NestedField(45, "utm_campaign", StringType(), required=False),
                
                # Partition fields
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            return self.catalog.create_table(TARGET_TABLE, schema=schema, partition_spec=partition_spec)

    def parse_and_transform(self, bronze_table):
        """
        Read Bronze -> Parse JSON -> Return Clean Data + Errors
        """
        logger.info(f"Scanning Bronze table {SOURCE_TABLE} for {self.snapshot_date}...")
        
        # Read Bronze Data (Filter by snapshot_date)
        # We need 'id' and 'input' columns
        # Filter: input LIKE '{'deal'%' (Simplified to startswith check in Python or if PyIceberg supports it)
        
        # Note: PyIceberg currently has limited string function support in filter.
        # We scan by partition first, then filter in memory (using PyArrow).
        
        scan = bronze_table.scan(
            row_filter=EqualTo("snapshot_date", self.snapshot_date),
            selected_fields=("id", "input")
        )
        
        arrow_table = scan.to_arrow()
        logger.info(f"Loaded {len(arrow_table)} records from Bronze.")
        
        if len(arrow_table) == 0:
            return [], []

        # Convert to Pylist for fast iteration (handling JSON parsing line by line)
        # Using pure Python loop as logic is complex
        
        success_rows = []
        error_ids = []
        
        # Helper for safer parsing
        def safe_eval(s):
            try:
                # Logic from old code: ast.literal_eval -> json dump -> json load
                # This handles single quotes vs double quotes issues in legacy data
                return json.loads(json.dumps(ast.literal_eval(s)))
            except:
                return None

        # Helper for timestamp conversion
        def parse_ts(val):
            if not val: return None
            try:
                # val is likely int (milliseconds)
                return datetime.fromtimestamp(float(val) / 1000.0)
            except:
                return None

        ids = arrow_table["id"].to_pylist()
        inputs = arrow_table["input"].to_pylist()
        
        for i, input_str in enumerate(inputs):
            row_id = ids[i]
            
            # 1. Filter Check (ILIKE '{'deal'%')
            if not input_str or not input_str.strip().startswith("{'deal'"):
                 continue
            
            # 2. Parse JSON
            json_data = safe_eval(input_str)
            if not json_data:
                error_ids.append(row_id)
                continue
            
            try:
                deal_info = json_data.get('deal', {})
                # Flatten structure
                
                # Basic fields
                record = {
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
                    
                    # Timestamps
                    "created_at": parse_ts(deal_info.get("created_at")),
                    "updated_at": parse_ts(deal_info.get("updated_at")),
                    "last_change_status_at": parse_ts(deal_info.get("last_change_status_at")),
                    "last_change_stage_at": parse_ts(deal_info.get("last_change_stage_at")),
                    
                    # Complex Objects as Strings
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
                    
                    # Optional Numeric fields (convert safely)
                    "fb_psid": float(deal_info.get("fb_psid")) if deal_info.get("fb_psid") else None,
                    "csa_acc": float(deal_info.get("csa_acc")) if deal_info.get("csa_acc") else None,
                    "commentator_id": float(deal_info.get("commentator_id")) if deal_info.get("commentator_id") else None,
                    "zalo_page_id": float(deal_info.get("zalo_page_id")) if deal_info.get("zalo_page_id") else None,
                    "convert_by": float(deal_info.get("convert_by")) if deal_info.get("convert_by") else None,

                    # String fields
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

                    # Partition
                    "snapshot_date": self.snapshot_date,
                    "ingestion_timestamp": datetime.now()
                }
                success_rows.append(record)
            except Exception as e:
                # Any transformation error results in error record
                # logger.error(e)
                error_ids.append(row_id)
        
        return success_rows, error_ids

    def run(self):
        # 1. Load Tables
        tbl_error = self.ensure_table_error_exists()
        tbl_target = self.ensure_table_exists()
        
        try:
            # Load Bronze Table from Catalog (Bronze Catalog needs to be loaded if different? Assuming shared or accessible via same credentials)
            # Actually, standard design uses different catalog instances or same one.
            # Let's assume SAME credentials, different URI maybe?
            # Re-using CATALOG_NAME_BRONZE env var logic might be safer.
            
            # Dynamic linking to Bronze Catalog
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

        # 2. Transform
        success_data, error_ids = self.parse_and_transform(tbl_source)
        
        logger.info(f"Transformation Result: {len(success_data)} Success, {len(error_ids)} Errors")

        # 3. Write Target (Overwrite Partition)
        if success_data:
            df = pa.Table.from_pylist(success_data)
            logger.info(f"Overwriting partition {self.snapshot_date} in {TARGET_TABLE}")
            tbl_target.overwrite(df, overwrite_filter=EqualTo("snapshot_date", self.snapshot_date))
        else:
             # If no data, we still assume successful run (just empty result)
             # But might want to clear partition?
             logger.warning("No success data to write.")
        
        # 4. Write Errors (Overwrite Partition)
        if error_ids:
            error_records = [
                {"id": eid, "snapshot_date": self.snapshot_date, "ingestion_timestamp": datetime.now()} 
                for eid in error_ids
            ]
            df_err = pa.Table.from_pylist(error_records)
            logger.info(f"Overwriting partition {self.snapshot_date} in {TARGET_TABLE_ERROR}")
            tbl_error.overwrite(df_err, overwrite_filter=EqualTo("snapshot_date", self.snapshot_date))
        else:
             logger.info("No error records found.")

        logger.success("Silver Transformation Completed Successfully.")

def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else date.today()
    
    start = datetime.now()
    job = WebhookTransformation(snapshot_date)
    job.run()
    logger.info(f"Total Duration: {(datetime.now() - start).total_seconds()}s")

if __name__ == "__main__":
    main()
