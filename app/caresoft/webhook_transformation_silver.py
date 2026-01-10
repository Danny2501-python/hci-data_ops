
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
from datetime import datetime, date
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from loguru import logger
import gc
import duckdb

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
                NestedField(1, "pk_id", LongType(), required=True),
                NestedField(2, "id", LongType(), required=False),
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

            schema = Schema(
                NestedField(1, "pk_id", LongType(), required=True),
                NestedField(2, "id", LongType(), required=False),
                
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
                NestedField(31, "fb_psid", LongType(), required=False),
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
                
                NestedField(100, "snapshot_date", DateType(), required=False),
                NestedField(101, "ingestion_timestamp", TimestampType(), required=False),
            )
            
            partition_spec = PartitionSpec(
                PartitionField(source_id=100, field_id=1000, transform=IdentityTransform(), name="snapshot_date")
            )
            
            return self.catalog.create_table(TARGET_TABLE, schema=schema, partition_spec=partition_spec)


    def _transform_with_duckdb(self, con, raw_data):
        """
        Helper method: Transform raw data using DuckDB SQL
        Eliminates code duplication (used in both main loop and final batch)
        
        Args:
            con: DuckDB connection
            raw_data: List of dict records to transform
            
        Returns:
            PyArrow Table (optimized - no conversion to pylist)
        """
        raw_arrow = pa.Table.from_pylist(raw_data)
        
        res = con.sql("""
            SELECT
                TRY_CAST(pk_id AS BIGINT) as pk_id,
                id,
                
                TRY_CAST(deal_id AS INTEGER) as deal_id,
                CAST(name AS VARCHAR) as name,
                TRY_CAST(value AS INTEGER) as value,
                TRY_CAST(probability AS INTEGER) as probability,
                TRY_CAST(pipeline_id AS INTEGER) as pipeline_id,
                TRY_CAST(pipeline_stage_id AS INTEGER) as pipeline_stage_id,
                TRY_CAST(closed_type AS INTEGER) as closed_type,
                CAST(source AS VARCHAR) as source,
                TRY_CAST(source_detail_id AS INTEGER) as source_detail_id,
                TRY_CAST(service_id AS INTEGER) as service_id,
                TRY_CAST(requester_id AS INTEGER) as requester_id,
                TRY_CAST(assignee_id AS INTEGER) as assignee_id,
                CAST(facebook_page_id AS VARCHAR) as facebook_page_id,
                TRY_CAST(ticket_source_end_status AS INTEGER) as ticket_source_end_status,
                
                to_timestamp(TRY_CAST(created_at AS DOUBLE) / 1000) as created_at,
                to_timestamp(TRY_CAST(updated_at AS DOUBLE) / 1000) as updated_at,
                to_timestamp(TRY_CAST(last_change_status_at AS DOUBLE) / 1000) as last_change_status_at,
                to_timestamp(TRY_CAST(last_change_stage_at AS DOUBLE) / 1000) as last_change_stage_at,
                
                CAST(activity AS VARCHAR) as activity,
                CAST(addition_fields AS VARCHAR) as addition_fields,
                CAST(call_info AS VARCHAR) as call_info,
                CAST(agent_submit AS VARCHAR) as agent_submit,
                CAST(updated_properties AS VARCHAR) as updated_properties,
                CAST(account_id AS VARCHAR) as account_id,
                CAST(activity_source AS VARCHAR) as activity_source,
                CAST(action AS VARCHAR) as action,
                CAST(convert_from_object AS VARCHAR) as convert_from_object,
                CAST(object AS VARCHAR) as object,
                
                TRY_CAST(fb_psid AS BIGINT) as fb_psid,
                CAST(gclid AS VARCHAR) as gclid,
                TRY_CAST(convert_by AS DOUBLE) as convert_by,
                TRY_CAST(csa_acc AS DOUBLE) as csa_acc,
                CAST(addition_details AS VARCHAR) as addition_details,
                TRY_CAST(commentator_id AS DOUBLE) as commentator_id,
                CAST(comment AS VARCHAR) as comment,
                CAST(convert_type AS VARCHAR) as convert_type,
                CAST(convert_at AS VARCHAR) as convert_at,
                CAST(closed_by AS VARCHAR) as closed_by,
                TRY_CAST(zalo_page_id AS DOUBLE) as zalo_page_id,
                CAST(closed_at AS VARCHAR) as closed_at,
                CAST(unqualified_reasons AS VARCHAR) as unqualified_reasons,
                CAST(lost_reasons AS VARCHAR) as lost_reasons,
                CAST(estimated_closed_date AS VARCHAR) as estimated_closed_date,
                CAST(utm_campaign AS VARCHAR) as utm_campaign,
                
                snapshot_date,
                CAST(now() AS TIMESTAMP) as ingestion_timestamp
            FROM raw_arrow
        """).arrow()
        
        # Return Arrow Table directly - no conversion to pylist
        return res.read_all()

    def parse_and_transform_batch(self, bronze_table):
        """
        Generator function that yields batches of (success_rows, error_ids)
        Uses batch streaming and DuckDB for efficient SQL-based transformation
        """
        BATCH_SIZE = 20000
        
        logger.info(f"Scanning Bronze table {SOURCE_TABLE} for {self.snapshot_date}...")
        logger.info(f"Processing batch size: {BATCH_SIZE:,} records")
        
        from pyiceberg.expressions import And, StartsWith
        
        scan = bronze_table.scan(
            row_filter=And(
                EqualTo("snapshot_date", self.snapshot_date),
                StartsWith("input", "{'deal'")
            ),
            selected_fields=("pk_id", "id", "input")
        )
        
        def safe_eval(s):
            try:
                if not s or not s.strip():
                    return None
                data = ast.literal_eval(s)
                if not isinstance(data, dict) or 'deal' not in data:
                    return None
                return data
            except:
                return None

        total_processed = 0
        con = duckdb.connect()
        
        accumulated_raw_data = []
        accumulated_errors = []
        
        EMPTY_JSON = json.dumps({})
        
        for arrow_batch in scan.to_arrow_batch_reader():
            batch_size = len(arrow_batch)
            
            for i in range(batch_size):
                pk = arrow_batch["pk_id"][i].as_py()
                row_id = arrow_batch["id"][i].as_py()
                input_str = arrow_batch["input"][i].as_py()
                
                data_dict = safe_eval(input_str)
                if not data_dict:
                     accumulated_errors.append((pk, row_id))
                     continue
                
                try:
                    deal_info = data_dict.get('deal', {})
                    
                    def safe_str(val):
                        """Convert value to string, handling None and booleans"""
                        if val is None:
                            return None
                        if isinstance(val, bool):
                            return str(int(val))
                        return str(val) if val != '' else None
                    
                    def safe_json(val):
                        """Safely serialize value - handles dict, list, and primitives"""
                        if not val:
                            return EMPTY_JSON
                        if isinstance(val, (dict, list)):
                            return json.dumps(val)
                        return str(val)
                    
                    row = {
                        "pk_id": pk,
                        "id": row_id,
                        "deal_id": safe_str(deal_info.get("deal_id")),
                        "value": safe_str(deal_info.get("value")),
                        "probability": safe_str(deal_info.get("probability")),
                        "pipeline_id": safe_str(deal_info.get("pipeline_id")),
                        "pipeline_stage_id": safe_str(deal_info.get("pipeline_stage_id")),
                        "closed_type": safe_str(deal_info.get("closed_type")),
                        "source_detail_id": safe_str(deal_info.get("source_detail_id")),
                        "service_id": safe_str(deal_info.get("service_id")),
                        "requester_id": safe_str(deal_info.get("requester_id")),
                        "assignee_id": safe_str(deal_info.get("assignee_id")),
                        "ticket_source_end_status": safe_str(deal_info.get("ticket_source_end_status")),
                        
                        "created_at": safe_str(deal_info.get("created_at")),
                        "updated_at": safe_str(deal_info.get("updated_at")),
                        "last_change_status_at": safe_str(deal_info.get("last_change_status_at")),
                        "last_change_stage_at": safe_str(deal_info.get("last_change_stage_at")),
                        
                        "name": safe_str(deal_info.get("name")),
                        "source": safe_str(deal_info.get("source")),
                        "facebook_page_id": safe_str(deal_info.get("facebook_page_id")),
                        "activity": safe_str(deal_info.get("activity")),
                        "addition_fields": safe_str(deal_info.get("addition_fields")),
                        "gclid": safe_str(deal_info.get("gclid")),
                        "addition_details": safe_str(deal_info.get("addition_details")),
                        "comment": safe_str(deal_info.get("comment")),
                        "convert_type": safe_str(deal_info.get("convert_type")),
                        "convert_at": safe_str(deal_info.get("convert_at")),
                        "closed_by": safe_str(deal_info.get("closed_by")),
                        "closed_at": safe_str(deal_info.get("closed_at")),
                        "unqualified_reasons": safe_str(deal_info.get("unqualified_reasons")),
                        "lost_reasons": safe_str(deal_info.get("lost_reasons")),
                        "estimated_closed_date": safe_str(deal_info.get("estimated_closed_date")),
                        "utm_campaign": safe_str(deal_info.get("utm_campaign")),
                        
                        "call_info": safe_json(data_dict.get("call_info")),
                        "agent_submit": safe_json(data_dict.get("agent_submit")),
                        "updated_properties": safe_json(data_dict.get("updated_properties")),
                        "account_id": safe_json(data_dict.get("account_id")),
                        "activity_source": safe_json(data_dict.get("activity_source")),
                        "action": safe_json(data_dict.get("action")),
                        "convert_from_object": safe_json(data_dict.get("convert_from_object")),
                        "object": safe_json(data_dict.get("object")),
                        
                        "fb_psid": safe_str(deal_info.get("fb_psid")),
                        "csa_acc": safe_str(deal_info.get("csa_acc")),
                        "commentator_id": safe_str(deal_info.get("commentator_id")),
                        "zalo_page_id": safe_str(deal_info.get("zalo_page_id")),
                        "convert_by": safe_str(deal_info.get("convert_by")),
                        
                        "snapshot_date": self.snapshot_date
                    }
                    accumulated_raw_data.append(row)
                    
                    # Check and flush immediately after each append
                    if len(accumulated_raw_data) >= BATCH_SIZE:
                        logger.info(f"Processing accumulated batch of {len(accumulated_raw_data):,} records (total scanned so far: ~{total_processed + i + 1:,})")
                        
                        batch_success = None  # Arrow Table or None
                        if accumulated_raw_data:
                            try:
                                batch_success = self._transform_with_duckdb(con, accumulated_raw_data)
                            except Exception as e:
                                logger.error(f"DuckDB Transformation Error: {e}")
                                for row in accumulated_raw_data:
                                    accumulated_errors.append((row["pk_id"], row["id"]))

                        yield batch_success, accumulated_errors
                        
                        accumulated_raw_data = []
                        accumulated_errors = []
                        gc.collect()
                        
                except Exception:
                    accumulated_errors.append((pk, row_id))

            total_processed += batch_size
            
            del arrow_batch
            gc.collect()
        
        if accumulated_raw_data or accumulated_errors:
            logger.info(
                f"Processing final batch: {len(accumulated_raw_data):,} records, "
                f"{len(accumulated_errors):,} accumulated errors"
            )
            
            batch_success = None  # Arrow Table or None
            if accumulated_raw_data:
                try:
                    batch_success = self._transform_with_duckdb(con, accumulated_raw_data)
                except Exception as e:
                    logger.error(f"DuckDB Transformation Error: {e}")
                    for row in accumulated_raw_data:
                         accumulated_errors.append((row["pk_id"], row["id"]))
            
            yield batch_success, accumulated_errors
        
        con.close()
        logger.info(f"Completed processing {total_processed:,} total records")

    def run(self):
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

        
        logger.info(f"Cleaning existing partition {self.snapshot_date}...")
        try:
            tbl_target.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
            tbl_error.delete(delete_filter=EqualTo("snapshot_date", self.snapshot_date))
        except Exception as e:
            logger.warning(f"Delete partition warning (first run?): {e}")

        total_success = 0
        total_error = 0
        
        target_schema = tbl_target.schema().as_arrow()
        error_schema = tbl_error.schema().as_arrow()

        for success_batch, error_batch in self.parse_and_transform_batch(tbl_source):
            if success_batch is not None:
                try:
                    # Reorder columns to match target schema, then cast types
                    column_order = target_schema.names
                    df = success_batch.select(column_order).cast(target_schema)
                    tbl_target.append(df)
                    total_success += len(success_batch)
                except Exception as e:
                    logger.error(f"Error appending success batch: {e}")
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
