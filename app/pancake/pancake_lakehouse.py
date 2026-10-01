"""
Pancake lakehouse helpers: cấu hình catalog, schema bảng Iceberg cho Bronze/Silver/Gold,
và hàm đọc/ghi theo partition snapshot_date (giống mô hình CareSoft).

Namespace Iceberg: `pancake` ở cả 3 catalog bronze / silver / gold.
Mọi bảng partition theo snapshot_date (ngày dữ liệu, giờ VN).
Ghi theo kiểu "xoá partition rồi append" -> chạy lại cùng ngày không nhân bản dữ liệu.
"""

import os
from datetime import date
from typing import Dict, List, Optional

import pyarrow as pa
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import And, EqualTo
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (BooleanType, DateType, IntegerType, LongType, NestedField, StringType,
                             TimestampType)

NAMESPACE = "pancake"

S3_PROPS = {
    "s3.endpoint": os.getenv("S3_ENDPOINT"),
    "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
    "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "s3.region": os.getenv("AWS_REGION", "us-east-1"),
}

ZONES = {
    "bronze": (os.getenv("CATALOG_NAME_BRONZE", "bronze"), os.getenv("ICEBERG_REST_URI_BRONZE")),
    "silver": (os.getenv("CATALOG_NAME_SILVER", "silver"), os.getenv("ICEBERG_REST_URI_SILVER")),
    "gold": (os.getenv("CATALOG_NAME_GOLD", "gold"), os.getenv("ICEBERG_REST_URI_GOLD")),
}

S, I, L, B, D, T = StringType, IntegerType, LongType, BooleanType, DateType, TimestampType

# ============================================================================
# SCHEMAS (tên cột, kiểu). snapshot_date luôn có, dùng làm partition.
# ============================================================================

BRONZE_COLUMNS = [
    ("page_id", S), ("record_id", S), ("parent_id", S), ("payload", S),
    ("window_since", L), ("window_until", L), ("brand_id", I),
    ("snapshot_date", D), ("ingestion_timestamp", T),
]

BRONZE_TABLES = {
    "conversations": "conversations_api",
    "messages": "messages_api",
    "customers": "page_customers_api",
    "statistics": "statistics_api",
}

SILVER_TABLES = {
    "conversation": [
        ("page_id", S), ("conversation_id", S), ("type", S), ("post_id", S), ("customer_id", S),
        ("customer_psid", S), ("customer_name", S), ("snippet", S), ("message_count", I),
        ("has_phone", B), ("seen", B), ("tags", S), ("assignee_ids", S), ("ad_ids", S),
        ("phone_numbers", S), ("inserted_at", T), ("updated_at", T), ("brand_id", I),
        ("snapshot_date", D), ("ingestion_timestamp", T),
    ],
    "message": [
        ("page_id", S), ("message_id", S), ("conversation_id", S), ("type", S), ("from_id", S),
        ("from_name", S), ("is_from_page", B), ("message", S), ("has_phone", B), ("is_removed", B),
        ("parent_message_id", S), ("attachment_count", I), ("inserted_at", T), ("brand_id", I),
        ("snapshot_date", D), ("ingestion_timestamp", T),
    ],
    "page_customer": [
        ("page_id", S), ("page_customer_id", S), ("customer_id", S), ("psid", S), ("name", S),
        ("thread_id", S), ("gender", S), ("birthday", S), ("lives_in", S), ("phone_numbers", S),
        ("emails", S), ("inserted_at", T), ("updated_at", T), ("brand_id", I),
        ("snapshot_date", D), ("ingestion_timestamp", T),
    ],
    "statistics": [
        ("page_id", S), ("stat_type", S), ("stat_date", D), ("payload", S), ("brand_id", I),
        ("snapshot_date", D), ("ingestion_timestamp", T),
    ],
    "stat_page_hourly": [
        ("page_id", S), ("hour", T), ("stat_date", D),
        ("customer_comment_count", I), ("customer_inbox_count", I), ("inbox_interactive_count", I),
        ("new_customer_count", I), ("new_inbox_count", I), ("page_comment_count", I),
        ("page_inbox_count", I), ("phone_number_count", I), ("uniq_phone_number_count", I),
        ("today_uniq_website_referral", I), ("today_website_guest_referral", I),
        ("brand_id", I), ("snapshot_date", D), ("ingestion_timestamp", T),
    ],
    "stat_user_daily": [
        ("page_id", S), ("stat_date", D), ("user_id", S), ("user_name", S), ("user_fb_id", S),
        ("inbox_count", I), ("unique_inbox_count", I), ("comment_count", I),
        ("unique_comment_count", I), ("private_reply_count", I), ("phone_number_count", I),
        ("average_response_time", L), ("brand_id", I), ("snapshot_date", D), ("ingestion_timestamp", T),
    ],
}


def _without_ingestion(cols):
    return [c for c in cols if c[0] != "ingestion_timestamp"]


GOLD_TABLES = {
    "fct_conversation": _without_ingestion(SILVER_TABLES["conversation"]) + [
        ("inserted_at_vn", T), ("updated_at_vn", T), ("inserted_date_vn", D),
        ("is_from_ad", B), ("first_ad_id", S), ("first_phone", S), ("etl_updated", T),
    ],
    "fct_message": _without_ingestion(SILVER_TABLES["message"]) + [
        ("inserted_at_vn", T), ("sent_date_vn", D), ("sender_role", S), ("etl_updated", T),
    ],
    "dim_page_customer": _without_ingestion(SILVER_TABLES["page_customer"]) + [
        ("inserted_at_vn", T), ("updated_at_vn", T), ("first_phone", S), ("etl_updated", T),
    ],
    "fct_statistics": _without_ingestion(SILVER_TABLES["statistics"]) + [("etl_updated", T)],
    "fct_stat_page_hourly": _without_ingestion(SILVER_TABLES["stat_page_hourly"]) + [
        ("hour_vn", T), ("etl_updated", T),
    ],
    "agg_stat_page_daily": [
        ("page_id", S), ("stat_date", D), ("hours_count", I),
        ("customer_comment_count", L), ("customer_inbox_count", L), ("inbox_interactive_count", L),
        ("new_customer_count", L), ("new_inbox_count", L), ("page_comment_count", L),
        ("page_inbox_count", L), ("phone_number_count", L), ("uniq_phone_number_count", L),
        ("brand_id", I), ("snapshot_date", D), ("etl_updated", T),
    ],
    "fct_stat_user_daily": _without_ingestion(SILVER_TABLES["stat_user_daily"]) + [("etl_updated", T)],
}


# ============================================================================
# CATALOG & TABLE HELPERS
# ============================================================================

_catalogs: Dict[str, object] = {}


def get_catalog(zone: str):
    if zone not in _catalogs:
        name, uri = ZONES[zone]
        if not uri:
            raise RuntimeError(f"Thiếu env ICEBERG_REST_URI_{zone.upper()}")
        cat = load_catalog(name, **{"uri": uri, **S3_PROPS})
        try:
            cat.create_namespace(NAMESPACE)
            logger.info(f"Tạo namespace {zone}.{NAMESPACE}")
        except NamespaceAlreadyExistsError:
            pass
        _catalogs[zone] = cat
    return _catalogs[zone]


def build_schema(columns) -> Schema:
    return Schema(*[NestedField(i + 1, name, typ(), required=False)
                    for i, (name, typ) in enumerate(columns)])


def ensure_table(zone: str, table: str, columns):
    """Load bảng, tạo mới nếu chưa có (partition theo snapshot_date)."""
    cat = get_catalog(zone)
    ident = f"{NAMESPACE}.{table}"
    try:
        return cat.load_table(ident)
    except NoSuchTableError:
        schema = build_schema(columns)
        snap_id = schema.find_field("snapshot_date").field_id
        spec = PartitionSpec(PartitionField(source_id=snap_id, field_id=1000,
                                            transform=IdentityTransform(), name="snapshot_date"))
        logger.info(f"Tạo bảng Iceberg {zone}.{ident}")
        return cat.create_table(identifier=ident, schema=schema, partition_spec=spec)


def overwrite_partition(tbl, records: List[Dict], snapshot_date: date, page_id: Optional[str] = None):
    """
    Xoá partition snapshot_date (và page_id nếu có) rồi append records.
    Không nuốt lỗi: lỗi ghi -> raise để task FAILED.
    """
    flt = EqualTo("snapshot_date", snapshot_date.isoformat())
    if page_id is not None:
        flt = And(flt, EqualTo("page_id", page_id))
    tbl.delete(flt)
    if not records:
        return 0
    arrow_schema = schema_to_pyarrow(tbl.schema())
    cols = [f.name for f in arrow_schema]
    df = pa.Table.from_pylist([{c: r.get(c) for c in cols} for r in records], schema=arrow_schema)
    tbl.append(df)
    return len(records)


def read_partition(tbl, snapshot_date: date, page_id: Optional[str] = None) -> List[Dict]:
    flt = EqualTo("snapshot_date", snapshot_date.isoformat())
    if page_id is not None:
        flt = And(flt, EqualTo("page_id", page_id))
    return tbl.scan(row_filter=flt).to_arrow().to_pylist()
