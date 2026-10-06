"""
HCI Care (App Kangnam Care) lakehouse helpers.

Nguồn: Postgres DB của app (Odoo) `hci_care`, schema public -> Iceberg namespace `hci_care`
ở 2 catalog bronze / silver. Mỗi bảng nguồn = 1 bảng Iceberg cùng tên, partition theo
snapshot_date. Mỗi lần chạy là 1 bản chụp FULL của bảng nguồn.

- Kiểu dữ liệu: int2/int4 -> int, int8 -> long, float/numeric -> double, bool, date,
  timestamp (timestamptz đổi về UTC), còn lại (varchar, text, jsonb, ...) -> string.
  Odoo lưu timestamp theo UTC (naive) -> khi làm báo cáo nhớ +7h.
- Cột bí mật (mật khẩu, token, OTP, khoá, ...) và cột file nhị phân (bytea) KHÔNG được kéo.
- Ghi theo kiểu "xoá partition rồi append" -> chạy lại cùng ngày không nhân bản.
- Chỉ giữ KEEP_DAYS partition gần nhất (mặc định 3), partition cũ hơn bị xoá logic.
"""

import os
import re
from datetime import date, timedelta
from typing import Dict, List, Tuple

import pyarrow as pa
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo, LessThan
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (BooleanType, DateType, DoubleType, IntegerType, LongType, NestedField,
                             StringType, TimestampType)

NAMESPACE = os.getenv("HCI_CARE_NAMESPACE", "hci_care")
KEEP_DAYS = int(os.getenv("HCI_CARE_KEEP_DAYS", "3"))
APPEND_BATCH_ROWS = int(os.getenv("HCI_CARE_APPEND_BATCH_ROWS", "50000"))

S3_PROPS = {
    "s3.endpoint": os.getenv("S3_ENDPOINT"),
    "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
    "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "s3.region": os.getenv("AWS_REGION", "us-east-1"),
}

ZONES = {
    "bronze": (os.getenv("CATALOG_NAME_BRONZE", "bronze"), os.getenv("ICEBERG_REST_URI_BRONZE")),
    "silver": (os.getenv("CATALOG_NAME_SILVER", "silver"), os.getenv("ICEBERG_REST_URI_SILVER")),
}

META_COLUMNS = [("snapshot_date", DateType), ("ingestion_timestamp", TimestampType)]

# ============================================================================
# CỘT KHÔNG KÉO (bí mật / nhị phân)
# ============================================================================

# Cột cụ thể theo bảng (tên chung chung, regex không bắt được)
EXCLUDED_COLUMNS = {
    "res_users_apikeys": {"key"},          # API key (hash)
    "auth_totp_device": {"key"},           # khoá 2FA
    "ir_config_parameter": {"value"},      # có database.secret và các khoá cấu hình
}

SECRET_PATTERN = re.compile(
    r"(password|passwd|secret|private_key|api_key$|^otp$|_token$|^token_|access_token|refresh_token)"
)


def is_excluded(table: str, column: str, udt_name: str) -> bool:
    return (udt_name == "bytea"
            or column in EXCLUDED_COLUMNS.get(table, set())
            or bool(SECRET_PATTERN.search(column)))


# ============================================================================
# KIỂU DỮ LIỆU
# ============================================================================

def map_source_type(column: str, udt_name: str) -> Tuple[type, str]:
    """udt_name Postgres -> (kiểu Iceberg, biểu thức SELECT)."""
    c = f'"{column}"'
    if udt_name in ("int2", "int4"):
        return IntegerType, c
    if udt_name == "int8":
        return LongType, c
    if udt_name in ("float4", "float8", "numeric"):
        return DoubleType, f"{c}::float8"
    if udt_name == "bool":
        return BooleanType, c
    if udt_name == "date":
        return DateType, c
    if udt_name == "timestamp":
        return TimestampType, c
    if udt_name == "timestamptz":
        return TimestampType, f"({c} AT TIME ZONE 'UTC')"
    return StringType, f"{c}::text"


# ============================================================================
# CATALOG & BẢNG
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


def list_tables(zone: str) -> List[str]:
    return sorted(ident[-1] for ident in get_catalog(zone).list_tables(NAMESPACE))


def build_schema(columns) -> Schema:
    return Schema(*[NestedField(i + 1, name, typ(), required=False)
                    for i, (name, typ) in enumerate(columns)])


def table_columns(tbl) -> List[Tuple[str, type]]:
    return [(f.name, type(f.field_type)) for f in tbl.schema().fields]


def _create(cat, ident: str, columns):
    schema = build_schema(columns)
    snap_id = schema.find_field("snapshot_date").field_id
    spec = PartitionSpec(PartitionField(source_id=snap_id, field_id=1000,
                                        transform=IdentityTransform(), name="snapshot_date"))
    logger.info(f"Tạo bảng Iceberg {ident}")
    return cat.create_table(identifier=ident, schema=schema, partition_spec=spec)


def ensure_table(zone: str, table: str, columns):
    """
    Load bảng, tạo mới nếu chưa có.
    Nguồn thêm cột -> thêm cột vào bảng Iceberg. Nguồn đổi kiểu cột -> xoá bảng tạo lại
    (mỗi partition là bản chụp full nên không mất gì quan trọng).
    """
    cat = get_catalog(zone)
    ident = f"{NAMESPACE}.{table}"
    try:
        tbl = cat.load_table(ident)
    except NoSuchTableError:
        return _create(cat, ident, columns)

    existing = dict(table_columns(tbl))
    if all(existing.get(n) is t for n, t in columns):
        return tbl
    changed = [n for n, t in columns if n in existing and existing[n] is not t]
    if not changed:
        new = [n for n, _ in columns if n not in existing]
        logger.info(f"{zone}.{ident}: thêm cột {new}")
        with tbl.update_schema() as upd:
            upd.union_by_name(build_schema(columns))
        return cat.load_table(ident)
    logger.warning(f"{zone}.{ident}: cột đổi kiểu {changed} -> xoá bảng Iceberg và tạo lại")
    cat.drop_table(ident)
    return _create(cat, ident, columns)


# ============================================================================
# ĐỌC / GHI
# ============================================================================

def align_to_table(tbl, data: pa.Table) -> pa.Table:
    """Sắp cột theo schema bảng Iceberg; cột thiếu -> null."""
    target = schema_to_pyarrow(tbl.schema())
    arrays = []
    for field in target:
        if field.name in data.column_names:
            arrays.append(data.column(field.name).cast(field.type))
        else:
            arrays.append(pa.nulls(data.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=target)


def delete_partition(tbl, snapshot_date: date):
    tbl.delete(EqualTo("snapshot_date", snapshot_date.isoformat()))


def append_rows(tbl, data: pa.Table) -> int:
    if data.num_rows == 0:
        return 0
    data = align_to_table(tbl, data)
    for start in range(0, data.num_rows, APPEND_BATCH_ROWS):
        tbl.append(data.slice(start, APPEND_BATCH_ROWS))
    return data.num_rows


def overwrite_partition(tbl, data: pa.Table, snapshot_date: date) -> int:
    delete_partition(tbl, snapshot_date)
    return append_rows(tbl, data)


def drop_old_partitions(tbl, snapshot_date: date):
    cutoff = snapshot_date - timedelta(days=KEEP_DAYS - 1)
    tbl.delete(LessThan("snapshot_date", cutoff.isoformat()))


def read_partition(tbl, snapshot_date: date) -> pa.Table:
    return tbl.scan(row_filter=EqualTo("snapshot_date", snapshot_date.isoformat())).to_arrow()


def parse_tables_arg(value) -> List[str]:
    if value in (None, "", "None"):
        return []
    return [t.strip() for t in str(value).split(",") if t.strip()]
