"""
ACB FX – helper Iceberg (theo khuôn app/hci_care/hci_care_lakehouse.py).

Namespace `acb_fx` ở 2 catalog bronze / silver, mỗi tầng 1 bảng `ty_gia_acb`, partition theo
snapshot_date (= ngày chạy theo giờ VN, xem SNAPSHOT_ARGS trong DAG). Mỗi partition chứa các ngày tỷ giá mà lượt chạy
đó lấy về (mặc định 7 ngày gần nhất, hoặc khoảng ngày khi backfill).
- Ghi theo kiểu "xoá partition rồi append" -> chạy lại cùng lượt không nhân bản.
- Chỉ giữ KEEP_DAYS partition gần nhất (mặc định 7); lịch sử đầy đủ nằm ở Postgres.
"""

import os
from datetime import date, timedelta
from typing import Dict, List

import pyarrow as pa
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo, LessThan
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType, TimestampType

NAMESPACE = os.getenv("ACB_FX_NAMESPACE", "acb_fx")
TABLE = "ty_gia_acb"
KEEP_DAYS = int(os.getenv("ACB_FX_KEEP_DAYS", "7"))

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

BRONZE_COLUMNS = [
    ("ngay", DateType),
    ("effective_datetime", StringType),
    ("currency", StringType),
    ("exchange_currency", StringType),
    ("exchange_rate", DoubleType),
    ("deal_type", StringType),
    ("instrument_type", StringType),
    ("max_sequence", LongType),
    ("denomination", LongType),
    ("region", LongType),
    ("raw", StringType),
] + META_COLUMNS

SILVER_COLUMNS = [
    ("ngay", DateType),
    ("loai_tien", StringType),
    ("deal_type", StringType),
    ("instrument_type", StringType),
    ("menh_gia", LongType),
    ("ty_gia", DoubleType),
    ("max_sequence", LongType),
    ("effective_datetime", StringType),
] + META_COLUMNS


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


def build_schema(columns) -> Schema:
    return Schema(*[NestedField(i + 1, name, typ(), required=False)
                    for i, (name, typ) in enumerate(columns)])


def arrow_schema(columns) -> pa.Schema:
    return schema_to_pyarrow(build_schema(columns))


def _create(cat, ident: str, columns):
    schema = build_schema(columns)
    snap_id = schema.find_field("snapshot_date").field_id
    spec = PartitionSpec(PartitionField(source_id=snap_id, field_id=1000,
                                        transform=IdentityTransform(), name="snapshot_date"))
    logger.info(f"Tạo bảng Iceberg {ident}")
    return cat.create_table(identifier=ident, schema=schema, partition_spec=spec)


def ensure_table(zone: str, columns):
    """Load bảng, tạo mới nếu chưa có. Schema lệch (thêm/đổi cột) -> xoá tạo lại
    (Iceberg chỉ là vùng đệm vài ngày, lịch sử đầy đủ nằm ở Postgres)."""
    cat = get_catalog(zone)
    ident = f"{NAMESPACE}.{TABLE}"
    try:
        tbl = cat.load_table(ident)
    except NoSuchTableError:
        return _create(cat, ident, columns)
    existing = [(f.name, type(f.field_type)) for f in tbl.schema().fields]
    if existing == [(n, t) for n, t in columns]:
        return tbl
    logger.warning(f"{zone}.{ident}: schema khác định nghĩa -> xoá bảng Iceberg và tạo lại")
    cat.drop_table(ident)
    return _create(cat, ident, columns)


def load_table(zone: str):
    return get_catalog(zone).load_table(f"{NAMESPACE}.{TABLE}")


# ============================================================================
# ĐỌC / GHI
# ============================================================================

def rows_to_arrow(rows: List[Dict], columns, snapshot_date: date, now) -> pa.Table:
    schema = arrow_schema(columns)
    data = {name: [r.get(name) for r in rows] for name, _ in columns
            if name not in ("snapshot_date", "ingestion_timestamp")}
    data["snapshot_date"] = [snapshot_date] * len(rows)
    data["ingestion_timestamp"] = [now] * len(rows)
    return pa.Table.from_pydict(data, schema=schema)


def overwrite_partition(tbl, data: pa.Table, snapshot_date: date) -> int:
    tbl.delete(EqualTo("snapshot_date", snapshot_date.isoformat()))
    if data.num_rows:
        tbl.append(data)
    return data.num_rows


def drop_old_partitions(tbl, snapshot_date: date):
    cutoff = snapshot_date - timedelta(days=KEEP_DAYS - 1)
    tbl.delete(LessThan("snapshot_date", cutoff.isoformat()))


def read_partition(tbl, snapshot_date: date) -> pa.Table:
    return tbl.scan(row_filter=EqualTo("snapshot_date", snapshot_date.isoformat())).to_arrow()
