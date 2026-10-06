"""
HCI Care – SERVING layer
silver.hci_care.<bảng> (Iceberg) -> Postgres <TARGET_SCHEMA>.<bảng>  (mặc định schema hci_care)

- Bảng Postgres tự tạo theo kiểu cột Iceberg; có khoá chính id nếu bảng có cột id.
- Nguồn thêm cột -> ALTER TABLE ADD COLUMN.
- Mỗi bảng 1 transaction: TRUNCATE + COPY rồi COMMIT. Lỗi -> ROLLBACK, bảng giữ dữ liệu cũ
  (không bao giờ để bảng rỗng giữa chừng).
- Chặn ghi đè 0 dòng lên bảng đang có dữ liệu (dấu hiệu upstream lỗi) -> báo lỗi.
- Bảng lỗi -> ghi log, chạy tiếp, cuối cùng exit 1.

Env: POSTGRES_URI (dwh_product_new / dwh_sandbox), TARGET_SCHEMA (mặc định hci_care)
Lưu ý: cột timestamp là giờ UTC như Odoo lưu; cột jsonb nguồn lưu dạng text (dbt ép ::jsonb khi cần).
"""

import argparse
import io
import os
import sys
from datetime import datetime, timedelta

import psycopg2
import pyarrow.csv as pacsv
from loguru import logger
from pyiceberg.types import (BooleanType, DateType, DoubleType, IntegerType, LongType, StringType,
                             TimestampType)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hci_care.hci_care_lakehouse import (NAMESPACE, get_catalog, list_tables,  # noqa: E402
                                         parse_tables_arg, read_partition)

POSTGRES_URI = os.getenv("POSTGRES_URI")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "hci_care")
SKIP_COLUMNS = {"snapshot_date"}

PG_TYPES = {
    StringType: "text", IntegerType: "integer", LongType: "bigint", DoubleType: "double precision",
    BooleanType: "boolean", DateType: "date", TimestampType: "timestamp",
}


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def load_table(conn, table: str, s_tbl, snapshot_date) -> int:
    cols = [(f.name, PG_TYPES[type(f.field_type)]) for f in s_tbl.schema().fields
            if f.name not in SKIP_COLUMNS]
    names = [n for n, _ in cols]
    data = read_partition(s_tbl, snapshot_date).select(names)
    full = f"{q(TARGET_SCHEMA)}.{q(table)}"

    with conn.cursor() as cur:
        col_defs = ", ".join(f"{q(n)} {t}" for n, t in cols)
        pk = f", PRIMARY KEY ({q('id')})" if "id" in names else ""
        cur.execute(f"CREATE TABLE IF NOT EXISTS {full} ({col_defs}{pk})")
        for n, t in cols:
            cur.execute(f"ALTER TABLE {full} ADD COLUMN IF NOT EXISTS {q(n)} {t}")

        if data.num_rows == 0:
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM {full})")
            if cur.fetchone()[0]:
                raise RuntimeError("silver 0 dòng nhưng bảng Postgres đang có dữ liệu -> không ghi đè")

        cur.execute(f"TRUNCATE TABLE {full}")
        if data.num_rows:
            buf = io.BytesIO()
            pacsv.write_csv(data, buf)
            buf.seek(0)
            cur.copy_expert(
                f"COPY {full} ({', '.join(q(n) for n in names)}) FROM STDIN WITH (FORMAT csv, HEADER true)",
                buf)
    conn.commit()
    return data.num_rows


def parse_args():
    p = argparse.ArgumentParser(description="HCI Care serving layer")
    p.add_argument("--snapshot-date", default=None)
    p.add_argument("--tables", default=None)
    a = p.parse_args()
    a.snapshot_date = None if a.snapshot_date in (None, "", "None") else a.snapshot_date
    return a


def main():
    args = parse_args()
    if not POSTGRES_URI:
        logger.error("Thiếu env POSTGRES_URI")
        sys.exit(1)
    snapshot_date = (datetime.strptime(args.snapshot_date, "%Y-%m-%d").date() if args.snapshot_date
                     else datetime.now().date() - timedelta(days=1))
    tables = parse_tables_arg(args.tables) or list_tables("silver")
    logger.info(f"Serving hci_care -> {TARGET_SCHEMA} | snapshot {snapshot_date} | {len(tables)} bảng")

    silver_cat = get_catalog("silver")
    conn = psycopg2.connect(POSTGRES_URI)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {q(TARGET_SCHEMA)}")
    conn.commit()

    failed = []
    for i, table in enumerate(tables, start=1):
        try:
            s_tbl = silver_cat.load_table(f"{NAMESPACE}.{table}")
            n = load_table(conn, table, s_tbl, snapshot_date)
            logger.success(f"✅ [{i}/{len(tables)}] {TARGET_SCHEMA}.{table}: {n} dòng")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ [{i}/{len(tables)}] {table}: {e}")
            failed.append(table)
    conn.close()

    if failed:
        logger.error(f"{len(failed)} bảng lỗi: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
