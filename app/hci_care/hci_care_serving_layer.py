
"""
HCI Care – SERVING layer
silver.hci_care.<bảng> (Iceberg) -> Postgres <TARGET_SCHEMA>.<bảng>

- Bảng Postgres tự tạo theo kiểu cột Iceberg; có khóa chính id nếu có.
- Nguồn thêm cột -> ALTER TABLE ADD COLUMN.
- Mỗi bảng 1 transaction: TRUNCATE + COPY rồi COMMIT.
- Lỗi -> ROLLBACK, giữ nguyên dữ liệu cũ.
- Không ghi đè 0 dòng lên bảng Postgres đang có dữ liệu.
- Riêng hai bảng change_password_user và change_password_wizard:
  Nếu Silver có 0 dòng và Postgres đang có dữ liệu -> SKIPPED.
  Không xóa hoặc ghi đè dữ liệu cũ.
- Các bảng khác vẫn giữ nguyên cơ chế bảo vệ.
- Bảng lỗi -> ghi log, chạy tiếp, cuối cùng exit 1.

Env: POSTGRES_URI (dwh_product_new / dwh_sandbox)
     TARGET_SCHEMA (mặc định hci_care)

Lưu ý:
- Cột timestamp là giờ UTC như Odoo lưu.
- Cột jsonb nguồn lưu dạng text (dbt ép ::jsonb khi cần).
"""

import argparse
import io
import os
import sys
from datetime import datetime, timedelta

import psycopg2
import pyarrow.csv as pacsv
from loguru import logger
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from hci_care.hci_care_lakehouse import (
    NAMESPACE,
    get_catalog,
    list_tables,
    parse_tables_arg,
    read_partition,
)


POSTGRES_URI = os.getenv("POSTGRES_URI")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "hci_care")

SKIP_COLUMNS = {"snapshot_date"}

# Chỉ hai bảng này được phép SKIP khi Silver rỗng
# nhưng PostgreSQL đang có dữ liệu.
SKIP_EMPTY_TABLES = {
    "change_password_user",
    "change_password_wizard",
    "res_users_identitycheck",
}


PG_TYPES = {
    StringType: "text",
    IntegerType: "integer",
    LongType: "bigint",
    DoubleType: "double precision",
    BooleanType: "boolean",
    DateType: "date",
    TimestampType: "timestamp",
}


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def load_table(conn, table: str, s_tbl, snapshot_date) -> int | None:
    cols = [
        (f.name, PG_TYPES[type(f.field_type)])
        for f in s_tbl.schema().fields
        if f.name not in SKIP_COLUMNS
    ]

    names = [n for n, _ in cols]

    data = read_partition(s_tbl, snapshot_date).select(names)

    full = f"{q(TARGET_SCHEMA)}.{q(table)}"

    skip_existing = False

    with conn.cursor() as cur:
        col_defs = ", ".join(
            f"{q(n)} {t}" for n, t in cols
        )

        pk = (
            f", PRIMARY KEY ({q('id')})"
            if "id" in names
            else ""
        )

        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {full} ({col_defs}{pk})"
        )

        for n, t in cols:
            cur.execute(
                f"ALTER TABLE {full} "
                f"ADD COLUMN IF NOT EXISTS {q(n)} {t}"
            )

        # Kiểm tra dữ liệu Silver rỗng
        if data.num_rows == 0:
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM {full})"
            )

            postgres_has_data = cur.fetchone()[0]

            if postgres_has_data:

                # Chỉ bỏ qua hai bảng được chỉ định.
                if table in SKIP_EMPTY_TABLES:
                    skip_existing = True

                else:
                    # Các bảng khác vẫn báo lỗi như cũ.
                    raise RuntimeError(
                        "silver 0 dòng nhưng bảng Postgres "
                        "đang có dữ liệu -> không ghi đè"
                    )

        # Không thực hiện TRUNCATE/COPY nếu SKIPPED.
        if not skip_existing:
            cur.execute(f"TRUNCATE TABLE {full}")

            if data.num_rows:
                buf = io.BytesIO()
                pacsv.write_csv(data, buf)
                buf.seek(0)

                cur.copy_expert(
                    f"COPY {full} "
                    f"({', '.join(q(n) for n in names)}) "
                    "FROM STDIN WITH "
                    "(FORMAT csv, HEADER true)",
                    buf,
                )

    # SKIPPED: rollback cả các thay đổi schema
    # trong transaction, giữ nguyên bảng đích.
    if skip_existing:
        conn.rollback()
        return None

    conn.commit()

    return data.num_rows


def parse_args():
    p = argparse.ArgumentParser(
        description="HCI Care serving layer"
    )

    p.add_argument("--snapshot-date", default=None)
    p.add_argument("--tables", default=None)

    a = p.parse_args()

    a.snapshot_date = (
        None
        if a.snapshot_date in (None, "", "None")
        else a.snapshot_date
    )

    return a


def main():
    args = parse_args()

    if not POSTGRES_URI:
        logger.error("Thiếu env POSTGRES_URI")
        sys.exit(1)

    snapshot_date = (
        datetime.strptime(
            args.snapshot_date,
            "%Y-%m-%d"
        ).date()
        if args.snapshot_date
        else datetime.now().date() - timedelta(days=1)
    )

    tables = (
        parse_tables_arg(args.tables)
        or list_tables("silver")
    )

    logger.info(
        f"Serving hci_care -> {TARGET_SCHEMA} | "
        f"snapshot {snapshot_date} | "
        f"{len(tables)} bảng"
    )

    silver_cat = get_catalog("silver")

    conn = psycopg2.connect(POSTGRES_URI)

    with conn.cursor() as cur:
        cur.execute(
            f"CREATE SCHEMA IF NOT EXISTS {q(TARGET_SCHEMA)}"
        )

    conn.commit()

    failed = []
    skipped = []

    for i, table in enumerate(tables, start=1):
        try:
            s_tbl = silver_cat.load_table(
                f"{NAMESPACE}.{table}"
            )

            n = load_table(
                conn,
                table,
                s_tbl,
                snapshot_date,
            )

            if n is None:
                skipped.append(table)

                logger.warning(
                    f"⏭️ SKIPPED [{i}/{len(tables)}] "
                    f"{TARGET_SCHEMA}.{table}: "
                    "Silver 0 dòng, PostgreSQL có dữ liệu "
                    "-> giữ nguyên dữ liệu cũ"
                )

            else:
                logger.success(
                    f"✅ [{i}/{len(tables)}] "
                    f"{TARGET_SCHEMA}.{table}: {n} dòng"
                )

        except Exception as e:
            conn.rollback()

            logger.error(
                f"❌ [{i}/{len(tables)}] "
                f"{table}: {e}"
            )

            failed.append(table)

    conn.close()

    logger.info(
        f"Hoàn thành Serving: "
        f"{len(tables) - len(failed) - len(skipped)} SUCCESS, "
        f"{len(skipped)} SKIPPED, "
        f"{len(failed)} FAILED"
    )

    if skipped:
        logger.warning(
            f"Các bảng được SKIPPED: {skipped}"
        )

    if failed:
        logger.error(
            f"{len(failed)} bảng lỗi: {failed}"
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
