"""
HCI Care – BRONZE ingestion
Postgres app hci_care (public.*) -> Iceberg bronze.hci_care.<bảng>  (bản chụp FULL mỗi ngày)

- Lấy mọi bảng BASE TABLE trong schema public (hoặc chỉ các bảng ở --tables).
- Bỏ cột bí mật / bytea (xem hci_care_lakehouse.is_excluded).
- Đọc trong 1 transaction REPEATABLE READ, READ ONLY -> mọi bảng cùng một thời điểm,
  không thể ghi nhầm vào DB của app.
- Bảng lỗi -> ghi log, chạy tiếp bảng khác, cuối cùng exit 1 (Airflow FAILED).

Env: SOURCE_URI (DB app), ICEBERG_REST_URI_BRONZE, S3_*, AWS_*
Ví dụ:
  python hci_care_bronze_ingestion.py --snapshot-date 2026-10-06 --tables app_booking,app_referral
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extensions
import pyarrow as pa
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hci_care.hci_care_lakehouse import (META_COLUMNS, build_schema, drop_old_partitions,  # noqa: E402
                                         ensure_table, is_excluded, map_source_type,
                                         overwrite_partition, parse_tables_arg)
from pyiceberg.io.pyarrow import schema_to_pyarrow  # noqa: E402

SOURCE_URI = os.getenv("SOURCE_URI")
FETCH_ROWS = int(os.getenv("HCI_CARE_FETCH_ROWS", "20000"))

COLUMNS_SQL = """
SELECT c.table_name, c.column_name, c.udt_name
FROM information_schema.columns c
JOIN information_schema.tables t
  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE'
ORDER BY c.table_name, c.ordinal_position
"""


def source_columns(conn):
    out = {}
    with conn.cursor() as cur:
        cur.execute(COLUMNS_SQL)
        for table, column, udt in cur.fetchall():
            out.setdefault(table, []).append((column, udt))
    return out


def read_table(conn, table, cols, snapshot_date, now):
    kept = [(c, u) for c, u in cols if not is_excluded(table, c, u)]
    if not kept:
        return None, None, 0
    mapped = [(c, *map_source_type(c, u)) for c, u in kept]
    ice_cols = [(c, t) for c, t, _ in mapped] + META_COLUMNS
    arrow_schema = schema_to_pyarrow(build_schema(ice_cols))
    data_fields = [arrow_schema.field(i) for i in range(len(mapped))]
    sql = f'SELECT {", ".join(expr for _, _, expr in mapped)} FROM public."{table}"'

    batches = []
    with conn.cursor(name=f"hci_{table}"[:60]) as cur:
        cur.itersize = FETCH_ROWS
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(FETCH_ROWS)
            if not rows:
                break
            cols_values = list(zip(*rows))
            arrays = [pa.array(list(v), type=f.type) for v, f in zip(cols_values, data_fields)]
            n = len(rows)
            arrays.append(pa.array([snapshot_date] * n, type=pa.date32()))
            arrays.append(pa.array([now] * n, type=pa.timestamp("us")))
            batches.append(pa.Table.from_arrays(arrays, schema=arrow_schema))
    data = pa.concat_tables(batches) if batches else arrow_schema.empty_table()
    return ice_cols, data, len(cols) - len(kept)


def parse_args():
    p = argparse.ArgumentParser(description="HCI Care bronze ingestion")
    p.add_argument("--snapshot-date", default=None, help="YYYY-MM-DD. Mặc định: hôm qua")
    p.add_argument("--tables", default=None, help="Danh sách bảng, cách nhau dấu phẩy. Trống = tất cả")
    a = p.parse_args()
    a.snapshot_date = None if a.snapshot_date in (None, "", "None") else a.snapshot_date
    return a


def main():
    args = parse_args()
    if not SOURCE_URI:
        logger.error("Thiếu env SOURCE_URI (Airflow connection hci_care_product)")
        sys.exit(1)
    snapshot_date = (datetime.strptime(args.snapshot_date, "%Y-%m-%d").date() if args.snapshot_date
                     else datetime.now().date() - timedelta(days=1))
    now = datetime.now()

    conn = psycopg2.connect(SOURCE_URI)
    conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
                     readonly=True, autocommit=False)
    all_cols = source_columns(conn)
    wanted = parse_tables_arg(args.tables)
    if wanted:
        missing = [t for t in wanted if t not in all_cols]
        if missing:
            logger.error(f"Không có bảng trong DB nguồn: {missing}")
            sys.exit(1)
        all_cols = {t: all_cols[t] for t in wanted}
    logger.info(f"Bronze hci_care | snapshot {snapshot_date} | {len(all_cols)} bảng")

    failed, total_rows = [], 0
    for i, (table, cols) in enumerate(all_cols.items(), start=1):
        try:
            ice_cols, data, n_excluded = read_table(conn, table, cols, snapshot_date, now)
            if ice_cols is None:
                logger.warning(f"[{i}/{len(all_cols)}] {table}: mọi cột bị loại -> bỏ qua")
                continue
            tbl = ensure_table("bronze", table, ice_cols)
            n = overwrite_partition(tbl, data, snapshot_date)
            drop_old_partitions(tbl, snapshot_date)
            total_rows += n
            note = f" (bỏ {n_excluded} cột bí mật/nhị phân)" if n_excluded else ""
            logger.success(f"✅ [{i}/{len(all_cols)}] {table}: {n} dòng{note}")
        except Exception as e:
            logger.error(f"❌ [{i}/{len(all_cols)}] {table}: {e}")
            failed.append(table)
            conn.rollback()  # transaction lỗi -> mở lại để đọc bảng tiếp theo
    conn.close()

    logger.info(f"Tổng {total_rows} dòng, {len(all_cols) - len(failed)}/{len(all_cols)} bảng OK")
    if failed:
        logger.error(f"{len(failed)} bảng lỗi: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
