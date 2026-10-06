"""
HCI Care – SILVER transformation
bronze.hci_care.<bảng> -> silver.hci_care.<bảng>  (cùng snapshot_date)

- Bỏ dòng trùng id (giữ dòng cuối). Dòng id null giữ nguyên.
- Schema silver theo bronze (nguồn thêm cột -> silver tự thêm cột).
- Bảng lỗi -> ghi log, chạy tiếp, cuối cùng exit 1.

Ví dụ:
  python hci_care_silver_transformation.py --snapshot-date 2026-10-06
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hci_care.hci_care_lakehouse import (drop_old_partitions, ensure_table, get_catalog,  # noqa: E402
                                         list_tables, overwrite_partition, parse_tables_arg,
                                         read_partition, table_columns, NAMESPACE)


def dedup_by_id(data: pa.Table) -> pa.Table:
    if "id" not in data.column_names or data.num_rows == 0:
        return data
    valid = pc.is_valid(data.column("id"))
    with_id = data.filter(valid)
    without_id = data.filter(pc.invert(valid))
    numbered = with_id.append_column("__row", pa.array(range(with_id.num_rows), type=pa.int64()))
    keep = numbered.group_by("id").aggregate([("__row", "max")]).column("__row_max")
    keep = pc.take(keep, pc.sort_indices(keep))
    deduped = with_id.take(keep)
    return pa.concat_tables([deduped, without_id]) if without_id.num_rows else deduped


def parse_args():
    p = argparse.ArgumentParser(description="HCI Care silver transformation")
    p.add_argument("--snapshot-date", default=None)
    p.add_argument("--tables", default=None)
    a = p.parse_args()
    a.snapshot_date = None if a.snapshot_date in (None, "", "None") else a.snapshot_date
    return a


def main():
    args = parse_args()
    snapshot_date = (datetime.strptime(args.snapshot_date, "%Y-%m-%d").date() if args.snapshot_date
                     else datetime.now().date() - timedelta(days=1))
    tables = parse_tables_arg(args.tables) or list_tables("bronze")
    logger.info(f"Silver hci_care | snapshot {snapshot_date} | {len(tables)} bảng")

    bronze_cat = get_catalog("bronze")
    failed = []
    for i, table in enumerate(tables, start=1):
        try:
            b_tbl = bronze_cat.load_table(f"{NAMESPACE}.{table}")
            data = read_partition(b_tbl, snapshot_date)
            before = data.num_rows
            data = dedup_by_id(data)
            s_tbl = ensure_table("silver", table, table_columns(b_tbl))
            n = overwrite_partition(s_tbl, data, snapshot_date)
            drop_old_partitions(s_tbl, snapshot_date)
            dup = f" (bỏ {before - n} dòng trùng id)" if before != n else ""
            logger.success(f"✅ [{i}/{len(tables)}] {table}: {n} dòng{dup}")
        except Exception as e:
            logger.error(f"❌ [{i}/{len(tables)}] {table}: {e}")
            failed.append(table)

    if failed:
        logger.error(f"{len(failed)} bảng lỗi: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
