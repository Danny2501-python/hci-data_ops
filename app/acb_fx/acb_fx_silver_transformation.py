"""
ACB FX – SILVER transformation
bronze.acb_fx.ty_gia_acb -> silver.acb_fx.ty_gia_acb (cùng snapshot_date)

- Chỉ giữ ngoại tệ quy ra VND: bỏ dòng vàng (ACB/XAU, có region), dòng thiếu loại giá,
  tỷ giá rỗng hoặc <= 0.
- Đổi tên cột: loai_tien, ty_gia, menh_gia (0 = không phân mệnh giá).
- Bỏ trùng theo (ngay, loai_tien, deal_type, instrument_type, menh_gia).
- Partition bronze rỗng -> exit 1.

Ví dụ:
  python acb_fx_silver_transformation.py --snapshot-date 2026-10-08
"""

import argparse
import os
import sys

from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from acb_fx.acb_fx_core import now_vn, parse_date_arg, to_silver_rows  # noqa: E402
from acb_fx.acb_fx_lakehouse import (SILVER_COLUMNS, drop_old_partitions, ensure_table,  # noqa: E402
                                     load_table, overwrite_partition, read_partition, rows_to_arrow)


def parse_args():
    p = argparse.ArgumentParser(description="ACB FX silver transformation")
    p.add_argument("--snapshot-date", required=True)
    return p.parse_args()


def main():
    snapshot_date = parse_date_arg(parse_args().snapshot_date)
    logger.info(f"Silver acb_fx | snapshot {snapshot_date}")

    bronze = read_partition(load_table("bronze"), snapshot_date).to_pylist()
    if not bronze:
        logger.error(f"bronze.acb_fx.ty_gia_acb không có dữ liệu snapshot {snapshot_date}")
        sys.exit(1)

    rows, stats = to_silver_rows(bronze)
    logger.info(f"{len(bronze)} dòng bronze -> {len(rows)} dòng silver | loại: {stats}")
    if not rows:
        logger.error("Không còn dòng ngoại tệ nào sau khi lọc")
        sys.exit(1)

    tbl = ensure_table("silver", SILVER_COLUMNS)
    n = overwrite_partition(tbl, rows_to_arrow(rows, SILVER_COLUMNS, snapshot_date, now_vn()),
                            snapshot_date)
    drop_old_partitions(tbl, snapshot_date)
    days = sorted({r["ngay"] for r in rows})
    logger.success(f"✅ silver.acb_fx.ty_gia_acb: {n} dòng, {len(days)} ngày ({days[0]} -> {days[-1]})")


if __name__ == "__main__":
    main()
