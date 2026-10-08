"""
ACB FX – BRONZE ingestion
API tỷ giá ACB -> Iceberg bronze.acb_fx.ty_gia_acb (partition snapshot_date = lượt chạy)

- Mỗi ngày D trong khoảng [start, end] gọi API 1 lần tại D 23:59:59 giờ VN (tỷ giá chốt ngày).
- Mặc định: 7 ngày gần nhất tới hôm qua (lấy lại để bắt kịp nếu ACB sửa số).
  Backfill: --start-date 2026-09-10 --end-date 2026-10-07.
- Giữ nguyên bản ghi API (kể cả dòng vàng) + JSON gốc; lọc ở silver.
- Bất kỳ ngày nào gọi API lỗi (sau 3 lần thử) -> KHÔNG ghi gì, exit 1 (Airflow retry,
  tầng sau không chạy, Postgres giữ nguyên).

Env: ICEBERG_REST_URI_BRONZE, CATALOG_NAME_BRONZE, S3_ENDPOINT, AWS_*
Ví dụ:
  python acb_fx_bronze_ingestion.py --snapshot-date 2026-10-08 --start-date 2026-09-10 --end-date 2026-10-07
"""

import argparse
import os
import sys
import time

import requests
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from acb_fx.acb_fx_core import (daterange, fetch_day, now_vn, parse_date_arg,  # noqa: E402
                                parse_records, resolve_date_range)
from acb_fx.acb_fx_lakehouse import (BRONZE_COLUMNS, drop_old_partitions, ensure_table,  # noqa: E402
                                     overwrite_partition, rows_to_arrow)

SLEEP_BETWEEN_CALLS = float(os.getenv("ACB_FX_SLEEP_SECONDS", "0.5"))


def parse_args():
    p = argparse.ArgumentParser(description="ACB FX bronze ingestion")
    p.add_argument("--snapshot-date", required=True, help="YYYY-MM-DD – khoá partition của lượt chạy")
    p.add_argument("--start-date", default=None, help="YYYY-MM-DD. Trống = end - (days_back - 1)")
    p.add_argument("--end-date", default=None, help="YYYY-MM-DD. Trống = hôm qua (giờ VN)")
    p.add_argument("--days-back", type=int, default=7)
    return p.parse_args()


def main():
    args = parse_args()
    snapshot_date = parse_date_arg(args.snapshot_date)
    start, end = resolve_date_range(parse_date_arg(args.start_date), parse_date_arg(args.end_date),
                                    args.days_back)
    days = daterange(start, end)
    logger.info(f"Bronze acb_fx | snapshot {snapshot_date} | tỷ giá {start} -> {end} ({len(days)} ngày)")

    rows, failed, empty = [], [], []
    with requests.Session() as session:
        for i, d in enumerate(days, start=1):
            try:
                raw = fetch_day(d, session=session)
                day_rows, bad = parse_records(raw, d)
                if not day_rows:
                    empty.append(d)
                    logger.warning(f"[{i}/{len(days)}] {d}: API trả về 0 bản ghi")
                else:
                    php = [r["exchange_rate"] for r in day_rows if r["exchange_currency"] == "PHP"]
                    note = f", bỏ {bad} bản ghi hỏng" if bad else ""
                    logger.info(f"[{i}/{len(days)}] {d}: {len(day_rows)} bản ghi, PHP {php}{note}")
                rows.extend(day_rows)
            except Exception as e:  # noqa: BLE001
                logger.error(f"❌ [{i}/{len(days)}] {d}: {e}")
                failed.append(d)
            time.sleep(SLEEP_BETWEEN_CALLS)

    if failed:
        logger.error(f"{len(failed)} ngày gọi API lỗi: {[str(d) for d in failed]} -> không ghi bronze")
        sys.exit(1)
    if not rows:
        logger.error("Không lấy được bản ghi nào -> không ghi bronze")
        sys.exit(1)

    tbl = ensure_table("bronze", BRONZE_COLUMNS)
    n = overwrite_partition(tbl, rows_to_arrow(rows, BRONZE_COLUMNS, snapshot_date, now_vn()),
                            snapshot_date)
    drop_old_partitions(tbl, snapshot_date)
    logger.success(f"✅ bronze.acb_fx.ty_gia_acb: {n} dòng ({len(days) - len(empty)}/{len(days)} ngày có dữ liệu)")


if __name__ == "__main__":
    main()
