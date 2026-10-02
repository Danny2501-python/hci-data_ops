"""
Pancake – SILVER transformation
bronze.pancake.<entity>_api (JSON thô) -> silver.pancake.<bảng> (cột có kiểu, khử trùng)

  --entity conversations -> silver.pancake.conversation
  --entity messages      -> silver.pancake.message
  --entity customers     -> silver.pancake.page_customer
  --entity statistics    -> silver.pancake.statistics, stat_page_hourly, stat_user_daily

Mỗi lần chạy dựng lại toàn bộ partition snapshot_date (mọi page) từ bronze cùng snapshot.
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils import get_current_timestamp_naive  # noqa: E402
from pancake import pancake_transforms as tf  # noqa: E402
from pancake.pancake_lakehouse import (BRONZE_COLUMNS, BRONZE_TABLES, SILVER_TABLES,  # noqa: E402
                                       ensure_table, overwrite_partition, read_partition)

# entity -> [(bảng silver, hàm transform)]
PLAN = {
    "conversations": [("conversation", tf.silver_conversations)],
    "messages": [("message", tf.silver_messages)],
    "customers": [("page_customer", tf.silver_page_customers)],
    "statistics": [
        ("statistics", tf.silver_statistics),
        ("stat_page_hourly", tf.silver_stat_page_hourly),
        ("stat_user_daily", tf.silver_stat_user_daily),
    ],
}


def run(entity: str, snapshot_date):
    bronze_tbl = ensure_table("bronze", BRONZE_TABLES[entity], BRONZE_COLUMNS)
    bronze = read_partition(bronze_tbl, snapshot_date)
    logger.info(f"Bronze {entity} {snapshot_date}: {len(bronze)} bản ghi")
    if not bronze:
        # Bước bronze đã chạy thành công (trigger rule all_success) -> 0 bản ghi là dữ liệu thật,
        # vd page không có khách mới trong ngày. Ghi partition rỗng và kết thúc bình thường.
        logger.warning(f"Bronze {entity} {snapshot_date} không có bản ghi – silver ghi partition rỗng")
    for table, fn in PLAN[entity]:
        rows = fn(bronze)
        tbl = ensure_table("silver", table, SILVER_TABLES[table])
        n = overwrite_partition(tbl, rows, snapshot_date)
        logger.success(f"✅ silver.pancake.{table}: {n} dòng")


def main():
    p = argparse.ArgumentParser(description="Pancake silver transformation")
    p.add_argument("--entity", required=True, choices=list(PLAN.keys()))
    p.add_argument("--snapshot-date", default=None)
    a = p.parse_args()
    sd = None if a.snapshot_date in (None, "", "None") else a.snapshot_date
    snapshot_date = (datetime.strptime(sd, "%Y-%m-%d").date() if sd
                     else get_current_timestamp_naive().date() - timedelta(days=1))
    try:
        run(a.entity, snapshot_date)
    except Exception as e:
        logger.error(f"Silver {a.entity} lỗi: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
