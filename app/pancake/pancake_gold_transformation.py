"""
Pancake – GOLD transformation
silver.pancake.* -> gold.pancake.* (sẵn sàng cho báo cáo: giờ VN, cờ nghiệp vụ, bảng tổng hợp)

  --entity conversations -> gold.pancake.fct_conversation
  --entity messages      -> gold.pancake.fct_message
  --entity customers     -> gold.pancake.dim_page_customer
  --entity statistics    -> gold.pancake.fct_statistics, fct_stat_page_hourly,
                            agg_stat_page_daily (tổng hợp theo ngày VN), fct_stat_user_daily
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils import get_current_timestamp_naive  # noqa: E402
from pancake import pancake_transforms as tf  # noqa: E402
from pancake.pancake_lakehouse import (GOLD_TABLES, SILVER_TABLES, ensure_table,  # noqa: E402
                                       overwrite_partition, read_partition)

# entity -> [(bảng silver nguồn, bảng gold đích, hàm transform)]
PLAN = {
    "conversations": [("conversation", "fct_conversation", tf.gold_conversations)],
    "messages": [("message", "fct_message", tf.gold_messages)],
    "customers": [("page_customer", "dim_page_customer", tf.gold_page_customers)],
    "statistics": [
        ("statistics", "fct_statistics", tf.gold_statistics),
        ("stat_page_hourly", "fct_stat_page_hourly", tf.gold_stat_page_hourly),
        ("stat_page_hourly", "agg_stat_page_daily", tf.gold_stat_page_daily),
        ("stat_user_daily", "fct_stat_user_daily", tf.gold_stat_user_daily),
    ],
}


def run(entity: str, snapshot_date):
    now = get_current_timestamp_naive()
    cache = {}
    for silver_table, gold_table, fn in PLAN[entity]:
        if silver_table not in cache:
            src = ensure_table("silver", silver_table, SILVER_TABLES[silver_table])
            cache[silver_table] = read_partition(src, snapshot_date)
            logger.info(f"silver.pancake.{silver_table} {snapshot_date}: {len(cache[silver_table])} dòng")
        rows = fn(cache[silver_table])
        for r in rows:
            r["etl_updated"] = now
        tbl = ensure_table("gold", gold_table, GOLD_TABLES[gold_table])
        n = overwrite_partition(tbl, rows, snapshot_date)
        logger.success(f"✅ gold.pancake.{gold_table}: {n} dòng")


def main():
    p = argparse.ArgumentParser(description="Pancake gold transformation")
    p.add_argument("--entity", required=True, choices=list(PLAN.keys()))
    p.add_argument("--snapshot-date", default=None)
    a = p.parse_args()
    sd = None if a.snapshot_date in (None, "", "None") else a.snapshot_date
    snapshot_date = (datetime.strptime(sd, "%Y-%m-%d").date() if sd
                     else get_current_timestamp_naive().date() - timedelta(days=1))
    try:
        run(a.entity, snapshot_date)
    except Exception as e:
        logger.error(f"Gold {a.entity} lỗi: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
