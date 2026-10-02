"""
Pancake – SERVING layer
gold.pancake.* (Iceberg) -> Postgres mart (POSTGRES_URI: dwh_product_new / dwh_sandbox)

Bảng Postgres có kiểu dữ liệu đúng (jsonb, date, timestamp, boolean...) và khoá chính;
ghi bằng UPSERT nên Postgres giữ lịch sử đầy đủ dù lakehouse chỉ giữ vài ngày gần nhất.
Cột mới thêm vào Gold sẽ tự được ALTER TABLE ADD COLUMN.

  --entity conversations -> public.pancake_conversation
  --entity messages      -> public.pancake_message
  --entity customers     -> public.pancake_page_customer
  --entity statistics    -> public.pancake_statistics, pancake_stat_page_hourly,
                            pancake_stat_page_daily, pancake_stat_user_daily
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List

from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.database import PostgreSQLHelper  # noqa: E402
from common.utils import bulk_load, get_current_timestamp_naive  # noqa: E402

POSTGRES_URI = os.getenv("POSTGRES_URI")
BATCH_SIZE = 5000

JSON_COLUMNS = {"tags", "assignee_ids", "ad_ids", "phone_numbers", "emails", "payload"}
SKIP_COLUMNS = {"snapshot_date"}  # chỉ có ý nghĩa trong lakehouse

# gold table -> (bảng Postgres, khoá chính)
SERVING = {
    "fct_conversation": ("pancake_conversation", ["page_id", "conversation_id"]),
    "fct_message": ("pancake_message", ["page_id", "message_id"]),
    "dim_page_customer": ("pancake_page_customer", ["page_id", "page_customer_id"]),
    "fct_statistics": ("pancake_statistics", ["page_id", "stat_type", "stat_date"]),
    "fct_stat_page_hourly": ("pancake_stat_page_hourly", ["page_id", "hour"]),
    "agg_stat_page_daily": ("pancake_stat_page_daily", ["page_id", "stat_date"]),
    "fct_stat_user_daily": ("pancake_stat_user_daily", ["page_id", "stat_date", "user_id"]),
}

PLAN = {
    "conversations": ["fct_conversation"],
    "messages": ["fct_message"],
    "customers": ["dim_page_customer"],
    "statistics": ["fct_statistics", "fct_stat_page_hourly", "agg_stat_page_daily", "fct_stat_user_daily"],
}

PG_TYPES = {"StringType": "text", "IntegerType": "integer", "LongType": "bigint",
            "BooleanType": "boolean", "DateType": "date", "TimestampType": "timestamp"}


def pg_columns(gold_columns) -> List[tuple]:
    """[(tên, kiểu Iceberg)] -> [(tên, kiểu Postgres)] (bỏ snapshot_date, JSON -> jsonb)."""
    out = []
    for name, typ in gold_columns:
        if name in SKIP_COLUMNS:
            continue
        pg = "jsonb" if name in JSON_COLUMNS else PG_TYPES[typ.__name__]
        out.append((name, pg))
    return out


def ensure_pg_table(db: PostgreSQLHelper, table: str, cols: List[tuple], pk: List[str]):
    col_defs = ",\n    ".join(f'"{n}" {t}' for n, t in cols)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS public.{table} (
            {col_defs},
            PRIMARY KEY ({", ".join(f'"{c}"' for c in pk)})
        )""")
    for n, t in cols:  # cột mới thêm sau này
        db.execute(f'ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS "{n}" {t}')


def upsert_rows(db: PostgreSQLHelper, gold_table: str, gold_columns, rows: List[Dict]) -> int:
    table, pk = SERVING[gold_table]
    cols = pg_columns(gold_columns)
    ensure_pg_table(db, table, cols, pk)
    names = [n for n, _ in cols]
    rows = [r for r in rows if all(r.get(k) is not None for k in pk)]
    for i in range(0, len(rows), BATCH_SIZE):
        cur = db.conn.cursor()
        try:
            bulk_load(cur, f"public.{table}", rows[i:i + BATCH_SIZE], names, pk, method="upsert")
            db.conn.commit()
        except Exception:
            db.conn.rollback()
            raise
        finally:
            cur.close()
    return len(rows)


def run(entity: str, snapshot_date):
    from pancake.pancake_lakehouse import GOLD_TABLES, ensure_table, read_partition
    with PostgreSQLHelper(POSTGRES_URI) as db:
        for gold_table in PLAN[entity]:
            tbl = ensure_table("gold", gold_table, GOLD_TABLES[gold_table])
            rows = read_partition(tbl, snapshot_date)
            n = upsert_rows(db, gold_table, GOLD_TABLES[gold_table], rows)
            logger.success(f"✅ gold.pancake.{gold_table} -> public.{SERVING[gold_table][0]}: {n} dòng")


def main():
    p = argparse.ArgumentParser(description="Pancake serving layer")
    p.add_argument("--entity", required=True, choices=list(PLAN.keys()))
    p.add_argument("--snapshot-date", default=None)
    a = p.parse_args()
    if not POSTGRES_URI:
        logger.error("Thiếu env POSTGRES_URI")
        sys.exit(1)
    sd = None if a.snapshot_date in (None, "", "None") else a.snapshot_date
    snapshot_date = (datetime.strptime(sd, "%Y-%m-%d").date() if sd
                     else get_current_timestamp_naive().date() - timedelta(days=1))
    try:
        run(a.entity, snapshot_date)
    except Exception as e:
        logger.error(f"Serving {a.entity} lỗi: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
