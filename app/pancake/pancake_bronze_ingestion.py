"""
Pancake – BRONZE ingestion
Pancake API -> Iceberg bronze.pancake.<entity>_api (giữ nguyên JSON gốc trong cột payload)

Entities (--entity):
  conversations : hội thoại có cập nhật trong cửa sổ ngày
  messages      : tin nhắn của các hội thoại trên (đọc danh sách từ bronze conversations
                  cùng snapshot -> phải chạy SAU conversations)
  customers     : khách hàng cập nhật trong cửa sổ ngày
  statistics    : các API thống kê theo từng ngày trong cửa sổ

Cửa sổ ngày = [snapshot_date - (days - 1), snapshot_date] theo giờ VN (mặc định 3 ngày, như CareSoft).
Ghi: xoá partition (snapshot_date, page_id) rồi append -> chạy lại an toàn.
Page lỗi -> exit 1 (Airflow FAILED); các page khác vẫn được ghi.

Danh sách page + token: bảng Postgres public.core_pancake_page (is_active = true).

Ví dụ:
  python pancake_bronze_ingestion.py --entity conversations --snapshot-date 2026-09-30 --days 3
"""

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Dict, List

from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.database import PostgreSQLHelper  # noqa: E402
from common.pancake_api import PancakeAPIError, PancakePageClient, day_window_utc7  # noqa: E402
from common.utils import get_current_timestamp_naive  # noqa: E402
from pancake.pancake_lakehouse import (BRONZE_COLUMNS, BRONZE_TABLES, ensure_table,  # noqa: E402
                                       overwrite_partition, read_partition)
from pancake.pancake_transforms import to_json  # noqa: E402

POSTGRES_URI = os.getenv("POSTGRES_URI")
MAX_PAGE_WORKERS = int(os.getenv("PANCAKE_MAX_PAGE_WORKERS", "4"))

# Không lấy statistics/page_customers: trả HTML với page Zalo, và đã có bảng pancake_page_customer
STAT_TYPES = ["pages", "users", "tags", "pages_campaigns", "ads_by_time",
              "customer_engagements", "customer_feedbacks"]

CONFIG_DDL = """
CREATE TABLE IF NOT EXISTS public.core_pancake_page (
    page_id            text PRIMARY KEY,
    page_name          text,
    platform           text,
    brand_id           integer,
    page_access_token  text,
    is_active          boolean DEFAULT true,
    note               text,
    created_at         timestamp DEFAULT now(),
    updated_at         timestamp DEFAULT now()
);
"""


# ============================================================================
# CONFIG
# ============================================================================

def get_active_pages(page_id: str = None) -> List[Dict]:
    with PostgreSQLHelper(POSTGRES_URI) as db:
        db.execute(CONFIG_DDL)
        sql = ("SELECT page_id, page_name, brand_id, page_access_token "
               "FROM public.core_pancake_page WHERE is_active = true")
        params = None
        if page_id:
            sql += " AND page_id = %s"
            params = (page_id,)
        rows = db.fetchall(sql, params)
    return [dict(page_id=str(r[0]), page_name=r[1], brand_id=r[2], page_access_token=r[3]) for r in rows]


def window_days(snapshot_date: date, days: int) -> List[date]:
    return [snapshot_date - timedelta(days=i) for i in range(max(days, 1) - 1, -1, -1)]


def bronze_record(page: Dict, record_id, payload, parent_id, since, until, snapshot_date, now) -> Dict:
    return {
        "page_id": page["page_id"],
        "record_id": str(record_id) if record_id is not None else None,
        "parent_id": str(parent_id) if parent_id is not None else None,
        "payload": to_json(payload),
        "window_since": since,
        "window_until": until,
        "brand_id": page.get("brand_id"),
        "snapshot_date": snapshot_date,
        "ingestion_timestamp": now,
    }


# ============================================================================
# FETCHERS (1 page)
# ============================================================================

def fetch_conversations(client: PancakePageClient, page: Dict, days: List[date], snapshot_date, now):
    since, _ = day_window_utc7(days[0])
    _, until = day_window_utc7(days[-1])
    seen, out = set(), []
    for c in client.iter_conversations(since, until, order_by="updated_at"):
        cid = c.get("id")
        if cid and cid not in seen:
            seen.add(cid)
            out.append(bronze_record(page, cid, c, None, since, until, snapshot_date, now))
    return out


def fetch_messages(client: PancakePageClient, page: Dict, days: List[date], snapshot_date, now):
    conv_tbl = ensure_table("bronze", BRONZE_TABLES["conversations"], BRONZE_COLUMNS)
    convs = read_partition(conv_tbl, snapshot_date, page["page_id"])
    if not convs:
        raise RuntimeError("Chưa có bronze conversations cho snapshot này – chạy entity conversations trước")
    since, _ = day_window_utc7(days[0])
    _, until = day_window_utc7(days[-1])
    out = []
    for idx, c in enumerate(convs, start=1):
        payload = json.loads(c["payload"]) if c.get("payload") else {}
        for m in client.get_all_messages(c["record_id"], expected_count=payload.get("message_count")):
            out.append(bronze_record(page, m.get("id"), m, c["record_id"], since, until, snapshot_date, now))
        if idx % 200 == 0:
            logger.info(f"[{page['page_id']}] messages: {idx}/{len(convs)} hội thoại")
    return out


def fetch_customers(client: PancakePageClient, page: Dict, days: List[date], snapshot_date, now):
    since, _ = day_window_utc7(days[0])
    _, until = day_window_utc7(days[-1])
    seen, out = set(), []
    for c in client.iter_page_customers(since, until, order_by="updated_at"):
        cid = c.get("id")
        if cid and cid not in seen:
            seen.add(cid)
            out.append(bronze_record(page, cid, c, None, since, until, snapshot_date, now))
    return out


def fetch_statistics(client: PancakePageClient, page: Dict, days: List[date], snapshot_date, now):
    """
    Gọi từng loại thống kê cho từng ngày.
    - Loại thống kê page không hỗ trợ / không có quyền (HTTP 400, 403, 404 hoặc success=false):
      chỉ cảnh báo, bỏ qua (vd page Zalo cá nhân không có thống kê quảng cáo).
    - Lỗi thật (token sai 401, lỗi mạng, 5xx) hoặc mọi lời gọi đều lỗi: trả về errors -> task FAILED.
    """
    out, errors, unsupported = [], [], set()
    for d in days:
        since, until = day_window_utc7(d)
        for st in STAT_TYPES:
            if st in unsupported:
                continue
            try:
                data = client.get_statistics(st, d)
            except PancakeAPIError as e:
                if e.status_code in (400, 403, 404, 200):
                    unsupported.add(st)
                    logger.warning(f"[{page['page_id']}] bỏ qua thống kê '{st}' (page không hỗ trợ/không có quyền): {str(e)[:200]}")
                else:
                    errors.append(f"{st} {d}: {e}")
                continue
            out.append(bronze_record(page, f"{st}|{d.isoformat()}", data, st, since, until, snapshot_date, now))
    if not out and not errors:
        errors.append("không lấy được loại thống kê nào cho page này")
    if errors:
        logger.error(f"[{page['page_id']}] {len(errors)} lời gọi thống kê lỗi: {' | '.join(errors[:5])}")
    return out, errors


FETCHERS = {
    "conversations": fetch_conversations,
    "messages": fetch_messages,
    "customers": fetch_customers,
    "statistics": fetch_statistics,
}


# ============================================================================
# MAIN
# ============================================================================

def fetch_page(entity: str, page: Dict, days: List[date], snapshot_date: date):
    """Chạy trong thread: chỉ gọi API, không ghi Iceberg (tránh xung đột commit)."""
    if not page.get("page_access_token"):
        raise RuntimeError("page_access_token trống trong core_pancake_page")
    client = PancakePageClient(page["page_id"], page["page_access_token"])
    now = get_current_timestamp_naive()
    result = FETCHERS[entity](client, page, days, snapshot_date, now)
    if entity == "statistics":
        return result
    return result, []


def parse_args():
    p = argparse.ArgumentParser(description="Pancake bronze ingestion")
    p.add_argument("--entity", required=True, choices=list(FETCHERS.keys()))
    p.add_argument("--snapshot-date", default=None, help="YYYY-MM-DD (giờ VN). Mặc định: hôm qua")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--page-id", default=None)
    a = p.parse_args()
    a.snapshot_date = None if a.snapshot_date in (None, "", "None") else a.snapshot_date
    a.page_id = None if a.page_id in (None, "", "None") else a.page_id
    return a


def main():
    args = parse_args()
    if not POSTGRES_URI:
        logger.error("Thiếu env POSTGRES_URI (đọc core_pancake_page)")
        sys.exit(1)
    snapshot_date = (datetime.strptime(args.snapshot_date, "%Y-%m-%d").date() if args.snapshot_date
                     else get_current_timestamp_naive().date() - timedelta(days=1))
    days = window_days(snapshot_date, args.days)
    pages = get_active_pages(args.page_id)
    if not pages:
        logger.error("Không có page active trong public.core_pancake_page")
        sys.exit(1)
    logger.info(f"Bronze {args.entity} | snapshot {snapshot_date} | {days[0]} -> {days[-1]} | {len(pages)} page")

    tbl = ensure_table("bronze", BRONZE_TABLES[args.entity], BRONZE_COLUMNS)
    failed = []
    with ThreadPoolExecutor(max_workers=MAX_PAGE_WORKERS) as ex:
        futs = {ex.submit(fetch_page, args.entity, pg, days, snapshot_date): pg for pg in pages}
        # Ghi tuần tự ở thread chính khi từng page lấy xong
        for f in as_completed(futs):
            pg = futs[f]
            label = f"{pg['page_id']} ({pg.get('page_name') or ''})"
            try:
                records, errors = f.result()
                n = overwrite_partition(tbl, records, snapshot_date, pg["page_id"])
                if errors:
                    raise PancakeAPIError(f"đã ghi {n} bản ghi nhưng {len(errors)} lời gọi thống kê lỗi")
                logger.success(f"✅ {label}: {n} bản ghi")
            except Exception as e:
                logger.error(f"❌ {label}: {e}")
                failed.append(label)
    if failed:
        logger.error(f"{len(failed)}/{len(pages)} page lỗi: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
