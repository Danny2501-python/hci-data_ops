"""
Pancake transforms – hàm thuần Python (không I/O) dùng cho Silver & Gold.
Tách riêng để test được mà không cần Iceberg / Postgres.

Bronze record (mọi entity):
  page_id, record_id, parent_id, payload (JSON text), brand_id, snapshot_date, ingestion_timestamp
"""

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from common.pancake_api import parse_ts

VN_OFFSET = timedelta(hours=7)


# ============================================================================
# Helpers
# ============================================================================

def clean_text(v: Any) -> Optional[str]:
    if v is None or v == "":
        return None
    return str(v).replace("\x00", "")


def to_int(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def to_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("true", "1", "t", "yes")


def to_json(v: Any) -> Optional[str]:
    if v is None:
        return None
    return json.dumps(v, ensure_ascii=False, default=str).replace("\\u0000", "")


def to_vn(ts: Optional[datetime]) -> Optional[datetime]:
    return ts + VN_OFFSET if ts else None


def first_phone(phones: Any) -> Optional[str]:
    """recent_phone_numbers có thể là list[str] hoặc list[{phone_number: ...}]."""
    for p in phones or []:
        if isinstance(p, dict):
            num = p.get("phone_number") or p.get("captured")
        else:
            num = p
        if num:
            return str(num)
    return None


def latest_by_key(rows: Iterable[Dict], key_fields: List[str], order_fields: List[str]) -> List[Dict]:
    """Giữ 1 bản ghi / khoá, ưu tiên giá trị lớn nhất của order_fields (None xếp cuối)."""
    best: Dict[tuple, Dict] = {}

    def sort_key(r):
        return tuple((r.get(f) is not None, r.get(f) or datetime.min) for f in order_fields)

    for r in rows:
        k = tuple(r.get(f) for f in key_fields)
        if None in k:
            continue
        cur = best.get(k)
        if cur is None or sort_key(r) > sort_key(cur):
            best[k] = r
    return list(best.values())


def _payload(b: Dict) -> Dict:
    try:
        return json.loads(b["payload"]) if b.get("payload") else {}
    except (TypeError, ValueError):
        return {}


# ============================================================================
# SILVER: Bronze (JSON) -> bảng có kiểu, khử trùng
# ============================================================================

def silver_conversations(bronze: List[Dict]) -> List[Dict]:
    rows = []
    for b in bronze:
        c = _payload(b)
        frm = c.get("from") or {}
        customers = c.get("customers") or []
        cus = customers[0] if customers else {}
        rows.append({
            "page_id": b["page_id"],
            "conversation_id": c.get("id") or b.get("record_id"),
            "type": c.get("type"),
            "post_id": clean_text(c.get("post_id")),
            "customer_id": clean_text(c.get("customer_id") or cus.get("customer_id") or cus.get("id")),
            "customer_psid": clean_text(cus.get("fb_id") or frm.get("id")),
            "customer_name": clean_text(cus.get("name") or frm.get("name")),
            "snippet": clean_text(c.get("snippet")),
            "message_count": to_int(c.get("message_count")),
            "has_phone": to_bool(c.get("has_phone")),
            "seen": to_bool(c.get("seen")),
            "tags": to_json(c.get("tags")),
            "assignee_ids": to_json(c.get("assignee_ids")),
            "ad_ids": to_json(c.get("ad_ids")),
            "phone_numbers": to_json(c.get("recent_phone_numbers")),
            "inserted_at": parse_ts(c.get("inserted_at")),
            "updated_at": parse_ts(c.get("updated_at")),
            "brand_id": b.get("brand_id"),
            "snapshot_date": b["snapshot_date"],
            "ingestion_timestamp": b["ingestion_timestamp"],
        })
    return latest_by_key(rows, ["page_id", "conversation_id"], ["updated_at", "ingestion_timestamp"])


def silver_messages(bronze: List[Dict]) -> List[Dict]:
    rows = []
    for b in bronze:
        m = _payload(b)
        frm = m.get("from") or {}
        rows.append({
            "page_id": b["page_id"],
            "message_id": m.get("id") or b.get("record_id"),
            "conversation_id": m.get("conversation_id") or b.get("parent_id"),
            "type": m.get("type"),
            "from_id": clean_text(frm.get("id")),
            "from_name": clean_text(frm.get("name")),
            "is_from_page": str(frm.get("id")) == str(b["page_id"]),
            "message": clean_text(m.get("original_message") or m.get("message")),
            "has_phone": to_bool(m.get("has_phone")),
            "is_removed": to_bool(m.get("is_removed")),
            "parent_message_id": clean_text(m.get("parent_id")),
            "attachment_count": len(m.get("attachments") or []),
            "inserted_at": parse_ts(m.get("inserted_at")),
            "brand_id": b.get("brand_id"),
            "snapshot_date": b["snapshot_date"],
            "ingestion_timestamp": b["ingestion_timestamp"],
        })
    return latest_by_key(rows, ["page_id", "message_id"], ["ingestion_timestamp"])


def silver_page_customers(bronze: List[Dict]) -> List[Dict]:
    rows = []
    for b in bronze:
        c = _payload(b)
        rows.append({
            "page_id": b["page_id"],
            "page_customer_id": c.get("id") or b.get("record_id"),
            "customer_id": clean_text(c.get("customer_id")),
            "psid": clean_text(c.get("psid")),
            "name": clean_text(c.get("name")),
            "thread_id": clean_text(c.get("thread_id")),
            "gender": clean_text(c.get("gender")),
            "birthday": clean_text(c.get("birthday")),
            "lives_in": clean_text(c.get("lives_in")),
            "phone_numbers": to_json(c.get("phone_numbers")),
            "emails": to_json(c.get("emails")),
            "inserted_at": parse_ts(c.get("inserted_at")),
            "updated_at": parse_ts(c.get("updated_at")),
            "brand_id": b.get("brand_id"),
            "snapshot_date": b["snapshot_date"],
            "ingestion_timestamp": b["ingestion_timestamp"],
        })
    return latest_by_key(rows, ["page_id", "page_customer_id"], ["updated_at", "ingestion_timestamp"])


PAGE_STAT_FIELDS = ["customer_comment_count", "customer_inbox_count", "inbox_interactive_count",
                    "new_customer_count", "new_inbox_count", "page_comment_count", "page_inbox_count",
                    "phone_number_count", "uniq_phone_number_count", "today_uniq_website_referral",
                    "today_website_guest_referral"]
USER_STAT_FIELDS = ["inbox_count", "unique_inbox_count", "comment_count", "unique_comment_count",
                    "private_reply_count", "phone_number_count", "average_response_time"]


def _stat_key(b: Dict):
    """record_id của statistics = '<stat_type>|<YYYY-MM-DD>'."""
    stat_type, _, d = (b.get("record_id") or "").partition("|")
    try:
        return stat_type, datetime.strptime(d, "%Y-%m-%d").date()
    except ValueError:
        return stat_type, None


def silver_statistics(bronze: List[Dict]) -> List[Dict]:
    """Bảng chung: mỗi (page, stat_type, stat_date) 1 dòng, giữ JSON để dùng về sau."""
    rows = []
    for b in bronze:
        stat_type, stat_date = _stat_key(b)
        rows.append({
            "page_id": b["page_id"], "stat_type": stat_type, "stat_date": stat_date,
            "payload": b.get("payload"), "brand_id": b.get("brand_id"),
            "snapshot_date": b["snapshot_date"], "ingestion_timestamp": b["ingestion_timestamp"],
        })
    return latest_by_key(rows, ["page_id", "stat_type", "stat_date"], ["ingestion_timestamp"])


def silver_stat_page_hourly(bronze: List[Dict]) -> List[Dict]:
    rows = []
    for b in bronze:
        stat_type, stat_date = _stat_key(b)
        if stat_type != "pages":
            continue
        data = _payload(b)
        for r in (data.get("data") if isinstance(data, dict) else None) or []:
            hour = parse_ts(r.get("hour"))
            if not hour:
                continue
            row = {"page_id": b["page_id"], "hour": hour, "stat_date": stat_date,
                   "brand_id": b.get("brand_id"), "snapshot_date": b["snapshot_date"],
                   "ingestion_timestamp": b["ingestion_timestamp"]}
            row.update({f: to_int(r.get(f)) for f in PAGE_STAT_FIELDS})
            rows.append(row)
    return latest_by_key(rows, ["page_id", "hour"], ["ingestion_timestamp"])


def silver_stat_user_daily(bronze: List[Dict]) -> List[Dict]:
    rows = []
    for b in bronze:
        stat_type, stat_date = _stat_key(b)
        if stat_type != "users":
            continue
        data = _payload(b)
        users = ((data.get("data") or {}).get("users") or {}) if isinstance(data, dict) else {}
        for uid, u in users.items():
            row = {"page_id": b["page_id"], "stat_date": stat_date, "user_id": str(uid),
                   "user_name": clean_text(u.get("user_name")), "user_fb_id": clean_text(u.get("user_fb_id")),
                   "brand_id": b.get("brand_id"), "snapshot_date": b["snapshot_date"],
                   "ingestion_timestamp": b["ingestion_timestamp"]}
            row.update({f: to_int(u.get(f)) for f in USER_STAT_FIELDS})
            rows.append(row)
    return latest_by_key(rows, ["page_id", "stat_date", "user_id"], ["ingestion_timestamp"])


# ============================================================================
# GOLD: Silver -> sẵn sàng cho báo cáo (giờ VN, cờ nghiệp vụ, tổng hợp)
# ============================================================================

def gold_conversations(silver: List[Dict]) -> List[Dict]:
    out = []
    for s in silver:
        ad_ids = json.loads(s["ad_ids"]) if s.get("ad_ids") else None
        phones = json.loads(s["phone_numbers"]) if s.get("phone_numbers") else None
        ins_vn, upd_vn = to_vn(s.get("inserted_at")), to_vn(s.get("updated_at"))
        out.append({
            **{k: s.get(k) for k in ("page_id", "conversation_id", "type", "post_id", "customer_id",
                                     "customer_psid", "customer_name", "snippet", "message_count",
                                     "has_phone", "seen", "tags", "assignee_ids", "ad_ids", "phone_numbers",
                                     "inserted_at", "updated_at", "brand_id", "snapshot_date")},
            "inserted_at_vn": ins_vn,
            "updated_at_vn": upd_vn,
            "inserted_date_vn": ins_vn.date() if ins_vn else None,
            "is_from_ad": bool(ad_ids),
            "first_ad_id": str(ad_ids[0]) if ad_ids else None,
            "first_phone": first_phone(phones),
        })
    return out


def gold_messages(silver: List[Dict]) -> List[Dict]:
    out = []
    for s in silver:
        ins_vn = to_vn(s.get("inserted_at"))
        out.append({
            **{k: s.get(k) for k in ("page_id", "message_id", "conversation_id", "type", "from_id", "from_name",
                                     "is_from_page", "message", "has_phone", "is_removed", "parent_message_id",
                                     "attachment_count", "inserted_at", "brand_id", "snapshot_date")},
            "inserted_at_vn": ins_vn,
            "sent_date_vn": ins_vn.date() if ins_vn else None,
            "sender_role": "page" if s.get("is_from_page") else "customer",
        })
    return out


def gold_page_customers(silver: List[Dict]) -> List[Dict]:
    out = []
    for s in silver:
        phones = json.loads(s["phone_numbers"]) if s.get("phone_numbers") else None
        out.append({
            **{k: s.get(k) for k in ("page_id", "page_customer_id", "customer_id", "psid", "name", "thread_id",
                                     "gender", "birthday", "lives_in", "phone_numbers", "emails",
                                     "inserted_at", "updated_at", "brand_id", "snapshot_date")},
            "inserted_at_vn": to_vn(s.get("inserted_at")),
            "updated_at_vn": to_vn(s.get("updated_at")),
            "first_phone": first_phone(phones),
        })
    return out


def gold_stat_page_hourly(silver: List[Dict]) -> List[Dict]:
    return [{**s, "hour_vn": to_vn(s.get("hour"))} for s in silver]


def gold_stat_page_daily(silver_hourly: List[Dict]) -> List[Dict]:
    """Tổng hợp thống kê page theo ngày giờ VN (tổng các giờ trong ngày)."""
    agg: Dict[tuple, Dict] = {}
    sum_fields = [f for f in PAGE_STAT_FIELDS if not f.startswith("today_")]
    for s in silver_hourly:
        hour_vn = to_vn(s.get("hour"))
        if not hour_vn:
            continue
        k = (s["page_id"], hour_vn.date())
        row = agg.setdefault(k, {"page_id": s["page_id"], "stat_date": hour_vn.date(),
                                 "brand_id": s.get("brand_id"), "snapshot_date": s["snapshot_date"],
                                 "hours_count": 0, **{f: 0 for f in sum_fields}})
        row["hours_count"] += 1
        for f in sum_fields:
            row[f] += s.get(f) or 0
    return list(agg.values())


def gold_stat_user_daily(silver: List[Dict]) -> List[Dict]:
    return [dict(s) for s in silver]


def gold_statistics(silver: List[Dict]) -> List[Dict]:
    return [dict(s) for s in silver]
