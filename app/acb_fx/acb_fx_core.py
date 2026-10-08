"""
ACB FX – phần lõi thuần Python (không phụ thuộc pyarrow / pyiceberg, test được độc lập).

Nguồn: API công khai mà trang https://acb.com.vn/ty-gia-hoi-doai gọi:
    GET https://acb.com.vn/api/front/v1/currency?currency=VND&effectiveDateTime=<YYYY-MM-DDTHH:MM:SS.mmm>
- effectiveDateTime là GIỜ VIỆT NAM (không có múi giờ). API trả bảng tỷ giá có hiệu lực tại
  thời điểm đó -> lấy được tỷ giá của ngày trong quá khứ (đã thử: 10/09/2026 17:00 PHP = 423,89).
- Mỗi ngày D lấy tại D 23:59:59 = tỷ giá CHỐT cuối ngày (ACB cập nhật nhiều lần trong ngày).
- Mỗi phần tử: {"currency":"VND","exchangeCurrency":"PHP","exchangeRate":422.92,
                "dealType":"ASK","instrumentType":"TRANSFER","maxSequence":5,
                ["denomination":3], ["region":1789624]}
    dealType       BID = ngân hàng MUA vào, ASK = ngân hàng BÁN ra
    instrumentType TRANSFER = chuyển khoản, CASH = tiền mặt
    denomination   chỉ USD: 3 = mệnh giá 50-100 (cả dòng chuyển khoản), 2 = 5-10-20, 1 = 1-2
    region         chỉ có ở dòng giá VÀNG (exchangeCurrency ACB / XAU) -> bị loại ở silver
"""

import json
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

API_URL = "https://acb.com.vn/api/front/v1/currency"
BASE_CURRENCY = "VND"
EFFECTIVE_TIME = "23:59:59.000"          # giờ chốt tỷ giá mỗi ngày (giờ VN)
GOLD_CODES = {"ACB", "XAU", "SJC"}       # mã vàng – không phải ngoại tệ
VN_UTC_OFFSET = timedelta(hours=7)

HTTP_HEADERS = {
    "Accept": "application/json",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Referer": "https://acb.com.vn/ty-gia-hoi-doai",
}


# ============================================================================
# NGÀY
# ============================================================================

def now_vn() -> datetime:
    """Giờ hiện tại theo giờ Việt Nam, naive (pod chạy UTC)."""
    return datetime.utcnow() + VN_UTC_OFFSET


def today_vn(now_utc: Optional[datetime] = None) -> date:
    """Ngày hiện tại theo giờ Việt Nam (pod chạy UTC)."""
    now_utc = now_utc or datetime.utcnow()
    return (now_utc + VN_UTC_OFFSET).date()


def parse_date_arg(value) -> Optional[date]:
    if value in (None, "", "None"):
        return None
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def resolve_date_range(start: Optional[date], end: Optional[date], days_back: int,
                       today: Optional[date] = None) -> Tuple[date, date]:
    """
    - end mặc định = hôm qua (giờ VN): tỷ giá của hôm qua đã chốt.
    - start mặc định = end - (days_back - 1): lấy lại vài ngày gần nhất phòng ACB sửa số.
    """
    today = today or today_vn()
    end = end or (today - timedelta(days=1))
    if end >= today:
        raise ValueError(f"end_date {end} phải trước hôm nay {today} (tỷ giá trong ngày chưa chốt)")
    if days_back < 1:
        raise ValueError("days_back phải >= 1")
    start = start or (end - timedelta(days=days_back - 1))
    if start > end:
        raise ValueError(f"start_date {start} sau end_date {end}")
    return start, end


def daterange(start: date, end: date) -> List[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def effective_datetime(d: date) -> str:
    return f"{d.isoformat()}T{EFFECTIVE_TIME}"


# ============================================================================
# GỌI API
# ============================================================================

def fetch_day(d: date, session: Optional[requests.Session] = None, retries: int = 3,
              timeout: int = 30, backoff: float = 5.0) -> list:
    """Gọi API cho 1 ngày. Lỗi mạng / HTTP khác 200 / không phải JSON list -> thử lại, hết lượt -> raise."""
    http = session or requests
    params = {"currency": BASE_CURRENCY, "effectiveDateTime": effective_datetime(d)}
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = http.get(API_URL, params=params, headers=HTTP_HEADERS, timeout=timeout)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            if not isinstance(data, list):
                raise RuntimeError(f"API không trả về danh sách: {str(data)[:200]}")
            return data
        except Exception as e:  # noqa: BLE001
            last_err = e
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"{d}: gọi API ACB thất bại sau {retries} lần: {last_err}")


# ============================================================================
# BRONZE: giữ nguyên bản ghi API (+ ngày, thời điểm hiệu lực, JSON gốc)
# ============================================================================

def _to_int(v) -> Optional[int]:
    if v is None or v == "":
        return None
    return int(v)


def _to_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    return float(v)


def parse_records(raw: list, d: date) -> Tuple[List[Dict], int]:
    """JSON API của ngày d -> danh sách dòng bronze. Trả về (rows, số bản ghi bị bỏ vì hỏng)."""
    rows, bad = [], 0
    eff = effective_datetime(d)
    for item in raw:
        try:
            if not isinstance(item, dict) or not item.get("exchangeCurrency"):
                raise ValueError("thiếu exchangeCurrency")
            rows.append({
                "ngay": d,
                "effective_datetime": eff,
                "currency": item.get("currency"),
                "exchange_currency": str(item["exchangeCurrency"]).strip().upper(),
                "exchange_rate": _to_float(item.get("exchangeRate")),
                "deal_type": (item.get("dealType") or "").strip().upper() or None,
                "instrument_type": (item.get("instrumentType") or "").strip().upper() or None,
                "max_sequence": _to_int(item.get("maxSequence")),
                "denomination": _to_int(item.get("denomination")),
                "region": _to_int(item.get("region")),
                "raw": json.dumps(item, ensure_ascii=False, sort_keys=True),
            })
        except Exception:  # noqa: BLE001
            bad += 1
    return rows, bad


# ============================================================================
# SILVER: chỉ ngoại tệ, chuẩn hoá, bỏ trùng
# ============================================================================

SILVER_KEY = ("ngay", "loai_tien", "deal_type", "instrument_type", "menh_gia")


def to_silver_rows(bronze_rows: List[Dict]) -> Tuple[List[Dict], Dict[str, int]]:
    """
    - Bỏ dòng vàng (region có giá trị hoặc mã ACB/XAU/SJC), dòng không phải VND,
      dòng thiếu dealType/instrumentType, tỷ giá rỗng hoặc <= 0.
    - menh_gia = denomination (chỉ USD) hoặc 0 khi không phân mệnh giá.
    - Trùng khoá (ngay, loai_tien, deal_type, instrument_type, menh_gia) -> giữ dòng cuối.
    """
    stats = {"vang": 0, "khong_phai_vnd": 0, "thieu_loai": 0, "ty_gia_loi": 0, "trung": 0}
    out: Dict[tuple, Dict] = {}
    for r in bronze_rows:
        if r.get("region") is not None or r.get("exchange_currency") in GOLD_CODES:
            stats["vang"] += 1
            continue
        if (r.get("currency") or "").upper() != BASE_CURRENCY:
            stats["khong_phai_vnd"] += 1
            continue
        if r.get("deal_type") not in ("BID", "ASK") or r.get("instrument_type") not in ("CASH", "TRANSFER"):
            stats["thieu_loai"] += 1
            continue
        rate = r.get("exchange_rate")
        if rate is None or rate <= 0:
            stats["ty_gia_loi"] += 1
            continue
        row = {
            "ngay": r["ngay"],
            "loai_tien": r["exchange_currency"],
            "deal_type": r["deal_type"],
            "instrument_type": r["instrument_type"],
            "menh_gia": r.get("denomination") or 0,
            "ty_gia": float(rate),
            "max_sequence": r.get("max_sequence"),
            "effective_datetime": r.get("effective_datetime"),
        }
        key = tuple(row[k] for k in SILVER_KEY)
        if key in out:
            stats["trung"] += 1
        out[key] = row
    rows = sorted(out.values(), key=lambda x: tuple(x[k] for k in SILVER_KEY))
    return rows, stats
