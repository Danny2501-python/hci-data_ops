"""
Pancake API Utilities
Client dùng chung để gọi Pancake API (https://developer.pancake.biz)

- User API     : https://pages.fm/api/v1                (access_token của user)
- Page API v1  : https://pages.fm/api/public_api/v1     (page_access_token)
- Page API v2  : https://pages.fm/api/public_api/v2     (page_access_token, dùng cho list conversations)

Token truyền qua query param, không dùng header.
Rate limit: 5 request / giây / page -> mỗi PancakePageClient tự giãn cách request
và tự retry khi gặp 429 / 5xx / lỗi mạng.
"""

import time
import threading
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from loguru import logger

USER_API_BASE = "https://pages.fm/api/v1"
PAGE_API_V1 = "https://pages.fm/api/public_api/v1"
PAGE_API_V2 = "https://pages.fm/api/public_api/v2"

UTC_PLUS_7 = timezone(timedelta(hours=7))

# 5 req/s/page -> 0.22s giữa 2 request để còn biên an toàn
MIN_INTERVAL_SECONDS = 0.22
MAX_RETRIES = 5
REQUEST_TIMEOUT = 60


class PancakeAPIError(Exception):
    """Lỗi gọi Pancake API sau khi đã retry hết."""


def day_window_utc7(day: date):
    """
    Trả về (since, until) Unix timestamp (giây) cho trọn 1 ngày theo giờ Việt Nam.
    Pancake nhận since/until ở UTC+0, nên quy đổi 00:00:00 -> 23:59:59 giờ VN sang epoch.
    """
    start = datetime(day.year, day.month, day.day, tzinfo=UTC_PLUS_7)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())


def date_range_str(day: date) -> str:
    """Định dạng date_range cho statistics/users & customer_engagements (giờ local của page)."""
    d = day.strftime("%d/%m/%Y")
    return f"{d} 00:00:00 - {d} 23:59:59"


def parse_ts(value: Any) -> Optional[datetime]:
    """
    Parse timestamp Pancake (ISO string, có/không có 'Z', hoặc epoch giây/ms) -> datetime naive UTC.
    Trả về None nếu không parse được (giữ bản gốc trong cột raw).
    """
    if value in (None, "", 0):
        return None
    try:
        if isinstance(value, (int, float)):
            v = float(value)
            if v > 1e12:  # milliseconds
                v = v / 1000
            return datetime.fromtimestamp(v, tz=timezone.utc).replace(tzinfo=None)
        s = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


class _RateLimiter:
    """Giãn cách tối thiểu giữa các request của cùng 1 page (thread-safe)."""

    def __init__(self, min_interval: float = MIN_INTERVAL_SECONDS):
        self.min_interval = min_interval
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.monotonic()
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.monotonic()


def _request(method: str, url: str, params: Dict, limiter: Optional[_RateLimiter] = None,
             json_body: Optional[Dict] = None) -> Any:
    """
    Gọi HTTP có retry. Raise PancakeAPIError khi hết retry hoặc gặp lỗi 4xx (trừ 429)
    -> KHÔNG nuốt lỗi, để job fail rõ ràng trên Airflow.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        if limiter:
            limiter.wait()
        try:
            resp = requests.request(method, url, params=params, json=json_body, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            last_err = f"network error: {e}"
            wait = min(2 ** attempt, 30)
            logger.warning(f"[Pancake] {last_err} – retry {attempt}/{MAX_RETRIES} sau {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code == 429 or resp.status_code >= 500:
            last_err = f"HTTP {resp.status_code}"
            wait = min(2 ** attempt, 30)
            logger.warning(f"[Pancake] {last_err} {url} – retry {attempt}/{MAX_RETRIES} sau {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code >= 400:
            # 401/403/404...: token sai/hết hạn, page không tồn tại -> không retry
            raise PancakeAPIError(f"HTTP {resp.status_code} {url}: {resp.text[:300]}")

        try:
            data = resp.json()
        except ValueError:
            raise PancakeAPIError(f"Response không phải JSON {url}: {resp.text[:300]}")

        # Một số endpoint trả {success: false, message: ...} với HTTP 200
        if isinstance(data, dict) and data.get("success") is False:
            raise PancakeAPIError(f"API báo lỗi {url}: {str(data)[:300]}")
        return data

    raise PancakeAPIError(f"Hết {MAX_RETRIES} lần retry {url}: {last_err}")


# ============================================================================
# USER-LEVEL API (access_token của user)
# ============================================================================

def list_pages(user_access_token: str) -> List[Dict]:
    """GET /pages – danh sách page user quản lý (không trả page_access_token)."""
    data = _request("GET", f"{USER_API_BASE}/pages", {"access_token": user_access_token})
    if isinstance(data, dict):
        cats = data.get("categorized") or {}
        pages = cats.get("activated") or data.get("pages") or data.get("data") or []
        return pages
    return data or []


# ============================================================================
# PAGE-LEVEL API (page_access_token)
# ============================================================================

class PancakePageClient:
    """Client cho 1 page. Mỗi page có limiter riêng (rate limit tính theo page_id)."""

    def __init__(self, page_id: str, page_access_token: str):
        self.page_id = str(page_id)
        self.token = page_access_token
        self.limiter = _RateLimiter()

    def _get(self, base: str, path: str, **params) -> Any:
        params = {k: v for k, v in params.items() if v is not None}
        params["page_access_token"] = self.token
        url = f"{base}/pages/{self.page_id}/{path}"
        return _request("GET", url, params, self.limiter)

    # ---------------- Conversations ----------------
    def iter_conversations(self, since: int, until: int, order_by: str = "updated_at",
                           max_pages: int = 5000):
        """
        Duyệt toàn bộ hội thoại trong khoảng since/until (60 hội thoại/lần, phân trang
        bằng last_conversation_id). Yield từng conversation (dict).
        """
        last_id = None
        seen_ids = set()
        for _ in range(max_pages):
            data = self._get(PAGE_API_V2, "conversations", since=since, until=until,
                             order_by=order_by, last_conversation_id=last_id)
            convs = (data or {}).get("conversations") or []
            new = [c for c in convs if c.get("id") not in seen_ids]
            if not new:
                return
            for c in new:
                seen_ids.add(c.get("id"))
                yield c
            if len(convs) < 60:
                return
            last_id = convs[-1].get("id")
        logger.warning(f"[Pancake] page {self.page_id}: chạm giới hạn {max_pages} lượt phân trang conversations")

    # ---------------- Messages ----------------
    def get_all_messages(self, conversation_id: str, expected_count: Optional[int] = None,
                         max_calls: int = 200) -> List[Dict]:
        """
        Lấy toàn bộ tin nhắn của 1 hội thoại. API trả 30 tin/lần, lùi dần bằng current_count
        (= số tin đã lấy). Dừng khi không còn tin mới hoặc đủ expected_count.
        """
        messages: Dict[str, Dict] = {}
        current = None
        for _ in range(max_calls):
            data = self._get(PAGE_API_V1, f"conversations/{conversation_id}/messages",
                             current_count=current)
            batch = (data or {}).get("messages") or []
            added = 0
            for m in batch:
                mid = m.get("id")
                if mid and mid not in messages:
                    messages[mid] = m
                    added += 1
            if added == 0 or len(batch) < 30:
                break
            if expected_count and len(messages) >= expected_count:
                break
            current = len(messages)
        return list(messages.values())

    # ---------------- Customers ----------------
    def iter_page_customers(self, since: int, until: int, order_by: str = "updated_at",
                            page_size: int = 100, max_pages: int = 10000):
        """Duyệt khách hàng của page cập nhật trong since/until (100/trang)."""
        for page_number in range(1, max_pages + 1):
            data = self._get(PAGE_API_V1, "page_customers", since=since, until=until,
                             page_number=page_number, page_size=page_size, order_by=order_by)
            customers = (data or {}).get("customers") or []
            for c in customers:
                yield c
            if len(customers) < page_size:
                return

    # ---------------- Statistics ----------------
    def get_statistics(self, stat_type: str, day: date) -> Any:
        """
        Gọi API thống kê cho 1 ngày. stat_type:
          pages, tags, pages_campaigns, customer_feedbacks  -> since/until (UTC+0 epoch)
          ads_by_time, ads_by_id                            -> since/until + type
          users, customer_engagements                        -> date_range (giờ local page)
          page_customers                                     -> date_range + group_by
        """
        since, until = day_window_utc7(day)
        if stat_type in ("pages", "tags", "pages_campaigns", "customer_feedbacks"):
            return self._get(PAGE_API_V1, f"statistics/{stat_type}", since=since, until=until)
        if stat_type in ("ads_by_time", "ads_by_id"):
            return self._get(PAGE_API_V1, "statistics/ads", since=since, until=until,
                             type=stat_type.replace("ads_", ""))
        if stat_type in ("users", "customer_engagements"):
            return self._get(PAGE_API_V1, f"statistics/{stat_type}", date_range=date_range_str(day))
        if stat_type == "page_customers":
            return self._get(PAGE_API_V1, "statistics/page_customers",
                             date_range=date_range_str(day), group_by="day")
        raise ValueError(f"stat_type không hỗ trợ: {stat_type}")

    # ---------------- Others ----------------
    def get_tags(self) -> Any:
        return self._get(PAGE_API_V1, "tags")

    def get_users(self) -> Any:
        return self._get(PAGE_API_V1, "users")
