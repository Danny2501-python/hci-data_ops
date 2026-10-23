"""
Deal Detail Enrichment (brand thường: sci/kn/pr/hh) — lấy deal_id qua API /deals

VÌ SAO CÓ FILE NÀY (đối chiếu 22/10/2026, xem doc cs_deal_vs_ticket_check_2026-10-22.md):
cs_deal_detail_api_v2 thiếu ~2/3 deal Kangnam (22/10: thiếu 789 / 1.152). Hai nguyên nhân:
  1. Silver webhook chỉ nhận dòng api_log bắt đầu bằng "{'deal'". Nhiều deal chỉ có webhook
     dạng "{'account_id': ..., 'action': 'create', 'lead': {...}}" → không bao giờ vào Silver
     → không bao giờ được enrich.
  2. Gọi GET /deal/{id} quá nhanh (0,05s/lần) → CareSoft trả HTTP 429, không thử lại, deal mất hẳn.

CÁCH SỬA (giống THV, xem deal_detail_enrichment_thv.py):
  - deal_id cần enrich = HỢP của 3 nguồn:
      a) API danh sách GET /deals theo 3 cửa sổ (updated hôm nay, created hôm nay,
         created hôm qua — vét deal đến trễ);
      b) cách cũ: webhook Silver (giữ lại để không bớt deal nào so với trước);
      c) deal lỗi tạm thời (429 / 5xx / timeout) trong bảng lỗi Gold của các ngày trước.
  - GET /deal/{id} có giãn cách tối thiểu giữa các lần gọi + tự thử lại khi 429 / 5xx / lỗi mạng.
  - Phần ghi Gold / bảng lỗi giữ nguyên logic của deal_detail_enrichment.py (lớp cha).

File gốc deal_detail_enrichment.py KHÔNG sửa. DAG caresoft_deal_etl chỉ đổi đường dẫn script
của task deal_detail_enrichment sang file này.

Biến môi trường (tuỳ chọn):
  CARESOFT_PAGE_SIZE        số deal / trang khi gọi /deals (mặc định 500, tối đa của CareSoft)
  CARESOFT_MIN_INTERVAL     số giây tối thiểu giữa 2 lần gọi API (mặc định 0.25)
  CARESOFT_RETRY_ERROR_DAYS số ngày lỗi trước đó cần gọi lại (mặc định 2)
"""

import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Optional, Tuple, Any

import requests
from loguru import logger
from pyiceberg.expressions import EqualTo, And

# Add parent directories to path for imports (giống deal_detail_enrichment_thv.py)
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

import deal_detail_enrichment
from common.brand_config import get_active_brands
from common.caresoft_api import get_api_url, get_api_headers

CARESOFT_PAGE_SIZE = int(os.getenv('CARESOFT_PAGE_SIZE', '500'))
MIN_INTERVAL = float(os.getenv('CARESOFT_MIN_INTERVAL', '0.25'))
RETRY_ERROR_DAYS = int(os.getenv('CARESOFT_RETRY_ERROR_DAYS', '2'))

# Thử lại khi 429 / 5xx / lỗi mạng: chờ tăng dần (giây)
RETRY_BACKOFF = (5, 15, 30, 60, 120)
RETRY_STATUS = (429, 500, 502, 503, 504)
MAX_RETRY_WAIT = 300

_last_call_ts = 0.0


def _pace():
    """Giữ khoảng cách tối thiểu MIN_INTERVAL giây giữa 2 lần gọi API."""
    global _last_call_ts
    wait = MIN_INTERVAL - (time.monotonic() - _last_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.monotonic()


def _parse_retry_after(value) -> Optional[int]:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def _get_with_retry(url: str, headers: dict, params: Optional[dict] = None, timeout: int = 30):
    """
    GET có giãn cách + thử lại khi 429 / 5xx / lỗi mạng.
    Trả về (response, error). response = None nếu hết lượt thử; lỗi 4xx khác trả ngay.
    """
    error_msg = None
    retry_after = None

    for attempt in range(len(RETRY_BACKOFF) + 1):
        if attempt:
            wait = RETRY_BACKOFF[attempt - 1]
            if retry_after:
                wait = min(max(wait, retry_after), MAX_RETRY_WAIT)
            logger.warning(f"{error_msg[:120]} — chờ {wait}s rồi thử lại (lần {attempt}/{len(RETRY_BACKOFF)})")
            time.sleep(wait)

        retry_after = None
        _pace()
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
        except Exception as e:
            error_msg = str(e)
            continue

        if response.status_code == 200:
            return response, None

        error_msg = f"HTTP {response.status_code}: {response.text[:500]}"
        if response.status_code not in RETRY_STATUS:
            return None, error_msg
        retry_after = _parse_retry_after(response.headers.get('Retry-After'))

    return None, f"{error_msg} (đã thử lại {len(RETRY_BACKOFF)} lần)"


def call_caresoft_api_with_retry(
    brand: dict,
    endpoint: str,
    resource_id: Optional[int] = None,
    timeout: int = 10,
    response_key: Optional[str] = None
) -> Tuple[Optional[Any], Optional[str]]:
    """Cùng chữ ký + cùng kiểu trả về với common.caresoft_api.call_caresoft_api, thêm giãn cách + thử lại."""
    path = f"{endpoint}/{resource_id}" if resource_id is not None else endpoint
    response, error = _get_with_retry(get_api_url(brand, path), get_api_headers(brand), timeout=max(timeout, 20))
    if error:
        return None, error
    try:
        data = response.json()
    except Exception as e:
        return None, f"JSON decode error: {e}"
    if response_key:
        data = data.get(response_key)
    return data, None


class DealDetailEnrichmentListApi(deal_detail_enrichment.DealDetailEnrichment):
    """
    Brand thường (lặp qua get_active_brands() như lớp cha).
    deal_id = /deals list API ∪ webhook Silver ∪ lỗi tạm thời của các ngày trước.
    """

    def __init__(self, snapshot_date: date = None):
        super().__init__(snapshot_date)
        self.brands_by_erp_id = {b['erp_id']: b for b in get_active_brands()}
        self.list_api_failed = []  # brand có cửa sổ /deals bị lỗi hẳn → task đỏ cuối job

    # ---------- nguồn a: API /deals ----------
    def _fetch_deal_ids_window(self, brand: dict, since_key: str, to_key: str,
                               date_since: str, date_to: str) -> Tuple[set, bool]:
        """Phân trang GET /deals cho 1 cửa sổ. Trả (tập deal_id, ok)."""
        deal_ids = set()
        page = 1
        while True:
            params = {since_key: date_since, to_key: date_to, 'page': page, 'count': CARESOFT_PAGE_SIZE}
            response, error = _get_with_retry(get_api_url(brand, 'deals'), get_api_headers(brand), params=params)
            if error:
                logger.error(f"❌ [{brand.get('name')}] /deals {since_key}={date_since} page {page}: {error[:200]}")
                return deal_ids, False

            data = response.json()
            deals = data.get('deals', []) or []
            for d in deals:
                if d.get('id'):
                    deal_ids.add(int(d['id']))

            logger.info(f"[{brand.get('name')}] [{since_key}={date_since}] page {page}: "
                        f"{len(deals)} deals (numFound={data.get('numFound')})")

            if len(deals) < CARESOFT_PAGE_SIZE:
                return deal_ids, True
            page += 1

    def _deal_ids_from_list_api(self, brand: dict) -> set:
        # Cùng 3 cửa sổ với THV (deal_detail_enrichment_thv.py)
        today = self.snapshot_date + timedelta(days=1)
        yesterday = today - timedelta(days=1)
        day_before_yesterday = yesterday - timedelta(days=1)

        def fmt(d):
            return d.strftime('%Y-%m-%d') + 'T00:00:00Z'

        windows = [
            ('updated_since', 'updated_to', fmt(yesterday), fmt(today)),
            ('created_since', 'created_to', fmt(yesterday), fmt(today)),
            ('created_since', 'created_to', fmt(day_before_yesterday), fmt(yesterday)),
        ]
        deal_ids = set()
        for since_key, to_key, date_since, date_to in windows:
            ids, ok = self._fetch_deal_ids_window(brand, since_key, to_key, date_since, date_to)
            deal_ids |= ids
            if not ok:
                self.list_api_failed.append(f"{brand.get('name')} {since_key}={date_since}")
        return deal_ids

    # ---------- nguồn c: lỗi tạm thời của các ngày trước ----------
    def _deal_ids_from_previous_errors(self, brand_erp_id: int) -> set:
        deal_ids = set()
        try:
            error_table = self.gold_catalog.load_table(deal_detail_enrichment.DEAL_DETAIL_ERROR_TABLE)
        except Exception as e:
            logger.warning(f"Không đọc được bảng lỗi: {e}")
            return deal_ids

        for n in range(1, RETRY_ERROR_DAYS + 1):
            d = self.snapshot_date - timedelta(days=n)
            try:
                scan = error_table.scan(
                    row_filter=And(EqualTo('snapshot_date', d), EqualTo('brand_id', brand_erp_id)),
                    selected_fields=('id', 'error_message'),
                )
                for batch in scan.to_arrow_batch_reader():
                    ids = batch['id'].to_pylist()
                    msgs = batch['error_message'].to_pylist()
                    for deal_id, msg in zip(ids, msgs):
                        # 404 "Deals not exist" = deal đã bị xoá, gọi lại cũng vô ích
                        if deal_id and not str(msg or '').startswith('HTTP 404'):
                            deal_ids.add(int(deal_id))
            except Exception as e:
                logger.warning(f"Không đọc được lỗi ngày {d}: {e}")
        return deal_ids

    # ---------- hợp 3 nguồn ----------
    def get_deal_ids_to_enrich(self, brand_erp_id: int):
        brand = self.brands_by_erp_id.get(brand_erp_id)

        from_list = set()
        if brand:
            from_list = self._deal_ids_from_list_api(brand)
        else:
            logger.warning(f"Không tìm thấy brand erp_id={brand_erp_id} trong get_active_brands()")

        from_webhook = set()
        try:
            from_webhook = {int(x) for x in (super().get_deal_ids_to_enrich(brand_erp_id) or []) if x}
        except Exception as e:
            logger.warning(f"Không đọc được webhook Silver cho brand {brand_erp_id}: {e}")

        from_errors = self._deal_ids_from_previous_errors(brand_erp_id)

        all_ids = from_list | from_webhook | from_errors
        logger.success(
            f"✅ brand {brand_erp_id}: {len(all_ids)} deal cần enrich "
            f"(/deals: {len(from_list)}, webhook: {len(from_webhook)}, "
            f"chỉ có ở webhook: {len(from_webhook - from_list)}, gọi lại lỗi cũ: {len(from_errors)})"
        )
        return list(all_ids)

    def run(self):
        # Thay hàm gọi API chi tiết trong lớp cha bằng bản có giãn cách + thử lại
        original_call = deal_detail_enrichment.call_caresoft_api
        try:
            deal_detail_enrichment.call_caresoft_api = call_caresoft_api_with_retry
            super().run()
        finally:
            deal_detail_enrichment.call_caresoft_api = original_call


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = (datetime.strptime(snapshot_date_str, "%Y-%m-%d").date()
                     if snapshot_date_str else (date.today() - timedelta(days=1)))

    logger.info(f"Starting Deal Detail Enrichment (list API) for {snapshot_date}")
    job = DealDetailEnrichmentListApi(snapshot_date)
    job.run()

    if job.list_api_failed:
        # Không để task xanh khi danh sách deal lấy không đủ
        logger.error(f"❌ /deals lỗi ở: {job.list_api_failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
