"""
Kéo bù deal CareSoft bị thiếu trong cs_deal_detail_api_v2 (brand thường: kn/pr/hh/sci)

Dùng một lần (hoặc khi cần), chạy qua DAG caresoft_deal_backfill_etl.

Vì sao cần (doc cs_deal_vs_ticket_check_2026-10-22.md):
  - Kangnam: deal gặp HTTP 429 khi gọi GET /deal/{id} trước 22/10/2026 không bao giờ được gọi lại.
  - Paris: CareSoft ngừng gửi webhook deal từ ~25/05/2026 → bảng deal Paris dừng ở 25/05.
  Code hằng ngày mới (deal_detail_enrichment_list_api.py) chỉ xử lý từ 22/10/2026 trở đi.

Cách làm cho mỗi brand:
  1. Lấy toàn bộ deal_id tạo trong [since, until) qua API GET /deals (created_since/created_to),
     chia từng khoảng 7 ngày, phân trang 500.
  2. Trừ đi deal đã có trong Postgres cs_deal_detail_api_v2 (theo brand_id) → chỉ gọi chi tiết
     cho deal còn thiếu (tránh hàng chục nghìn lượt gọi thừa).
  3. Gọi GET /deal/{id} + ghi Gold bằng đúng logic của DealDetailEnrichmentListApi
     (giãn cách + thử lại 429/5xx), vào partition snapshot_date = tham số NHÃN.
  Các task sau trong DAG (serving Postgres, comments) chạy cho cùng snapshot_date đó.

Tham số dòng lệnh:
  argv[1] snapshot_date  nhãn partition Gold (YYYY-MM-DD). Nên dùng ngày cũ hơn 3 ngày mà job
                         hằng ngày không ghi nữa (mặc định DAG: 2026-10-15) để không đụng nhau.
  argv[2] since          ngày bắt đầu (YYYY-MM-DD, tính cả ngày này)
  argv[3] until          ngày kết thúc (YYYY-MM-DD, KHÔNG tính ngày này)
  argv[4] brands         danh sách erp_id, cách nhau dấu phẩy, ví dụ "3" hoặc "1,3"
  argv[5] dry_run        "true" = chỉ đếm, không gọi chi tiết, không ghi Gold (mặc định "false")

Biến môi trường: POSTGRES_URI (Postgres để đọc deal đã có), cộng các biến của
deal_detail_enrichment_list_api.py (CARESOFT_MIN_INTERVAL, ...).
"""

import os
import sys
from datetime import date, datetime, timedelta

from loguru import logger

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

import deal_detail_enrichment
import deal_detail_enrichment_list_api as list_api

POSTGRES_URI = os.getenv('POSTGRES_URI', '')
SERVING_TABLE_NAME = os.getenv('SERVING_TABLE_NAME', 'cs_deal_detail_api_v2')
CHUNK_DAYS = int(os.getenv('BACKFILL_CHUNK_DAYS', '7'))


def _connect_postgres():
    try:
        import psycopg2
        return psycopg2.connect(POSTGRES_URI)
    except ImportError:
        import psycopg
        return psycopg.connect(POSTGRES_URI)


def get_existing_deal_ids(brand_erp_id: int) -> set:
    """Toàn bộ id deal của brand đã có trong Postgres."""
    if not POSTGRES_URI:
        raise RuntimeError("Thiếu POSTGRES_URI")
    conn = _connect_postgres()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT id FROM public.{SERVING_TABLE_NAME} WHERE brand_id = %s", (brand_erp_id,))
        ids = {int(r[0]) for r in cur.fetchall() if r[0] is not None}
        cur.close()
        return ids
    finally:
        conn.close()


class DealBackfillListApi(list_api.DealDetailEnrichmentListApi):

    def __init__(self, snapshot_date: date, since: date, until: date, brand_erp_ids: list, dry_run: bool):
        super().__init__(snapshot_date)
        self.since = since
        self.until = until
        self.brand_erp_ids = brand_erp_ids
        self.dry_run = dry_run
        self.summary = []

    def _deal_ids_created_in_range(self, brand: dict) -> set:
        deal_ids = set()
        start = self.since
        while start < self.until:
            end = min(start + timedelta(days=CHUNK_DAYS), self.until)
            ids, ok = self._fetch_deal_ids_window(
                brand, 'created_since', 'created_to',
                start.strftime('%Y-%m-%d') + 'T00:00:00Z',
                end.strftime('%Y-%m-%d') + 'T00:00:00Z',
            )
            deal_ids |= ids
            if not ok:
                self.list_api_failed.append(f"brand {brand.get('erp_id')} created {start} → {end}")
            start = end
        return deal_ids

    def get_deal_ids_to_enrich(self, brand_erp_id: int):
        brand = self.brands_by_erp_id.get(brand_erp_id)
        if not brand:
            logger.warning(f"Không tìm thấy brand erp_id={brand_erp_id}")
            return []

        on_caresoft = self._deal_ids_created_in_range(brand)
        existing = get_existing_deal_ids(brand_erp_id)
        missing = on_caresoft - existing

        msg = (f"brand {brand_erp_id}, deal tạo {self.since} → {self.until} (không tính ngày cuối): "
               f"CareSoft {len(on_caresoft)}, đã có trong Postgres {len(on_caresoft & existing)}, "
               f"THIẾU {len(missing)}")
        logger.success(f"✅ {msg}")
        self.summary.append(msg)

        if self.dry_run:
            logger.info("dry_run = true → không gọi chi tiết, không ghi Gold")
            return []
        return sorted(missing)

    def run(self):
        # Chỉ chạy các brand được chọn
        selected = [self.brands_by_erp_id[b] for b in self.brand_erp_ids if b in self.brands_by_erp_id]
        missing_cfg = [b for b in self.brand_erp_ids if b not in self.brands_by_erp_id]
        if missing_cfg:
            logger.warning(f"Brand không active / không có trong core_brand: {missing_cfg}")

        original_get_active_brands = deal_detail_enrichment.get_active_brands
        try:
            deal_detail_enrichment.get_active_brands = lambda: selected
            super().run()
        finally:
            deal_detail_enrichment.get_active_brands = original_get_active_brands


def main():
    if len(sys.argv) < 5:
        print("Usage: deal_backfill_list_api.py <snapshot_date> <since> <until> <brands> [dry_run]")
        sys.exit(1)

    snapshot_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    since = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
    until = datetime.strptime(sys.argv[3], "%Y-%m-%d").date()
    brand_erp_ids = [int(x) for x in sys.argv[4].split(',') if x.strip()]
    dry_run = len(sys.argv) > 5 and sys.argv[5].strip().lower() in ('true', '1', 'yes')

    if since >= until:
        logger.error("since phải nhỏ hơn until")
        sys.exit(1)

    logger.info(f"Backfill deal: snapshot(nhãn)={snapshot_date}, {since} → {until}, brands={brand_erp_ids}, dry_run={dry_run}")
    job = DealBackfillListApi(snapshot_date, since, until, brand_erp_ids, dry_run)
    job.run()

    logger.info("=" * 60)
    for line in job.summary:
        logger.info(f"TỔNG KẾT: {line}")

    if job.list_api_failed:
        logger.error(f"❌ /deals lỗi ở: {job.list_api_failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
