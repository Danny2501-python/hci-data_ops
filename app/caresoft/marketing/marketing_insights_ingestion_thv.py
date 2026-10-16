"""
CareSoft Marketing Insights Ingestion - Tiêu Hóa Việt (erp_id = 8, domain drgiang)
API -> PostgreSQL (public.cs_facebook_insight)

Dùng lại NGUYÊN logic của marketing_insights_ingestion.py: cùng API marketing/insights,
cùng breakdowns / fields, cùng cách ghi (bulk_load delete_insert theo khóa) vào public.cs_facebook_insight.

Khác duy nhất: lấy brand theo erp_id = 8 bằng get_brand_by_erp_id().
Lý do: caresoft.core_brand để THV is_active = false để các DAG CareSoft chung bỏ qua
(THV có các DAG *_thv riêng), nên job marketing insights chung không chạy cho THV.

Thêm 1 chốt: nếu cả khoảng ngày không lấy được dòng nào -> exit 1 để task Airflow báo đỏ
(token sai / mất kết nối tài khoản quảng cáo sẽ không bị "xanh giả").

Usage: python marketing_insights_ingestion_thv.py 2026-10-01 2026-10-07
"""

import os
import sys
from datetime import datetime, timedelta

from loguru import logger

# Cùng cách import như các script *_thv khác (vd call/call_ingestion_thv.py)
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.dirname(os.path.dirname(current_dir)))

from marketing_insights_ingestion import MarketingInsightsIngestion
from common.brand_config import get_brand_by_erp_id

THV_ERP_ID = 8


class MarketingInsightsIngestionTHV(MarketingInsightsIngestion):
    def run(self):
        brand = get_brand_by_erp_id(THV_ERP_ID)
        if not brand or not brand.get("caresoft_domain") or not brand.get("caresoft_key"):
            logger.error(f"Không tìm thấy brand erp_id={THV_ERP_ID} (hoặc thiếu domain/key) trong core_brand")
            sys.exit(1)

        delta = self.until_date - self.since_date
        date_list = [self.since_date + timedelta(days=i) for i in range(delta.days + 1)]

        total_records = 0
        for target_date in date_list:
            logger.info(f"===> Processing date: {target_date} <===")
            raw_data = self.fetch_insights(brand, target_date)
            if not raw_data:
                logger.info(f"No insights found for brand {brand['name']} on {target_date}")
                continue
            transformed = [self.transform_record(r, brand["erp_id"], target_date) for r in raw_data]
            self.save_to_postgres(transformed, brand["erp_id"])  # lỗi ghi DB -> raise -> task đỏ
            total_records += len(transformed)

        self.report_api_failures()

        if total_records == 0:
            logger.error(
                f"Không lấy được dòng nào cho {brand['caresoft_domain']} từ {self.since_date} đến "
                f"{self.until_date}. Kiểm tra token CareSoft / kết nối tài khoản quảng cáo."
            )
            sys.exit(1)
        logger.success(f"THV ingestion completed. Total records saved: {total_records}")


if __name__ == "__main__":
    since_str = sys.argv[1] if len(sys.argv) > 1 else None
    until_str = sys.argv[2] if len(sys.argv) > 2 else None
    try:
        since_date = datetime.strptime(since_str, "%Y-%m-%d").date() if since_str else None
        until_date = datetime.strptime(until_str, "%Y-%m-%d").date() if until_str else None
    except ValueError as e:
        logger.error(f"Invalid date format. Use YYYY-MM-DD. Error: {e}")
        sys.exit(1)

    MarketingInsightsIngestionTHV(since_date=since_date, until_date=until_date).run()
