"""
CMS mkt – BRONZE ingestion
Postgres CMS mkt (public.*) -> Iceberg bronze.cms_mkt.<bảng>

Dùng lại NGUYÊN hci_care/hci_care_bronze_ingestion.py (không sửa file gốc). File này chỉ
thêm các cột KHÔNG kéo riêng cho CMS rồi gọi main() của hci_care:

- partner_erp_data.data_erp_kn: JSON dữ liệu ERP Kangnam thô, rất nặng (bảng 441 MB,
  211 MB nằm ở TOAST) -> làm pod OOMKilled (4 GB) ngày 21/10/2026. Dữ liệu ERP đã có
  trong kho qua ERP sync; nối bằng customer_id / lead_id / phone của partner_erp_data.
- Cột giấy tờ tuỳ thân (hộ chiếu, CCCD, CMND...): báo cáo không cần, không đưa vào kho.

Nếu bảng Iceberg bronze/silver đã lỡ có cột nay bị loại -> xoá bảng Iceberg đó để
tạo lại sạch (mỗi partition là bản chụp FULL nên không mất gì). Bảng Postgres đích
(nếu đã có) giữ cột cũ, giá trị null.

Env / tham số: như hci_care_bronze_ingestion.py (SOURCE_URI, HCI_CARE_NAMESPACE,
--snapshot-date, --tables, ...).
"""

import os
import re
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger  # noqa: E402
from pyiceberg.exceptions import NoSuchTableError  # noqa: E402

from hci_care import hci_care_lakehouse as lh  # noqa: E402
from hci_care import hci_care_bronze_ingestion as bronze  # noqa: E402

# Cột không kéo theo bảng (CMS)
CMS_EXCLUDED_COLUMNS = {
    "partner_erp_data": {"data_erp_kn"},
}

# Cột giấy tờ tuỳ thân ở mọi bảng
PII_PATTERN = r"(pass_?port|cccd|cmnd|identification|id_card|citizen_id)"


def apply_exclusions():
    """Bổ sung luật loại cột vào module hci_care_lakehouse (chỉ trong process này)."""
    if not isinstance(getattr(lh, "EXCLUDED_COLUMNS", None), dict) or not hasattr(lh, "SECRET_PATTERN"):
        raise RuntimeError("hci_care_lakehouse không còn EXCLUDED_COLUMNS / SECRET_PATTERN -> "
                           "code hci_care đã đổi, cần sửa lại cms_mkt_bronze_ingestion.py")
    for table, cols in CMS_EXCLUDED_COLUMNS.items():
        lh.EXCLUDED_COLUMNS.setdefault(table, set()).update(cols)
    lh.SECRET_PATTERN = re.compile(f"{lh.SECRET_PATTERN.pattern}|{PII_PATTERN}")
    logger.info(f"CMS: thêm cột không kéo {CMS_EXCLUDED_COLUMNS} + mẫu giấy tờ {PII_PATTERN}")


def drop_stale_iceberg_tables(tables):
    """Xoá bảng Iceberg (bronze, silver) đang chứa cột nay đã bị loại."""
    for zone in ("bronze", "silver"):
        cat = lh.get_catalog(zone)
        for table in tables:
            ident = f"{lh.NAMESPACE}.{table}"
            try:
                tbl = cat.load_table(ident)
            except NoSuchTableError:
                continue
            stale = [name for name, _ in lh.table_columns(tbl) if lh.is_excluded(table, name, "")]
            if stale:
                logger.warning(f"{zone}.{ident}: có cột nay không kéo {stale} -> xoá bảng Iceberg để tạo lại")
                cat.drop_table(ident)


def main():
    apply_exclusions()
    args = bronze.parse_args()
    tables = lh.parse_tables_arg(args.tables) or lh.list_tables("bronze")
    drop_stale_iceberg_tables(tables)
    bronze.main()


if __name__ == "__main__":
    main()
