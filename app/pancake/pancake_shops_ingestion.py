"""Fetch Pancake POS shops and sync them to PostgreSQL."""

import csv
import json
import os
import sys
import tempfile
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.database import PostgreSQLHelper
from common.pancake_api import PancakeAPIClient


def _unique_strings(values: Iterable[Any]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def extract_shop_phone_numbers(
    shop: Dict[str, Any], managed_pages: Optional[List[Dict[str, Any]]] = None
) -> List[str]:
    """Collect shop phones exposed on POS shop and its connected pages."""
    values: List[Any] = [shop.get("phone"), shop.get("phone_number")]
    shop_id = str(shop.get("id") or "")
    connected_page_ids = set()
    for page in shop.get("pages") or []:
        if isinstance(page, dict):
            values.extend([page.get("phone"), page.get("phone_number")])
            if page.get("id"):
                connected_page_ids.add(str(page["id"]))
    for page in managed_pages or []:
        if not isinstance(page, dict):
            continue
        page_id = str(page.get("id") or "")
        if str(page.get("shop_id") or "") != shop_id and page_id not in connected_page_ids:
            continue
        platform_info = page.get("platform_extra_info") or {}
        if not isinstance(platform_info, dict):
            platform_info = {}
        values.extend(
            [
                page.get("phone"),
                page.get("phone_number"),
                platform_info.get("phone"),
                platform_info.get("phone_number"),
            ]
        )
    return _unique_strings(values)


class PancakeShopsIngestion:
    TABLE_NAME = "dwh_pancake_shops"

    def __init__(self, db: PostgreSQLHelper):
        self.db = db
        self.client = PancakeAPIClient()

    def ensure_table(self) -> None:
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                shop_id VARCHAR(100) PRIMARY KEY,
                name VARCHAR(255),
                warehouse_id VARCHAR(100),
                currency VARCHAR(50),
                phone VARCHAR(50),
                phone_numbers JSONB NOT NULL DEFAULT '[]'::jsonb,
                is_active BOOLEAN DEFAULT TRUE,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            ALTER TABLE {self.TABLE_NAME}
                ADD COLUMN IF NOT EXISTS phone_numbers JSONB
                NOT NULL DEFAULT '[]'::jsonb;
            """
        )

    def run(self) -> None:
        logger.info("Starting Pancake Shops Ingestion...")
        self.ensure_table()

        shops = self.client.get_shops()
        if not shops:
            raise RuntimeError("Pancake returned no shops; refusing a silent success")
        logger.info(f"Fetched {len(shops)} shops from Pancake POS API")
        managed_pages = self.client.get_pages()
        logger.info(
            f"Fetched {len(managed_pages)} managed pages for shop phone enrichment"
        )

        row_count = 0
        with tempfile.NamedTemporaryFile(
            mode="w", newline="", encoding="utf-8", delete=False
        ) as tmp_file:
            writer = csv.writer(tmp_file, delimiter="\t")
            synced_at = datetime.now().isoformat()

            for shop in shops:
                shop_id = str(shop.get("id") or "").strip()
                if not shop_id:
                    logger.warning("Skipping Pancake shop without id")
                    continue

                phone_numbers = extract_shop_phone_numbers(shop, managed_pages)
                writer.writerow(
                    [
                        shop_id,
                        shop.get("name") or "",
                        str(shop.get("warehouse_id") or ""),
                        shop.get("currency") or "VND",
                        phone_numbers[0] if phone_numbers else "",
                        json.dumps(phone_numbers, ensure_ascii=False),
                        bool(shop.get("is_active", True)),
                        synced_at,
                    ]
                )
                row_count += 1
            tmp_path = tmp_file.name

        try:
            staging_table = f"staging_{self.TABLE_NAME}"
            self.db.execute(
                f"CREATE TEMP TABLE {staging_table} "
                f"(LIKE {self.TABLE_NAME} INCLUDING ALL);"
            )
            columns = (
                "shop_id, name, warehouse_id, currency, phone, phone_numbers, "
                "is_active, synced_at"
            )
            copy_sql = (
                f"COPY {staging_table} ({columns}) FROM STDIN "
                "WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', ESCAPE '\"')"
            )
            with open(tmp_path, "r", encoding="utf-8") as source_file:
                with self.db.conn.cursor() as cursor:
                    cursor.copy_expert(copy_sql, source_file)
                self.db.conn.commit()

            self.db.execute(
                f"""
                INSERT INTO {self.TABLE_NAME} (
                    shop_id, name, warehouse_id, currency, phone, phone_numbers,
                    is_active, synced_at
                )
                SELECT
                    shop_id, name, warehouse_id, currency, phone, phone_numbers,
                    is_active, synced_at
                FROM {staging_table}
                ON CONFLICT (shop_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    warehouse_id = EXCLUDED.warehouse_id,
                    currency = EXCLUDED.currency,
                    phone = EXCLUDED.phone,
                    phone_numbers = EXCLUDED.phone_numbers,
                    is_active = EXCLUDED.is_active,
                    synced_at = EXCLUDED.synced_at;
                """
            )
            logger.info(
                f"Successfully upserted {row_count} shops into {self.TABLE_NAME}"
            )
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


def _required_postgres_uri() -> str:
    uri = os.getenv("POSTGRES_URI")
    if not uri:
        raise RuntimeError("POSTGRES_URI is required")
    return uri


if __name__ == "__main__":
    with PostgreSQLHelper(_required_postgres_uri()) as db_helper:
        PancakeShopsIngestion(db_helper).run()
