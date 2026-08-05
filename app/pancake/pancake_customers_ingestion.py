"""Ingest Pancake POS customers and optional Page customer enrichment."""

import csv
import json
import os
import sys
import tempfile
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.database import PostgreSQLHelper
from common.pancake_api import PancakeAPIClient


def _contact_values(value: Any, keys: Iterable[str]) -> List[str]:
    """Normalise strings or contact objects into a list of non-empty strings."""
    if value is None:
        return []
    values = value if isinstance(value, list) else [value]
    result: List[str] = []
    for item in values:
        candidate: Any = item
        if isinstance(item, dict):
            candidate = next((item.get(key) for key in keys if item.get(key)), None)
        text = str(candidate or "").strip()
        if text:
            result.append(text)
    return result


def _unique(values: Iterable[Any]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def normalize_customer(
    customer: Dict[str, Any],
    shop_id: str = "",
    page_id: str = "",
) -> Optional[Dict[str, Any]]:
    """Map documented Page customers and current POS customers to one contract."""
    shop_customer = customer.get("shop_customer") or {}
    if not isinstance(shop_customer, dict):
        shop_customer = {}

    page_customer_id = str(customer.get("id") or "").strip()
    shop_customer_id = str(shop_customer.get("id") or "").strip()
    customer_id = str(
        customer.get("customer_id")
        or customer.get("global_id")
        or page_customer_id
        or shop_customer_id
        or ""
    ).strip()
    if not customer_id:
        return None

    resolved_shop_id = str(
        shop_id or shop_customer.get("shop_id") or customer.get("shop_id") or ""
    ).strip()
    resolved_page_id = str(page_id or customer.get("page_id") or "").strip()

    phone_values: List[str] = []
    for source in (
        customer.get("phone"),
        customer.get("phone_number"),
        customer.get("phone_numbers"),
        shop_customer.get("phone"),
        shop_customer.get("phone_number"),
        shop_customer.get("phone_numbers"),
    ):
        phone_values.extend(
            _contact_values(source, ("phone_number", "phone", "value"))
        )
    for address in shop_customer.get("shop_customer_addresses") or []:
        if isinstance(address, dict):
            phone_values.extend(
                _contact_values(
                    address.get("phone_number"), ("phone_number", "phone", "value")
                )
            )

    email_values: List[str] = []
    for source in (
        customer.get("email"),
        customer.get("emails"),
        shop_customer.get("email"),
        shop_customer.get("emails"),
    ):
        email_values.extend(_contact_values(source, ("email", "value")))

    phones = _unique(phone_values)
    emails = _unique(email_values)
    now = datetime.now().isoformat()
    return {
        "customer_id": customer_id,
        "shop_ids": _unique([resolved_shop_id]),
        "page_ids": _unique([resolved_page_id]),
        "page_customer_ids": _unique([page_customer_id]),
        "shop_customer_ids": _unique([shop_customer_id]),
        "name": customer.get("name") or shop_customer.get("name") or "",
        "phone_numbers": phones,
        "emails": emails,
        "gender": customer.get("gender") or shop_customer.get("gender") or "",
        "created_at": (
            customer.get("inserted_at")
            or customer.get("created_at")
            or shop_customer.get("inserted_at")
            or now
        ),
        "updated_at": (
            customer.get("updated_at") or shop_customer.get("updated_at") or now
        ),
    }


def merge_customer(
    customers: Dict[str, Dict[str, Any]], record: Dict[str, Any]
) -> None:
    """Merge the same global customer seen on multiple pages/shops."""
    customer_id = record["customer_id"]
    existing = customers.get(customer_id)
    if existing is None:
        customers[customer_id] = record
        return

    for key in (
        "shop_ids",
        "page_ids",
        "page_customer_ids",
        "shop_customer_ids",
        "phone_numbers",
        "emails",
    ):
        existing[key] = _unique([*existing.get(key, []), *record.get(key, [])])

    for key in ("name", "gender"):
        if not existing.get(key) and record.get(key):
            existing[key] = record[key]
    if record.get("created_at") and record["created_at"] < existing["created_at"]:
        existing["created_at"] = record["created_at"]
    if record.get("updated_at") and record["updated_at"] > existing["updated_at"]:
        existing["updated_at"] = record["updated_at"]


def _page_token_map() -> Dict[str, str]:
    raw = os.getenv("PANCAKE_PAGE_ACCESS_TOKENS", "{}").strip() or "{}"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("PANCAKE_PAGE_ACCESS_TOKENS must be a JSON object") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("PANCAKE_PAGE_ACCESS_TOKENS must be a JSON object")
    return {
        str(page_id): str(token)
        for page_id, token in parsed.items()
        if page_id and token
    }


class PancakeCustomersIngestion:
    TABLE_NAME = "dwh_pancake_customers"

    def __init__(self, db: PostgreSQLHelper):
        self.db = db
        self.client = PancakeAPIClient()

    def ensure_table(self) -> None:
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                customer_id VARCHAR(100) PRIMARY KEY,
                shop_id VARCHAR(100),
                shop_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                page_id VARCHAR(100),
                page_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                page_customer_id VARCHAR(100),
                page_customer_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                shop_customer_id VARCHAR(100),
                shop_customer_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                name VARCHAR(255),
                phone VARCHAR(50),
                phone_numbers JSONB NOT NULL DEFAULT '[]'::jsonb,
                email VARCHAR(255),
                emails JSONB NOT NULL DEFAULT '[]'::jsonb,
                gender VARCHAR(20),
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            ALTER TABLE {self.TABLE_NAME}
                ADD COLUMN IF NOT EXISTS shop_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                ADD COLUMN IF NOT EXISTS page_id VARCHAR(100),
                ADD COLUMN IF NOT EXISTS page_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                ADD COLUMN IF NOT EXISTS page_customer_id VARCHAR(100),
                ADD COLUMN IF NOT EXISTS page_customer_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                ADD COLUMN IF NOT EXISTS shop_customer_id VARCHAR(100),
                ADD COLUMN IF NOT EXISTS shop_customer_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                ADD COLUMN IF NOT EXISTS phone_numbers JSONB NOT NULL DEFAULT '[]'::jsonb,
                ADD COLUMN IF NOT EXISTS emails JSONB NOT NULL DEFAULT '[]'::jsonb;
            """
        )

    def _collect_pos_customers(
        self, customers: Dict[str, Dict[str, Any]]
    ) -> None:
        shops = self.client.get_shops()
        if not shops:
            raise RuntimeError("Pancake returned no shops; refusing a silent success")
        logger.info(f"Collecting POS customers across {len(shops)} shops")

        for shop in shops:
            shop_id = str(shop.get("id") or "").strip()
            if not shop_id:
                continue
            page = 1
            while True:
                rows = self.client.get_customers(shop_id, page=page, page_size=100)
                for customer in rows:
                    record = normalize_customer(customer, shop_id=shop_id)
                    if record:
                        merge_customer(customers, record)
                if len(rows) < 100:
                    break
                page += 1

    def _collect_page_customers(
        self, customers: Dict[str, Dict[str, Any]]
    ) -> None:
        tokens = _page_token_map()
        if not tokens:
            logger.info(
                "PANCAKE_PAGE_ACCESS_TOKENS is not configured; using POS customer "
                "phones/emails without Page API enrichment"
            )
            return

        pages = self.client.get_pages()
        since = int(os.getenv("PANCAKE_PAGE_CUSTOMERS_SINCE", "0"))
        until = int(time.time())
        enriched_pages = 0

        for page_info in pages:
            page_id = str(page_info.get("id") or "").strip()
            page_token = tokens.get(page_id)
            if not page_id or not page_token:
                continue
            enriched_pages += 1
            page_number = 1
            while True:
                rows = self.client.get_page_customers(
                    page_id=page_id,
                    page_access_token=page_token,
                    since=since,
                    until=until,
                    page=page_number,
                    page_size=100,
                )
                for customer in rows:
                    record = normalize_customer(
                        customer,
                        shop_id=str(page_info.get("shop_id") or ""),
                        page_id=page_id,
                    )
                    if record:
                        merge_customer(customers, record)
                if len(rows) < 100:
                    break
                page_number += 1
                time.sleep(0.21)

        logger.info(
            f"Enriched customers from Page API for {enriched_pages}/{len(pages)} pages"
        )

    def fetch_all_customers(self) -> List[Dict[str, Any]]:
        customers: Dict[str, Dict[str, Any]] = {}
        self._collect_pos_customers(customers)
        self._collect_page_customers(customers)
        logger.info(f"Collected {len(customers)} unique Pancake customers")
        return list(customers.values())

    def run(self) -> None:
        logger.info("Starting Pancake Customers Ingestion...")
        self.ensure_table()
        customers = self.fetch_all_customers()
        if not customers:
            raise RuntimeError("Pancake returned no customers; refusing a silent success")

        with tempfile.NamedTemporaryFile(
            mode="w", newline="", encoding="utf-8", delete=False
        ) as tmp_file:
            writer = csv.writer(tmp_file, delimiter="\t")
            synced_at = datetime.now().isoformat()
            for customer in customers:
                shop_ids = customer["shop_ids"]
                page_ids = customer["page_ids"]
                page_customer_ids = customer["page_customer_ids"]
                shop_customer_ids = customer["shop_customer_ids"]
                phones = customer["phone_numbers"]
                emails = customer["emails"]
                writer.writerow(
                    [
                        customer["customer_id"],
                        shop_ids[0] if shop_ids else "",
                        json.dumps(shop_ids, ensure_ascii=False),
                        page_ids[0] if page_ids else "",
                        json.dumps(page_ids, ensure_ascii=False),
                        page_customer_ids[0] if page_customer_ids else "",
                        json.dumps(page_customer_ids, ensure_ascii=False),
                        shop_customer_ids[0] if shop_customer_ids else "",
                        json.dumps(shop_customer_ids, ensure_ascii=False),
                        customer["name"],
                        phones[0] if phones else "",
                        json.dumps(phones, ensure_ascii=False),
                        emails[0] if emails else "",
                        json.dumps(emails, ensure_ascii=False),
                        customer["gender"],
                        customer["created_at"],
                        customer["updated_at"],
                        synced_at,
                    ]
                )
            tmp_path = tmp_file.name

        try:
            staging_table = f"staging_{self.TABLE_NAME}"
            self.db.execute(
                f"CREATE TEMP TABLE {staging_table} "
                f"(LIKE {self.TABLE_NAME} INCLUDING ALL);"
            )
            columns = (
                "customer_id, shop_id, shop_ids, page_id, page_ids, "
                "page_customer_id, page_customer_ids, shop_customer_id, "
                "shop_customer_ids, name, phone, phone_numbers, email, emails, "
                "gender, created_at, updated_at, synced_at"
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
                    customer_id, shop_id, shop_ids, page_id, page_ids,
                    page_customer_id, page_customer_ids, shop_customer_id,
                    shop_customer_ids, name, phone, phone_numbers, email, emails,
                    gender, created_at, updated_at, synced_at
                )
                SELECT
                    customer_id, shop_id, shop_ids, page_id, page_ids,
                    page_customer_id, page_customer_ids, shop_customer_id,
                    shop_customer_ids, name, phone, phone_numbers, email, emails,
                    gender, created_at, updated_at, synced_at
                FROM {staging_table}
                ON CONFLICT (customer_id) DO UPDATE SET
                    shop_id = EXCLUDED.shop_id,
                    shop_ids = EXCLUDED.shop_ids,
                    page_id = EXCLUDED.page_id,
                    page_ids = EXCLUDED.page_ids,
                    page_customer_id = EXCLUDED.page_customer_id,
                    page_customer_ids = EXCLUDED.page_customer_ids,
                    shop_customer_id = EXCLUDED.shop_customer_id,
                    shop_customer_ids = EXCLUDED.shop_customer_ids,
                    name = EXCLUDED.name,
                    phone = EXCLUDED.phone,
                    phone_numbers = EXCLUDED.phone_numbers,
                    email = EXCLUDED.email,
                    emails = EXCLUDED.emails,
                    gender = EXCLUDED.gender,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    synced_at = EXCLUDED.synced_at;
                """
            )
            logger.info(
                f"Successfully upserted {len(customers)} customers into "
                f"{self.TABLE_NAME}"
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
        PancakeCustomersIngestion(db_helper).run()
