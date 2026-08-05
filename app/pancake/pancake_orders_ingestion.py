"""Fetch recent Pancake POS orders and upsert them into PostgreSQL."""

import csv
import os
import sys
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List

from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.database import PostgreSQLHelper
from common.pancake_api import PancakeAPIClient


def _first_not_none(*values: Any, default: Any = "") -> Any:
    return next((value for value in values if value is not None), default)


def _first_contact(values: Iterable[Any]) -> str:
    for value in values:
        items = value if isinstance(value, list) else [value]
        for item in items:
            if isinstance(item, dict):
                item = (
                    item.get("phone_number")
                    or item.get("phone")
                    or item.get("value")
                )
            text = str(item or "").strip()
            if text:
                return text
    return ""


class PancakeOrdersIngestion:
    TABLE_NAME = "dwh_pancake_orders"

    def __init__(self, db: PostgreSQLHelper, days_back: int = 7):
        self.db = db
        self.client = PancakeAPIClient()
        self.days_back = days_back

    def ensure_table(self) -> None:
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                order_id VARCHAR(100) PRIMARY KEY,
                shop_id VARCHAR(100),
                customer_id VARCHAR(100),
                customer_name VARCHAR(255),
                customer_phone VARCHAR(50),
                status VARCHAR(100),
                total_price NUMERIC(15, 2),
                discount_amount NUMERIC(15, 2),
                shipping_fee NUMERIC(15, 2),
                partner_id VARCHAR(100),
                bill_full_name VARCHAR(255),
                bill_phone_number VARCHAR(50),
                bill_address TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

    def fetch_and_stage(self, shops: List[Dict[str, Any]]) -> tuple[str, int]:
        now = datetime.now()
        since_date = (now - timedelta(days=self.days_back)).strftime("%Y-%m-%d")
        total_orders = 0

        with tempfile.NamedTemporaryFile(
            mode="w", newline="", encoding="utf-8", delete=False
        ) as tmp_file:
            writer = csv.writer(tmp_file, delimiter="\t")
            synced_at = now.isoformat()

            for shop in shops:
                shop_id = str(shop.get("id") or "").strip()
                if not shop_id:
                    continue
                page = 1
                while True:
                    orders = self.client.get_orders(
                        shop_id,
                        page=page,
                        page_size=100,
                        since=since_date,
                    )
                    for order in orders:
                        order_id = str(order.get("id") or "").strip()
                        if not order_id:
                            logger.warning(
                                f"Skipping Pancake order without id in shop {shop_id}"
                            )
                            continue

                        customer = order.get("customer") or {}
                        if not isinstance(customer, dict):
                            customer = {}
                        shop_customer = customer.get("shop_customer") or {}
                        if not isinstance(shop_customer, dict):
                            shop_customer = {}
                        shipping_address = order.get("shipping_address") or {}
                        if not isinstance(shipping_address, dict):
                            shipping_address = {}

                        customer_id = str(
                            customer.get("customer_id")
                            or customer.get("global_id")
                            or customer.get("id")
                            or order.get("shop_customer_id")
                            or ""
                        )
                        customer_phone = _first_contact(
                            (
                                customer.get("phone_number"),
                                customer.get("phone_numbers"),
                                shop_customer.get("phone_number"),
                                shop_customer.get("phone_numbers"),
                                order.get("phone_number"),
                                shipping_address.get("phone_number"),
                            )
                        )

                        writer.writerow(
                            [
                                order_id,
                                shop_id,
                                customer_id,
                                customer.get("name") or "",
                                customer_phone,
                                str(order.get("status") or ""),
                                _first_not_none(order.get("total_price"), default=0),
                                _first_not_none(
                                    order.get("discount_amount"),
                                    order.get("total_discount"),
                                    order.get("discount"),
                                    default=0,
                                ),
                                _first_not_none(order.get("shipping_fee"), default=0),
                                str(
                                    _first_not_none(
                                        order.get("partner_id"),
                                        order.get("shop_partner_id"),
                                        order.get("third_party_id"),
                                        default="",
                                    )
                                ),
                                order.get("bill_full_name")
                                or shipping_address.get("full_name")
                                or "",
                                order.get("bill_phone_number")
                                or shipping_address.get("phone_number")
                                or "",
                                order.get("bill_address")
                                or shipping_address.get("full_address")
                                or shipping_address.get("address")
                                or "",
                                order.get("inserted_at") or order.get("created_at"),
                                order.get("updated_at")
                                or order.get("inserted_at")
                                or order.get("created_at"),
                                synced_at,
                            ]
                        )
                        total_orders += 1

                    if len(orders) < 100:
                        break
                    page += 1
            tmp_path = tmp_file.name

        return tmp_path, total_orders

    def run(self) -> None:
        logger.info("Starting Pancake Orders Ingestion...")
        self.ensure_table()
        shops = self.client.get_shops()
        if not shops:
            raise RuntimeError("Pancake returned no shops; refusing a silent success")

        logger.info(
            f"Processing orders for {len(shops)} shops (days_back={self.days_back})"
        )
        tmp_path, count = self.fetch_and_stage(shops)
        if count == 0:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise RuntimeError(
                "Pancake returned no recent orders; refusing a silent success"
            )

        try:
            staging_table = f"staging_{self.TABLE_NAME}"
            self.db.execute(
                f"CREATE TEMP TABLE {staging_table} "
                f"(LIKE {self.TABLE_NAME} INCLUDING ALL);"
            )
            columns = (
                "order_id, shop_id, customer_id, customer_name, customer_phone, "
                "status, total_price, discount_amount, shipping_fee, partner_id, "
                "bill_full_name, bill_phone_number, bill_address, created_at, "
                "updated_at, synced_at"
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
                    order_id, shop_id, customer_id, customer_name, customer_phone,
                    status, total_price, discount_amount, shipping_fee, partner_id,
                    bill_full_name, bill_phone_number, bill_address,
                    created_at, updated_at, synced_at
                )
                SELECT
                    order_id, shop_id, customer_id, customer_name, customer_phone,
                    status, total_price, discount_amount, shipping_fee, partner_id,
                    bill_full_name, bill_phone_number, bill_address,
                    created_at, updated_at, synced_at
                FROM {staging_table}
                ON CONFLICT (order_id) DO UPDATE SET
                    shop_id = EXCLUDED.shop_id,
                    customer_id = EXCLUDED.customer_id,
                    customer_name = EXCLUDED.customer_name,
                    customer_phone = EXCLUDED.customer_phone,
                    status = EXCLUDED.status,
                    total_price = EXCLUDED.total_price,
                    discount_amount = EXCLUDED.discount_amount,
                    shipping_fee = EXCLUDED.shipping_fee,
                    partner_id = EXCLUDED.partner_id,
                    bill_full_name = EXCLUDED.bill_full_name,
                    bill_phone_number = EXCLUDED.bill_phone_number,
                    bill_address = EXCLUDED.bill_address,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    synced_at = EXCLUDED.synced_at;
                """
            )
            logger.info(
                f"Successfully upserted {count} orders into {self.TABLE_NAME}"
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
        PancakeOrdersIngestion(db_helper, days_back=7).run()
