"""
Pancake Orders Ingestion
Fetch POS Orders across Pancake shops and bulk ingest to DWH table dwh_pancake_orders.
Strategy: Bulk COPY to Temp Staging Table + ON CONFLICT UPSERT
"""

import os
import sys
import csv
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from loguru import logger

# Add parent directory for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pancake_api import PancakeAPIClient
from common.database import PostgreSQLHelper

POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://admin:QtdlxxrisA%40%21sfsgo%232024@10.10.10.224:5432/dev_staging_sci")


class PancakeOrdersIngestion:
    """
    Pancake Orders Ingestion Engine
    """

    TABLE_NAME = "dwh_pancake_orders"

    def __init__(self, db: PostgreSQLHelper, days_back: int = 7):
        self.db = db
        self.client = PancakeAPIClient()
        self.days_back = days_back

    def ensure_table(self):
        """Create orders target table if not exists."""
        sql = f"""
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
        self.db.execute(sql)

    def fetch_and_stage(self, shops: List[Dict]):
        """Fetch orders from API for given shops and write to CSV."""
        now = datetime.now()
        since_date = (now - timedelta(days=self.days_back)).strftime("%Y-%m-%d")

        total_orders_count = 0
        with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False) as tmp_file:
            writer = csv.writer(tmp_file, delimiter="\t")
            synced_at = now.isoformat()

            for shop in shops:
                shop_id = str(shop.get("id"))
                shop_name = shop.get("name")
                page = 1
                
                while True:
                    orders = self.client.get_orders(shop_id, page=page, page_size=100, since=since_date)
                    if not orders:
                        break

                    for ord_data in orders:
                        order_id = str(ord_data.get("id"))
                        customer = ord_data.get("customer", {}) or {}
                        customer_id = str(customer.get("id", ""))
                        customer_name = customer.get("name", "")
                        customer_phone = customer.get("phone_number", "") or ord_data.get("phone_number", "")

                        status = str(ord_data.get("status", ""))
                        total_price = ord_data.get("total_price", 0)
                        discount_amount = ord_data.get("discount_amount", 0)
                        shipping_fee = ord_data.get("shipping_fee", 0)
                        partner_id = str(ord_data.get("partner_id", ""))

                        bill_full_name = ord_data.get("bill_full_name", "")
                        bill_phone_number = ord_data.get("bill_phone_number", "")
                        bill_address = ord_data.get("bill_address", "")

                        created_at = ord_data.get("inserted_at") or ord_data.get("created_at")
                        updated_at = ord_data.get("updated_at") or created_at

                        writer.writerow([
                            order_id, shop_id, customer_id, customer_name, customer_phone,
                            status, total_price, discount_amount, shipping_fee, partner_id,
                            bill_full_name, bill_phone_number, bill_address,
                            created_at, updated_at, synced_at
                        ])
                        total_orders_count += 1

                    if len(orders) < 100:
                        break
                    page += 1

            tmp_path = tmp_file.name

        return tmp_path, total_orders_count

    def run(self):
        logger.info("Starting Pancake Orders Ingestion...")
        self.ensure_table()

        shops = self.client.get_shops()
        if not shops:
            logger.warning("No shops found to process orders.")
            return

        logger.info(f"Processing orders for {len(shops)} shops (days_back={self.days_back})...")
        tmp_path, count = self.fetch_and_stage(shops)

        if count == 0:
            logger.info("No orders retrieved.")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return

        try:
            staging_table = f"staging_{self.TABLE_NAME}"
            self.db.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {self.TABLE_NAME} INCLUDING ALL);")

            with open(tmp_path, "r", encoding="utf-8") as f:
                cursor = self.db.conn.cursor()
                cursor.copy_from(
                    f, staging_table, sep="\t",
                    columns=(
                        "order_id", "shop_id", "customer_id", "customer_name", "customer_phone",
                        "status", "total_price", "discount_amount", "shipping_fee", "partner_id",
                        "bill_full_name", "bill_phone_number", "bill_address",
                        "created_at", "updated_at", "synced_at"
                    )
                )
                self.db.conn.commit()

            upsert_sql = f"""
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
                status = EXCLUDED.status,
                total_price = EXCLUDED.total_price,
                discount_amount = EXCLUDED.discount_amount,
                shipping_fee = EXCLUDED.shipping_fee,
                updated_at = EXCLUDED.updated_at,
                synced_at = EXCLUDED.synced_at;
            """
            self.db.execute(upsert_sql)
            logger.info(f"Successfully upserted {count} orders into {self.TABLE_NAME}")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


if __name__ == "__main__":
    db_helper = PostgreSQLHelper(POSTGRES_URI)
    ingestion = PancakeOrdersIngestion(db_helper, days_back=7)
    ingestion.run()
    db_helper.close()
