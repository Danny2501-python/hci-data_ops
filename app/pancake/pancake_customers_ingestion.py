"""
Pancake Customers Ingestion
Fetch Customers across Pancake shops and bulk ingest to DWH table dwh_pancake_customers.
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


class PancakeCustomersIngestion:
    """
    Pancake Customers Ingestion Engine
    """

    TABLE_NAME = "dwh_pancake_customers"

    def __init__(self, db: PostgreSQLHelper):
        self.db = db
        self.client = PancakeAPIClient()

    def ensure_table(self):
        """Create target table if not exists."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            customer_id VARCHAR(100) PRIMARY KEY,
            shop_id VARCHAR(100),
            name VARCHAR(255),
            phone VARCHAR(50),
            email VARCHAR(255),
            gender VARCHAR(20),
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.db.execute(sql)

    def fetch_all_customers(self) -> List[Dict]:
        """Iterate over all shops and fetch customers."""
        shops = self.client.get_shops()
        logger.info(f"Fetched {len(shops)} shops to process customers")

        all_customers = []
        for s in shops:
            shop_id = str(s.get("id", ""))
            if not shop_id:
                continue

            page = 1
            while True:
                custs = self.client.get_customers(shop_id=shop_id, page=page, page_size=100)
                if not custs:
                    break

                for c in custs:
                    c["shop_id"] = shop_id
                    all_customers.append(c)

                if len(custs) < 100:
                    break
                page += 1

        logger.info(f"Total customers collected across all shops: {len(all_customers)}")
        return all_customers

    def run(self):
        logger.info("Starting Pancake Customers Ingestion...")
        self.ensure_table()

        customers = self.fetch_all_customers()
        if not customers:
            logger.warning("No customer records found to ingest.")
            return

        with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False) as tmp_file:
            writer = csv.writer(tmp_file, delimiter="\t")
            now_str = datetime.now().isoformat()

            count = 0
            for c in customers:
                customer_id = str(c.get("id") or c.get("customer_id") or "")
                if not customer_id:
                    continue

                shop_id = str(c.get("shop_id", ""))
                name = c.get("name", "")
                phone = c.get("phone", "")
                email = c.get("email", "")
                gender = str(c.get("gender", ""))
                created_at = c.get("inserted_at") or c.get("created_at") or now_str
                updated_at = c.get("updated_at") or now_str

                writer.writerow([
                    customer_id, shop_id, name, phone, email, gender,
                    created_at, updated_at, now_str
                ])
                count += 1

            tmp_path = tmp_file.name

        try:
            staging_table = f"staging_{self.TABLE_NAME}"
            self.db.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {self.TABLE_NAME} INCLUDING ALL);")

            with open(tmp_path, "r", encoding="utf-8") as f:
                cursor = self.db.conn.cursor()
                cursor.copy_from(
                    f, staging_table, sep="\t",
                    columns=(
                        "customer_id", "shop_id", "name", "phone", "email",
                        "gender", "created_at", "updated_at", "synced_at"
                    )
                )
                self.db.conn.commit()

            upsert_sql = f"""
            INSERT INTO {self.TABLE_NAME} (
                customer_id, shop_id, name, phone, email, gender,
                created_at, updated_at, synced_at
            )
            SELECT 
                customer_id, shop_id, name, phone, email, gender,
                created_at, updated_at, synced_at
            FROM {staging_table}
            ON CONFLICT (customer_id) DO UPDATE SET
                name = EXCLUDED.name,
                phone = EXCLUDED.phone,
                email = EXCLUDED.email,
                updated_at = EXCLUDED.updated_at,
                synced_at = EXCLUDED.synced_at;
            """
            self.db.execute(upsert_sql)
            logger.info(f"Successfully upserted {count} customers into {self.TABLE_NAME}")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


if __name__ == "__main__":
    db_helper = PostgreSQLHelper(POSTGRES_URI)
    ingestion = PancakeCustomersIngestion(db_helper)
    ingestion.run()
    db_helper.close()
