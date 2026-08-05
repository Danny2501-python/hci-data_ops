"""
Pancake Shops Ingestion
Fetch list of all 94 Pancake POS Shops and sync to DWH table dwh_pancake_shops.
"""

import os
import sys
import csv
import tempfile
from datetime import datetime
from loguru import logger

# Add parent directory for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.pancake_api import PancakeAPIClient
from common.database import PostgreSQLHelper

POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://admin:QtdlxxrisA%40%21sfsgo%232024@10.10.10.224:5432/dev_staging_sci")


class PancakeShopsIngestion:
    """
    Ingest Pancake Shops to DWH
    """

    TABLE_NAME = "dwh_pancake_shops"

    def __init__(self, db: PostgreSQLHelper):
        self.db = db
        self.client = PancakeAPIClient()

    def ensure_table(self):
        """Create target table if it does not exist."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            shop_id VARCHAR(100) PRIMARY KEY,
            name VARCHAR(255),
            warehouse_id VARCHAR(100),
            currency VARCHAR(50),
            phone VARCHAR(50),
            is_active BOOLEAN DEFAULT TRUE,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.db.execute(sql)

    def run(self):
        logger.info("Starting Pancake Shops Ingestion...")
        self.ensure_table()

        shops = self.client.get_shops()
        logger.info(f"Fetched {len(shops)} shops from Pancake POS API")

        if not shops:
            logger.warning("No shops found to ingest.")
            return

        with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False) as tmp_file:
            writer = csv.writer(tmp_file, delimiter="\t")
            now_str = datetime.now().isoformat()

            for s in shops:
                shop_id = str(s.get("id", ""))
                name = s.get("name", "")
                warehouse_id = str(s.get("warehouse_id", ""))
                currency = s.get("currency", "VND")
                phone = s.get("phone", "")
                is_active = True

                writer.writerow([shop_id, name, warehouse_id, currency, phone, is_active, now_str])

            tmp_path = tmp_file.name

        try:
            # Stage & Upsert
            staging_table = f"staging_{self.TABLE_NAME}"
            self.db.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {self.TABLE_NAME} INCLUDING ALL);")

            with open(tmp_path, "r", encoding="utf-8") as f:
                cursor = self.db.conn.cursor()
                cursor.copy_from(f, staging_table, sep="\t", columns=("shop_id", "name", "warehouse_id", "currency", "phone", "is_active", "synced_at"))
                self.db.conn.commit()

            upsert_sql = f"""
            INSERT INTO {self.TABLE_NAME} (shop_id, name, warehouse_id, currency, phone, is_active, synced_at)
            SELECT shop_id, name, warehouse_id, currency, phone, is_active, synced_at FROM {staging_table}
            ON CONFLICT (shop_id) DO UPDATE SET
                name = EXCLUDED.name,
                warehouse_id = EXCLUDED.warehouse_id,
                currency = EXCLUDED.currency,
                phone = EXCLUDED.phone,
                is_active = EXCLUDED.is_active,
                synced_at = EXCLUDED.synced_at;
            """
            self.db.execute(upsert_sql)
            logger.info(f"Successfully upserted {len(shops)} shops into {self.TABLE_NAME}")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


if __name__ == "__main__":
    db_helper = PostgreSQLHelper(POSTGRES_URI)
    ingestion = PancakeShopsIngestion(db_helper)
    ingestion.run()
    db_helper.close()
