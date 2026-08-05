"""
Pancake Chat Logs 30-Day Retention Cleanup
Automatically removes chat logs older than 30 days from Data Warehouse table dwh_pancake_chat_logs.
"""

import os
import sys
from loguru import logger

# Add parent directory for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.database import PostgreSQLHelper

POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://admin:QtdlxxrisA%40%21sfsgo%232024@10.10.10.224:5432/dev_staging_sci")
RETENTION_DAYS = int(os.getenv("PANCAKE_CHAT_RETENTION_DAYS", "30"))


class PancakeChatCleanup:
    """
    Retention policy engine for Pancake Chat Logs
    """

    TABLE_NAME = "dwh_pancake_chat_logs"

    def __init__(self, db: PostgreSQLHelper, retention_days: int = RETENTION_DAYS):
        self.db = db
        self.retention_days = retention_days

    def ensure_table(self):
        """Ensure target table exists."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            message_id VARCHAR(100) PRIMARY KEY,
            conversation_id VARCHAR(100),
            page_id VARCHAR(100),
            customer_id VARCHAR(100),
            customer_name VARCHAR(255),
            sender_type VARCHAR(50),
            message_text TEXT,
            payload_json JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.db.execute(sql)

    def run(self):
        logger.info(f"Starting Pancake Chat Cleanup (Retention Policy: {self.retention_days} days)...")
        self.ensure_table()

        cleanup_sql = f"""
        DELETE FROM {self.TABLE_NAME}
        WHERE created_at < NOW() - INTERVAL '{self.retention_days} days';
        """
        cursor = self.db.conn.cursor()
        cursor.execute(cleanup_sql)
        deleted_count = cursor.rowcount
        self.db.conn.commit()

        logger.info(f"Successfully cleaned up {deleted_count} chat records older than {self.retention_days} days from {self.TABLE_NAME}.")


if __name__ == "__main__":
    db_helper = PostgreSQLHelper(POSTGRES_URI)
    cleanup = PancakeChatCleanup(db_helper)
    cleanup.run()
    db_helper.close()
