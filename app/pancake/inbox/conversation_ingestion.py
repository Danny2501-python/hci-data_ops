"""
Pancake Inbox — Conversation Ingestion
Pull conversations (INBOX/COMMENT/...) from the Pancake Page API and store raw-ish
data in a Bronze Iceberg table. One page = one Facebook/Instagram/... page connected
to Pancake; credentials come from Postgres table `core_pancake_page` (see common/pancake_api.py).

Mirrors app/caresoft/chat/chat_ingestion.py structure (Bronze layer, snapshot_date
partitioning, delete-then-append per partition for reruns).
"""

import os
import sys
import json
import gc
from datetime import date, datetime, timedelta, timezone
from loguru import logger
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, BooleanType, TimestampType, DateType, LongType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.io.pyarrow import schema_to_pyarrow

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from common.utils import get_current_timestamp_naive
from common.database import PostgreSQLHelper
from common.pancake_api import get_active_pancake_pages, fetch_conversations

# Iceberg configuration
ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

# S3 configuration
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Postgres — holds core_pancake_page (page_id -> page_access_token)
POSTGRES_URI = os.getenv('POSTGRES_URI')

# snapshot_date is a Vietnam business day (UTC+7, no DST). Pancake since/until are unix
# seconds and updated_at in responses is UTC, so the day boundary must be built with an
# explicit tz — a naive .timestamp() would depend on the host timezone (pod=UTC, laptop=VN).
VN_TZ = timezone(timedelta(hours=7))


class ConversationIngestion:
    """Pull Conversations from Pancake Page API and store in Bronze Iceberg table"""
    CONVERSATION_BRONZE_TABLE = 'pancake.pk_conversation_api'

    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None

        logger.info(f"Initializing ConversationIngestion for snapshot_date={self.snapshot_date}")

        self.bronze_catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            **{
                "uri": ICEBERG_REST_URI_BRONZE,
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
                "s3.region": AWS_REGION,
            }
        )

        self.ensure_table_exists()

    def ensure_table_exists(self):
        """Create conversation bronze table if not exists"""
        try:
            self.bronze_catalog.load_table(self.CONVERSATION_BRONZE_TABLE)
            logger.info(f"Table {self.CONVERSATION_BRONZE_TABLE} already exists")
        except Exception:
            logger.info(f"Creating table {self.CONVERSATION_BRONZE_TABLE}")

            schema = Schema(
                NestedField(1, "id", StringType(), required=False),           # conversation id
                NestedField(2, "page_id", StringType(), required=False),
                NestedField(3, "platform", StringType(), required=False),     # facebook / instagram / ...
                NestedField(4, "type", StringType(), required=False),         # INBOX / COMMENT / COMMENT_LIVESTREAM / POST
                NestedField(5, "post_id", StringType(), required=False),
                NestedField(6, "snippet", StringType(), required=False),
                NestedField(7, "seen", BooleanType(), required=False),
                NestedField(8, "message_count", IntegerType(), required=False),
                NestedField(9, "has_phone", BooleanType(), required=False),
                NestedField(10, "inserted_at", TimestampType(), required=False),
                NestedField(11, "updated_at", TimestampType(), required=False),
                NestedField(12, "from_id", StringType(), required=False),
                NestedField(13, "from_name", StringType(), required=False),
                NestedField(14, "customer_id", StringType(), required=False),
                NestedField(15, "page_customer_id", StringType(), required=False),
                NestedField(16, "page_customer_name", StringType(), required=False),
                NestedField(17, "ad_ids", StringType(), required=False),      # JSON array string, null if organic
                NestedField(18, "ads", StringType(), required=False),        # JSON array string (ad details), null if organic
                NestedField(19, "tags", StringType(), required=False),        # JSON array string
                NestedField(20, "assignee_ids", StringType(), required=False),  # JSON array string
                NestedField(21, "brand_id", IntegerType(), required=False),

                # Metadata
                NestedField(22, "snapshot_date", DateType(), required=True),
                NestedField(23, "ingestion_timestamp", TimestampType(), required=True),
            )

            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=22,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="snapshot_date"
                )
            )

            self.bronze_catalog.create_table(
                identifier=self.CONVERSATION_BRONZE_TABLE,
                schema=schema,
                partition_spec=partition_spec
            )
            logger.success(f"Created table {self.CONVERSATION_BRONZE_TABLE}")

    def run(self, page: dict, days_ago: int = 3):
        page_name = page["name"]
        page_id = page["page_id"]
        brand_id = page.get("current_brand_id")

        logger.info(f"Processing conversation ingestion for page '{page_name}' ({page_id})")

        # Date window: [snapshot_date - days_ago, snapshot_date + 1) — same anchor
        # convention as chat_ingestion.py, expressed as unix seconds for Pancake.
        # Day boundaries are 00:00 Vietnam time (UTC+7).
        anchor_date = datetime.combine(self.snapshot_date, datetime.min.time(), tzinfo=VN_TZ)
        until_dt = anchor_date + timedelta(days=1)
        since_dt = until_dt - timedelta(days=days_ago)
        since_ts = int(since_dt.timestamp())
        until_ts = int(until_dt.timestamp())

        conversations = fetch_conversations(page, since=since_ts, until=until_ts)

        if not conversations:
            logger.warning(f"No conversations found for page '{page_name}'")
            return

        ingestion_timestamp = get_current_timestamp_naive()
        records = []

        def safe_ts(val):
            if not val:
                return None
            try:
                return datetime.fromisoformat(str(val).replace('Z', '+00:00')).replace(tzinfo=None)
            except ValueError:
                return None

        def as_json(val):
            if val is None:
                return None
            return json.dumps(val, ensure_ascii=False)

        for conv in conversations:
            from_obj = conv.get("from") or {}
            page_customer = conv.get("page_customer") or {}

            record = {
                "id": str(conv.get("id")) if conv.get("id") is not None else None,
                "page_id": str(page_id),
                "platform": page.get("platform"),
                "type": conv.get("type"),
                "post_id": str(conv.get("post_id")) if conv.get("post_id") is not None else None,
                "snippet": conv.get("snippet"),
                "seen": conv.get("seen"),
                "message_count": conv.get("message_count"),
                "has_phone": conv.get("has_phone"),
                "inserted_at": safe_ts(conv.get("inserted_at")),
                "updated_at": safe_ts(conv.get("updated_at")),
                "from_id": from_obj.get("id"),
                "from_name": from_obj.get("name"),
                "customer_id": str(conv.get("customer_id")) if conv.get("customer_id") is not None else None,
                "page_customer_id": page_customer.get("id"),
                "page_customer_name": page_customer.get("name"),
                # ad_ids / ads = None when the conversation is organic (not ad-originated)
                "ad_ids": as_json(conv.get("ad_ids")),
                "ads": as_json(conv.get("ads")),
                "tags": as_json(conv.get("tags")),
                "assignee_ids": as_json(conv.get("assignee_ids")),
                "brand_id": brand_id,
                "snapshot_date": self.snapshot_date,
                "ingestion_timestamp": ingestion_timestamp,
            }
            records.append(record)

        table = self.bronze_catalog.load_table(self.CONVERSATION_BRONZE_TABLE)

        try:
            logger.info(f"Deleting old data for page '{page_name}' at {self.snapshot_date}")
            table.delete(
                And(
                    EqualTo("snapshot_date", self.snapshot_date),
                    EqualTo("page_id", str(page_id))
                )
            )
        except Exception:
            pass

        try:
            pa_schema = schema_to_pyarrow(table.schema())
            df = pa.Table.from_pylist(records, schema=pa_schema)
            table.append(df)
            logger.success(f"Successfully wrote {len(records)} conversations for page '{page_name}'")
        except Exception as e:
            logger.error(f"Failed to write conversations for page '{page_name}': {e}")

        gc.collect()


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))

    logger.info(f"Starting Conversation Ingestion for {snapshot_date}")

    if not POSTGRES_URI:
        raise ValueError("POSTGRES_URI environment variable not set")

    with PostgreSQLHelper(POSTGRES_URI) as db:
        pages = get_active_pancake_pages(db)

    if not pages:
        logger.warning("No active Pancake pages found in core_pancake_page — nothing to do")
        return

    job = ConversationIngestion(snapshot_date)
    for page in pages:
        try:
            job.run(page, days_ago=3)
        except Exception as e:
            logger.error(f"Failed page '{page['name']}': {e}")


if __name__ == "__main__":
    main()
