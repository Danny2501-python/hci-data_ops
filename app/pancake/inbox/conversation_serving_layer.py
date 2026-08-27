"""
Pancake Inbox — Conversation Serving Layer
Bronze (Iceberg) -> PostgreSQL. Dedups per (id, page_id) and upserts two tables:

  pancake_conversations      one row per conversation (flattened)
  pancake_conversation_ads   bridge table: one row per (conversation, ad_id) pair,
                              exploded from the conversation's ad_ids[] — this is the
                              join key to bring in campaign/ad performance data.

Mirrors app/caresoft/chat/chat_serving_layer.py (Bronze -> DuckDB dedup -> bulk_load upsert).
"""

import os
import sys
import json
from datetime import datetime, date, timedelta
import psycopg2
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from common.utils import bulk_load

ICEBERG_REST_URI_BRONZE = os.getenv('ICEBERG_REST_URI_BRONZE')
CATALOG_NAME_BRONZE = os.getenv('CATALOG_NAME_BRONZE')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

POSTGRES_URI = os.getenv('POSTGRES_URI')


class ConversationServingLayer:
    SOURCE_TABLE = 'pancake.pk_conversation_api'
    CONVERSATIONS_TABLE = 'pancake_conversations'
    CONVERSATION_ADS_TABLE = 'pancake_conversation_ads'

    CONVERSATION_COLUMNS = [
        "id", "page_id", "platform", "type", "post_id", "snippet", "seen",
        "message_count", "has_phone", "inserted_at", "updated_at",
        "from_id", "from_name", "customer_id", "page_customer_id", "page_customer_name",
        "tags", "assignee_ids", "brand_id", "snapshot_date", "ingestion_timestamp",
    ]

    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or date.today()
        self.bronze_catalog = None
        self.pg_conn = None

    def connect_iceberg(self):
        self.bronze_catalog = load_catalog(
            CATALOG_NAME_BRONZE,
            uri=ICEBERG_REST_URI_BRONZE,
            **{
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": AWS_ACCESS_KEY,
                "s3.secret-access-key": AWS_SECRET_KEY,
                "s3.region": AWS_REGION,
            }
        )
        logger.info("Connected to Bronze Iceberg")

    def connect_postgres(self):
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI not set")
        self.pg_conn = psycopg2.connect(POSTGRES_URI)
        self.pg_conn.autocommit = False
        logger.info("Connected to PostgreSQL")

    def ensure_tables_exist(self):
        cursor = self.pg_conn.cursor()
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.CONVERSATIONS_TABLE} (
                    id TEXT,
                    page_id TEXT,
                    platform TEXT,
                    type TEXT,
                    post_id TEXT,
                    snippet TEXT,
                    seen TEXT,
                    message_count TEXT,
                    has_phone TEXT,
                    inserted_at TEXT,
                    updated_at TEXT,
                    from_id TEXT,
                    from_name TEXT,
                    customer_id TEXT,
                    page_customer_id TEXT,
                    page_customer_name TEXT,
                    tags TEXT,
                    assignee_ids TEXT,
                    brand_id TEXT,
                    snapshot_date TEXT,
                    ingestion_timestamp TEXT,
                    PRIMARY KEY (id, page_id)
                )
            """)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.CONVERSATION_ADS_TABLE} (
                    conversation_id TEXT,
                    page_id TEXT,
                    ad_id TEXT,
                    brand_id TEXT,
                    snapshot_date TEXT,
                    PRIMARY KEY (conversation_id, page_id, ad_id)
                )
            """)
            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def get_conversations_for_date(self):
        """Read Bronze conversations for snapshot_date, dedup with DuckDB by (id, page_id)."""
        logger.info(f"Reading conversations for {self.snapshot_date}")
        table = self.bronze_catalog.load_table(self.SOURCE_TABLE)

        temp_dir = os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb')
        os.makedirs(temp_dir, exist_ok=True)
        temp_parquet = os.path.join(temp_dir, f"conversation_serving_{os.getpid()}.parquet")

        try:
            scan = table.scan(row_filter=EqualTo("snapshot_date", self.snapshot_date.isoformat()))

            batches = [b for b in scan.to_arrow_batch_reader() if len(b) > 0]
            if not batches:
                return [], []

            combined = pa.Table.from_batches(batches)
            pq.write_table(combined, temp_parquet)
            del batches, combined

            con = duckdb.connect(config={
                'memory_limit': '2GB',
                'preserve_insertion_order': 'false',
                'temp_directory': temp_dir,
                'threads': '1',
            })

            # Dedup: keep the most recently *updated* copy of each conversation
            # (a conversation can be re-fetched every day it's still active).
            query = f"""
            SELECT
                id, page_id, platform, type, post_id, snippet, seen,
                message_count, has_phone, inserted_at, updated_at,
                from_id, from_name, customer_id, page_customer_id, page_customer_name,
                ad_ids, ads, tags, assignee_ids, brand_id, snapshot_date, ingestion_timestamp
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY id, page_id
                        ORDER BY updated_at DESC NULLS LAST, ingestion_timestamp DESC
                    ) as rn
                FROM read_parquet('{temp_parquet}')
                WHERE id IS NOT NULL AND page_id IS NOT NULL
            ) WHERE rn = 1
            """

            result = con.execute(query).fetch_arrow_table()
            con.close()

            columns = result.schema.names
            rows = []
            for i in range(len(result)):
                rows.append({col: result.column(j)[i].as_py() for j, col in enumerate(columns)})

            logger.success(f"Loaded {len(rows)} unique conversations")
            return columns, rows

        finally:
            if os.path.exists(temp_parquet):
                try:
                    os.remove(temp_parquet)
                except Exception:
                    pass

    def upsert_conversations(self, rows: list):
        """Upsert the flattened conversation rows (drop ad_ids/ads — they feed the bridge table)."""
        if not rows:
            return

        records = [{k: r.get(k) for k in self.CONVERSATION_COLUMNS} for r in rows]
        cursor = self.pg_conn.cursor()
        try:
            logger.info(f"Bulk upserting {len(records)} conversations into {self.CONVERSATIONS_TABLE}...")
            bulk_load(
                cursor=cursor,
                target_table=self.CONVERSATIONS_TABLE,
                records=records,
                columns=self.CONVERSATION_COLUMNS,
                conflict_columns=['id', 'page_id'],
                method='upsert',
            )
            self.pg_conn.commit()
            logger.success(f"Upserted {len(records)} conversations")
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def upsert_conversation_ads(self, rows: list):
        """Explode ad_ids[] per conversation into the bridge table (inbox <-> campaign join key)."""
        bridge_records = []
        for r in rows:
            ad_ids_raw = r.get("ad_ids")
            if not ad_ids_raw:
                continue
            try:
                ad_ids = json.loads(ad_ids_raw)
            except (TypeError, ValueError):
                continue
            if not ad_ids:
                continue

            for ad_id in ad_ids:
                bridge_records.append({
                    "conversation_id": r.get("id"),
                    "page_id": r.get("page_id"),
                    "ad_id": str(ad_id),
                    "brand_id": r.get("brand_id"),
                    "snapshot_date": r.get("snapshot_date"),
                })

        if not bridge_records:
            logger.info("No ad-originated conversations in this batch")
            return

        cols = ["conversation_id", "page_id", "ad_id", "brand_id", "snapshot_date"]
        cursor = self.pg_conn.cursor()
        try:
            logger.info(f"Bulk upserting {len(bridge_records)} conversation<->ad links into {self.CONVERSATION_ADS_TABLE}...")
            bulk_load(
                cursor=cursor,
                target_table=self.CONVERSATION_ADS_TABLE,
                records=bridge_records,
                columns=cols,
                conflict_columns=['conversation_id', 'page_id', 'ad_id'],
                method='upsert',
            )
            self.pg_conn.commit()
            logger.success(f"Upserted {len(bridge_records)} conversation<->ad links")
        except Exception as e:
            self.pg_conn.rollback()
            raise e
        finally:
            cursor.close()

    def run(self):
        try:
            self.connect_iceberg()
            self.connect_postgres()
            self.ensure_tables_exist()

            _, rows = self.get_conversations_for_date()
            self.upsert_conversations(rows)
            self.upsert_conversation_ads(rows)
        finally:
            if self.pg_conn:
                self.pg_conn.close()


def main():
    snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d").date() if snapshot_date_str else (date.today() - timedelta(days=1))

    job = ConversationServingLayer(snapshot_date)
    job.run()


if __name__ == "__main__":
    main()
