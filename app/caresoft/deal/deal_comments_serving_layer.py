"""
Deal Comments Serving Layer
Sync comments from Iceberg Gold to PostgreSQL DWH

Flow:
1. Read from gold.caresoft.cs_deal_api_trans_comments
2. Delete existing records by ticket_id
3. Insert new records
"""

import os
import sys
from datetime import datetime, date, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from loguru import logger
import gc

# Iceberg configuration
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD')

S3_ENDPOINT = os.getenv('S3_ENDPOINT')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# PostgreSQL configuration
POSTGRES_URI = os.getenv('POSTGRES_URI')

# Table names
SOURCE_TABLE = 'caresoft.cs_deal_api_trans_comments'
TARGET_TABLE = os.getenv('SERVING_TABLE_NAME', 'cs_deal_api_trans_comments_v1')


class DealCommentsServingLayer:
    """
    Sync deal comments from Iceberg to PostgreSQL
    """

    INT4_MIN = -2147483648
    INT4_MAX = 2147483647

    def __init__(self, snapshot_date: date = None):
        self.snapshot_date = snapshot_date or (date.today() - timedelta(days=1))

        catalog_config = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_ACCESS_KEY,
            "s3.secret-access-key": AWS_SECRET_KEY,
            "s3.region": AWS_REGION,
        }

        self.gold_catalog = load_catalog(
            CATALOG_NAME_GOLD,
            uri=ICEBERG_REST_URI_GOLD,
            **catalog_config
        )

        self.pg_conn = None
        logger.info(f"Initialized DealCommentsServingLayer for {self.snapshot_date}")

    @staticmethod
    def safe_int4(x, col_name: str):
        """Safe convert to PostgreSQL int4, overflow/invalid -> NULL"""
        if pd.isna(x):
            return None
        try:
            v = int(float(x))
            if v < DealCommentsServingLayer.INT4_MIN or v > DealCommentsServingLayer.INT4_MAX:
                logger.warning(f"[{col_name}] int4 overflow: {v} -> NULL")
                return None
            return v
        except Exception as e:
            logger.warning(f"[{col_name}] invalid int4 value '{x}': {e} -> NULL")
            return None

    def connect_postgres(self):
        if not POSTGRES_URI:
            raise ValueError("POSTGRES_URI not set")
        self.pg_conn = psycopg2.connect(POSTGRES_URI)
        self.pg_conn.autocommit = False
        logger.info("Connected to PostgreSQL")

    def ensure_serving_table_exists(self):
        cursor = self.pg_conn.cursor()
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                    ticket_comments_id numeric(20),
                    ticket_id numeric(20),
                    "comment" text,
                    commentator_id numeric(20),
                    username text,
                    facebook text,
                    facebook_name text,
                    comment_source text,
                    created_at timestamp,
                    attack_file_id text,
                    file_name text,
                    file_id text,
                    download_connection_string text,
                    call_id text,
                    email_receive_id text,
                    addition_details text,
                    is_public int4,
                    can_hide text,
                    can_remove text,
                    can_reply_privately text,
                    facebook_comment_state int4,
                    role_id int4,
                    avatar text,
                    "path" text,
                    file_path text,
                    snapshot_date date,
                    ingestion_timestamp timestamp
                )
            """)
            self.pg_conn.commit()
            logger.info(f"Ensured table {TARGET_TABLE} exists")
        except Exception as e:
            self.pg_conn.rollback()
            raise
        finally:
            cursor.close()

    def get_comments_from_iceberg(self) -> pd.DataFrame:
        table = self.gold_catalog.load_table(SOURCE_TABLE)
        scan = table.scan(row_filter=EqualTo("snapshot_date", self.snapshot_date))
        df = scan.to_pandas()
        logger.info(f"Read {len(df)} comments from Iceberg")
        return df

    def delete_existing_comments(self, ticket_ids: list):
        if not ticket_ids:
            return

        valid_ids = []
        for tid in ticket_ids:
            if pd.notna(tid):
                try:
                    v = float(tid)
                    if -1e20 < v < 1e20:
                        valid_ids.append(int(v))
                except Exception:
                    pass

        if not valid_ids:
            return

        cursor = self.pg_conn.cursor()
        try:
            placeholders = ",".join(["%s"] * len(valid_ids))
            cursor.execute(
                f"DELETE FROM {TARGET_TABLE} WHERE ticket_id IN ({placeholders})",
                valid_ids
            )
            self.pg_conn.commit()
            logger.info(f"Deleted records for {len(set(valid_ids))} ticket_ids")
        except Exception:
            self.pg_conn.rollback()
            raise
        finally:
            cursor.close()

    def insert_comments(self, df: pd.DataFrame):
        if df.empty:
            return

        columns = [
            'ticket_comments_id', 'ticket_id', 'comment', 'commentator_id',
            'username', 'facebook', 'facebook_name', 'comment_source',
            'created_at', 'attack_file_id', 'file_name', 'file_id',
            'download_connection_string', 'call_id', 'email_receive_id',
            'addition_details', 'is_public', 'can_hide', 'can_remove',
            'can_reply_privately', 'facebook_comment_state', 'role_id',
            'avatar', 'path', 'file_path', 'snapshot_date', 'ingestion_timestamp'
        ]

        for col in columns:
            if col not in df.columns:
                df[col] = None

        df = df[columns].copy()

        # HARD FIX int4
        df['is_public'] = df['is_public'].apply(lambda x: self.safe_int4(x, 'is_public'))
        df['facebook_comment_state'] = df['facebook_comment_state'].apply(
            lambda x: self.safe_int4(x, 'facebook_comment_state')
        )
        df['role_id'] = df['role_id'].apply(lambda x: self.safe_int4(x, 'role_id'))

        # Final guard
        for col in ['is_public', 'facebook_comment_state', 'role_id']:
            bad = df[df[col].notna() & (
                (df[col] < self.INT4_MIN) | (df[col] > self.INT4_MAX)
            )]
            if not bad.empty:
                logger.error(f"❌ int4 overflow still exists in {col}")
                logger.error(bad[[col]].head())
                raise ValueError(f"int4 overflow detected in {col}")

        records = [tuple(r) for r in df.values]
        cols_str = ", ".join([f'"{c}"' for c in columns])

        cursor = self.pg_conn.cursor()
        try:
            execute_values(
                cursor,
                f"INSERT INTO {TARGET_TABLE} ({cols_str}) VALUES %s",
                records,
                page_size=1000
            )
            self.pg_conn.commit()
            logger.success(f"Inserted {len(records)} records")
        except Exception:
            self.pg_conn.rollback()
            raise
        finally:
            cursor.close()

    def run(self):
        try:
            self.connect_postgres()
            self.ensure_serving_table_exists()
            df = self.get_comments_from_iceberg()
            if df.empty:
                return
            self.delete_existing_comments(df['ticket_id'].unique().tolist())
            self.insert_comments(df)
            logger.success("Deal comments serving layer completed")
        finally:
            if self.pg_conn:
                self.pg_conn.close()
            gc.collect()


def main():
    snapshot_date = (
        datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        if len(sys.argv) > 1 else date.today() - timedelta(days=1)
    )
    DealCommentsServingLayer(snapshot_date).run()


if __name__ == "__main__":
    main()
