import psycopg2
import pandas as pd
import numpy as np
import tempfile
import os
from loguru import logger
from typing import List

class StagMailMessageErpManager:
    def __init__(self, conn_uris: dict = None):
        self.conn_uris = conn_uris or {}

    def get_conn(self, conn_name):
        uri = self.conn_uris.get(conn_name)
        if not uri:
            logger.error(f"No connection URI found for {conn_name}. Check environment variables.")
            return None
        try:
            return psycopg2.connect(uri)
        except Exception as e:
            logger.error(f"Failed to connect to {conn_name}: {e}")
            raise e

    def fetch_data_from_erp(self, conn_name: str, brand_etl: int):
        query = """       
			with cte_create_uid as (
            select
                mtv.id as id_line,
                mtv.field_desc as field_desc_line,
                mtv.old_value_integer::int as old_value_integer_line,
                mtv.old_value_char as old_value_char_line,
                mtv.new_value_integer::int as new_value_integer_line,
                mtv.new_value_char as new_value_char_line,
                mtv.mail_message_id as mail_message_id_line,
                mtv.write_uid as write_uid_line,
                mtv.write_date as write_date_line,
                mm."date" as date_changed,
                mm.model,
                mm.res_id
            from
                mail_tracking_value mtv
            join mail_message mm on
                mm.id = mtv.mail_message_id
            where
                mtv.create_date >= TIMESTAMP '2025-08-01 00:00:00'
                and mtv.field = 'create_uid'
                and mm.model = 'crm.lead'
                and mtv.old_value_char <> 'Sync'),
            cte_crm_lead as (
            select
                mtv.id AS id_mail_crm_lead,
                mtv.field_desc,
                mtv.old_value_integer::int as old_value_integer,
                mtv.old_value_char,
                mtv.new_value_integer::int as new_value_integer,
                mtv.new_value_char,
                mtv.mail_message_id,
                mtv.write_uid,
                mtv.write_date,
                mm."date" as date_changed,
                mm.model,
                mm.res_id AS booking_id,
                cl."name" AS booking,
                'won' AS status_bk
            from
                mail_tracking_value mtv
            join mail_message mm on
                mm.id = mtv.mail_message_id
            join crm_lead cl on
                cl.id = mm.res_id
            where
                mtv.field = 'stage_id'
                and mtv.new_value_integer = 4
                and mm.model = 'crm.lead'
                and cl.type_crm_id <> '1'
                and mtv.write_date >= TIMESTAMP '2024-08-01 00:00:00'),
            cte_cline as (
            select
                mtv.id as id_line,
                mtv.field_desc as field_desc_line,
                mtv.old_value_integer::int as old_value_integer_line,
                mtv.old_value_char as old_value_char_line,
                mtv.new_value_integer::int as new_value_integer_line,
                mtv.new_value_char as new_value_char_line,
                mtv.mail_message_id as mail_message_id_line,
                mtv.write_uid as write_uid_line,
                mtv.write_date as write_date_line,
                mm."date" as date_changed,
                mm.model,
                mm.res_id,
                cl.crm_id
            from
                mail_tracking_value mtv
            join mail_message mm on
                mm.id = mtv.mail_message_id
            join crm_line cl on
                cl.id = mm.res_id
            where
                mtv.create_date >= TIMESTAMP '2025-08-01 00:00:00'
                and mtv.field = 'stage'
                and mm.model = 'crm.line'
                and mtv.new_value_char ='Kết thúc'
            union all
                select
                    mtv.id as id_line,
                    mtv.field_desc as field_desc_line,
                    mtv.old_value_integer::int as old_value_integer_line,
                    mtv.old_value_char as old_value_char_line,
                    mtv.new_value_integer::int as new_value_integer_line,
                    mtv.new_value_char as new_value_char_line,
                    mtv.mail_message_id as mail_message_id_line,
                    mtv.write_uid as write_uid_line,
                    mtv.write_date as write_date_line,
                    mm."date" as date_changed,
                    mm.model,
                    mm.res_id,
                    clp.booking_id as crm_id
                from
                    mail_tracking_value mtv
                join mail_message mm on
                    mm.id = mtv.mail_message_id
                join crm_line_product clp on
                    clp.id = mm.res_id
                where
                    mtv.create_date >= TIMESTAMP '2025-08-01 00:00:00'
                and mtv.field = 'stage'
                and mm.model = 'crm.line'
                and mtv.new_value_char ='Kết thúc'),
                        cte_consultant as (
            select
                mtv.id as id_line,
                mtv.field_desc as field_desc_line,
                mtv.old_value_integer::int as old_value_integer_line,
                mtv.old_value_char as old_value_char_line,
                mtv.new_value_integer::int as new_value_integer_line,
                mtv.new_value_char as new_value_char_line,
                mtv.mail_message_id as mail_message_id_line,
                mtv.write_uid as write_uid_line,
                mtv.write_date as write_date_line,
                mm."date" as date_changed,
                mm.model,
                mm.res_id,
                cl.crm_id
            from
                mail_tracking_value mtv
            join mail_message mm on
                mm.id = mtv.mail_message_id
            join crm_line cl on
                cl.id = mm.res_id
            where
                mtv.create_date >= TIMESTAMP '2025-08-01 00:00:00'
                and mtv.field in ('consultants_1', 'consultants_2', 'consultants_3')
                    and mm.model = 'crm.line'
                    and cl.stage <> 'cancel'
            union all
                select
                    mtv.id as id_line,
                    mtv.field_desc as field_desc_line,
                    mtv.old_value_integer::int as old_value_integer_line,
                    mtv.old_value_char as old_value_char_line,
                    mtv.new_value_integer::int as new_value_integer_line,
                    mtv.new_value_char as new_value_char_line,
                    mtv.mail_message_id as mail_message_id_line,
                    mtv.write_uid as write_uid_line,
                    mtv.write_date as write_date_line,
                    mm."date" as date_changed,
                    mm.model,
                    mm.res_id,
                    clp.booking_id as crm_id
                from
                    mail_tracking_value mtv
                join mail_message mm on
                    mm.id = mtv.mail_message_id
                join crm_line_product clp on
                    clp.id = mm.res_id
                where
                    mtv.create_date >= TIMESTAMP '2025-08-01 00:00:00'
                    and mtv.field in ('consultants_1', 'consultants_2', 'consultants_3')
                        and mm.model = 'crm.line.product'
                        and clp.stage_line_product <> 'cancel'),
                cte_source as (
            select
                mtv.id as id_line,
                mtv.field_desc as field_desc_line,
                mtv.old_value_integer::int as old_value_integer_line,
                mtv.old_value_char as old_value_char_line,
                mtv.new_value_integer::int as new_value_integer_line,
                mtv.new_value_char as new_value_char_line,
                mtv.mail_message_id as mail_message_id_line,
                mtv.write_uid as write_uid_line,
                mtv.write_date as write_date_line,
                mm."date" as date_changed,
                mm.model,
                mm.res_id
            from
                mail_tracking_value mtv
            join mail_message mm on
                mm.id = mtv.mail_message_id
            where
                mtv.create_date >= TIMESTAMP '2025-08-01 00:00:00'
                and mtv.field = 'source_id'
                and mm.model = 'crm.lead'),
            output_data as (
            select
                'changed_consultant' as type_data,
                ccl2.id_line as id,
                ccl2.field_desc_line as field_desc,
                ccl2.old_value_integer_line::int as old_value_integer,
                ccl2.old_value_char_line as old_value_char,
                ccl2.new_value_integer_line::int as new_value_integer,
                ccl2.new_value_char_line as new_value_char,
                ccl2.mail_message_id_line mail_message_id,
                ccl2.write_uid_line as write_uid,
                ccl2.write_date_line + interval '7 hours' as write_date,
                ccl2.date_changed + interval '7 hours' as date_changed,
                ccl2.model,
                ccl2.res_id,
                ccl2.crm_id
            from
                cte_consultant ccl2
            union all
            select
                'changed_source' as type_data,
                ccl2.id_line as id,
                ccl2.field_desc_line as field_desc,
                ccl2.old_value_integer_line::int as old_value_integer,
                ccl2.old_value_char_line as old_value_char,
                ccl2.new_value_integer_line::int as new_value_integer,
                ccl2.new_value_char_line as new_value_char,
                ccl2.mail_message_id_line mail_message_id,
                ccl2.write_uid_line as write_uid,
                ccl2.write_date_line + interval '7 hours' as write_date,
                ccl2.date_changed + interval '7 hours' as date_changed,
                ccl2.model,
                ccl2.res_id,
                ccl2.res_id as crm_id
            from
                cte_source ccl2
            union all
            select
                'changed_create_uid' as type_data,
                ccl3.id_line as id,
                ccl3.field_desc_line as field_desc,
                ccl3.old_value_integer_line::int as old_value_integer,
                ccl3.old_value_char_line as old_value_char,
                ccl3.new_value_integer_line::int as new_value_integer,
                ccl3.new_value_char_line as new_value_char,
                ccl3.mail_message_id_line mail_message_id,
                ccl3.write_uid_line as write_uid,
                ccl3.write_date_line + interval '7 hours' as write_date,
                ccl3.date_changed + interval '7 hours' as date_changed,
                ccl3.model,
                ccl3.res_id,
                ccl3.res_id as crm_id
            from
                cte_create_uid ccl3
            union all
            select
                'won_book' as type_data,
                ccl4.id_mail_crm_lead,
                ccl4.field_desc,
                ccl4.old_value_integer::int,
                ccl4.old_value_char,
                ccl4.new_value_integer::int,
                ccl4.new_value_char,
                ccl4.mail_message_id,
                ccl4.write_uid,
                ccl4.write_date + interval '7 hours' as write_date,
                ccl4.date_changed + interval '7 hours' as date_changed,
                ccl4.model,
                ccl4.booking_id,
                ccl4.booking_id
            from
                cte_crm_lead ccl4
            union all
            select
                'won_line' as type_data,
                ccl5.id_line as id,
                ccl5.field_desc_line as field_desc,
                ccl5.old_value_integer_line::int as old_value_integer,
                ccl5.old_value_char_line as old_value_char,
                ccl5.new_value_integer_line::int as new_value_integer,
                ccl5.new_value_char_line as new_value_char,
                ccl5.mail_message_id_line mail_message_id,
                ccl5.write_uid_line as write_uid,
                ccl5.write_date_line + interval '7 hours' as write_date,
                ccl5.date_changed + interval '7 hours' as date_changed,
                ccl5.model,
                ccl5.res_id,
                ccl5.crm_id
            from
                cte_cline ccl5
            )
            select
                *
            from
                output_data
        """
        erp_conn = self.get_conn(conn_name)
        try:
            with erp_conn.cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(data, columns=colnames)
                df['brand_etl'] = brand_etl
                return df
        finally:
            erp_conn.close()

    def transfer(self, erp_conn_name: str, brand_etl: int):
        logger.info(f"Starting transfer for {erp_conn_name} (brand_etl={brand_etl})")
        
        # 1. Fetch data
        df = self.fetch_data_from_erp(erp_conn_name, brand_etl)
        if df.empty:
            logger.info(f"No data fetched from {erp_conn_name}")
            return

        # 2. Preprocess data (following original logic)
        df.columns = [col.lower() for col in df.columns]
        integer_columns = ['id', 'old_value_integer', 'new_value_integer', 'mail_message_id', 'write_uid', 'res_id', 'crm_id', 'booking_id']
        for col in integer_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                except Exception as e:
                    logger.warning(f"Warning converting column {col}: {e}")

        # 3. Insert into Staging
        staging_conn = self.get_conn('staging_sci')
        try:
            with staging_conn.cursor() as cursor:
                # Delete existing data for this brand_etl
                cursor.execute(f'DELETE FROM stag_mail_message_erp WHERE brand_etl = %s;', (brand_etl,))
                
                # Use COPY for efficiency
                temp_dir = os.getenv('TEMP_DIR', '/tmp')
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', dir=temp_dir) as f:
                    df.to_csv(f.name, index=False, header=False)
                    f_name = f.name
                
                try:
                    with open(f_name, 'r', encoding='utf-8') as f:
                        cursor.copy_expert(
                            f"COPY stag_mail_message_erp ({', '.join(df.columns)}) FROM STDIN WITH CSV NULL AS ''",
                            f
                        )
                    staging_conn.commit()
                    logger.info(f"Successfully transferred {len(df)} rows for {erp_conn_name}")
                finally:
                    if os.path.exists(f_name):
                        os.remove(f_name)
        except Exception as e:
            staging_conn.rollback()
            logger.error(f"Failed to transfer data for {erp_conn_name}: {e}")
            raise e
        finally:
            staging_conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Transfer stag_mail_message_erp data')
    parser.add_argument('--task', type=str, required=True, help='Task to run (e.g., sync_sci, sync_kn, sync_pr, sync_hh)')
    args = parser.parse_args()

    conn_uris = {
        'staging_sci': os.getenv('STAGING_SCI_URI'),
        'sci': os.getenv('SCI_URI'),
        'kn': os.getenv('KN_URI'),
        'pr': os.getenv('PR_URI'),
        'hh': os.getenv('HH_URI')
    }

    manager = StagMailMessageErpManager(conn_uris=conn_uris)
    
    tasks = {
        'sync_sci': ('sci', 7),
        'sync_kn': ('kn', 1),
        'sync_pr': ('pr', 3),
        'sync_hh': ('hh', 4)
    }

    if args.task in tasks:
        erp_conn_name, brand_etl = tasks[args.task]
        manager.transfer(erp_conn_name, brand_etl)
    else:
        logger.error(f"Unknown task: {args.task}")
