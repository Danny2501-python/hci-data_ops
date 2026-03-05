import psycopg2
import pandas as pd
import numpy as np
import tempfile
import os
import re
import urllib.parse
import unicodedata
from loguru import logger
from typing import List, Optional

class ERPInsightSyncManager:
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

    def query_df(self, conn_name: str, query: str, ret='df'):
        connection = self.get_conn(conn_name)
        if not connection: return pd.DataFrame() if ret == 'df' else []
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                if ret == 'df':
                    if cursor.description is None:
                        return pd.DataFrame()
                    columns = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(cursor.fetchall(), columns=columns)
                else:
                    return [desc[0] for desc in cursor.description]
        finally:
            connection.close()

    def format_value(self, value):
        if isinstance(value, (float, complex, np.floating, np.complexfloating)):
            if value.is_integer():
                return int(value)
        return value

    def process_table_1(self, conn_name: str, table, table_dich):
        """Prepares a temporary table for loading."""
        connection = self.get_conn(conn_name)
        if not connection: return False
        try:
            with connection.cursor() as cursor:
                query = f"DROP TABLE IF EXISTS {table}; SELECT * INTO {table} FROM {table_dich} LIMIT 0"
                cursor.execute(query)
                connection.commit()
                return True
        except Exception as e:
            logger.error(f"Error processing table {table}: {e}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def process_table_2(self, conn_name: str, table, table_dich):
        """Performs UPSERT and drops temp table."""
        connection = self.get_conn(conn_name)
        if not connection: return False
        try:
            with connection.cursor() as cursor:
                # Delete existing records in table_dich that match IDs in temp table
                query_delete = f"DELETE FROM {table_dich} a WHERE EXISTS (SELECT 1 FROM {table} b WHERE a.id = b.id);"
                cursor.execute(query_delete)
                
                # Insert new records from temp table into table_dich
                query_insert = f"INSERT INTO {table_dich} SELECT * FROM {table};"
                cursor.execute(query_insert)
                
                # Drop temp table
                cursor.execute(f"DROP TABLE IF EXISTS {table};")
                connection.commit()
                logger.info(f"Table {table_dich} UPSERT and cleanup completed successfully.")
                return True
        except Exception as e:
            logger.error(f"Error merging table {table}: {e}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def sync_erp_insight_table(self, table_dich, source_query, process_fn=None):
        logger.info(f"Starting sync for {table_dich}")
        dfc = self.query_df('etl_insight', source_query)
        
        if dfc.empty:
            logger.info(f"No data to sync for {table_dich}")
            return table_dich

        if process_fn:
            dfc = process_fn(dfc)

        temp_table = f"{table_dich}_sync_tmp"
        self.process_table_1('staging_sci', temp_table, table_dich)
        
        # Get existing columns to match order
        existing_columns = self.query_df('staging_sci', f"SELECT * FROM {table_dich} LIMIT 0", ret='column')
        
        # Ensure only columns existing in target are used
        cols_to_use = [col for col in existing_columns if col in dfc.columns]
        insert_query = f"COPY {temp_table} ({', '.join(cols_to_use)}) FROM STDIN WITH CSV HEADER DELIMITER ','"

        df = dfc.applymap(self.format_value)

        # Handle numeric precision for COPY
        for col in df.select_dtypes(include=['float']):
            has_decimal = df[col].dropna().apply(lambda x: isinstance(x, float) and not x.is_integer()).any()
            if not has_decimal:
                df[col] = df[col].astype(pd.Int64Dtype())

        temp_dir = os.getenv('TEMP_DIR', '/tmp')
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', dir=temp_dir) as temp_file:
            result = df[cols_to_use]
            result.to_csv(temp_file.name, index=False, header=True)
            
            connection = self.get_conn('staging_sci')
            try:
                with connection.cursor() as cursor:
                    with open(temp_file.name, 'r', encoding='utf-8') as f:
                        cursor.copy_expert(insert_query, f)
                    connection.commit()
            finally:
                connection.close()
                if os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)
        
        self.process_table_2('staging_sci', temp_table, table_dich)
        logger.info(f"COMPLETED! Sync for {table_dich}")
        return table_dich

    # --- Table Specific Implementations ---

    def sync_crm_lead(self):
        query = """
            SELECT id, company_id, lost_reason, message_main_attachment_id, campaign_id, source_id, medium_id, create_uid, create_date, write_uid, write_date, user_create_oppo, birth_date, assgin_time, booking_date, lead_id, check_stage, brand_sci, calendar, amount_untaxed, amount_tax, amount_total, check_enroll, visible_stage_2, visible_stage_3, payment_term, open_enroll, create_date_sci, camp_mkt, content, term, source_mkt, is_missing, sci_company, partner_id, active, date_action_last, team_id, date_closed, stage_id, user_id, date_open, day_open, day_close, date_last_stage_update, date_conversion, message_bounce, probability, planned_revenue, expected_revenue, date_deadline, color, state_id, country_id, title, name, gender, phone, mobile, email_from, website, function, email_cc, description, contact_name, partner_name, type, priority, stage_id_3, log_insert, ticket_id, referred, code_campaign, month, year, name_campaign, name_location, log_api, customer_come, facebook_acc, first_link, \"desc\" as desc_moi, id_form, street, street2, zip, city, stage_id_2 
            FROM crm_lead 
            WHERE write_date > CURRENT_DATE - INTERVAL '10 days'
        """
        self.sync_erp_insight_table('erp_insight_crm_lead', query)

    def sync_source_mkt(self):
        query = "SELECT *, '' as name_fix FROM source_mkt WHERE write_date > CURRENT_DATE - INTERVAL '10 days'"
        def process(df):
            df['name_fix'] = df['name'].apply(lambda x: urllib.parse.unquote(str(x)) if pd.notnull(x) else "")
            return df
        self.sync_erp_insight_table('erp_insight_source_mkt', query, process)

    def sync_camp_mkt(self):
        query = "SELECT *, '' as name_fix FROM camp_mkt WHERE write_date > CURRENT_DATE - INTERVAL '10 days'"
        def process(df):
            df['name_fix'] = df['name'].apply(lambda x: urllib.parse.unquote(str(x)) if pd.notnull(x) else "")
            return df
        self.sync_erp_insight_table('erp_insight_camp_mkt', query, process)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Sync ERP Insight Tables')
    parser.add_argument('--task', type=str, required=True, help='Task to run')
    args = parser.parse_args()

    conn_uris = {
        'staging_sci': os.getenv('STAGING_SCI_URI'),
        'etl_insight': os.getenv('ETL_INSIGHT_URI')
    }

    manager = ERPInsightSyncManager(conn_uris=conn_uris)
    
    tasks = {
        'sync_erp_insight_crm_lead': manager.sync_crm_lead,
        'sync_erp_insight_source_mkt': manager.sync_source_mkt,
        'sync_erp_insight_camp_mkt': manager.sync_camp_mkt
    }

    if args.task in tasks:
        tasks[args.task]()
    else:
        logger.error(f"Unknown task: {args.task}")
