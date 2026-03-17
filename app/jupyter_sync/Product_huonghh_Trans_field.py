import psycopg2
import pandas as pd
import numpy as np
import tempfile
import os
from loguru import logger
from typing import List

class ProductTransFieldManager:
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
            logger.error(f"Failed to connect to {dbname}: {e}")
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
        connection = self.get_conn(conn_name)
        if not connection: return False
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DELETE FROM {table_dich};")
                cursor.execute(f"INSERT INTO {table_dich} SELECT * FROM {table};")
                cursor.execute(f"DROP TABLE IF EXISTS {table};")
                connection.commit()
                logger.info(f"Table {table_dich} processing completed successfully.")
                return True
        except Exception as e:
            logger.error(f"Error processing table {table}: {e}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def execute_sql(self, conn_name: str, queries: List[str]):
        connection = self.get_conn(conn_name)
        if not connection: return False
        try:
            with connection.cursor() as cursor:
                for query in queries:
                    cursor.execute(query)
                connection.commit()
                return True
        except Exception as e:
            connection.rollback()
            logger.error(f"SQL Execution failed: {e}")
            raise e
        finally:
            connection.close()

    def sync_trans_field(self, table_name, insert_moi_query, update_cu_query, insert_cu_query):
        logger.info(f"Starting sync for {table_name}")
        try:
            self.execute_sql('staging_sci', [update_cu_query, insert_cu_query, insert_moi_query])
            logger.info(f"COMPLETED! Sync for {table_name}")
        except Exception as e:
            logger.error(f"Failed sync for {table_name}: {e}")

    def sync_ks_table(self, source_table, target_table):
        query_sp = f"""
            SELECT * FROM {source_table}
            WHERE (id, brand_etl) IN (SELECT DISTINCT id, brand_etl FROM {source_table} WHERE status = 'N')
            ORDER BY id, brand_etl
        """
        dfc = self.query_df('staging_sci', query_sp)
        
        if dfc.empty:
            logger.info(f"No data to sync for {target_table}")
            return target_table

        temp_table = f"{target_table}_1xjs2qcuj"
        self.process_table_1('staging_sci', temp_table, target_table)
        
        existing_columns = self.query_df('staging_sci', f"SELECT * FROM {target_table} LIMIT 0", ret='column')
        insert_query = f"COPY {temp_table} ({', '.join(existing_columns)}) FROM STDIN WITH CSV HEADER DELIMITER ','"

        df = dfc.applymap(self.format_value)

        for col in df.select_dtypes(include=['float']):
            has_decimal = df[col].dropna().apply(lambda x: isinstance(x, float) and not x.is_integer()).any()
            if not has_decimal:
                df[col] = df[col].astype(pd.Int64Dtype())

        temp_dir = os.getenv('TEMP_DIR', '/tmp')
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', dir=temp_dir) as temp_file:
            result = df[existing_columns]
            result.to_csv(temp_file.name, index=False, header=True)
            
            connection = self.get_conn('staging_sci')
            with connection.cursor() as cursor:
                with open(temp_file.name, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(insert_query, f)
                connection.commit()
        
        self.process_table_2('staging_sci', temp_table, target_table)
        logger.info(f"HOÀN THÀNH! CẬP NHẬT BẢNG {target_table}")
        return target_table

    # --- Specific Sync Methods ---

    def sync_sh_medical_specialty_supply(self):
        insert_moi = """
            INSERT INTO sh_medical_specialty_supply_trans_field (id, "name", qty_used, supply,state, start_date, expiration_date, status,write_uid, brand_etl)
            SELECT smsss.id, smsss."name", smsss.qty_used, smsss.supply, smsss.state, smsss.write_date as start_date, 
                   '2100-12-31 00:00:00.000' as expiration_date, 'Y' as status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss."name", smss.qty_used, smss.supply, sms.state, smss.write_date, smss.write_uid, smss.brand_etl
                FROM sh_medical_specialty_supply smss 
                INNER JOIN sh_medical_specialty sms ON smss."name" = sms.id and smss.brand_etl=sms.brand_etl 
                WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss 
            WHERE NOT EXISTS (
                SELECT 1 
                FROM sh_medical_specialty_supply_trans_field smsstf  
                WHERE smsstf.status = 'Y' AND smsstf.id = smsss.id and smsstf.brand_etl = smsss.brand_etl
            );
        """
        update_cu = """
            UPDATE sh_medical_specialty_supply_trans_field AS smsstf1
            SET expiration_date = sms_moi.write_date,
                status = 'N'
            FROM (
                SELECT smsss.id, smsss.write_date, smsss.brand_etl
                FROM (
                    SELECT smss.id, smss."name", smss.qty_used, smss.supply, smss.write_date, smss.write_uid, smss.brand_etl
                    FROM sh_medical_specialty_supply smss
                    INNER JOIN sh_medical_specialty sms ON smss."name" = sms.id and smss.brand_etl=sms.brand_etl 
                    WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
                ) smsss
                INNER JOIN sh_medical_specialty_supply_trans_field smsstf
                ON smsss.id = smsstf.id and smsstf.brand_etl = smsss.brand_etl
                WHERE smsstf.status = 'Y'
                  AND (smsstf.qty_used <> smsss.qty_used OR smsstf.supply <> smsss.supply)
            ) AS sms_moi
            WHERE smsstf1.id = sms_moi.id and smsstf1.brand_etl = sms_moi.brand_etl;
        """
        insert_cu = """
            INSERT INTO sh_medical_specialty_supply_trans_field (id, "name", qty_used, supply,state, start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss."name", smsss.qty_used, smsss.supply,smsss.state, smsss.write_date AS start_date, 
                   '2100-12-31 00:00:00.000' AS expiration_date, 
                   'Y' AS status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss."name", smss.qty_used, smss.supply, sms.state, smss.write_date, smss.write_uid, smss.brand_etl
                FROM sh_medical_specialty_supply smss
                INNER JOIN sh_medical_specialty sms ON smss."name" = sms.id and smss.brand_etl=sms.brand_etl 
                WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss
            INNER JOIN sh_medical_specialty_supply_trans_field smsstf ON smsss.id = smsstf.id and smsstf.brand_etl = smsss.brand_etl
            WHERE smsstf.status = 'Y' 
              AND (smsstf.qty_used <> smsss.qty_used OR smsstf.supply <> smsss.supply);
        """
        self.sync_trans_field('sh_medical_specialty_supply_trans_field', insert_moi, update_cu, insert_cu)

    def sync_sh_medical_specialty_team(self):
        insert_moi = """
            INSERT INTO sh_medical_specialty_team_trans_field (id, "name", team_member , "role" ,state, start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss."name", smsss.team_member, smsss."role" , smsss.state, smsss.write_date as start_date, 
                   '2100-12-31 00:00:00.000' as expiration_date, 'Y' as status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smst.id, smst."name", smst.team_member , smst."role"  , sms.state, smst.write_date, smst.write_uid, smst.brand_etl
                FROM sh_medical_specialty_team smst  
                INNER JOIN sh_medical_specialty sms ON smst."name" = sms.id and smst.brand_etl = sms.brand_etl
                WHERE sms.state = 'Done' AND smst.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss 
            WHERE NOT EXISTS (
                SELECT 1 
                FROM sh_medical_specialty_team_trans_field smsstf  
                WHERE smsstf.status = 'Y' AND smsstf.id = smsss.id and smsstf.brand_etl = smsss.brand_etl 
            );
        """
        update_cu = """
            UPDATE sh_medical_specialty_team_trans_field AS smsstf1
            SET expiration_date = sms_moi.write_date,
                status = 'N'
            FROM (
                SELECT smsss.id, smsss.write_date, smsss.brand_etl
                FROM (
                    SELECT smss.id, smss."name", smss.team_member, smss."role", smss.write_date, smss.write_uid, smss.brand_etl
                    FROM sh_medical_specialty_team smss
                    INNER JOIN sh_medical_specialty sms ON smss."name" = sms.id and smss.brand_etl = sms.brand_etl
                    WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
                ) smsss
                INNER JOIN sh_medical_specialty_team_trans_field smsstf
                ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
                WHERE smsstf.status = 'Y'
                  AND (smsstf.team_member <> smsss.team_member OR smsstf."role" <> smsss."role")
            ) AS sms_moi
            WHERE smsstf1.id = sms_moi.id and smsstf1.brand_etl = sms_moi.brand_etl;
        """
        insert_cu = """
            INSERT INTO sh_medical_specialty_team_trans_field (id, "name", team_member, "role", state, start_date, expiration_date, status, write_uid,brand_etl)
            SELECT smsss.id, smsss."name", smsss.team_member, smsss."role",smsss.state, smsss.write_date AS start_date, 
                   '2100-12-31 00:00:00.000' AS expiration_date, 
                   'Y' AS status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss."name", smss.team_member, smss."role", sms.state, smss.write_date, smss.write_uid, smss.brand_etl
                FROM sh_medical_specialty_team smss
                INNER JOIN sh_medical_specialty sms ON smss."name" = sms.id and smss.brand_etl = sms.brand_etl
                WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss
            INNER JOIN sh_medical_specialty_team_trans_field smsstf ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
            WHERE smsstf.status = 'Y' 
              AND (smsstf.team_member <> smsss.team_member OR smsstf."role" <> smsss."role");
        """
        self.sync_trans_field('sh_medical_specialty_team_trans_field', insert_moi, update_cu, insert_cu)

    def sync_sh_medical_surgery_supply(self):
        insert_moi = """
            INSERT INTO sh_medical_surgery_supply_trans_field (id, "name", qty_used, supply,state, start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss."name", smsss.qty_used, smsss.supply, smsss.state, smsss.write_date as start_date, 
                   '2100-12-31 00:00:00.000' as expiration_date, 'Y' as status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss."name", smss.qty_used, smss.supply, sms.state, smss.write_date, smss.write_uid, smss.brand_etl
                FROM sh_medical_surgery_supply smss 
                INNER JOIN sh_medical_surgery sms ON smss."name" = sms.id and smss.brand_etl = sms.brand_etl
                WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss 
            WHERE NOT EXISTS (
                SELECT 1 
                FROM sh_medical_surgery_supply_trans_field smsstf  
                WHERE smsstf.status = 'Y' AND smsstf.id = smsss.id and smsstf.brand_etl = smsss.brand_etl 
            );
        """
        update_cu = """
            UPDATE sh_medical_surgery_supply_trans_field AS smsstf1
            SET expiration_date = sms_moi.write_date,
                status = 'N'
            FROM (
                SELECT smsss.id, smsss.write_date, smsss.brand_etl
                FROM (
                    SELECT smss.id, smss."name", smss.qty_used, smss.supply, smss.write_date, smss.write_uid, smss.brand_etl
                    FROM sh_medical_surgery_supply smss
                    INNER JOIN sh_medical_surgery sms ON smss."name" = sms.id and smss.brand_etl = sms.brand_etl
                    WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
                ) smsss
                INNER JOIN sh_medical_surgery_supply_trans_field smsstf
                ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
                WHERE smsstf.status = 'Y'
                  AND (smsstf.qty_used <> smsss.qty_used OR smsstf.supply <> smsss.supply)
            ) AS sms_moi
            WHERE smsstf1.id = sms_moi.id and smsstf1.brand_etl = sms_moi.brand_etl;
        """
        insert_cu = """
            INSERT INTO sh_medical_surgery_supply_trans_field (id, "name", qty_used, supply,state, start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss."name", smsss.qty_used, smsss.supply,smsss.state, smsss.write_date AS start_date, 
                   '2100-12-31 00:00:00.000' AS expiration_date, 
                   'Y' AS status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss."name", smss.qty_used, smss.supply, sms.state, smss.write_date, smss.write_uid, smss.brand_etl
                FROM sh_medical_surgery_supply smss
                INNER JOIN sh_medical_surgery sms ON smss."name" = sms.id and smss.brand_etl = sms.brand_etl
                WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss
            INNER JOIN sh_medical_surgery_supply_trans_field smsstf ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
            WHERE smsstf.status = 'Y' 
              AND (smsstf.qty_used <> smsss.qty_used OR smsstf.supply <> smsss.supply);
        """
        self.sync_trans_field('sh_medical_surgery_supply_trans_field', insert_moi, update_cu, insert_cu)

    def sync_sh_medical_surgery_team(self):
        insert_moi = """
            INSERT INTO sh_medical_surgery_team_trans_field (id, "name", team_member , "role" ,state, start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss."name", smsss.team_member, smsss."role" , smsss.state, smsss.write_date as start_date, 
                   '2100-12-31 00:00:00.000' as expiration_date, 'Y' as status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smst.id, smst."name", smst.team_member , smst."role"  , sms.state, smst.write_date, smst.write_uid, smst.brand_etl
                FROM sh_medical_surgery_team smst  
                INNER JOIN sh_medical_surgery sms ON smst."name" = sms.id and smst.brand_etl = sms.brand_etl
                WHERE sms.state = 'Done' AND smst.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss 
            WHERE NOT EXISTS (
                SELECT 1 
                FROM sh_medical_surgery_team_trans_field smsstf  
                WHERE smsstf.status = 'Y' AND smsstf.id = smsss.id and smsstf.brand_etl = smsss.brand_etl 
            );
        """
        update_cu = """
            UPDATE sh_medical_surgery_team_trans_field AS smsstf1
            SET expiration_date = sms_moi.write_date,
                status = 'N'
            FROM (
                SELECT smsss.id, smsss.write_date, smsss.brand_etl
                FROM (
                    SELECT smss.id, smss."name", smss.team_member, smss."role", smss.write_date, smss.write_uid, smss.brand_etl
                    FROM sh_medical_surgery_team smss
                    INNER JOIN sh_medical_surgery sms ON smss."name" = sms.id and smss.brand_etl = sms.brand_etl
                    WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
                ) smsss
                INNER JOIN sh_medical_surgery_team_trans_field smsstf
                ON smsss.id = smsstf.id  and smsss.brand_etl = smsstf.brand_etl
                WHERE smsstf.status = 'Y'
                  AND (smsstf.team_member <> smsss.team_member OR smsstf."role" <> smsss."role")
            ) AS sms_moi
            WHERE smsstf1.id = sms_moi.id and smsstf1.brand_etl = sms_moi.brand_etl;
        """
        insert_cu = """
            INSERT INTO sh_medical_surgery_team_trans_field (id, "name", team_member, "role", state, start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss."name", smsss.team_member, smsss."role",smsss.state, smsss.write_date AS start_date, 
                   '2100-12-31 00:00:00.000' AS expiration_date, 
                   'Y' AS status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss."name", smss.team_member, smss."role", sms.state, smss.write_date, smss.write_uid, smss.brand_etl
                FROM sh_medical_surgery_team smss
                INNER JOIN sh_medical_surgery sms ON smss."name" = sms.id and smss.brand_etl = sms.brand_etl
                WHERE sms.state = 'Done' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss
            INNER JOIN sh_medical_surgery_team_trans_field smsstf ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
            WHERE smsstf.status = 'Y' 
              AND (smsstf.team_member <> smsss.team_member OR smsstf."role" <> smsss."role");
        """
        self.sync_trans_field('sh_medical_surgery_team_trans_field', insert_moi, update_cu, insert_cu)

    def sync_sale_order_line(self):
        insert_moi = """
            INSERT INTO sale_order_line_trans_field (id, order_id , product_uom_qty  , price_subtotal ,state, start_date, expiration_date, status,write_uid, brand_etl)
            SELECT smsss.id, smsss.order_id, smsss.product_uom_qty, smsss.price_subtotal , smsss.state, smsss.write_date as start_date, 
                   '2100-12-31 00:00:00.000' as expiration_date, 'Y' as status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smst.id, smst.order_id, smst.product_uom_qty , smst.price_subtotal , sms.state, smst.write_date, smst.write_uid, smst.brand_etl
                FROM sale_order_line smst  
                INNER JOIN sale_order sms ON smst.order_id = sms.id and smst.brand_etl = sms.brand_etl
                WHERE sms.state = 'sale' AND smst.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss 
            WHERE NOT EXISTS (
                SELECT 1 
                FROM sale_order_line_trans_field smsstf  
                WHERE smsstf.status = 'Y' AND smsstf.id = smsss.id and smsstf.brand_etl = smsss.brand_etl
            );
        """
        update_cu = """
            UPDATE sale_order_line_trans_field AS smsstf1
            SET expiration_date = sms_moi.write_date,
                status = 'N'
            FROM (
                SELECT smsss.id, smsss.write_date, smsss.brand_etl
                FROM (
                    SELECT smss.id, smss.order_id, smss.product_uom_qty, smss.price_subtotal, smss.write_date, smss.write_uid, smss.brand_etl
                    FROM sale_order_line smss
                    INNER JOIN sale_order sms ON smss.order_id = sms.id and smss.brand_etl = sms.brand_etl
                    WHERE sms.state = 'sale' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
                ) smsss
                INNER JOIN sale_order_line_trans_field smsstf
                ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
                WHERE smsstf.status = 'Y'
                  AND (smsstf.product_uom_qty <> smsss.product_uom_qty OR smsstf.price_subtotal <> smsss.price_subtotal)
            ) AS sms_moi
            WHERE smsstf1.id = sms_moi.id and smsstf1.brand_etl = sms_moi.brand_etl;
        """
        insert_cu = """
            INSERT INTO sale_order_line_trans_field (id, order_id , product_uom_qty  , price_subtotal ,state, start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss.order_id, smsss.product_uom_qty, smsss.price_subtotal,smsss.state, smsss.write_date AS start_date, 
                   '2100-12-31 00:00:00.000' AS expiration_date, 
                   'Y' AS status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss.order_id, smss.product_uom_qty, smss.price_subtotal, sms.state, smss.write_date, smss.write_uid, smss.brand_etl
                FROM sale_order_line smss
                INNER JOIN sale_order sms ON smss.order_id = sms.id and smss.brand_etl = sms.brand_etl
                WHERE sms.state = 'sale' AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss
            INNER JOIN sale_order_line_trans_field smsstf ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
            WHERE smsstf.status = 'Y' 
              AND (smsstf.product_uom_qty <> smsss.product_uom_qty OR smsstf.price_subtotal <> smsss.price_subtotal);
        """
        self.sync_trans_field('sale_order_line_trans_field', insert_moi, update_cu, insert_cu)

    def sync_crm_line(self):
        insert_moi = """
            INSERT INTO crm_line_trans_field (id, crm_id , consultants_1 ,consulting_role_1  , consultants_2 ,consulting_role_2 ,consultants_3 ,consulting_role_3 
            ,product_id ,quantity  ,stage_id , start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss.crm_id, smsss.consultants_1 , smsss.consulting_role_1  , smsss.consultants_2 ,smsss.consulting_role_2 ,
                smsss.consultants_3 ,smsss.consulting_role_3 ,smsss.product_id ,smsss.quantity  , smsss.stage_id, smsss.write_date as start_date, 
                   '2100-12-31 00:00:00.000' as expiration_date, 'Y' as status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smst.id, smst.crm_id, smst.consultants_1 , smst.consulting_role_1  , smst.consultants_2 ,smst.consulting_role_2 ,
                smst.consultants_3 ,smst.consulting_role_3 ,smst.product_id ,smst.quantity  , sms.stage_id , smst.write_date, smst.write_uid, smst.brand_etl
                FROM crm_line smst  
                INNER JOIN crm_lead sms ON smst.crm_id = sms.id and smst.brand_etl = sms.brand_etl
                WHERE sms.stage_id  = 4 AND smst.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss 
            WHERE NOT EXISTS (
                SELECT 1 
                FROM crm_line_trans_field smsstf  
                WHERE smsstf.status = 'Y' AND smsstf.id = smsss.id and smsstf.brand_etl = smsss.brand_etl
            );
        """
        update_cu = """
            UPDATE crm_line_trans_field AS smsstf1
            SET expiration_date = sms_moi.write_date,
                status = 'N'
            FROM (
                SELECT smsss.id, smsss.write_date, smsss.brand_etl
                FROM (
                    SELECT smss.id, smss.crm_id, smss.consultants_1 , smss.consulting_role_1  , smss.consultants_2 ,smss.consulting_role_2 ,
                smss.consultants_3 ,smss.consulting_role_3 ,smss.product_id ,smss.quantity  , sms.stage_id, smss.write_date, smss.write_uid, smss.brand_etl
                    FROM crm_line smss
                    INNER JOIN crm_lead sms ON smss.crm_id = sms.id and smss.brand_etl = sms.brand_etl
                    WHERE sms.stage_id  = 4 AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
                ) smsss
                INNER JOIN crm_line_trans_field smsstf
                ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl 
                WHERE smsstf.status = 'Y'
                  AND (smsstf.consultants_1 <> smsss.consultants_1 OR smsstf.consultants_2 <> smsss.consultants_2 or 
                  smsstf.consultants_3 <> smsss.consultants_3 OR smsstf.product_id <> smsss.product_id or smsstf.quantity <> smsss.quantity)
            ) AS sms_moi
            WHERE smsstf1.id = sms_moi.id and smsstf1.brand_etl = sms_moi.brand_etl;
        """
        insert_cu = """
            INSERT INTO crm_line_trans_field (id, crm_id , consultants_1 ,consulting_role_1  , consultants_2 ,consulting_role_2 ,consultants_3 ,consulting_role_3 
            ,product_id ,quantity  ,stage_id , start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss.crm_id, smsss.consultants_1 , smsss.consulting_role_1  , smsss.consultants_2 ,smsss.consulting_role_2 ,
                smsss.consultants_3 ,smsss.consulting_role_3 ,smsss.product_id ,smsss.quantity  , smsss.stage_id, smsss.write_date AS start_date, 
                   '2100-12-31 00:00:00.000' AS expiration_date, 
                   'Y' AS status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss.crm_id, smss.consultants_1 , smss.consulting_role_1  , smss.consultants_2 ,smss.consulting_role_2 ,
                smss.consultants_3 ,smss.consulting_role_3 ,smss.product_id ,smss.quantity  , sms.stage_id, smss.write_date, smss.write_uid, smss.brand_etl
                FROM crm_line smss
                INNER JOIN crm_lead sms ON smss.crm_id = sms.id and smss.brand_etl = sms.brand_etl
                WHERE sms.stage_id  = 4 AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss
            INNER JOIN crm_line_trans_field smsstf ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
            WHERE smsstf.status = 'Y' 
              AND (smsstf.consultants_1 <> smsss.consultants_1 OR smsstf.consultants_2 <> smsss.consultants_2 or 
                  smsstf.consultants_3 <> smsss.consultants_3 OR smsstf.product_id <> smsss.product_id or smsstf.quantity <> smsss.quantity);
        """
        self.sync_trans_field('crm_line_trans_field', insert_moi, update_cu, insert_cu)

    def sync_crm_line_product(self):
        insert_moi = """
            INSERT INTO crm_line_product_trans_field (id, booking_id , consultants_1 ,consulting_role_1  , consultants_2 ,consulting_role_2 ,consultants_3 ,consulting_role_3 
            ,product_id ,product_uom_qty  ,stage_id , start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss.booking_id, smsss.consultants_1 , smsss.consulting_role_1  , smsss.consultants_2 ,smsss.consulting_role_2 ,
                smsss.consultants_3 ,smsss.consulting_role_3 ,smsss.product_id ,smsss.product_uom_qty  , smsss.stage_id, smsss.write_date as start_date, 
                   '2100-12-31 00:00:00.000' as expiration_date, 'Y' as status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smst.id, smst.booking_id, smst.consultants_1 , smst.consulting_role_1  , smst.consultants_2 ,smst.consulting_role_2 ,
                smst.consultants_3 ,smst.consulting_role_3 ,smst.product_id ,smst.product_uom_qty  , sms.stage_id , smst.write_date, smst.write_uid, smst.brand_etl 
                FROM crm_line_product smst  
                INNER JOIN crm_lead sms ON smst.booking_id = sms.id and smst.brand_etl = sms.brand_etl
                WHERE sms.stage_id  = 4 AND smst.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss 
            WHERE NOT EXISTS (
                SELECT 1 
                FROM crm_line_product_trans_field smsstf  
                WHERE smsstf.status = 'Y' AND smsstf.id = smsss.id and smsstf.brand_etl = smsss.brand_etl
            );
        """
        update_cu = """
            UPDATE crm_line_product_trans_field AS smsstf1
            SET expiration_date = sms_moi.write_date,
                status = 'N'
            FROM (
                SELECT smsss.id, smsss.write_date, smsss.brand_etl
                FROM (
                    SELECT smss.id, smss.booking_id, smss.consultants_1 , smss.consulting_role_1  , smss.consultants_2 ,smss.consulting_role_2 ,
                smss.consultants_3 ,smss.consulting_role_3 ,smss.product_id ,smss.product_uom_qty  , sms.stage_id, smss.write_date, smss.write_uid, smss.brand_etl
                    FROM crm_line_product smss
                    INNER JOIN crm_lead sms ON smss.booking_id = sms.id and smss.brand_etl = sms.brand_etl
                    WHERE sms.stage_id  = 4 AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
                ) smsss
                INNER JOIN crm_line_product_trans_field smsstf
                ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
                WHERE smsstf.status = 'Y'
                  AND (smsstf.consultants_1 <> smsss.consultants_1 OR smsstf.consultants_2 <> smsss.consultants_2 or 
                  smsstf.consultants_3 <> smsss.consultants_3 OR smsstf.product_id <> smsss.product_id or smsstf.product_uom_qty <> smsss.product_uom_qty)
            ) AS sms_moi
            WHERE smsstf1.id = sms_moi.id and smsstf1.brand_etl = sms_moi.brand_etl;
        """
        insert_cu = """
            INSERT INTO crm_line_product_trans_field (id, booking_id , consultants_1 ,consulting_role_1  , consultants_2 ,consulting_role_2 ,consultants_3 ,consulting_role_3 
            ,product_id ,product_uom_qty  ,stage_id , start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss.booking_id, smsss.consultants_1 , smsss.consulting_role_1  , smsss.consultants_2 ,smsss.consulting_role_2 ,
                smsss.consultants_3 ,smsss.consulting_role_3 ,smsss.product_id ,smsss.product_uom_qty  , smsss.stage_id, smsss.write_date AS start_date, 
                   '2100-12-31 00:00:00.000' AS expiration_date, 
                   'Y' AS status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss.booking_id, smss.consultants_1 , smss.consulting_role_1  , smss.consultants_2 ,smss.consulting_role_2 ,
                smss.consultants_3 ,smss.consulting_role_3 ,smss.product_id ,smss.product_uom_qty  , sms.stage_id, smss.write_date, smss.write_uid, smss.brand_etl
                FROM crm_line_product smss
                INNER JOIN crm_lead sms ON smss.booking_id = sms.id and smss.brand_etl = sms.brand_etl
                WHERE sms.stage_id  = 4 AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss
            INNER JOIN crm_line_product_trans_field smsstf ON smsss.id = smsstf.id and smsss.brand_etl = smsstf.brand_etl
            WHERE smsstf.status = 'Y' 
              AND (smsstf.consultants_1 <> smsss.consultants_1 OR smsstf.consultants_2 <> smsss.consultants_2 or 
                  smsstf.consultants_3 <> smsss.consultants_3 OR smsstf.product_id <> smsss.product_id or smsstf.product_uom_qty <> smsss.product_uom_qty);
        """
        self.sync_trans_field('crm_line_product_trans_field', insert_moi, update_cu, insert_cu)

    def sync_crm_lead(self):
        insert_moi = """
            INSERT INTO crm_lead_trans_field (id, create_by , create_uid ,source_id  , stage_id , start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss.create_by, smsss.create_uid , smsss.source_id , smsss.stage_id, smsss.write_date as start_date, 
                   '2100-12-31 00:00:00.000' as expiration_date, 'Y' as status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smst.id, smst.create_by, smst.create_uid , smst.source_id , smst.stage_id , smst.write_date, smst.write_uid, smst.brand_etl
                FROM crm_lead smst  
                WHERE smst.stage_id  = 4 AND smst.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss 
            WHERE NOT EXISTS (
                SELECT 1 
                FROM crm_lead_trans_field smsstf  
                WHERE smsstf.status = 'Y' AND smsstf.id = smsss.id and smsss.brand_etl = smsstf.brand_etl 
            );
        """
        update_cu = """
            UPDATE crm_lead_trans_field AS smsstf1
            SET expiration_date = sms_moi.write_date,
                status = 'N'
            FROM (
                SELECT smsss.id, smsss.write_date, smsss.brand_etl
                FROM (
                    SELECT smss.id, smss.create_by, smss.create_uid , smss.source_id  , smss.stage_id, smss.write_date, smss.write_uid, smss.brand_etl
                    FROM crm_lead smss
                    WHERE smss.stage_id  = 4 AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
                ) smsss
                INNER JOIN crm_lead_trans_field smsstf
                ON smsss.id = smsstf.id and smsstf.brand_etl = smsss.brand_etl 
                WHERE smsstf.status = 'Y'
                  AND (smsstf.create_by <> smsss.create_by OR smsstf.create_uid <> smsss.create_uid or smsstf.source_id <> smsss.source_id)
            ) AS sms_moi
            WHERE smsstf1.id = sms_moi.id and smsstf1.brand_etl = sms_moi.brand_etl ;
        """
        insert_cu = """
            INSERT INTO crm_lead_trans_field (id, create_by , create_uid ,source_id  , stage_id , start_date, expiration_date, status, write_uid, brand_etl)
            SELECT smsss.id, smsss.create_by, smsss.create_uid , smsss.source_id  , smsss.stage_id, smsss.write_date AS start_date, 
                   '2100-12-31 00:00:00.000' AS expiration_date, 
                   'Y' AS status, smsss.write_uid, smsss.brand_etl
            FROM (
                SELECT smss.id, smss.create_by, smss.create_uid , smss.source_id  , smss.stage_id, smss.write_date, smss.write_uid, smss.brand_etl
                FROM crm_lead smss
                WHERE smss.stage_id  = 4 AND smss.write_date >= CURRENT_DATE - INTERVAL '10 days'
            ) smsss
            INNER JOIN crm_lead_trans_field smsstf ON smsss.id = smsstf.id and smsstf.brand_etl = smsss.brand_etl 
            WHERE smsstf.status = 'Y' 
              AND (smsstf.create_by <> smsss.create_by OR smsstf.create_uid <> smsss.create_uid or smsstf.source_id <> smsss.source_id);
        """
        self.sync_trans_field('crm_lead_trans_field', insert_moi, update_cu, insert_cu)

    # --- KS Sync Methods ---

    def sync_ks_all(self):
        ks_mappings = [
            ('crm_line_trans_field', 'ks_crm_line_trans_field'),
            ('crm_line_product_trans_field', 'ks_crm_line_product_trans_field'),
            ('crm_lead_trans_field', 'ks_crm_lead_trans_field'),
            ('sale_order_line_trans_field', 'ks_sale_order_line_trans_field'),
            ('sh_medical_specialty_supply_trans_field', 'ks_sh_medical_specialty_supply_trans_field'),
            ('sh_medical_surgery_supply_trans_field', 'ks_sh_medical_surgery_supply_trans_field'),
            ('sh_medical_specialty_team_trans_field', 'ks_sh_medical_specialty_team_trans_field'),
            ('sh_medical_surgery_team_trans_field', 'ks_sh_medical_surgery_team_trans_field'),
        ]
        for source, target in ks_mappings:
            self.sync_ks_table(source, target)

if __name__ == "__main__":
    import argparse
    import os
    parser = argparse.ArgumentParser(description='Sync Product Trans Fields')
    parser.add_argument('--task', type=str, required=True, help='Task to run')
    args = parser.parse_args()

    # Receive URIs from environment variables set by DAG
    conn_uris = {
        'staging_sci': os.getenv('STAGING_SCI_URI'),
        'erp': os.getenv('ERP_URI'),
        'dev': os.getenv('DEV_URI'),
        'sandbox': os.getenv('SANDBOX_URI')
    }

    manager = ProductTransFieldManager(conn_uris=conn_uris)
    
    if args.task == 'sync_sh_medical_specialty_supply':
        manager.sync_sh_medical_specialty_supply()
    elif args.task == 'sync_sh_medical_specialty_team':
        manager.sync_sh_medical_specialty_team()
    elif args.task == 'sync_sh_medical_surgery_supply':
        manager.sync_sh_medical_surgery_supply()
    elif args.task == 'sync_sh_medical_surgery_team':
        manager.sync_sh_medical_surgery_team()
    elif args.task == 'sync_sale_order_line':
        manager.sync_sale_order_line()
    elif args.task == 'sync_crm_line':
        manager.sync_crm_line()
    elif args.task == 'sync_crm_line_product':
        manager.sync_crm_line_product()
    elif args.task == 'sync_crm_lead':
        manager.sync_crm_lead()
    elif args.task.startswith('sync_ks_'):
        # Target table mapping
        ks_mappings = {
            'sync_ks_sh_medical_specialty_supply': ('sh_medical_specialty_supply_trans_field', 'ks_sh_medical_specialty_supply_trans_field'),
            'sync_ks_sh_medical_specialty_team': ('sh_medical_specialty_team_trans_field', 'ks_sh_medical_specialty_team_trans_field'),
            'sync_ks_sh_medical_surgery_supply': ('sh_medical_surgery_supply_trans_field', 'ks_sh_medical_surgery_supply_trans_field'),
            'sync_ks_sh_medical_surgery_team': ('sh_medical_surgery_team_trans_field', 'ks_sh_medical_surgery_team_trans_field'),
            'sync_ks_sale_order_line': ('sale_order_line_trans_field', 'ks_sale_order_line_trans_field'),
            'sync_ks_crm_line': ('crm_line_trans_field', 'ks_crm_line_trans_field'),
            'sync_ks_crm_line_product': ('crm_line_product_trans_field', 'ks_crm_line_product_trans_field'),
            'sync_ks_crm_lead': ('crm_lead_trans_field', 'ks_crm_lead_trans_field'),
        }
        if args.task in ks_mappings:
            source, target = ks_mappings[args.task]
            manager.sync_ks_table(source, target)
        else:
            print(f"Unknown KS task: {args.task}")
    else:
        print(f"Unknown task: {args.task}")
