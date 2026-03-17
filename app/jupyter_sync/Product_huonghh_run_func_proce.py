import os
import tempfile
from typing import List

import numpy as np
import pandas as pd
import psycopg2
from loguru import logger


class ProductSyncManager:
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
        if not connection:
            return pd.DataFrame() if ret == 'df' else []
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
        if not connection:
            return False
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
        if not connection:
            return False
        try:
            with connection.cursor() as cursor:
                if table_dich == 'stag_tongquan_khachhang_sm_huonghh1':
                    query_delete = f"DELETE FROM {table_dich} a WHERE EXISTS (SELECT 1 FROM {table} b WHERE a.fk = b.fk)"
                else:
                    query_delete = f"DELETE FROM {table_dich}"

                cursor.execute(query_delete)
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

    def sync_table(self, query_sp, table_dich, conn_source='staging_sci', extra_cols=None):
        dfc = self.query_df(conn_source, query_sp)
        if dfc.empty:
            logger.info(f"No data to sync for {table_dich}")
            return

        if extra_cols:
            for col, val in extra_cols.items():
                dfc[col] = val

        temp_table = f"{table_dich}_1xjs2qcuj"
        self.process_table_1('staging_sci', temp_table, table_dich)

        existing_columns = self.query_df('staging_sci', f"SELECT * FROM {table_dich} LIMIT 0", ret='column')

        # Handle quoted columns for safety (some columns might have spaces or special names)
        quoted_columns = [
            f'"{col}"' if ' ' in col or col.lower() in ('count', 'order', 'group') else col
            for col in existing_columns
        ]
        # In the original code, they quoted ALL for doanhsoban
        if table_dich == 'stag_tongquan_doanhsoban':
            quoted_columns = [f'"{col}"' for col in existing_columns]

        insert_query = f"COPY {temp_table} ({', '.join(quoted_columns)}) FROM STDIN WITH CSV HEADER DELIMITER ','"

        # Use map instead of deprecated applymap if pandas version allows, or just stay safe with applymap
        df = dfc.applymap(self.format_value)
        for col in df.select_dtypes(include=['float']):
            has_decimal = df[col].dropna().apply(lambda x: isinstance(x, float) and not x.is_integer()).any()
            if not has_decimal:
                df[col] = df[col].astype(pd.Int64Dtype())

        temp_dir = os.getenv('TEMP_DIR', '/tmp')
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', dir=temp_dir) as temp_file:
            result = df[existing_columns]
            result.to_csv(temp_file.name, index=False, header=True)
            f_name = temp_file.name

        try:
            connection = self.get_conn('staging_sci')
            with connection.cursor() as cursor:
                with open(f_name, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(insert_query, f)
                connection.commit()
            connection.close()
            self.process_table_2('staging_sci', temp_table, table_dich)
            logger.info(f"COMPLETED! Updated {table_dich}")
        finally:
            if os.path.exists(f_name):
                os.remove(f_name)

    def write_stag_tongquan_doanhsoban(self):
        query = """
            WITH ecl AS (
                SELECT 
                    cl.ticket_id, cl.overseas_vietnamese, cl.contact_name, cl.id, cl.code_customer, 
                    cl.phone, cl.arrival_date, cl.create_date, cl.create_on, cl.booking_date, 
                    cl.name, cl.amount_paid, cl.type_crm_id, cl.type_data_partner, cl.type, 
                    cl.active, cl.brand_id, cl.company_id, cl.campaign_id, cl.stage_id, 
                    cl.department_id, cl.source_id, cl.create_by, cl.create_by_department, 
                    cl.brand_etl, 4
                FROM crm_lead cl 
                WHERE cl.brand_etl = 0
                    AND NOT EXISTS (SELECT 1 FROM crm_lead cl2 WHERE cl2.brand_etl = 1 AND cl2.id = cl.id)
                    AND NOT EXISTS (SELECT 1 FROM crm_sync_brand csb WHERE csb.brand_etl = 1 AND csb.id_old = cl.id)
                    AND brand_id = 1 
                    AND cl.create_date >= '20240101'
                
                UNION ALL 
                
                SELECT 
                    cl.ticket_id, cl.overseas_vietnamese, cl.contact_name, cl.id, cl.code_customer, 
                    cl.phone, cl.arrival_date, cl.create_date, cl.create_on, cl.booking_date, 
                    cl.name, cl.amount_paid, cl.type_crm_id, cl.type_data_partner, cl.type, 
                    cl.active, cl.brand_id, cl.company_id, cl.campaign_id, cl.stage_id, 
                    cl.department_id, cl.source_id, cl.create_by, cl.create_by_department, 
                    cl.brand_etl, 3
                FROM crm_lead cl 
                WHERE cl.brand_etl = 1
                    AND cl.create_date < '20250801'
                    AND cl.create_date >= '20230101'
                
                UNION ALL 
                
                SELECT 
                    cl2.ticket_id, cl3.overseas_vietnamese, cl3.contact_name, cl3.id, cl3.code_customer, 
                    cl3.phone,
                    CASE 
                        WHEN cl2.arrival_date IS NOT NULL THEN cl2.arrival_date 
                        WHEN cl2.arrival_date IS NULL AND cl3.arrival_date IS NOT NULL THEN cl3.arrival_date 
                        ELSE NULL
                    END AS arrival_date,
                    cl2.create_date, cl2.create_on, cl2.booking_date, cl3.name, cl3.amount_paid, 
                    cl3.type_crm_id, cl3.type_data_partner, cl3.type, cl3.active, cl3.brand_id, 
                    cl3.company_id, cl2.campaign_id, cl3.stage_id, cl3.department_id, 
                    cl2.source_id, cl2.create_by, cl2.create_by_department, cl3.brand_etl, 2
                FROM crm_sync_brand csb 
                LEFT JOIN crm_lead cl2 ON cl2.id = csb.id_old AND cl2.brand_etl = 0 
                LEFT JOIN crm_lead cl3 ON cl3.id = csb.crm_id AND csb.brand_etl = cl3.brand_etl 
                WHERE csb.brand_etl = 1 
                
                UNION ALL 
                
                SELECT 
                    cl.ticket_id, cl.overseas_vietnamese, cl.contact_name, cl.id, cl.code_customer, 
                    cl.phone, cl.arrival_date, cl.create_date, cl.create_on, cl.booking_date, 
                    cl.name, cl.amount_paid, cl.type_crm_id, cl.type_data_partner, cl.type, 
                    cl.active, cl.brand_id, cl.company_id, cl.campaign_id, cl.stage_id, 
                    cl.department_id, cl.source_id, cl.create_by, cl.create_by_department, 
                    cl.brand_etl, 1
                FROM crm_lead cl 
                WHERE cl.brand_etl = 1
                    AND cl.create_date >= '20250801'
                    AND NOT EXISTS (SELECT 1 FROM crm_sync_brand csb WHERE csb.brand_etl = 1 AND csb.crm_id = cl.id) 
            ),
            cl_cu AS (
                SELECT * FROM crm_line cl WHERE cl.brand_etl = 0
            ),
            cline_cu AS (
                SELECT * FROM cl_cu clc 
                WHERE NOT EXISTS (SELECT 1 FROM crm_line cl2 WHERE cl2.id = clc.id AND cl2.brand_etl = 1)
            ),
            cline AS (
                SELECT id, crm_id, product_id, stage, create_date, brand_etl, total_received 
                FROM crm_line WHERE brand_etl = 1 AND create_date >= '20240101'
                UNION ALL
                SELECT id, crm_id, product_id, stage, create_date, brand_etl, total_received 
                FROM cline_cu WHERE create_date >= '20240101'
                UNION ALL 
                SELECT id, booking_id, product_id, stage_line_product, create_date, brand_etl, total_received 
                FROM crm_line_product WHERE brand_etl = 1 AND create_date >= '20240101'
                UNION ALL 
                SELECT id, booking_id, product_id, stage_line_product, create_date, brand_etl, total_received 
                FROM crm_line_product WHERE brand_etl = 0 AND create_date >= '20240101'
            ),
            A AS (
                SELECT 
                    ss.old_id AS id_source,
                    DATE(spt.payment_date + '07:00:00'::interval) AS "ngay",
                    ecl2.name AS "booking",
                    ecl2.phone AS dien_thoai,
                    ecl2.type_data_partner AS loai_kh,
                    ecl.id AS "id",
                    spt.ma_phieu AS "phieu",
                    ss."source" AS "nguon",
                    ss.category_source AS "nhom_nguon",
                    sp.product AS "DV/SP",
                    sp.default_code AS "service_code",
                    sp.his_service_type AS "Loai_DV/SP",
                    sp.product_category AS "nhom_dv",
                    erb.name AS "thuong_hieu",
                    erb.id AS brandid,
                    mrc.name AS "chi_nhanh",
                    mrc.id AS chinhanhphanquyen,
                    (spt.payment/1000) AS "thanh_toan",
                    ecl.total_received AS "tong_tien",
                    ecl.stage AS "TT_DV/SP",
                    CASE
                        WHEN spt.ma_phieu LIKE '%PDC%' THEN 'Transfer'
                        ELSE spt.ma_phieu 
                    END AS "loai",
                    spt.id AS pk
                FROM (SELECT * FROM stag_payment_transfer spt WHERE spt.brand_etl IN (0,1)) spt
                LEFT JOIN ecl ecl2 ON spt.crm_id = ecl2.id
                LEFT JOIN cline ecl ON spt.clineid = ecl.id AND spt.product_id = ecl.product_id 
                LEFT JOIN stag_products sp ON sp.product_id = ecl.product_id
                LEFT JOIN m_res_company mrc ON spt.company_id = mrc.id AND mrc.brand_etl = 1
                LEFT JOIN res_brand erb ON mrc.brand_id = erb.id AND erb.brand_etl = 1
                LEFT JOIN stag_sources ss ON ss.old_id = ecl2.source_id 
                WHERE spt.payment <> 0 
            ),
            f AS (
                SELECT 
                    A.*,
                    company_new.id AS "chinhanhphanquyenfinal", 
                    company_new.name AS "chi_nhanh_1",
                    company_new.brand_id AS "thuonghieuphanquyen"
                FROM A
                LEFT JOIN LATERAL (
                    SELECT rctf.id, rctf."name", rctf.brand_id  
                    FROM res_company_trans_field rctf 
                    WHERE DATE(rctf.expiration_date) > A."ngay" AND rctf.id = A.chinhanhphanquyen 
                    ORDER BY rctf.expiration_date LIMIT 1
                ) company_new ON TRUE 
            )
            SELECT * FROM f 
            WHERE ngay >= '20240101'
        """
        extra_cols = {
            'count': np.nan, 'Count': np.nan, 'tt_phieu': np.nan, 'Chi_nhanh (phieu thu)': np.nan,
            'ma_nv_1': np.nan, 'tu_van_1': np.nan, 'chuc_vu_1': np.nan, 'bo_phan_1': np.nan,
            'ma_nv_2': np.nan, 'tu_van_2': np.nan, 'chuc_vu_2': np.nan, 'bo_phan_2': np.nan,
            'bp': np.nan, 'ban_cheo_thuong_hieu': np.nan, 'Ban_cheo/Ban_MR': np.nan
        }
        self.sync_table(query, 'stag_tongquan_doanhsoban', conn_source='staging_sci', extra_cols=extra_cols)

    def write_stag_tongquan_khachhang_sm(self):
        query = """
            WITH ecl AS (
                SELECT * FROM crm_lead WHERE brand_etl = 1
            ),
            cline AS (
                SELECT * FROM crm_line WHERE brand_etl = 1
            ),
            a AS (
                SELECT   
                    ROW_NUMBER() OVER (PARTITION BY ecl.id ORDER BY ecl2.id) AS thu_tu,
                    ecl.type_data_partner AS kh_cu_moi,
                    ecl.overseas_vietnamese AS vk,
                    ecl.contact_name AS Ten_Khach,
                    ecl2.id AS line_id,
                    ecl.id AS id,
                    ecl.code_customer AS Ma_KH,
                    ecl.phone AS sdt,
                    DATE(ecl.arrival_date + '07:00:00'::interval) AS Ngay_den_cua,
                    ecl.type_crm_id AS Loai_ban_ghi, 
                    DATE(ecl.create_on + '07:00:00'::interval) AS Ngay_tao,
                    DATE(ecl.arrival_date + '07:00:00'::interval) AS Ngay_dc,
                    DATE(ecl.booking_date + '07:00:00'::interval) AS Ngay_hl,
                    DATE(spt.payment_date + '00:00:00'::interval) AS Ngay_tt,
                    CASE 
                        WHEN ecl.arrival_date IS NOT NULL THEN ecl.arrival_date::DATE 
                        ELSE ecl.create_on::DATE 
                    END AS Date_Time,
                    ecl."name" AS Ma_Booking,
                    se.employee AS Nhan_Vien,
                    spt.payment AS doanh_so,
                    ecl2.total_received AS Tong_tien,
                    CASE WHEN spt.id IS NOT NULL THEN 'posted' ELSE 'no_money' END AS vao_so,
                    ecs2.name AS Trang_Thai,
                    CASE 
                        WHEN spt.id LIKE '%to%' THEN 'Transfer'
                        WHEN spt.id LIKE '%ti%' THEN 'Recieve'
                        ELSE ecl2.stage
                    END AS Line_tt,
                    CASE
                        WHEN ecl.arrival_date IS NOT NULL AND ecs2.name IN ('Won', 'Đặt cọc', 'Đang xử lý') THEN 'Thành Công'
                        WHEN ecl.arrival_date IS NOT NULL AND ecs2.name NOT IN ('Won', 'Đặt cọc', 'Đang xử lý') THEN 'Đến Cửa'
                        WHEN ecl.arrival_date IS NULL AND ecl.type_crm_id = '2' THEN 'Booking'
                        WHEN ecl.arrival_date IS NULL AND ecl.type_crm_id = '1' THEN 'Lead'
                    END AS Phan_Loai_Khach,
                    ect.name AS Loai_BK,
                    CASE 
                        WHEN spt.payment = 0 OR spt.payment IS NULL THEN erb3."name" 
                        ELSE erb."name" 
                    END AS Thuong_hieu,
                    sp.product_category AS Nhom_DV,
                    sp.product AS Ten_DV,
                    CASE 
                        WHEN spt.payment = 0 OR spt.payment IS NULL THEN mrc3."name" 
                        ELSE mrc."name" 
                    END AS Chi_Nhanh,
                    CASE 
                        WHEN spt.payment = 0 OR spt.payment IS NULL THEN mrc3.id 
                        ELSE mrc.id 
                    END AS chinhanhphanquyen,
                    ecl.create_by_department AS Phong_ban_nguoi_tao,
                    ss.source AS Nguon_moi,
                    ss.newcat_source AS Nhom_nguon_moi,
                    ecl.source_id AS Nguon_bk_id,
                    sp.default_code AS id_dv,
                    spt.ma_phieu,
                    CONCAT(ecl.id, ecl2.id, spt.id) AS fk
                FROM ecl 
                LEFT JOIN res_brand erb3 ON erb3.id = ecl.brand_id AND erb3.brand_etl = 1 
                LEFT JOIN m_res_company mrc3 ON mrc3.id = ecl.company_id AND mrc3.brand_etl = 1
                LEFT JOIN cline ecl2 ON ecl2.crm_id = ecl.id
                LEFT JOIN stag_products sp ON sp.product_id = ecl2.product_id  
                LEFT JOIN (SELECT * FROM stag_payment_transfer spt WHERE spt.brand_etl IN (0,1)) spt 
                    ON spt.clineid = ecl2.id AND spt.crm_id = ecl.id 
                LEFT JOIN (SELECT * FROM account_payment eap WHERE eap.brand_etl IN (0,1)) eap 
                    ON eap."name" = spt.ma_phieu  
                LEFT JOIN m_res_company mrc ON mrc.id = spt.company_id AND mrc.brand_etl = 1
                LEFT JOIN res_brand erb ON erb.id = mrc.brand_id AND erb.brand_etl = 1 
                LEFT JOIN stag_employees se ON se.kn_user_id = ecl.create_by
                LEFT JOIN crm_stage ecs2 ON ecs2.id = ecl.stage_id AND ecs2.brand_etl = 1
                LEFT JOIN crm_type ect ON ect.id = ecl.type_crm_id AND ect.brand_etl = 1
                LEFT JOIN stag_sources ss ON ss.old_id = ecl.source_id
                WHERE DATE(ecl.create_on) >= '20240101'
                    AND ecl.active = true 
                    AND ecl.create_by <> 0
            )
            SELECT * FROM a;
        """
        self.sync_table(query, 'stag_tongquan_khachhang_sm', conn_source='staging_sci', extra_cols={'tien_ap': np.nan})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Sync Product Data')
    parser.add_argument('--task', type=str, required=True, help='Task to run')
    args = parser.parse_args()

    conn_uris = {
        'staging_sci': os.getenv('STAGING_SCI_URI'),
    }

    manager = ProductSyncManager(conn_uris=conn_uris)

    if args.task == 'write_stag_tongquan_doanhsoban':
        manager.write_stag_tongquan_doanhsoban()
    elif args.task == 'write_stag_tongquan_khachhang_sm':
        manager.write_stag_tongquan_khachhang_sm()
    else:
        logger.error(f"Unknown task: {args.task}")
