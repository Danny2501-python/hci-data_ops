# -*- coding: utf-8 -*-
"""
UPDATE STATS DATA

Chuyen tu quentk_5_update_stats_data.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/update_stats_data_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python update_stats_data.py --task update_ggs_target_company_kdtc
"""

import argparse
import ast
import datetime as dt
import gc
import inspect
import io
import json
import os
import re
import sys
import tempfile
import time
import unicodedata
import warnings
from contextlib import closing
from datetime import datetime, timedelta
from itertools import chain
from uuid import uuid4

import numpy as np
import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta
from loguru import logger
from psycopg2 import sql as pgsql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from hci_common import (                                        # noqa: E402
    clean_temp_table, clear_df, connect_database, copy_df_to_table,
    check_substr, err_msg, fix_type, get_df_cache, get_engine,
    get_fmatch, get_table_columns, insert_data_to_db, join_cols,
    ok_msg, query_df, remove_accent, replace_table_data, warn_msg,
)

pd.set_option('display.max_columns', None)
pd.options.display.float_format = '{:,}'.format
warnings.filterwarnings('ignore')


# khai báo DAG


conn = 'dev'   # Dev trên Jupyterlab -> kết nối với Sandbox, đồng bộ lên Airflow -> Tự động connect "new_product"
_CACHE = {}          # nhớ kết quả trong một tiến trình task, tránh truy vấn lặp

# Khởi tạo các brand dữ liệu xử lý
DICT_BRAND_ETL = {0: '', 1: 'kn', 3: 'pr', 4: 'hh', 8: 'thv', 9: 'sunwoo', 7: 'sci'}   # them brand: 8=thv, 9=sunwoo
LS_BRAND_ETL = list(DICT_BRAND_ETL.keys())
IDB = -1 
IDB_KN = 1
BR = ''
BR_SCI = 7

DATE_SYS = dt.date(2025,8,1)
DATE_SYS_STR = DATE_SYS.strftime("%Y-%m-%d")
DATE_LB = dt.date(2025,12,1)

DATETRUNC = (dt.date.today() - relativedelta(days=10)).replace(day=1)         # Giới hạn Ngày chạy update Dữ liệu từ (T - 1) thàng
DATETRUNC = DATE_LB if DATETRUNC < DATE_LB else DATETRUNC
DATETRUNC_STR = DATETRUNC.strftime("%Y-%m-%d")
LIMIT_START_DATE = DATETRUNC_STR
LIMIT_START_DATE


# IMPORT BẢNG CHỈ TIÊU CƠ CHẾ TỰ CHỦ VÀ THƯỞNG KINH DOANH (DÙNG 1 LẦN)
# UPDATE GIÁ TRỊ DOANH SỐ ĐÃ CHỐT (XÁC NHẬN) TỪ CÁC BÊN, LOCK GIÁ TRỊ (DA CHẠY THỦ CÔNG TỪ FILE EXCEL)

file_path = "/opt/notebooks/tempExcel_kpi/Data Import/"
filename = 'ggs_target_company_kdtc'
table = 'ggs_target_company_kdtc'
lscol_ud = ['actual_sales_service', 'actual_avg_sales_md', 'actual_sales_service_md', 'is_locked']

def import_ggs_target_company_kdtc(file_path, filename, table, lscol_ud, type='update'):
    df = pd.read_excel(f'''{file_path}{filename}.xlsx''', sheet_name=table)
    for c in df.columns: 
        if ('0000' + c)[-4:] == 'date': 
            df[c] = pd.to_datetime(df[c]).dt.date
    if 'target' in table:
        df[['active','is_locked']] = df[['active','is_locked']].astype('boolean')

    if type=='update': 
        # Tạo Bảng tạm lưu dữ liệu được import từ Excel vào
        table_temp = 'temp_update_'+table
        #conn = "postgresql://views_staging_sci:Assjgsklsfs%402024@10.10.10.226:5432/staging_sci"
        #sql_create_tb_text = pd.io.sql.get_schema(df, table_temp, conn)                                           # Tự động viết lệnh tạo Bảng theo DataFrame
        #sql_create_tb_text = re.sub(r',\n.*CONSTRAINT.*"\)', '', sql_create_tb_text).replace('REAL', 'NUMERIC')   # Bỏ đoạn tạo Key trong mẫu lệnh thực thi trên 
        
        query_df(conn, f'''DROP TABLE IF EXISTS {table_temp}; \nCREATE TABLE {table_temp} (LIKE {table})''', ret='exe')               # Thực hiện lệnh tạo mới Bảng tạm đã set
        insert_data_to_db(df, table_temp)
        
        join_col_ud = ', \n'.join(f'''{c} = ud.{c}''' for c in lscol_ud)
        qr_ud = f'''UPDATE {table} t SET \n{join_col_ud} FROM {table_temp} ud 
                    WHERE t.id_target_company = ud.id_target_company; 
                    '''
        #query_df(conn, qr_ud, ret='exe')
        #query_df(conn, f"DROP TABLE IF EXISTS {table_temp}", ret='exe')
    elif type=='replace': 
        query_df(conn, f"DELETE FROM {table}", ret='exe')
        insert_data_to_db(df, table)

    return 'Done'

#import_ggs_target_company_kdtc(file_path, filename, table, lscol_ud, type='update')


qr_pm = f'''\
WITH pm AS (
SELECT k.id, k.clineid, k.code_customer, k.booking, k.contact_name,  
    COALESCE(spa.payment_date, k.payment_date) AS payment_date, 
	COALESCE(spa.payment, k.payment) AS payment, 
	COALESCE(spa.category_source_id, k.category_source_id) AS category_source_id, 
	COALESCE(spa.category_source, k.category_source) AS category_source, 
	COALESCE(spa.source_id, k.source_id) AS source_id, 
	COALESCE(spa.source, k.source) AS source, k.his_service_type, 
    COALESCE(spa.company_id, k.company_id) AS company_id, 
    COALESCE(spa.company, k.company) AS company, 
    sp.is_no_kpi
	, TO_CHAR(COALESCE(spa.payment_date, k.payment_date), 'mm/yyyy') AS mth
    , COALESCE(CASE WHEN k.product_category = 'SAN PHAM' THEN 'cosmetic' ELSE k.his_service_type END, 'ChiPhi') AS service_type
    , CASE WHEN eco.main_bl_completed_date IS NOT NULL AND eco.cline_first_services_date IS NOT NULL THEN eco.is_cross_sale END AS is_cross_sale
    , eco.main_bl_completed_date, eco.cline_first_services_date
    , GREATEST(eco.main_bl_completed_date, eco.cline_first_services_date)::date AS eco_done_date
FROM report_kpi_bonus_commission k 
LEFT JOIN view_stag_products sp ON k.product_id = sp.product_id and k.brand_etl = sp.brand_etl 
LEFT JOIN stag_payment_adjusted spa ON (k.id, k.clineid) = (spa.id, spa.clineid)
LEFT JOIN stag_crm_line_ecosystem eco ON k.clineid = eco.cline_id AND k.brand_etl = eco.brand_etl 
WHERE ---   --- payment_date >= '20260101' AND payment_date < '20260501'
	k.source_id NOT IN (80,81,82,83,84,87,88,89,90,124,215,216,217,219,278,279,280,287,288,289,292,535,569,570)
	AND k.category_source_id NOT IN (68,69,71)
	AND NOT (k.payment_date >= '20240613' AND k.source_id IN (75,167,218,224,300))
	AND NOT COALESCE(sp.is_no_kpi, FALSE) 
	AND (k.payment_date >= '{LIMIT_START_DATE}'
		OR 
		(k.payment_date >= '2026-06-01' AND GREATEST(eco.main_bl_completed_date, eco.cline_first_services_date) >= '{LIMIT_START_DATE}')
		)
)
SELECT *  ---mth, company_id, company, sum(payment) as actual_sales
FROM pm 
--GROUP BY mth, company_id, company
'''

# dfpm = query_df(conn, qr_pm)


qr_crm = f'''\
WITH gift_loy AS (
--- Bảng phụ check Booking có sử dụng Quà tặng thẻ thành viên KH loyalty?
SELECT DISTINCT ON (crm_id, brand_etl) 
	lkh.crm_id, lkh.brand_etl, lkd.name loyalty_discount, lkd.type_gift
FROM loyalty_kangnam_history lkh 
LEFT JOIN loyalty_kangnam_discount lkd on lkh.loyalty_discount = lkd.id and lkh.brand_etl = lkd.brand_etl
WHERE lkh.stage = 'used' AND lkh.brand_etl = 1
)
SELECT o.brand_etl, o.crm_id, o.type_crm_id, o.booking, o.code_customer, o.contact_name, 
    o.booking_date, o.customer_come, o.arrival_date, 
	o.amount_paid, o.stage_id, o.type_customer_arrived, o.reason_close_booking, 
	cgc.booking_guarantee_id, 
    o.category_source_id, o.source_id, o.source, 
	loy.date_diff, loy.date_diff_service, 
	g.type_gift, 
    o.product_category, 
	sp.is_no_kpi, 
	o.creator_department, 
	o.report_date, o.report_company_id AS company_id, o.report_company AS company  
FROM report_crm_lead_customer_out_sold o 
LEFT JOIN view_stag_products sp ON o.product_id = sp.product_id AND o.brand_etl = sp.brand_etl 
LEFT JOIN crm_guarantee_cancel cgc ON (o.crm_id, o.brand_etl) = (cgc.booking_id, cgc.brand_etl) 
LEFT JOIN gift_loy g ON (o.crm_id, o.brand_etl) = (g.crm_id, g.brand_etl)
LEFT JOIN (
	SELECT DISTINCT ON (crm_id, brand_etl) crm_id, brand_etl, date_diff, date_diff_service
	FROM stag_crm_booking_walkin_loyalty scbwl 
	WHERE brand_etl <> 0 AND "type" = 'opportunity'
	) loy ON (o.crm_id, o.brand_etl) = (loy.crm_id, loy.brand_etl)
WHERE o.brand_etl <> 0
	AND o.report_date >= '{LIMIT_START_DATE}' 
	AND o.report_date < NOW()::date
---ORDER BY crm_id, brand_etl, COALESCE(total_received, 0) DESC, clineid )
'''

#dfcrm = query_df(conn, qr_crm)
#dfcrm.shape


def get_data_stats(): 
    starttime = datetime.now()
    # lẤY DỮ LIỆU DOANH SỐ BÁN
    df_pm = query_df(conn, qr_pm)
    for d in [c for c in df_pm.columns if 'date' in c]: 
        df_pm[d] = pd.to_datetime(df_pm[d])
        
    df_pm['company_id'] = np.where(df_pm.payment_date.dt.date.ge(dt.date(2026,8,1)) & df_pm.company_id.eq(12), 4, df_pm.company_id) 
    df_pm['id_target_company'] = df_pm.mth.str[-4:] + df_pm.mth.str[:2] + df_pm.company_id.astype(str)
    
    dfpm = df_pm[df_pm.payment_date.dt.date.ge(DATETRUNC)].copy()

    # Group by các Chỉ số doanh số đạt trong tháng
    colgr = ['id_target_company', 'company']
    dfpms = dfpm[dfpm.service_type.ne('cosmetic')].groupby('id_target_company').agg(company=('company', 'first'), 
                                                                      actual_sales_service=('payment', 'sum'))
    is_pm_source_md = (dfpm.category_source_id.isin([105, 119]) | dfpm.source.str.contains('MD -',na=False)) \
                    & ~dfpm.source.str.contains('cơ sở|loyalty|quay lại', case=False, na=False)
    dfpmmd = dfpm[dfpm.service_type.ne('cosmetic') & is_pm_source_md].groupby('id_target_company')['payment'].sum()
    dfpmall = dfpm.groupby(colgr, as_index=False)['payment'].sum()

    # Doanh số bán chéo Hệ sinh thái
    dfeco = df_pm[df_pm.is_cross_sale & df_pm.main_bl_completed_date.notnull() & df_pm.cline_first_services_date.notnull()].copy()
    is_mainbl_done = dfeco.payment_date.dt.to_period('M').dt.to_timestamp().ge(dfeco.main_bl_completed_date.dt.to_period('M').dt.to_timestamp())
    dfeco = df_pm[df_pm.is_cross_sale & df_pm.main_bl_completed_date.notnull()].copy()
    dfeco['eco_date'] = np.where(dfeco.eco_done_date.gt(dfeco.payment_date), dfeco.eco_done_date, dfeco.payment_date)
    dfeco['id_target_company'] = dfeco.eco_date.dt.strftime('%Y%m') + dfeco.company_id.astype(str)

    dfeco = dfeco.groupby('id_target_company')['payment'].sum()


    # LẤY DỮ LIỆU BOOKING ĐẾN CỬA TƯ VẤN
    #dfcrm = query_df(conn, qr_crm)
    dfc = query_df(conn, qr_crm)
    dfc['report_date'] = pd.to_datetime(dfc.report_date)
    dfc['company_id'] = np.where(dfc.report_date.dt.date.ge(dt.date(2026,8,1)) & dfc.company_id.eq(12), 4, dfc.company_id) 
    dfc['report_date'] = pd.to_datetime(dfc.report_date)
    dfc['id_target_company'] = dfc.report_date.dt.strftime('%Y%m') + dfc.company_id.astype(str)
    
    dfc['is_source_md'] = (dfc.category_source_id.isin([105, 119]) | dfc.source.str.contains('MD -',na=False)) \
                        & ~dfc.source.str.contains('cơ sở|loyalty|quay lại', case=False, na=False) 
    dfc['is_source_return_ml'] = dfc.source_id.fillna(0).isin([91,94,95,96,123,127,209,283,290,291,531,565]) \
                            | dfc.source.str.contains('quay lại', case=False, na=False)
    dfc['is_kpi'] = ~(dfc.is_no_kpi.fillna(False) | dfc.source.str.contains(r'cbnv|đối ngoại|thuê phòn', regex=True, case=False, na=False))
    dfc['label_type_schedule'] = np.select([dfc.type_crm_id.eq(3) | (dfc.reason_close_booking.fillna('').eq('tao_booking_bh') & dfc.booking_guarantee_id.notnull()) 
                                            , ~dfc.is_kpi | \
                                                (dfc.amount_paid.fillna(0).eq(0) & (dfc.stage_id.fillna(0).eq(4) 
                                                                                    | (dfc.is_source_return_ml & dfc.type_gift.notnull()) ) ) 
                                            , dfc.creator_department.str.contains('sale', case=False, na=False)
                                            , dfc.creator_department.str.contains(r'lễ tân|kinh doanh dịch vụ', regex=True, case=False, na=False)
                                           ], 
                                           ['4.guarantee'
                                            , '5.no_kpi'
                                            , '1.sale_online'
                                            , '2.recept'
                                           ],
                                           '3.others')
    # GROUP BY ĐẾM SỐ KHÁCH ĐẾN CỬA CHỐT THÀNH CÔNG THEO CÔNG TY/THÁNG
    drt = 180    # Số ngày giới hạn tính KH quay lại
    # Các Booking đến cửa tính tư vấn thành công trong tháng
    dfc_tc = dfc[dfc.arrival_date.notnull() 
                & dfc.type_customer_arrived.isin(['deposit', 'success_return', 'success', 'outsold_return']) 
                & ~dfc.label_type_schedule.str[0].isin(['4','5']) 
                & dfc.product_category.fillna('').ne('SAN PHAM')].copy()
    # Các Booking đến cửa của KH cũ quay lại tính tư vấn thành công (Tệp con của tổng Booking đến cửa)
    dfc_tc_rt = dfc_tc[dfc_tc.date_diff_service.replace(0, np.nan).fillna(drt+1).le(drt)].copy()
    # Các Booking đến cửa MD tính tư vấn thành công (Tệp con của tổng Booking đến cửa)
    dfc_tc_md = dfc_tc[dfc_tc.is_source_md].copy()
    
    dfc_tc = dfc_tc.groupby('id_target_company')['code_customer'].nunique()
    dfc_tc_rt = dfc_tc_rt.groupby('id_target_company')['code_customer'].nunique()
    dfc_tc_md = dfc_tc_md.groupby('id_target_company')['code_customer'].nunique()


    # TỔNG HỢP CÁC CHỈ SỐ VỀ 1 BẢNG
    df = dfpms.copy()
    dict_col_add = {'actual_cus_come_succ':        dfc_tc, 
                    'actual_cus_come_succ_return': dfc_tc_rt, 
                    'actual_cus_come_succ_md':     dfc_tc_md, 
                    'actual_sales_service_md':     dfpmmd, 
                    'actual_sales_cross':          dfeco
                   }
    for col, dfname in dict_col_add.items(): 
        df[col] = dfname
    
    df['days_limit_return'] = drt
    df['actual_avg_sales_md'] = df.actual_sales_service_md/df.actual_cus_come_succ_md
    df['actual_cus_return_rate'] = df.actual_cus_come_succ_return/df.actual_cus_come_succ
    df['actual_values_updated_date'] = datetime.now()
    df.reset_index(inplace=True)

    endtime = datetime.now()
    print(f'''Đã Tổng hợp Dataframe các chỉ số Kinh doanh tự chủ từ ngày {LIMIT_START_DATE}''', endtime - starttime)
    
    if df.id_target_company.is_unique: 
        df['id_target_company'] = df.id_target_company.astype(int)
        return df
    else: 
        print('⚠ !!! KIỂM TRA LẠI ID TRÙNG: id_target_company')

#df = get_data_stats()


# # XUẤT FILE EXCEL TEST KẾT QUẢ => CHUYỂN MARKDOWN
# name = 'KNSG_dfc_dc_20260519'      # Chọn tên file lưu
# out_path = '/opt/notebooks/tempExcel_kpi/' + name + '.xlsx'
# #export_excel(df, out_path, qr=None, saveqr=True)
# dfc_tc.to_excel(out_path, sheet_name = 'data', index=False)


def update_ggs_target_company_kdtc():
    # Tạo Bảng tạm trong Database để Update dữ liệu về Bảng chính
    df = get_data_stats()
    TABLE = 'ggs_target_company_kdtc'
    gtck_col = query_df(conn, f'''SELECT * FROM {TABLE} LIMIT 0''', ret='cols')
    exists_col = [c for c in df.columns if c in gtck_col and 'company' not in c] 
    
    TABLE_TEMP = 'temp_update_'+TABLE
    sql_create_tb_text = pd.io.sql.get_schema(df, TABLE_TEMP, conn)                     # Tự động viết lệnh tạo Bảng theo DataFrame
    sql_create_tb_text = re.sub(r',\n.*CONSTRAINT.*"\)', '', sql_create_tb_text).replace('REAL', 'NUMERIC')   # Bỏ đoạn tạo Key trong mẫu lệnh thực thi trên 
    query_df(conn, f'''DROP TABLE IF EXISTS {TABLE_TEMP}; \n{sql_create_tb_text}''', ret='exe')   # Thực hiện lệnh tạo mới Bảng tạm đã set
    insert_data_to_db(df, TABLE_TEMP)

    # Update Giá trị từ Bảng tạm vào Bảng chính (Không Update các Bản ghi đã đánh dấu locked)
    join_ud = ',\n\t'.join(f'''{c} = ud.{c}''' for c in exists_col)
    sql_update_text = f'''\
    UPDATE {TABLE} t \nSET {join_ud} 
    FROM {TABLE_TEMP} ud 
    WHERE t.id_target_company = ud.id_target_company 
        AND NOT COALESCE(t.is_locked, FALSE) \
    '''
    query_df(conn, sql_update_text, ret='exe')

    print(f'''HOÀN THÀNH! UPDATE CÁC CHỈ SỐ KINH DOANH TỰ CHỦ: {TABLE} (TỪ {LIMIT_START_DATE})''')

#update_ggs_target_company_kdtc()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'update_ggs_target_company_kdtc': update_ggs_target_company_kdtc,
}


def main():
    parser = argparse.ArgumentParser(description='ETL update_stats_data')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
