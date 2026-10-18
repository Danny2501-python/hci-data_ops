# -*- coding: utf-8 -*-
"""
REPORT CRM LEAD CUSTOMER OUT SOLD

Chuyen tu quentk_3_report_crm_lead_customer_out_sold_New_sys.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/report_crm_lead_customer_out_sold_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python report_crm_lead_customer_out_sold.py --task report_crm_lead_customer_out_sold
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

DATE_SYS = dt.date(2025,8,1)            # Ngày chặn Cập nhật dữ liệu toàn hệ thống
DATE_SYS_STR = DATE_SYS.strftime("%Y-%m-%d")


conn = 'dev'   # Dev trên Jupyterlab -> kết nối với Sandbox, đồng bộ lên Airflow -> Tự động connect "new_product"
_CACHE = {}          # nhớ kết quả trong một tiến trình task, tránh truy vấn lặp

DATETRUNC = (dt.date.today() - relativedelta(days=15)).replace(day=1)
DATETRUNC = DATE_SYS if DATETRUNC < DATE_SYS else DATETRUNC
DATETRUNC_STR = DATETRUNC.strftime("%Y-%m-%d")
LIMIT_START_DATE = DATETRUNC_STR


# Khởi tạo các brand dữ liệu xử lý
DICT_BRAND_ETL = {1: 'kn', 3: 'pr', 4: 'hh', 8: 'thv', 7: 'sci', 9: 'sunwoo'}   # them brand: 9=sunwoo
DICT_BR_ETL = {v: k for k, v in DICT_BRAND_ETL.items() if v}
LS_BRAND_ETL = list(DICT_BRAND_ETL.keys())
LS_BR = list(v for k, v in DICT_BRAND_ETL.items() if k > 0)
idb = -1 
idb_kn = 1
br = ''
br_sci = 7


TABLE_TEMP = 'temp_crm_update_os'
QR_LIMIT_CRM = f'''\
DROP TABLE IF EXISTS {TABLE_TEMP}; 
CREATE TABLE {TABLE_TEMP} AS (
SELECT brand_etl, id FROM crm_lead cl
WHERE "type" = 'opportunity' AND brand_etl <> 0
    AND cl.name NOT ILIKE '%-book%' AND cl.active
    AND write_date >= '{LIMIT_START_DATE}'
    AND cl.company_id IS NOT NULL
);
CREATE INDEX ON {TABLE_TEMP} (id, brand_etl);
ANALYZE {TABLE_TEMP} ;
'''

query_os = \
f'''
WITH dccw AS                  
(     --- Ly do out/Cancel Booking
SELECT value_case_when, MIN("result") "result" 
FROM dm_convert_case_when
WHERE column_name IN ('reason_out_sold', 'reason_cancel_booking', 'reason_not_booking_date')
GROUP BY value_case_when
       --- DONE dữ liệu tạm: Lý do Out/Cancel Booking
), emp AS
(     --- View nhân viên: tính 1 lần, dùng lại cho 4 lần join (sec, sew, sewl, se)
SELECT user_id, brand_etl, employee_code, INITCAP(employee) employee, job, department, company_id, company, brand_id,
    job_id, team_id, group_job_id, department_id, team, group_job
FROM view_stag_employees_user
), fconsult AS (     ---  Tư vấn đón tiếp đầu tiên tại Cơ sở
SELECT DISTINCT ON (sccr.brand_etl, sccr.crm_id, sccr.consultants) 
	sccr.brand_etl as fconsult_brand_etl, sccr.crm_id crm_id_1, sccr.consultants AS consultants_bk, 
	consulting_role AS consulting_role_bk, ordinal_consult AS ordinal_consult_bk, rn_consult_crm AS rn_consult_crm_bk, 
	priority_consult_crm AS priority_consult_crm_bk, min_priority_consult_crm AS min_priority_consult_crm_bk 
FROM stag_crm_consult_rel sccr
INNER JOIN {TABLE_TEMP} tmp ON (sccr.crm_id, sccr.brand_etl) = (tmp.id, tmp.brand_etl)
WHERE rn_fconsult_crm = 1 AND sccr.brand_etl <> 0    /*  Tu van uu tien tai Co so: Le tan, CVTV, BS, khac */	
ORDER BY sccr.brand_etl, sccr.crm_id, sccr.consultants, sccr.ordinal_consult
      --- DONE dữ liệu tạm
)
, sdate AS  (     --- Ngày thực hiện dịch vụ đầu tiên/Booking
SELECT DISTINCT ON (smarw.brand_etl, smarw.booking_id)
    smarw.brand_etl, smarw.booking_id, service_date_start AS services_date
FROM sh_medical_appointment_register_walkin smarw 
INNER JOIN {TABLE_TEMP} tmp ON (smarw.booking_id, smarw.brand_etl) = (tmp.id, tmp.brand_etl)
WHERE service_date_start IS NOT NULL
ORDER BY smarw.brand_etl, smarw.booking_id, smarw.service_date_start
)
, crm_bk AS (     --- THÔNG TIN BOOKING CÓ WRITE_DATE TRONG THỜI GIAN GIỚI HẠN CẬP NHẬT BÁO CÁO
SELECT cl.brand_etl 
	, cl.id, cl.type_crm_id , cl.name booking, cl.code_customer, cl.contact_name, cl.type_data_partner, cl.booking_date 
	, cl.arrival_date AS erp_arrival_date 
	, cl.customer_come, cl.stage_id, cl.date_last_stage_update, cl.partner_id, cl.source_id 
	, cl.amount_total, cl.amount_paid, cl.note, cl.special_note, cl.effect, cl.reason_out_sold, cl.reason_cancel_booking 
	, CASE WHEN cl.reason_out_sold IS NOT NULL then cl.reason_out_sold 
		WHEN cl.reason_cancel_booking IS NOT NULL then reason_cancel_booking 
		else reason_not_booking_date 
		end reason_close_booking
	, CASE WHEN arrival_date is null then 'no_come'
			WHEN date(cl.booking_date) = date(arrival_date) then 'arrived_on_schedule'
			WHEN date(cl.booking_date) > date(arrival_date) then 'arrived_before_schedule'
			WHEN date(cl.booking_date) < date(arrival_date) then 'arrived_after_schedule'
			end stage_arrival
	, cl.create_on, cl.create_by, cl.create_by_department, cl.company_id, cl.write_date, cl.write_uid 
	, coalesce(cline.write_uid, clp.write_uid) AS write_cline_uid
	, CASE WHEN cline.id is null then clp.clineid else cline.id END clineid_fix
	, CASE WHEN cline.id is null then clp.stage_line_product else cline.stage END stage_product
	, CASE WHEN cline.id is null then clp.product_id else cline.product_id END product_id
	, cline.quantity, cline.uom_price, cline.number_used , cline.total, cline.total_received, cline.reason_line_cancel, cline.note note_line_product
	, ccr.*, ccr.consultants_bk consult_first
	, sccr.consultants, sccr.consulting_role, sccr.ordinal_consult, sccr.rn_consult_crm, sccr.priority_consult_crm, sccr.min_priority_consult_crm 
FROM crm_lead cl
INNER JOIN {TABLE_TEMP} tmp ON (cl.id, cl.brand_etl) = (tmp.id, tmp.brand_etl)
LEFT JOIN crm_line cline ON cline.crm_id = cl.id AND cline.brand_etl = cl.brand_etl 
LEFT JOIN (SELECT DISTINCT ON (p.brand_etl, p.booking_id) 
				p.brand_etl, p.booking_id, 
				p.id AS clineid, 
				p.product_id AS product_id,
				p.write_uid AS write_uid,
				p.stage_line_product AS stage_line_product
			FROM crm_line_product p
			INNER JOIN {TABLE_TEMP} tmp ON (p.booking_id, p.brand_etl) = (tmp.id, tmp.brand_etl)
			WHERE p.stage_line_product IS NOT NULL AND p.brand_etl <> 0
			ORDER BY p.brand_etl, p.booking_id, p.stage_line_product DESC, p.id
			) clp ON (cl.id, cl.brand_etl) = (clp.booking_id, clp.brand_etl)
LEFT JOIN fconsult ccr ON (cl.id, cl.brand_etl) = (ccr.crm_id_1, ccr.fconsult_brand_etl)        --- Tư vấn đón tiếp theo thứ hạng ưu tiên cao nhất / Booking
LEFT JOIN 
    --- Tư vấn viên (Với TH Paris, lấy ở bảng Thông tin tư vấn riêng, không lấy TV 1,2)
	(SELECT s.*
	FROM stag_crm_consult_rel s
    INNER JOIN {TABLE_TEMP} tmp ON (s.crm_id, s.brand_etl) = (tmp.id, tmp.brand_etl)
	LEFT JOIN fconsult f ON (s.crm_id, s.consultants, s.brand_etl) = (f.crm_id_1, f.consultants_bk, f.fconsult_brand_etl) 
	WHERE f.crm_id_1 is null AND s.brand_etl <> 0 AND NOT (s.brand_etl = 3 AND s.ordinal_consult like 'consult%')  
	) sccr ON (cline.id, cl.id, cl.brand_etl) = (sccr.clineid, sccr.crm_id, sccr.brand_etl) 
)
     --- DỮ LIỆU ĐẦU RA
SELECT cb.* 
	, COALESCE(os.fix_arrival_date, cb.erp_arrival_date + INTERVAL '7 hours') AS arrival_date
    , COALESCE(os.fix_arrival_date, COALESCE(cb.erp_arrival_date, cb.booking_date) + INTERVAL '7 hours') AS "report_date" 
	, cs.name stage_crm
	, dccw.result reason_close_booking_name
	, os.*
	, dccw2.result outsold_reason_recent_name
	, CASE WHEN coalesce(cb.amount_paid, 0) <= 0 then CASE WHEN cb.customer_come = 'yes' AND cb.stage_id <> 4 then 'outsold' else null END
		WHEN os.services_date_recent IS NOT NULL AND (COALESCE(cb.erp_arrival_date, os.fix_arrival_date):: date) - (os.services_date_recent:: date) <= 365*2 then 'success_return'
		WHEN os.outsold_date_recent IS NOT NULL then 'outsold_return'
		WHEN so.services_date IS NOT NULL then 'success'
		else 'deposit' END type_customer_arrived
	, sp.product_ctg_id, sp.his_service_type , sp.surgery_type, sp.product_category , sp.product, sp.is_no_kpi
	, CASE WHEN cb.number_used > 0 then so.services_date else null END services_date
	, ss.category_id category_source_id, ss.category_source, ss."source", ss.category_channel, ss.channel, ss.channel_code
	, se.employee_code, INITCAP(se.employee) employee, se.job employee_job, se.department, se.company_id employee_company_id, se.company employee_company, se.brand_id employee_brand_id 
   	, se.job_id, se.team_id, se.group_job_id, se.department_id, se.team, se.group_job
	, sec.employee_code creator_employee_code
	, INITCAP(sec.employee) creator_employee
	, CASE WHEN cb.create_by_department is null then sec.department else cb.create_by_department END creator_department
	, sew.employee_code writer_employee_code
	, INITCAP(sew.employee) writer_employee
	, sew.department writer_department
	, sew.job writer_job
	, sew.company writer_company
	, sewl.employee_code writer_cline_employee_code
	, INITCAP(sewl.employee) writer_cline_employee
	, sewl.department writer_cline_department
	, rc.name company
	, rc.brand_id
	, ccsr.company company_id_share
	, rcs.name company_share
	, rcs.brand_id brand_id_share 
FROM crm_bk cb 
LEFT JOIN stag_sources ss ON cb.source_id = ss.old_id 
LEFT JOIN crm_stage cs ON cb.stage_id = cs.id AND cb.brand_etl = cs.brand_etl 
LEFT JOIN m_res_company rc ON cb.company_id = rc.id AND cb.brand_etl = rc.brand_etl 
LEFT JOIN company_crm_share_ref ccsr ON cb.id = ccsr.crm AND ccsr.brand_etl = cb.brand_etl
LEFT JOIN m_res_company rcs ON ccsr.company = rcs.id AND ccsr.brand_etl = rcs.brand_etl 
LEFT JOIN emp sec ON cb.create_by = sec.user_id AND cb.brand_etl = sec.brand_etl 
LEFT JOIN emp sew ON cb.write_uid = sew.user_id AND cb.brand_etl = sew.brand_etl 
LEFT JOIN emp sewl ON cb.write_cline_uid = sewl.user_id AND cb.brand_etl = sewl.brand_etl 
LEFT JOIN view_stag_products sp ON cb.product_id = sp.product_id AND cb.brand_etl = sp.brand_etl 
LEFT JOIN emp se ON cb.consult_first = se.user_id AND cb.brand_etl = se.brand_etl 
LEFT JOIN sdate so ON (cb.brand_etl, cb.id) = (so.brand_etl, so.booking_id)
LEFT JOIN 
	(SELECT l.brand_etl os_brand_etl, l.crm_id, l.crm_id_old, l.partner_id os_partner_id, l.arrival_date fix_arrival_date, 
		l.services_date_recent, l.date_diff, l.outsold_date_recent, l.outsold_reason_recent
	FROM stag_crm_booking_walkin_loyalty l 
	INNER JOIN {TABLE_TEMP} tmp ON (l.crm_id, l.brand_etl) = (tmp.id, tmp.brand_etl)
	WHERE "type" = 'opportunity' AND l.brand_etl <> 0)
	os ON (cb.brand_etl, cb.id, cb.partner_id) = (os.os_brand_etl, os.crm_id, os.os_partner_id)
LEFT JOIN dccw ON cb.reason_close_booking = dccw.value_case_when 
LEFT JOIN dccw dccw2 ON os.outsold_reason_recent = dccw2.value_case_when 
'''

#dfqr = query_df(conn, query_os)
dict_get_df = {'cpn': 'SELECT * FROM stag_crm_booking_arrival_company WHERE brand_etl <> 0', 
               'brands': 'SELECT * FROM res_brand WHERE brand_etl = 7', 
               'df': query_os
              }
#print(query_os)


#df[df.clineid.eq(2513417)][['crm_id', 'product', 'employee', 'consultants', 'ordinal_consult', 'ordinal_consult_bk', 'is_branch_consult', 'employee_brand_id', 'employee_company_id', 'employee_company', 'company_id', 'company_id_share', 'company', 'brand_id']]


def get_data_crm_consult():
    query_df(conn, QR_LIMIT_CRM, ret='exe')                      # Tạo Bảng tạm giới hạn các Booking cần Update
    dfcrm = get_df_cache('df', dict_get_df, conn=conn)              #   query_df(conn, query_os)   dfqr.copy()    
    brands = get_df_cache('brands', dict_get_df, conn=conn)
    cpn = get_df_cache('cpn', dict_get_df, conn=conn)

    cols_rename = {'crm_id': 'crm_id_line', 'id': 'crm_id', 'clineid': 'clineid_consult', 'clineid_fix': 'clineid', 'employee_job': 'job'}
    df = dfcrm.rename(columns=cols_rename)
    df['stage'] = df.stage_crm
    cl_date = ['booking_date', 'write_date', 'create_on']
    df[cl_date] = df[cl_date] + timedelta(hours=7)     # Xử lý Thời gian UTC + 7h các field trong bảng crm_lead gốc 
    
    # Bảng chi nhánh đến cửa các Booking share   (Code mới: sử dụng Bảng đến cửa đã xử lý sẵn)
    cpn_arrival = cpn[['brand_etl', 'crm_id', 'arrival_company', 'arrival_company_id', 'arrival_brand_id']].copy()
    cpn_arrival.rename(columns={c: c.replace('arrival', 'report') for c in cpn_arrival.columns[-4:]}, inplace=True)
    df = df.merge(cpn_arrival, how='left', on=['brand_etl', 'crm_id'])
    
    for c in df.columns:    # Chi nhánh báo cáo = chi nhánh đến cửa, mặc định = chi nhánh hẹn lịch
        if c[:6] == 'report' and c[7:] in df.columns:
            df[c] = df[c].fillna(df[c[7:]])
            
    df = df.merge(brands[['id', 'name']].rename(columns={'id': 'report_brand_id', 'name': 'report_brand'}), how='left', on='report_brand_id')
    df['report_brand'] = np.where(df.report_brand.eq('SCI'), 'SCI GROUP', df.report_brand.str.upper()) 
    df['chinhanhphanquyen'] = df.report_company_id 
    df['thuonghieuphanquyen'] = np.where(df.report_company_id.eq(12), 1, df.report_brand_id)    # company_id = 12: Bệnh viện 218 Nguyễn Trãi (Đông Á cũ)

    for c in [e for e in df.columns if e[-3:] == '_bk']: 
        df[c[:-3]] = np.where(df.consultants.fillna(0).eq(0) | df[c[:-3]].isnull(), df[c], df[c[:-3]])
    df['ordinal_consult'] = np.where(df.ordinal_consult_bk.isnull(), df.ordinal_consult, df.ordinal_consult_bk)
    df['customer_come'] = np.where(df.arrival_date.notnull(), 'yes', df['customer_come'].fillna('no'))
    df['channel_code'] = np.select([df.is_no_kpi.eq(True), df.product_category.eq('SAN PHAM')], \
                                   ['KH100', 'KH099'], df.channel_code)
    df['channel'] = np.select([df.is_no_kpi | df.product_id.isnull(), df.channel_code.eq('KH099')], \
                              ['K - Thu khác', 'K - Sản phẩm'], df.channel)
    df['category_channel'] = np.where(df.channel_code.str.contains('KH', na=False), 'Nguồn khác', df.category_channel)
    
    for c in ['note', 'special_note', 'reason_line_cancel', 'note_line_product']:
        df[c] = df[c].fillna('')
    ls_col_int = [c for c in df.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|(id|create_by)$|consultants|consult_first|priority|phanquyen|diff|_id_')]
    fix_type(df, ls_col_int, 'Int64', np.nan)
    df['id_etl'] = join_cols(df, ['crm_id', 'clineid', 'consultants', 'consult_first', 'company_id', 'company_id_share'], sep='-', na='0')
    df.drop_duplicates('id_etl', inplace=True)
    return df
    

#df = get_data_crm_consult()


#cur_dev = connect_database('dev_staging_sci').cursor()

def report_crm_consult():
    # lấy dữ liệu
    dfc = get_data_crm_consult()         #    df.copy()     get_data_crm_consult() 
    TABLE = 'report_crm_lead_customer_out_sold'

    WHERE_UD = f'''brand_etl <> 0 AND (crm_id, brand_etl) IN (SELECT id, brand_etl FROM {TABLE_TEMP}) '''
    replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    query_df(conn, f'DROP TABLE IF EXISTS {TABLE_TEMP};', ret='exe')
    print('HOÀN THÀNH! BÁO CÁO TỶ LỆ TƯ VẤN BÁN HÀNG. \nDữ liệu Booking cập nhật gần nhất từ ngày ', LIMIT_START_DATE)

    return TABLE

#report_crm_consult()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'report_crm_lead_customer_out_sold': report_crm_consult,
}


def main():
    parser = argparse.ArgumentParser(description='ETL report_crm_lead_customer_out_sold')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
