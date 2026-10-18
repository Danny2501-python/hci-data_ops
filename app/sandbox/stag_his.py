# -*- coding: utf-8 -*-
"""
STAG HIS

Chuyen tu quentk_1_stag_his_New_sys.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/stag_his_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python stag_his.py --task task_1_stag_his_team_service
"""

import argparse
import ast
import datetime as dt
import gc
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

DATETRUNC = (dt.date.today() - relativedelta(days=7)).replace(day=1)
DATETRUNC = DATE_SYS if DATETRUNC < DATE_SYS else DATETRUNC
DATETRUNC_STR = DATETRUNC.strftime("%Y-%m-%d")
LIMIT_START_DATE = DATETRUNC_STR

DATETRUNC_1M = (dt.date.today() - relativedelta(months=1)).replace(day=1)
DATETRUNC_1M = DATE_SYS if DATETRUNC_1M < DATE_SYS else DATETRUNC_1M
DATETRUNC_STR_1M = DATETRUNC_1M.strftime("%Y-%m-01")
LIMIT_START_DATE_1M = DATETRUNC_STR_1M

DATETRUNC_3M = (dt.date.today() - relativedelta(months=3)).replace(day=1)
DATETRUNC_3M = DATE_SYS if DATETRUNC_3M < DATE_SYS else DATETRUNC_3M
DATETRUNC_STR_3M = DATETRUNC_3M.strftime("%Y-%m-01")
LIMIT_START_DATE_3M = DATETRUNC_STR_3M


# Khởi tạo các brand dữ liệu xử lý
DICT_BRAND_ETL = {1: 'kn', 3: 'pr', 4: 'hh', 8: 'thv', 7: 'sci'}
DICT_BR_ETL = {v: k for k, v in DICT_BRAND_ETL.items() if v}
LS_BRAND_ETL = list(DICT_BRAND_ETL.keys())
LS_BR = list(v for k, v in DICT_BRAND_ETL.items() if k > 0)
idb = -1 
idb_kn = 1
br = ''
br_sci = 7


# DANH MỤC DỮ LIỆU SỬ DỤNG CHUNG

# --- 2. Lấy danh mục các Lý Hủy/Out booking nhóm theo value_case_when       rs_close_bk AS (
qr_reason = '''
SELECT value_case_when reason_close_booking, MIN("result") reason_close_booking_name 
FROM dm_convert_case_when dccw 
WHERE dccw.column_name IN ('reason_out_sold', 'reason_cancel_booking', 'reason_not_booking_date')
GROUP BY value_case_when
''' 

_CACHE = {}          # nhớ kết quả trong một tiến trình task, tránh truy vấn lặp
dict_get_df = {'stag_products': 'SELECT * FROM view_stag_products', 
              'se': 'SELECT * FROM stag_employees', 
              'users': 'SELECT * FROM view_stag_employees_user',
              'sources': 'SELECT old_id::int AS source_id, category_id, code AS source_code, newcat_source AS category_source, source FROM stag_sources',
              'reasons': qr_reason}

def get_roles():
    if 'roles' not in _CACHE:
        roles = query_df(conn, 'SELECT * FROM import_team_role_group')
        _CACHE['roles'] = roles.rename(columns={'id': 'roleid', 'name': 'role'})
    return _CACHE['roles']

def get_se():
    if 'se' not in _CACHE:
        se = query_df(conn, 'SELECT * FROM stag_employees')
        se[['nhanvienphanquyen','chinhanhphanquyen','thuonghieuphanquyen','company_employee']] = se[['id','company_id','brand_id','company']]
        se['employee'] = se.employee.str.title()
        _CACHE['se'] = se
    return _CACHE['se']


ls_col_str = ['id', 'team_service_id', 'order_product_id', 'form_id', 'lag_form_id', 'aggroleid']

last_partner_id = 786601         # Giá trị partner_id cuối cùng trên hệ thống ERP trước tách, sau giá trị này id trùng giữa các TH
last_patient = 164363            # Giá trị id bệnh nhân cuối cùng trên hệ thống ERP trước tách, sau giá trị này id trùng giữa các TH
last_crm_id = 2174164            # Giá trị crm_id cuối cùng trên hệ thống ERP trước tách, sau giá trị này crm_id trùng giữa các TH
last_walkin = 646777
# Hàm tạo lại id duy nhất xuyên suốt lịch sử khách hàng từ Brand_etl = 0 trở đi, id lấy kiểu string
def newid_br(df, field='crm_id', last_value=0): 
    df['char_' + field] = np.where(df[field].fillna(0).astype(int).gt(last_value) & df.brand_etl.lt(4), 
                                   df[[field, 'brand_etl']].apply(lambda r: '-'.join(r.dropna().astype(int).values.astype(str)), axis=1), 
                                   df[field].fillna(0).astype(int).astype(str))
    return df


# '''
# case when smst.role in (46,48,49,50) then 0 				/* BS bi bao hanh */
# 		when smst.role = 1 then 1 				/* BS mo chinh */
# 		when smst.role = 2 then 2						/* BS mo phu */
# 		when smst.role = 36 then 3						/* Bac si kiem soat chuyen mon */
# 		when smst.role = 3 then 4						/* Bac si hoc viec */
# 		when smst.role = 5 and tspt.service_id in (2072, 4606, 4607, 8579, 8580, 8581, 8582) then 5			/* Dieu duong vong trong - Hut tuyen | product_cat_id IN  (1354, 444, 394)*/
# 		when smst.role in (9,12,14,16) then 6			/* Bac si da lieu/laser/spa/nha */
# 		when smst.role in (28, 30, 32) then 7			/* Bac si chiu trach nhiem Spa, Laser, Nha */
# 		when smst.role in (10, 13, 15, 17) then 8		/* Ky thuat vien laser/spa/thuc hien doc lap/Tro thu Nha */
# '''


# CẬP NHẬT BẢNG GIÁ TRỊ DỊCH VỤ ĐƠN HÀNG (SALE ORDER), KHÔNG CÓ SẢN PHẨM
query_so = \
'''
SELECT 
	MIN(sol.id) id, 
    sol.order_id, sol.product_id, MIN(sol.name) product, MIN(sol.state) state, 
    SUM(sol.price_subtotal) so_revenue, 
    SUM(sol.price_subtotal*COALESCE(cw.rto_value_actual, 1)) as revenue, MAX(cw.wallet_id) AS wallet_id, 
    MAX(sol.product_uom_qty) product_uom_qty, SUM(sol.uom_price) uom_price, 
    MIN(sol.crm_line_id) crm_line_id, 
    MAX(CASE WHEN cline.type_guarantee_2 = 'totality' THEN '11'
		WHEN cline.type_guarantee_2 = 'not_totality' THEN '10' 
		ELSE cline.type_guarantee END) type_guarantee, 
    MIN(cline.bh_ngoai_goi) bh_ngoai_goi, 
    MIN(sol.company_id) company_id, MIN(sol.pricelist_type) pricelist_type, 
    MIN(case when cline.price_list_id IS NULL THEN so.pricelist_id ELSE cline.price_list_id end) pricelist_id, MIN(pp.name) pricelist, 
    CONCAT(sol.order_id, '-', sol.product_id, '-', sol.brand_etl) order_product_id, 
    --MAX(cline.reason_guarantee_id) reason_guarantee_id, 
    MAX(cline.unit_price) price_unit, 
    MAX(cline.total) AS total, 
    MAX(cline.total_before_discount) AS total_before_discount, 
    COUNT(cline.product_id) AS count_dup_product_so, 
    MAX(so.date_order) date_order, MAX(so.write_date) AS write_date, 
    sol.brand_etl 
FROM sale_order_line sol 
LEFT JOIN sale_order so on sol.order_id = so.id and so.brand_etl = sol.brand_etl 
LEFT JOIN crm_line cline on sol.crm_line_id = cline.id and cline.brand_etl = sol.brand_etl 
LEFT JOIN product_pricelist pp on coalesce(cline.price_list_id, so.pricelist_id) = pp.id and pp.brand_etl = so.brand_etl 
LEFT JOIN stag_card_wallet_crm_line_value cw on (sol.crm_line_id, sol.brand_etl) = (cw.clineid, cw.brand_etl) and cw.is_line_use 
WHERE sol.brand_etl <> 0
	and sol.state IN ('sale', 'draft', 'done') and sol.pricelist_type in ('service', 'guarantee') 
GROUP BY sol.order_id, sol.product_id, sol.brand_etl 
'''

#cur_dev = connect_database('dev_staging_sci').cursor()
def write_stag_sale_order_services():
    dfc = query_df(conn, query_so)
    TABLE = 'stag_sale_order_services'

    replace_table_data(dfc, TABLE, where='brand_etl <> 0', conn=conn)
    return TABLE

#dfc = query_df(conn, query_so)
#write_stag_sale_order_services()


TEMP_PATIENT_LIMIT =  'temp_limit_patient_walkin'
QR_CREATE_TEMP_WALKIN = F'''
DROP TABLE IF EXISTS {TEMP_PATIENT_LIMIT} ;
CREATE TABLE {TEMP_PATIENT_LIMIT} AS (
SELECT w.partner_id, w.brand_etl 
FROM sh_medical_appointment_register_walkin w 
	WHERE w.brand_etl <> 0 AND w.service_date_start >= '{LIMIT_START_DATE_3M}'
UNION
SELECT e.partner_id, e.brand_etl 
FROM sh_medical_evaluation e
	WHERE e.brand_etl = 0 AND e.evaluation_start_date >= '{LIMIT_START_DATE_3M}' 
) ;
CREATE INDEX ON {TEMP_PATIENT_LIMIT} (partner_id, brand_etl);
ANALYZE {TEMP_PATIENT_LIMIT} ;
'''


# LỊCH SỬ THỨ TỰ LẦN THỰC HIỆN DỊCH VỤ + LIỆU TRÌNH CỦA KHÁCH HÀNG

query_treat = f'''
WITH forms AS 
( --- Tổng hợp phiếu thực hiện dịch vụ
SELECT 'specialty' form_type,  							--- Phiếu Chuyên khoa
	'ck-' || (sssr.specialty_id::text) form_id,
	sssr.service_id,
	sms.walkin walkinid, sms.name form_name, sms.services_date, sms.info, 
	sssr.brand_etl 
FROM sh_specialty_service_rel sssr 
LEFT JOIN sh_medical_specialty sms ON sssr.specialty_id = sms.id AND sms.brand_etl = sssr.brand_etl 
LEFT JOIN sh_medical_patient smp on sms.patient = smp.id AND smp.brand_etl = sms.brand_etl 
INNER JOIN {TEMP_PATIENT_LIMIT} tmp ON smp.partner_id = tmp.partner_id AND smp.brand_etl = tmp.brand_etl
WHERE sms.state = 'Done' 
UNION ALL 
SELECT 'surgery' form_type,  							--- Phiếu Phẫu thuật thủ thuật
	'pt-' || (sssrg.surgery_id::text) form_id,
	sssrg.service_id, smsg.walkin walkinid, smsg.name form_name, smsg.surgery_date services_date, smsg.info, 
	sssrg.brand_etl 
FROM sh_surgery_service_rel sssrg 
LEFT JOIN sh_medical_surgery smsg ON sssrg.surgery_id = smsg.id AND smsg.brand_etl = sssrg.brand_etl
LEFT JOIN sh_medical_patient smp on smsg.patient = smp.id AND smp.brand_etl = smsg.brand_etl 
INNER JOIN {TEMP_PATIENT_LIMIT} tmp ON smp.partner_id = tmp.partner_id AND smp.brand_etl = tmp.brand_etl
WHERE smsg.state = 'Done' 
UNION ALL 
SELECT 'evaluation' form_type,  						--- Phiếu Tái khám (liệu trình)
	'tk-' || (sesr.evaluation_id::text) AS form_id,
	sesr.service_id,
	sme.walkin walkinid, sme.name form_name, sme.evaluation_start_date services_date, 
	COALESCE(COALESCE(sme.notes_complaint, info_diagnosis), sme.chief_complaint) AS info, 
	sesr.brand_etl 
FROM sh_evaluation_service_rel sesr 
LEFT JOIN sh_medical_evaluation sme ON sesr.evaluation_id = sme.id and sme.brand_etl = sesr.brand_etl 
LEFT JOIN sh_medical_health_center_service sp ON sesr.service_id = sp.id and sp.brand_etl = sesr.brand_etl 
INNER JOIN {TEMP_PATIENT_LIMIT} tmp ON sme.partner_id = tmp.partner_id AND sme.brand_etl = tmp.brand_etl
WHERE sme.state = 'Completed' AND sp.his_service_type NOT IN ('Surgery', 'ChiPhi') 
)
, forms_union AS (   --- Giới hạn toàn bộ phiếu thực hiện dịch vụ của Danh sách khách hàng limit_partner
--- 1. Lịch sử thực hiện của KH có brand_etl = 0
SELECT swet.form_type, swet.form_id, swet.service_id, swet.walkinid, swet.form_name, swet.services_date, swet.info_diagnosis AS info, 
	swet.brand_etl, 
	swet.crm_id, swet.partner_id, 
	swet.walkin, swet.services_date AS service_date_start, swet.state_walkin, swet.his_service_type, swet.product_id, swet.info_diagnosis, 
	swet.group_fix_product_id, swet.sale_order_id, 
	CASE WHEN swet.drk_grt_times IS NOT NULL THEN TRUE END AS is_grt 
FROM stag_walkin_evaluation_treatment swet 
INNER JOIN (SELECT DISTINCT partner_id FROM {TEMP_PATIENT_LIMIT}) tmp ON swet.partner_id = tmp.partner_id
WHERE swet.brand_etl = 0
UNION ALL
--- 2. Phiếu thực hiện của KH từ 01/08/2025
SELECT f.*,
	w.booking_id crm_id, 
	w.partner_id,
	w.name walkin, w.service_date_start, 
	w.state state_walkin,
	smhcs.his_service_type,
	smhcs.product_id,
	CASE WHEN f.form_type like 'eval%' THEN f.info else w.info_diagnosis END info_diagnosis,
    COALESCE(sp.group_id::int, sp.product_id) AS group_fix_product_id, 
	w.sale_order_id, 
	CASE WHEN w.type_crm_id = 3 THEN TRUE END AS is_grt 
FROM forms f
LEFT JOIN sh_medical_appointment_register_walkin w ON f.walkinid = w.id and w.brand_etl = f.brand_etl
INNER JOIN {TEMP_PATIENT_LIMIT} tmp ON w.partner_id = tmp.partner_id AND w.brand_etl = tmp.brand_etl
LEFT JOIN sh_medical_health_center_service smhcs ON f.service_id = smhcs.id and smhcs.brand_etl = f.brand_etl
LEFT JOIN view_stag_products sp ON smhcs.product_id = sp.product_id and sp.brand_etl = smhcs.brand_etl 
WHERE NOT (COALESCE(w.state, 'Completed') = 'Cancelled' AND form_type <> 'evaluation')  
) 
--- Lấy thông tin: Ngày đầu làm dịch vụ, thời gian thực hiện liệu trình...
SELECT fu.*, 
	cline.total_product, 
    cline.total_received, 
    cline.initial_product_id, 
	ssos.revenue, 
	ssos.order_product_id,
	CASE WHEN fu.is_grt AND fu.form_type <> 'evaluation' THEN
		DENSE_RANK() OVER (PARTITION BY fu.partner_id, fu.group_fix_product_id, 
		fu.is_grt and fu.form_type <> 'evaluation', CASE WHEN fu.partner_id < 786601 THEN -1 ELSE fu.brand_etl END
      ORDER BY fu.services_date::date, fu.walkinid) 
	END AS drk_grt_times    --- Thứ tự lần bảo hành 
FROM forms_union fu
LEFT JOIN stag_sale_order_services ssos ON (fu.sale_order_id, fu.product_id, fu.brand_etl) = (ssos.order_id, ssos.product_id, ssos.brand_etl)
LEFT JOIN ( 
--- Tổng giá trị phải thu / Dịch vụ chốt trên Booking, trường hợp Booking có nhiều dòng dịch vụ cùng mã, total_product = Tổng total
	select cline.brand_etl, cline.crm_id, cline.product_id, 
        SUM(COALESCE(scli.total, cline.total)) AS total_product, MAX(scli.total_received) AS total_received, 
        MAX(cline.initial_product_id) AS initial_product_id
	from crm_line cline 
	left join (select brand_etl, crm_id from forms_union group by brand_etl, crm_id) limit_crm on (cline.crm_id, cline.brand_etl) = (limit_crm.crm_id, limit_crm.brand_etl) 
	left join stag_crm_line_info scli on cline.id = scli.clineid and cline.brand_etl = scli.brand_etl 
    where cline.brand_etl <> 0 and limit_crm.crm_id is not null 
		and (cline.number_used > 0 or cline.stage in ('processing', 'waiting', 'done') )
	group by cline.crm_id, cline.product_id, cline.brand_etl 
	) cline ON (fu.crm_id, fu.product_id, fu.brand_etl) = (cline.crm_id, cline.product_id, cline.brand_etl)  
'''
#df = query_df(conn, query_treat)


# Lịch sử thanh toán => Kiểm tra Số tiền đã thu đến ngày thực hiện dịch vụ
query_pm = \
f'''
 --- Dữ liệu lịch sử thanh toán dịch vụ của Khách hàng
SELECT s.brand_etl, max(s.write_date) as write_date, 
    clineid, crm_id, product_id, max(payment_date) as payment_date, MAX(total_received_recal) AS total_received_acc 
FROM stag_payment_transfer s
LEFT JOIN crm_lead cl ON s.crm_id = cl.id AND s.brand_etl = cl.brand_etl
INNER JOIN (SELECT DISTINCT partner_id FROM {TEMP_PATIENT_LIMIT}) tmp ON cl.partner_id = tmp.partner_id
WHERE product_type = 'service'
GROUP BY s.brand_etl, s.crm_id, s.clineid, s.product_id
'''

def get_dfpm_last():
    #query_df(conn, QR_CREATE_TEMP_WALKIN, ret='exe')
    dfpm = query_df(conn, query_pm) 
    cols_gr = ['crm_id', 'product_id', 'isin_mth']
    frames = []
    for idb in list(e for e in dfpm.brand_etl.unique() if e != 0): 
        dfcut = dfpm[dfpm.brand_etl.isin([0, idb])].copy()          # Tách data từng thương hiệu
        dfcut['brand_etl'] = idb
        dfcut['isin_mth'] = dfcut.payment_date >= DATETRUNC_1M
        dfcut.sort_values(cols_gr, ascending=[True,True,False], inplace=True)
        dfcut['drk_crm_pd_desc'] = dfcut.groupby(['crm_id', 'product_id'])['payment_date'].transform('rank', method='dense', ascending=False)
        dfcut['rn_drk_crm_cline'] = dfcut.sort_values(['drk_crm_pd_desc', 'clineid'], ascending=[True,False])\
                                    .groupby(['crm_id', 'product_id', 'clineid'])['crm_id'].cumcount() + 1
        frames.append(dfcut)
    dfpm_last = pd.concat(frames, ignore_index=True) if frames else dfpm.iloc[0:0]
        
        #dfpm_last = pd.concat([dfpm_last, dfcut])
    return dfpm_last

#dfpm_last = get_dfpm_last()
#pm[pm[['brand_etl', 'crm_id', 'product_id']].duplicated()


def get_data_walkin_evaluation_treatment():
    query_df(conn, QR_CREATE_TEMP_WALKIN, ret='exe')
    
    # Lịch sử lần thanh toán gần nhất
    dfpm_last = get_dfpm_last()
    colgr = ['brand_etl', 'crm_id', 'product_id', 'total_received_acc']
    pm = dfpm_last[dfpm_last.rn_drk_crm_cline.eq(1)]
    pm = pm.groupby(colgr[:-1], as_index=False).agg({'total_received_acc': 'sum', **{c: 'last' for c in pm if c not in colgr}})

    stag_products = get_df_cache('stag_products', dict_get_df, conn)

    # Dữ liệu phiếu khám thực hiện 
    dfc = query_df(conn, query_treat)   # df.copy()     query_df(conn, query_treat)
    dfc['form_id'] = np.where(dfc.brand_etl.ne(0), dfc.form_id + '-' + dfc.brand_etl.astype(str), dfc.form_id)
    dfc = dfc.merge(stag_products[['brand_etl','product_id','group_id']], how='left', 
                    left_on=['brand_etl','initial_product_id'], right_on=['brand_etl','product_id'], suffixes=('', '_spi'))
    dfc['group_initial_product_id'] = dfc.group_id.fillna(dfc.initial_product_id).fillna(dfc.group_fix_product_id).fillna(dfc.product_id).astype(int)
    dfc['partner_gr_product'] = dfc[['partner_id', 'group_initial_product_id']].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1)
    dfc['concat_walkin_pd'] = dfc[['brand_etl', 'form_type', 'walkinid', 'product_id']].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1)
    dfc['rn_form_walkin'] = dfc.groupby(['concat_walkin_pd'])['form_id'].rank(method='dense')
    df_treat = pd.DataFrame()
    for idb in list(e for e in dfc.brand_etl.unique() if e != 0): 
        dfcut = dfc[dfc.brand_etl.isin([0, idb])].copy()          # Tách data từng thương hiệu
        dfcut = dfcut.merge(dfcut[dfcut.form_type.ne('evaluation')].groupby(['partner_id', 'service_id'])['services_date'].min(), 
                            how='left', on=['partner_id', 'service_id'], suffixes=('','_first'))     # Lấy Ngày đầu tiên KH làm dịch vụ
        df1 = dfcut[dfcut.rn_form_walkin.eq(1)].copy()            # Dịch vụ/Phiếu khám hoặc Phiếu tái khám xuất hiện 1 lần
        df2 = dfcut[dfcut.rn_form_walkin.gt(1)].copy()            # Dịch vụ/Phiếu khám có nhiều hơn 1 phiếu CK/PTTT (không đếm thứ hạng là 1 lần thực hiện mới)
    
        # Xử lý mã phiếu thực hiện dịch vụ liền kề 
        df1.sort_values(by=['partner_id', 'group_initial_product_id', 'services_date'], inplace=True)
        df1['brand_etl_fix'] = idb 
        df1['partner_treat'] = df1.partner_gr_product.eq(df1.partner_gr_product.shift(1))         # Check Dòng kế tiếp là cùng dịch vụ/KH làm liệu trình/BH 
        df1['rn_partner_treat'] = df1.groupby('partner_gr_product').cumcount()+1                  # Đếm Dòng liệu trình Dịch vụ/KH
        df1['group_evl'] = df1.form_type.shift(1).ne('evaluation').cumsum()+1                     # Check lần thực hiện gần nhất là phiếu Tái khám?
        df1['group_form'] = df1.walkinid.shift(-1).eq(df1.walkinid).cumsum()+1                    # Nhóm các phiếu Khám 
        df1['addform'] = df1.groupby('group_form')['group_form'].cumcount()                       # Giá trị dịch chuyển nếu tồn tại cùng Dịch vụ/Phiếu khám
        # step_shift: bước lùi check mã phiếu thực hiện Dịch vụ liền kề (ngoại trừ Tái khám và Chỉ định thêm) 
        df1['step_shift'] = np.where(df1.partner_treat, df1.partner_treat.astype(int), np.nan) \
                            + np.where(df1.form_type.shift(1).eq('evaluation'), df1.groupby('group_evl')['group_evl'].cumcount(), 0) 
        df1.reset_index(drop=True,inplace=True)
        #idx_shift = np.where((df1.index - df1.step_shift) >= 0, (df1.index - df1.step_shift).fillna(0).astype(int), 0)   # Lấy mã phiếu tại dòng index lùi theo step_shift
        # clip(lower=0): tránh KeyError khi bước lùi vượt quá đầu bảng
        df1['lag_form_id'] = np.where(df1.partner_treat, df1.loc[(df1.index - df1.step_shift).fillna(0).clip(lower=0).astype(int), 'form_id'], np.nan)
        df1['step_shift_fix'] = np.where(df1.form_id.eq(df1.lag_form_id), df1.step_shift.add(df1.addform), df1.step_shift)
        df1['lag_form_id'] = np.where(df1.form_id.eq(df1.lag_form_id) & df1.step_shift_fix.lt(df1.rn_partner_treat) & (df1.index >= df1.step_shift_fix), 
                                      df1.loc[(df1.index - df1.step_shift_fix).fillna(0).abs().astype(int), 'form_id'], df1.lag_form_id) 
        
        # Đếm thứ tự lần làm liệu trình hoặc bảo hành lại dịch vụ theo Booking hoặc cả vòng đời KH
        df1['drk_customer_product'] = df1.groupby(['partner_id', 'group_fix_product_id'])['services_date'].transform('rank', method='dense')
        df1['drk_booking_product'] = df1.groupby(['crm_id', 'service_id'])['services_date'].transform('rank', method='dense')
        df1['drk_customer_product_walkin'] = df1.groupby(['crm_id', 'service_id'])['service_date_start'].transform('rank', method='dense')
        
        cols_rank = ['step_shift_fix', 'drk_customer_product', 'drk_booking_product', 'drk_customer_product_walkin']
        dfgr = df1.groupby('concat_walkin_pd')[cols_rank].first()
        for c in cols_rank: 
            df2[c] = df2['concat_walkin_pd'].map(dfgr[c])
        dfcc = pd.concat([df1, df2], ignore_index=True)
        dfcc['rn_booking_product'] = dfcc.sort_values(['crm_id', 'product_id', 'drk_booking_product']).groupby(['crm_id', 'product_id'])\
                                            ['crm_id'].cumcount()+1
        dfcc['rn_walkin_product'] = dfcc.sort_values(['walkinid', 'product_id', 'form_type', 'services_date'], ascending=[True,True,False,True])\
                                .groupby(['walkinid', 'product_id'])['walkinid'].cumcount()+1
        df_treat = pd.concat([df_treat, dfcc[dfcc.brand_etl.ne(0)]], ignore_index=True)             # Đổ dữ liệu tập trung về 1 bảng

    df_treat['diff_services_date'] = (df_treat.services_date.dt.date - df_treat.services_date_first.dt.date).apply(lambda x: pd.Timedelta(x).days)
    df_treat['is_treatment_time'] = df_treat.drk_customer_product.gt(1) & ~df_treat.his_service_type.str.contains('Surgery', na=False) 
    fix_type(df_treat, [c for c in df_treat.columns if check_substr(c, r'(_|cline)id$|^(drk|rn_|dr_|diff_)') and c not in ls_col_str], 'Int64', np.nan)
    df_treat['id_etl'] = df_treat[['form_id', 'sale_order_id', 'product_id', 'brand_etl']].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1) 

    # Nối dữ liệu thanh toán
    keys = ['brand_etl', 'crm_id', 'product_id']
    
    for k in keys:
        df_treat[k] = pd.to_numeric(df_treat[k], errors='coerce').astype('Int64')
        pm[k] = pd.to_numeric(pm[k], errors='coerce').astype('Int64')
    dfc_pm = df_treat.merge(pm, how='left', on=keys, suffixes=('', '_pm'))           # Nối dữ liệu dịch sử thanh toán dịch vụ/Booking
    dfc_pm['check_date'] = (dfc_pm.payment_date <= dfc_pm.services_date.dt.date).fillna(False)             # Kiểm tra Ngày thanh toán <= Ngày làm Dịch vụ

    dfc_pm['total_received_acc'] = dfc_pm.total_received_acc.fillna(dfc_pm.total_received).astype(float)
    dfc_pm['paid_percent'] = dfc_pm.total_received_acc/(dfc_pm.total_product.replace(0, np.nan).astype(float))
    return dfc_pm 
#dfc_pm = get_data_walkin_evaluation_treatment()

def write_stag_walkin_evaluation_treatment():
    dfc = get_data_walkin_evaluation_treatment()     #dfc_pm.copy()    #    dfc_pm.copy()          get_data_walkin_evaluation_treatment()
    TABLE = 'stag_walkin_evaluation_treatment'

    WHERE_UD = 'brand_etl <> 0 AND (partner_id, brand_etl) IN (SELECT partner_id, brand_etl FROM temp_limit_patient_walkin)'
    replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    print(f'HOÀN THÀNH! BẢNG THỨ TỰ LIỆU TRÌNH DỊCH VỤ THEO BOOKING, MÃ BỆNH NHÂN')
#write_stag_walkin_evaluation_treatment()


# # 
# Test in kết quả => Chuyển markdown khi không dùng
# dfc_pm[dfc_pm.partner_id.eq(12536)]\
# [['brand_etl', 'form_type', 'form_id', 'services_date', 
#  'partner_id','product_id','total_product','lag_form_id', 'group_evl','step_shift', 'drk_customer_product','partner_treat']]


# CẬP NHẬT BẢNG ID PHIẾU THỰC HIỆN - THÀNH VIÊN THAM GIA - DỊCH VỤ
query_ts = \
f'''
WITH rnk_walkin AS   
( --- Thứ tự phiếu khám thực hiện DV cuối cùng của Book liệu trình ERP cũ
SELECT csb.brand_etl, csb.id_old crm_id_old, csb.crm_id, shts.product_id, MAX(shts.rn_walkin_product) AS last_drk_crm_product 
FROM stag_his_team_service shts 
LEFT JOIN crm_sync_brand csb ON shts.crm_id = csb.id_old 
WHERE shts.brand_etl = 0 AND csb.id_old IS NOT NULL 
GROUP BY csb.brand_etl, csb.id_old, csb.crm_id, shts.product_id
), 
db_role AS
( --- Thành viên tham gia - dịch vụ bảng Phẫu thuật thủ thuật + Chuyên khoa
SELECT 
	CONCAT('surg-', tspt.surgery_team_id, '-', tspt.service_id, '-', tspt.brand_etl) team_service_id
,	CONCAT('pt-', sms.id, '-', sms.brand_etl) id
, 	sms.id form_id,	sms.name ck_pttt, (surgery_date + interval '7 hours') services_date, (surgery_end_date + interval '7 hours') services_end_date
, 	tspt.service_id 
,	sms.state state_ckpttt,	sms.walkin walkinid, smst.team_member 
,	smst.role roleid, itrg.order_priorole
, 	sms.institution, sms.department, sms.operating_room room, sms.booking_id crm_id,	sms.is_new_request
,	sms.brand_etl
FROM sh_surgery_team_services_rel tspt
LEFT JOIN sh_medical_surgery_team smst ON tspt.surgery_team_id = smst.id AND smst.brand_etl = tspt.brand_etl 
INNER JOIN sh_medical_surgery sms ON smst.name = sms.id AND sms.brand_etl = smst.brand_etl 
LEFT JOIN import_team_role_group itrg ON smst.role = itrg.id
WHERE tspt.brand_etl <> 0 ---AND sms.id is not null
	AND tspt.ingestion_timestamp::date = (SELECT MAX(ingestion_timestamp)::date FROM sh_surgery_team_services_rel) 
	AND NOT COALESCE(smst.is_delete, False)
UNION ALL
SELECT 
	CONCAT('spec-', tsck.specialty_team_id, '-', tsck.service_id, '-', tsck.brand_etl) team_service_id
,	CONCAT('ck-', sms2.id, '-', sms2.brand_etl) id
, 	sms2.id form_id, sms2.name ck_pttt,	(services_date + interval '7 hours') services_date,	(services_end_date + interval '7 hours') services_end_date
, 	tsck.service_id 
,	sms2.state state_ckpttt, sms2.walkin walkinid, smst2.team_member 
,	smst2.role roleid, itrg2.order_priorole
, 	sms2.institution, sms2.department, sms2.perform_room room, sms2.booking_id crm_id, sms2.is_new_request
, 	sms2.brand_etl 
FROM sh_specialty_team_services_rel tsck
INNER JOIN sh_medical_specialty_team smst2 ON tsck.specialty_team_id = smst2.id AND smst2.brand_etl = tsck.brand_etl 
INNER JOIN sh_medical_specialty sms2 ON smst2.name = sms2.id AND sms2.brand_etl = smst2.brand_etl 
LEFT JOIN import_team_role_group itrg2 ON smst2.role = itrg2.id
WHERE sms2.brand_etl <> 0 
	AND tsck.ingestion_timestamp::date = (SELECT MAX(ingestion_timestamp)::date FROM sh_specialty_team_services_rel) 
	AND NOT COALESCE(smst2.is_delete, False) 
), db AS 
(
SELECT d.*
,		smhcs.product_id
,		smhcs.service_category
, 		DENSE_RANK() OVER (PARTITION BY d.crm_id, smhcs.product_id ORDER BY services_date) + COALESCE(w.last_drk_crm_product,0) AS rn_walkin_product
, 		smarw.name walkin
, 		smarw.state state_walkin
, 		smarw.partner_id
, 		smarw.sale_order_id
,		smp.employee_id
,		DENSE_RANK() over (PARTITION BY d.crm_id, smhcs.product_id ORDER BY order_priorole) dr_order_role
, 		MIN(d.order_priorole) over (PARTITION BY d.crm_id, smhcs.product_id) min_order_priorole
,		smhc.his_company company_id, smhc.brand brand_id 
,		ROW_NUMBER() OVER (PARTITION BY d.walkinid, smhcs.product_id ORDER BY order_priorole) rn_walkin_product_fix
FROM db_role d
INNER JOIN sh_medical_health_center_service smhcs ON d.service_id = smhcs.id AND smhcs.brand_etl = d.brand_etl
LEFT JOIN sh_medical_appointment_register_walkin smarw ON d.walkinid = smarw.id AND smarw.brand_etl = d.brand_etl
LEFT JOIN sh_medical_health_center smhc ON d.institution = smhc.id AND smhc.brand_etl = d.brand_etl
LEFT JOIN sh_medical_physician smp ON d.team_member = smp.id AND smp.brand_etl = d.brand_etl
LEFT JOIN rnk_walkin w on d.crm_id = w.crm_id AND d.brand_etl = w.brand_etl AND smhcs.product_id = w.product_id 
) SELECT db.*, he.employee_code 
FROM db
LEFT JOIN hr_employee he ON db.employee_id = he.id AND db.brand_etl = he.brand_etl
---WHERE services_date >= '{LIMIT_START_DATE_3M}'
'''

def write_stag_his_team_service():
    dfc = query_df(conn, query_ts)
    TABLE = 'stag_his_team_service'
    
    # CẬP NHẬT GIÁ TRỊ BẢNG 
    replace_table_data(dfc, TABLE, where='brand_etl <> 0', conn=conn)
    print(f'HOÀN THÀNH! BẢNG ID CÁC PHIẾU THỰC HIỆN - THÀNH VIÊN THAM GIA - DỊCH VỤ')

    return TABLE

#write_stag_his_team_service()


# CẬP NHẬT BẢNG ID PHIẾU THỰC HIỆN - THÀNH VIÊN THAM GIA - DỊCH VỤ HẬU PHẪU, TÁI KHÁM

# Giới hạn các Khách hàng phát sinh phiếu Tái khám/Chăn sóc lưu trong thời gian gần: bảng tạm temp_limit_patient
TEMP_PATIENT_AS = 'temp_limit_patient'
QR_CREATE_TEMP_PATIENT = f''' 
DROP TABLE IF EXISTS {TEMP_PATIENT_AS} ;
CREATE TABLE {TEMP_PATIENT_AS} AS 
(
SELECT partner_id, brand_etl FROM sh_medical_evaluation sme 
	WHERE brand_etl <> 0 AND sme.evaluation_start_date >= '{LIMIT_START_DATE_1M}'
UNION 
SELECT smp.partner_id, smp.brand_etl FROM sh_medical_patient_rounding smpr 
	LEFT JOIN sh_medical_patient smp ON smpr.patient = smp.id AND smpr.brand_etl = smp.brand_etl 
	WHERE smpr.brand_etl <> 0 AND smpr.evaluation_start_date >= '{LIMIT_START_DATE_1M}'
UNION 
SELECT smp.partner_id, smp.brand_etl FROM sh_medical_inpatient smi 
	LEFT JOIN sh_medical_patient smp on smi.patient = smp.id AND smi.brand_etl = smp.brand_etl 
	WHERE smi.brand_etl <> 0 AND GREATEST(smi.admission_date, smi.discharge_date) >= '{LIMIT_START_DATE_1M}'
)
'''

# Data lịch sử Ngày thực hiện dịch vụ
query_sdate = f'''
SELECT swsr.brand_etl, swsr.walkin_id walkinid, swsr.service_id, smarw.partner_id, smarw.patient, 
	COALESCE(sms.surgery_date, smarw.service_date_start) AS service_date_start, sms.surgery_date 
FROM sh_walkin_service_rel swsr
LEFT JOIN sh_medical_appointment_register_walkin smarw ON (smarw.brand_etl, smarw.id) = (swsr.brand_etl, swsr.walkin_id) 
LEFT JOIN sh_medical_surgery sms ON (sms.walkin, sms.brand_etl) = (smarw.id, smarw.brand_etl) and sms.state = 'Done'
LEFT JOIN sh_surgery_service_rel sssr ON sms.id = sssr.surgery_id AND sssr.service_id = swsr.service_id AND sms.brand_etl = sssr.brand_etl 
INNER JOIN (SELECT DISTINCT partner_id FROM {TEMP_PATIENT_AS}) tmp ON smarw.partner_id = tmp.partner_id 
'''
def get_data_services_date():
    dfd = query_df(conn, query_sdate)
    dfd = newid_br(dfd, 'partner_id', last_partner_id)
    dfd = newid_br(dfd, 'walkinid', last_partner_id)
    dfd['rn_walkin_service'] = dfd.sort_values(['char_walkinid', 'service_id', 'service_date_start']).groupby(['char_walkinid', 'service_id']).cumcount()+1
    dfd['rn_partner_service'] = dfd.sort_values(['char_partner_id', 'service_id', 'service_date_start']).groupby(['char_partner_id', 'service_id']).cumcount()+1
    dfd['rn_partner_service_br'] = dfd.sort_values('service_date_start', ascending=False).groupby(['brand_etl', 'partner_id', 'service_id']).cumcount()+1
    return dfd
    
# Data phiếu Lưu - Chăm sóc hậu phẫu
query_hp = \
f'''
SELECT COALESCE(smrt.brand_etl, smpr.brand_etl) AS brand_etl, 
'postoperative' AS type_form, 
CASE WHEN smpr.id IS NULL THEN concat('hp-0', smi.id, '-', smi.brand_etl)
    ELSE concat('hp-', smpr.id, '-', smpr.brand_etl) END AS id,     
COALESCE(smpr.id, smi.id) AS form_id, 
smarw.patient_level, smpr.name AS form_name,     
COALESCE(smpr.state, smi.state) AS state_form, 
smi.id AS inpatient_id, smi.name AS inpatient,     
CASE WHEN smpr.id IS NULL AND smi.is_new_request THEN 'new_request'::character varying
    ELSE smpr.evolution END AS evolution, 
COALESCE(smpr.evaluation_start_date, smi.admission_date)+ '07:00:00'::interval AS services_date, 
COALESCE(smpr.evaluation_start_date, smi.admission_date) + '07:00:00'::interval AS services_end_date, 
smi.service_id, 
smi.product_id, 'Chăm sóc hậu phẫu'::text AS service, smi.walkin AS walkinid, 
smarw.name AS walkin, smarw.state AS state_walkin, --COALESCE(csd.surgery_date, smarw.service_date_start) + '07:00:00'::interval AS service_date_start, 
smhcw.name AS ward, smi.room AS room_id, smhco.name AS room, smhcb.info AS bed, 
smi.admission_date + '07:00:00'::interval AS admission_date, smi.discharge_date + '07:00:00'::interval AS discharge_date, 
smi.info notes_complaint, smi.state AS state_inpatient,     
smrt.id AS form_team_id, smp5.employee_id, smrt.role AS roleid, smp2.employee_id AS physician_id, smp.employee_id AS doctor_id, 
smp4.employee_id AS main_doctor_surgery_id, NULL::int AS sub_doctor_surgery_id, 
smhc.his_company AS company_id, 
smarw.booking_id AS crm_id, smarw.partner_id, smarw.sale_order_id, smi.is_new_request,
DENSE_RANK() OVER (PARTITION BY smarw.brand_etl, smarw.id ORDER BY smi.id) AS drk_form_walkin, 
smi.write_date + '07:00:00'::interval write_date
FROM sh_medical_rounding_team smrt
FULL JOIN sh_medical_patient_rounding smpr ON smrt.name = smpr.id and smpr.brand_etl = smpr.brand_etl 
FULL JOIN ( SELECT i.*, smhcs.product_id, smhcs.id AS service_id
     FROM sh_medical_inpatient i
     LEFT JOIN sh_inpatient_service_rel sisr ON i.id = sisr.inpatient_id and i.brand_etl = sisr.brand_etl 
     LEFT JOIN sh_medical_health_center_service smhcs ON sisr.service_id = smhcs.id and sisr.brand_etl = smhcs.brand_etl
     where i.brand_etl <> 0) 
 smi ON (smpr.inpatient_id, smpr.brand_etl) = (smi.id, smi.brand_etl)
LEFT JOIN sh_medical_health_center smhc ON smi.institution = smhc.id and smi.brand_etl = smhc.brand_etl 
LEFT JOIN sh_medical_physician smp3 ON smi.attending_physician = smp3.id and smi.brand_etl = smp3.brand_etl 
LEFT JOIN sh_medical_physician smp4 ON smi.operating_physician = smp4.id and smi.brand_etl = smp4.brand_etl 
LEFT JOIN sh_medical_health_center_ward smhcw ON smi.ward = smhcw.id and smi.brand_etl = smhcw.brand_etl 
LEFT JOIN sh_medical_health_center_ot smhco ON smi.room = smhco.id and smi.brand_etl = smhco.brand_etl 
LEFT JOIN sh_medical_health_center_beds smhcb ON smi.bed = smhcb.id and smi.brand_etl = smhcb.brand_etl 
LEFT JOIN sh_medical_appointment_register_walkin smarw ON smi.walkin = smarw.id and smi.brand_etl = smarw.brand_etl 
LEFT JOIN sh_medical_physician smp ON smpr.doctor = smp.id and smpr.brand_etl = smp.brand_etl 
LEFT JOIN sh_medical_physician smp2 ON smpr.physician = smp2.id and smpr.brand_etl = smp2.brand_etl 
LEFT JOIN sh_medical_physician smp5 ON smrt.team_member = smp5.id and smrt.brand_etl = smp5.brand_etl 
INNER JOIN {TEMP_PATIENT_AS} tmp ON smarw.partner_id = tmp.partner_id AND smarw.brand_etl = tmp.brand_etl
WHERE COALESCE(smrt.brand_etl, smpr.brand_etl) <> 0 
    AND COALESCE(smpr.id, smi.id) IS NOT NULL
'''
#dfhp = query_df(conn, query_hp)
#print(dfhp.shape, dfhp.brand_etl.value_counts())


# Data phiếu Tái khám
query_tk = \
f'''
with vhes as 
( --- Thông tin Bác sĩ, ngày phẫu thuật trên Phiếu khảo sát KH tái khám
SELECT smeshs.brand_etl, 
	seshmdr.surgery_history_id,
    smeshs.sh_evaluation_surgery_id AS evaluation_id, 
    smeshs.doctor_attitude,
    smeshs.satisfaction_level,
    smesh.surgery_date,
    smhcs.product_id,
    smhcs.id AS service_id,
    smpm.employee_id AS main_doctor_id,
    smpms.id AS sub_doctor_id,
    sme.his_company 
   FROM sh_medical_evaluation_surgery_history_survey smeshs
     LEFT JOIN sh_medical_evaluation sme ON smeshs.sh_evaluation_surgery_id = sme.id AND sme.brand_etl = smeshs.brand_etl
     INNER JOIN {TEMP_PATIENT_AS} tmp ON sme.partner_id = tmp.partner_id AND sme.brand_etl = tmp.brand_etl
     LEFT JOIN sh_medical_evaluation_surgery_history smesh ON smeshs.evaluation_surgery_history_id = smesh.id AND smesh.brand_etl = smeshs.brand_etl
     LEFT JOIN sh_medical_health_center_service smhcs ON smesh.service_performances = smhcs.id AND smhcs.brand_etl = smesh.brand_etl
     LEFT JOIN sh_evaluation_surgery_history_main_doctor_rel seshmdr ON smesh.id = seshmdr.surgery_history_id AND seshmdr.brand_etl = smesh.brand_etl
     LEFT JOIN sh_medical_physician smpm ON seshmdr.main_doctor_id = smpm.id AND smpm.brand_etl = seshmdr.brand_etl 
     LEFT JOIN sh_evaluation_surgery_history_sub_doctor_rel seshsd ON smesh.id = seshsd.surgery_history_id AND seshsd.brand_etl = smesh.brand_etl
     LEFT JOIN sh_medical_physician smpms ON seshsd.sub_doctor_id = smpms.id AND smpms.brand_etl = seshsd.brand_etl
  WHERE smeshs.brand_etl <> 0
  )
SELECT sme.brand_etl,  
	'evaluation' AS type_form, 
    CONCAT('tk-', sme.id, '-', sme.brand_etl) AS id, 
    sme.id AS form_id, sme.patient_level, sme.name AS form_name, sme.state AS state_form, 
    sme.evaluation_start_date + '07:00:00'::interval AS services_date, 
    sme.evaluation_end_date + '07:00:00'::interval AS services_end_date, 
    sesr.service_id, 
    smhcs.product_id, smes.name AS service, sme.walkin AS walkinid, smarw.name AS walkin, smarw.state AS state_walkin, 
    ---smarw.service_date_start, vhes.surgery_date, 
    COALESCE(smarw.service_date_start, vhes.surgery_date) + '07:00:00'::interval AS service_date_start, 
    smhcw.name AS ward, sme.room AS room_id, smhco.name AS room, sme.next_appointment_date, sme.check_state, 
    sme.chief_complaint, sme.notes_complaint, sme.customer_reviews, 
    smet.id AS form_team_id, smp.employee_id, smet.role AS roleid, 
    smp2.employee_id AS doctor_id,     
    vhes.main_doctor_id AS main_doctor_surgery_id,     
    vhes.sub_doctor_id AS sub_doctor_surgery_id, 
    NULLIF(vhes.satisfaction_level, '7') AS satisfaction_level_id, 
    NULLIF(vhes.doctor_attitude, '6') AS doctor_attitude_id, 
    smp3.employee_id AS doctor_bh_id, sme.warranty_appointment_date, smhc.his_company AS company_id, 
    smarw.booking_id AS crm_id, smarw.partner_id, smarw.sale_order_id,
    DENSE_RANK() OVER (PARTITION BY smarw.brand_etl, smarw.id ORDER BY sme.id) AS drk_form_walkin, 
    sme.write_date + '07:00:00'::interval write_date
FROM sh_medical_evaluation_team smet 
     FULL JOIN sh_medical_evaluation sme ON sme.id = smet.name and sme.brand_etl = smet.brand_etl 
     INNER JOIN {TEMP_PATIENT_AS} tmp ON sme.partner_id = tmp.partner_id AND sme.brand_etl = tmp.brand_etl
     LEFT JOIN sh_medical_health_center smhc ON sme.institution = smhc.id and sme.brand_etl = smhc.brand_etl 
     LEFT JOIN sh_medical_physician smp ON smet.team_member = smp.id and smet.brand_etl = smp.brand_etl 
     LEFT JOIN sh_medical_physician smp2 ON sme.doctor = smp2.id and sme.brand_etl = smp2.brand_etl 
     LEFT JOIN sh_medical_physician smp3 ON sme.doctor_bh = smp3.id and sme.brand_etl = smp3.brand_etl 
     LEFT JOIN sh_evaluation_service_rel sesr ON sesr.evaluation_id = sme.id and sesr.brand_etl = sme.brand_etl 
     LEFT JOIN sh_medical_health_center_service smhcs ON sesr.service_id = smhcs.id and sesr.brand_etl = smhcs.brand_etl 
     LEFT JOIN sh_evaluation_services_type_rel sestr ON sestr.evaluation_id = sme.id and sestr.brand_etl = sme.brand_etl 
     LEFT JOIN sh_medical_evaluation_services smes ON sestr.evaluation_services_id = smes.id and sestr.brand_etl = smes.brand_etl 
     LEFT JOIN sh_medical_appointment_register_walkin smarw ON sme.walkin = smarw.id and sme.brand_etl = smarw.brand_etl 
     LEFT JOIN sh_medical_health_center_ward smhcw ON sme.ward = smhcw.id and sme.brand_etl = smhcw.brand_etl 
     LEFT JOIN sh_medical_health_center_ot smhco ON sme.room = smhco.id and sme.brand_etl = smhco.brand_etl 
     LEFT JOIN vhes ON sme.id = vhes.evaluation_id AND smhcs.product_id = vhes.product_id and smhcs.brand_etl = vhes.brand_etl 
WHERE sme.brand_etl <> 0
'''
#dftk = query_df(conn, query_tk)
#dftk.drop_duplicates(['id', 'product_id', 'employee_id', 'roleid', 'main_doctor_surgery_id', 'sub_doctor_surgery_id', 'service'], inplace=True)
#print('Data Tái khám: ', dftk.shape, dftk.brand_etl.value_counts())


def get_data_after_service(): 
    starttime = datetime.now()
    query_df(conn, QR_CREATE_TEMP_PATIENT, ret='exe')
    
    dfd = get_data_services_date()
    dfhp = query_df(conn, query_hp) 
    dftk = query_df(conn, query_tk)
    dftk.drop_duplicates(['id', 'product_id', 'employee_id', 'roleid', 'main_doctor_surgery_id', 'sub_doctor_surgery_id', 'service'], inplace=True)

    for d in [dftk, dfhp]:
        d.sort_values(['id', 'form_id', 'services_date', 'roleid'], inplace=True)
        d['rn_form'] = d.groupby('id')['id'].cumcount()+1
        d['rn_form_product'] = d.groupby(['id', 'product_id']).cumcount()+1
        d['rn_form_team_id'] = d.groupby(['id', 'form_team_id']).cumcount()+1
        ls_col_int = [c for c in d.columns if check_substr(c, r'^(drk|rn_|rnk)') and c not in ls_col_str]
        fix_type(d, ls_col_int, int, 0)
        d['aggroleid'] = d.groupby(['id', 'rn_form_team_id'])['roleid'].transform(lambda r: ','.join(r.dropna().astype(int).values.astype(str)))
        d['service_notaccent'] = d.service.apply(remove_accent)

    # Gộp dữ liệu Tái khám và Chăm sóc hậu phẫu vào 1 Dataframe
    df = pd.concat([dftk, dfhp], ignore_index=True)
    df = newid_br(df, 'walkinid', last_walkin)           # Tạo id xuyên suốt hệ thống Cũ - Mới
    df = newid_br(df, 'partner_id', last_partner_id)

    # Truy thông tin Ngày KH đã làm dịch vụ được Tái khám, nằm lưu
    sdate_walkin = df[['char_walkinid', 'service_id']].merge(dfd[dfd.rn_walkin_service.eq(1)], how='left', on=['char_walkinid', 'service_id'])
    sdate_partner = df[['char_partner_id', 'service_id']].merge(dfd[dfd.rn_partner_service_br.eq(1) & dfd.brand_etl.eq(0)], how='left', on=['char_partner_id', 'service_id'])
    df.update(sdate_walkin[['service_date_start']])
    df.update(sdate_partner[['service_date_start']], overwrite=False)

    # Convert kiểu dữ liệu số nguyên, tạo ID bảng
    ls_col_int = [c for c in df.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|id$') and c not in ['id', 'team_service_id', 'order_product_id', 'aggroleid'] and c[:5] != 'char_']
    fix_type(df, ls_col_int, 'Int64', np.nan)
    ls_id = ['id', 'product_id', 'employee_id', 'roleid', 'main_doctor_surgery_id', 'sub_doctor_surgery_id', 'service_notaccent']
    df['id_etl'] = join_cols(df, ls_id, sep='-', na='0')

    endtime = datetime.now()
    print('Xử lý Bảng Dữ liệu Tái khám, Hậu phẫu: ', endtime - starttime)
    return df
#dfas = get_data_after_service()


def write_his_team_after_service():
    dfc = get_data_after_service()    #get_data_after_service()
    TABLE = 'stag_his_team_after_service'

    WHERE_UD = f'brand_etl <> 0 AND (partner_id, brand_etl) IN (SELECT partner_id, brand_etl FROM {TEMP_PATIENT_AS})'
    replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    query_df(conn, 'DROP TABLE IF EXISTS temp_limit_patient', ret='exe')
    print(f'HOÀN THÀNH! CẬP NHẬT BẢNG ID DATA HẬU PHẪU, TÁI KHÁM')
    
    return TABLE

#write_his_team_after_service()


# DÒNG DỊCH VỤ BẢO HÀNH (crm_line)

qr_kn = \
f'''
WITH partner_grt AS 
( -- Giới hạn các id KH có Booking bảo hành
SELECT partner_id FROM  crm_lead WHERE type_crm_id = 3 AND brand_etl = 1
GROUP BY partner_id ) 
SELECT cl.partner_id, cl.id crm_id, cl.type_crm_id, cl.name booking, 
	cline.id, cline.product_id, COALESCE(sp.group_id::int, cline.product_id) as product_group_id, 
    sp.product_ctg_id, sp.product_category, sp.product, cline.stage, 
    cline.unit_price, cline.total_before_discount, cline.discount_percent, cline.discount_cash, cline.other_discount, cline.total_discount, 
    cline.total, cline.total_discount_review, 
    coalesce(cline.type_guarantee_2, cline.type_guarantee)::varchar as type_guarantee, cline.reason_guarantee_id, cline.bh_ngoai_goi, 
    pp."type" pricelist_type, pp.name pricelist, sp.list_price, 
    COALESCE(NULLIF(cline.booking_guarantee_id, 0), cl.lead_id) AS lead_id, clbh.name initial_booking, 
    COALESCE(NULLIF(cline.initial_product_id, 0), CASE WHEN pp.type = 'guarantee' THEN cline.product_id END) AS initial_product_id,
    cscc.id_old clineid_old, csb.id_old, cline.brand_etl, cline.write_date, cl.write_date write_date_booking
FROM crm_line cline 
LEFT JOIN crm_lead cl ON (cline.crm_id, cline.brand_etl) = (cl.id, cl.brand_etl) 
LEFT JOIN crm_lead clbh ON COALESCE(NULLIF(cline.booking_guarantee_id, 0), cl.lead_id) = clbh.id AND clbh.brand_etl = 1
LEFT JOIN partner_grt pg ON cl.partner_id = pg.partner_id
LEFT JOIN view_stag_products sp ON cline.product_id = sp.product_id AND sp.brand_etl = 1  
LEFT JOIN product_pricelist pp ON cline.price_list_id = pp.id AND pp.brand_etl = 1 
LEFT JOIN crm_sync_crm_line cscc ON cscc.crm_line_id = cline.id AND cscc.brand_etl = 1
LEFT JOIN crm_sync_brand csb ON cl.id = csb.crm_id AND csb.brand_etl = 1
WHERE pg.partner_id IS NOT NULL 
	AND ((cline.brand_etl = 0 AND cline.stage <> 'cancel')   --- Lịch sử Dịch vụ PM cũ mà KH có Book BH
		OR cline.brand_etl = 1) 
ORDER BY cl.partner_id, cline.id
'''

def get_dfline_bh():
    frames = []
    for idb in [i for i in LS_BRAND_ETL if i != br_sci]:
        qr = qr_kn.replace('brand_etl = 1', f'''brand_etl = {idb}''')
        dftemp = query_df(conn, qr)
        print(f'Dữ liệu crm_line {DICT_BRAND_ETL[idb]}: ', dftemp.shape)
        frames.append(dftemp)
    return pd.concat(frames, ignore_index=True)

#dfline = get_dfline_bh()


# Thông tin Nhân sự bị bảo hành theo vai trò người dùng khai báo trên ERP
erp_bh_qr = 'SELECT crm_id, product_id, employee_id, employee_code, roleid, services_date, brand_etl \
                        FROM public.stag_his_team_service shts \
                        WHERE brand_etl <> 0 AND shts.roleid IN (46,48,49,50)'

# Thông tin dịch vụ bị bảo hành ban đầu theo Hồ sơ bệnh án CRM FPT cũ
ihc_qr = '''
SELECT booking initial_booking, pcatid initial_product_ctg_id, MAX(ihc.employee_id) employee_id, se.employee_code, MIN(roleid) roleid, MAX(services_date) services_date 
FROM import_hsba_crmfpt ihc
LEFT JOIN (SELECT clbh.name FROM crm_lead clbh LEFT JOIN crm_lead cl ON clbh.id = cl.lead_id WHERE cl.type_crm_id = 3 AND cl.brand_etl <> 0 GROUP BY clbh.name)
    clbh ON ihc.booking = clbh.name
LEFT JOIN stag_employees se ON ihc.employee_id = se.id AND se.id < 6124
WHERE clbh.name IS NOT NULL
GROUP BY booking, pcatid, se.employee_code  '''

# Thông tin dòng dịch vụ đã xử lý ở hệ thống cũ: brand_etl = 0
line_grt_info_qr = '''
SELECT *, 
	ROW_NUMBER() OVER (PARTITION BY crm_id) AS rn_crm, 
	ROW_NUMBER() OVER (PARTITION BY id) AS rn_cline
FROM stag_service_employee_guarantee sseg 
WHERE brand_etl = 0 '''

# Thông tin dịch vụ bị bảo hành ban đầu theo Phiếu thực hiện gần nhất trên ERP
walkin_bh_qr = f'''\
WITH limit_partner AS  
( --- Giới hạn id khách hàng 
SELECT partner_id FROM stag_his_team_service WHERE state_ckpttt = 'Done' AND brand_etl <> 0 GROUP BY partner_id 
), cline_bh as 
( --- Các Dòng dịch vụ bảo hành = Dịch vụ thay thế khác DV thực hiện
select cline.brand_etl, cline.crm_id, cline.product_id, cline.initial_product_id,
	row_number() over (partition by cline.brand_etl, cline.crm_id, cline.product_id order by 
		case when cline.stage = 'Done' then 1 when cline.stage in ('processing', 'waiting') then 2 else 3 end, cline.id desc) 
	as rn_crm_pd
from crm_line cline 
LEFT JOIN crm_lead cl on cline.crm_id = cl.id and cline.brand_etl = cl.brand_etl 
--LEFT JOIN view_stag_products sp ON cline.product_id = sp.product_id AND cline.brand_etl = sp.brand_etl
where cl.type_crm_id = 3 and cline.brand_etl <> 0
	and cline.product_id <> cline.initial_product_id and cline.initial_product_id is not null
), cline_init as 
( --- Fix null initial_product_id (nếu có)
select *, 
	first_value(initial_product_id) over (partition by brand_etl, crm_id, product_id order by rn_crm_pd) 
	as fix_initial_product_id
from cline_bh
)
SELECT t.brand_etl, t.form_type, t.partner_id, t.crm_id, t.walkinid, t.form_id, 
	t.walkin, t.form_name, sp.product, t.drk_customer_product, t.drk_grt_times, 
	t.lag_form_id, s.ck_pttt AS initial_ck_pttt, s.services_date, s.employee_id, s.employee_code, s.roleid, 
	sp.product_ctg_id, t.product_id, COALESCE(sp.group_id::int, t.product_id) product_group_id, 
	sps.product_ctg_id old_product_ctg_id, s.product_id old_product_id, COALESCE(sps.group_id::int, s.product_id) old_product_group_id, 
	ssos.revenue initial_total, 
	ci.fix_initial_product_id, 
    DENSE_RANK() OVER (PARTITION BY t.brand_etl, t.walkinid, t.product_id ORDER BY t.services_date::date, t.form_id) drk_walkin_product, 
	ROW_NUMBER() OVER (PARTITION BY t.crm_id, t.product_id, s.employee_id ORDER BY 
		CASE WHEN COALESCE(sp.group_id::int, t.product_id) = COALESCE(sps.group_id::int, s.product_id) THEN 1 ELSE 2 END, 
		CASE WHEN sp.product_ctg_id = sps.product_ctg_id then 1 ELSE 2 END)
	as rn_prio_grt  --- Đếm dòng ưu tiên Nhấn sự bị bảo hành theo Dịch vụ --> Nhóm dịch vụ 
FROM stag_walkin_evaluation_treatment t 
LEFT JOIN limit_partner lp ON lp.partner_id = t.partner_id 
LEFT JOIN stag_his_team_service s ON t.form_id = s.id AND t.product_id = s.product_id 
LEFT JOIN stag_sale_order_services ssos ON (s.sale_order_id, s.product_id, s.brand_etl) = (ssos.order_id, ssos.product_id, ssos.brand_etl)
LEFT JOIN view_stag_products sp ON t.product_id = sp.product_id AND sp.brand_etl = t.brand_etl 
LEFT JOIN view_stag_products sps ON s.product_id = sps.product_id AND sps.brand_etl = s.brand_etl 
LEFT JOIN (select * from cline_init where rn_crm_pd = 1) ci ON (ci.brand_etl, ci.crm_id, ci.product_id) = (t.brand_etl, t.crm_id, t.product_id)
WHERE lp.partner_id is not null and (s.id is null or 
	(
	t.form_type <> 'evaluation'
	AND (s.dr_order_role = 1 OR s.roleid = 1 or s.id is null)   -- BS mổ chính hoặc Vai trò thực hiện chính
	AND s.roleid not in (46,48,49,50)                       -- Không lấy vai trò bị BH nhập bởi người dùng 
	AND s.order_priorole <> 99                  			-- Không lấy vai trò không tính trách nhiệm chuyên môn thực hiện
	AND s.state_ckpttt = 'Done'                 			-- Phiếu Thủ thuật/chuyên khoa đã hoàn thành 
	) )
'''

'''
walkin_bh = query_df(conn, walkin_bh_qr)
erp_bh = query_df(conn, erp_bh_qr)
line_grt_info = query_df(conn, line_grt_info_qr)
ihc = query_df(conn, ihc_qr)
'''


def get_data_guarantee():
    # 0. Dữ liệu lịch sử thực hiện các KH bảo hành
    walkin_bh = query_df(conn, walkin_bh_qr)
    erp_bh = query_df(conn, erp_bh_qr)
    line_grt_info = query_df(conn, line_grt_info_qr)
    ihc = query_df(conn, ihc_qr) 
    stag_products = get_df_cache('stag_products', dict_get_df, conn)
    se = get_se()
    roles = get_roles()
    
    # 1. XỬ LÝ THÔNG TIN DÒNG DỊCH VỤ BỊ BẢO HÀNH, KHỬ NULL LÀM SẠCH
    dfc = get_dfline_bh()              #dfline.copy() 
    dict_grt = {'totality': '11', 'not_totality': '10'}
    dict_st = {'done': 1, 'processing': 2, 'waiting': 3, 'new': 4, 'chotuvan': 5, 'cancel': 6}
    dfc['type_guarantee'] = dfc.type_guarantee.replace(dict_grt)
    dfc['order_stage'] = dfc.stage.apply(lambda x: dict_st[x])
    
    # Thông tin dòng dịch vụ được đồng bộ từ ERP cũ sang - dùng data đã xử lý sẵn
    info_bk = ['lead_id', 'initial_booking']
    col_fixnull = ['lead_id', 'initial_booking', 'product_id', 'product_ctg_id', 'initial_product_ctg_id', 'product_category', 'product', 'kpi_point', 
                    'initial_his_service_type', 'initial_product_category', 'initial_product', 'initial_total', 'initial_kpi_point', 
                    'initial_ck_pttt', 'initial_services_date', 'service_guarantee_type', 
                    'employee_id', 'roleid', 'count_employee_grt', 'employee_code', 'employee', 'job', 'role', 'department', 'company_employee',
                    'pos_join', 'doctor_consult_id', 'doctor_consult', 'ordinal_consult', 
                    'type_guarantee', 'reason_guarantee_id', 'list_price', 'initial_list_price', 
                    'nhanvienphanquyen', 'phongbanphanquyen', 'chinhanhphanquyen', 'thuonghieuphanquyen']
    cols_not_ud = ['crm_id','type_crm_id','booking','partner_id','id','id_old','stage','pricelist','write_date_booking','write_date']
    col_exists = list(c for c in col_fixnull if c in dfc.columns)
    col_new = list(c for c in col_fixnull if c not in dfc.columns)
    
    temp_cline_old = pd.DataFrame(dfc['clineid_old'].fillna(dfc.id)).fillna(dfc.id).merge(line_grt_info[line_grt_info.rn_cline.eq(1)], how='left', left_on='clineid_old', right_on='id')
    
    if dfc.id_old.notnull().any(): 
        temp_crm_product =  dfc[['id_old', 'product_id']].merge(line_grt_info[line_grt_info.rn_crm_product.eq(1)], how='left', 
                                                            left_on=['id_old', 'product_id'], right_on=['crm_id', 'product_id']) 
        temp_crm_old =  dfc[['id_old']].merge(line_grt_info[line_grt_info.rn_crm.eq(1)], how='left', left_on='id_old', right_on='crm_id') 
        
        dfc.update(temp_crm_product, overwrite=False)    
        dfc.update(temp_crm_old[info_bk], overwrite=False)
    
    temp_crm_grt =  dfc[['crm_id']].merge(line_grt_info[line_grt_info.rn_crm.eq(1)], how='left', on='crm_id')
    # Update vào Bảng dữ liệu các Dòng dịch vụ đồng bộ 
    dfc.update(temp_cline_old[list(c for c in temp_cline_old.columns if c not in cols_not_ud)], overwrite=False)                 
    dfc[col_new] = temp_cline_old.loc[dfc.index, col_new]
    dfc.update(temp_crm_grt[info_bk], overwrite=False)
    
    # 2. UPDATE THÔNG TIN DỊCH VỤ VÀ DỊCH VỤ BAN ĐẦU BỊ BẢO HÀNH 

    # Cut dữ liệu lịch sử phiếu khám thực hiện của khách hàng: bỏ trùng phiếu chỉ định + nhân sự thực hiện/dịch vụ
    walkin_bhc = walkin_bh[walkin_bh.drk_walkin_product.eq(1) & walkin_bh.rn_prio_grt.eq(1)].copy()
    fix_type(walkin_bhc, list(c for c in walkin_bhc.columns if check_substr(c, r'id$|^(count|rn|drk|)_|type_gua') and c not in ls_col_str), 'Int64', np.nan)
    # Xác định lại Dịch vụ bị Bảo hành
    walkin_bhc['fix_product_id'] = walkin_bhc['fix_initial_product_id'].map(stag_products[stag_products.brand_etl.eq(0)].set_index('product_id')['group_id'])\
                                                                        .fillna(walkin_bhc.fix_initial_product_id).fillna(walkin_bhc.product_group_id)
    walkin_bhc['partner_gr_product'] = walkin_bhc[['partner_id', 'fix_product_id']].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1) 
    walkin_bhc.sort_values(by=['partner_id', 'fix_product_id', 'services_date'], inplace=True)

    '''
    # drk_partner_product_fix: Lần thực hiện liệu trình hoặc bảo hành dịch vụ ban đầu sắp xếp theo Ngày thực hiện
    # Mã phiếu Chuyên khoa/Thủ thuật liền kề
    walkin_bhc['lag_form_id'] = np.where(walkin_bhc.partner_treat, walkin_bhc.loc[(walkin_bhc.index - walkin_bhc.step_shift).replace(-1,0), 'form_id'], np.nan)
    '''
    # Nối dữ liệu xác định thông tin Nhân sự/thời gian đã thực hiện dịch vụ ban đầu
    w = walkin_bhc[['crm_id', 'form_id', 'product_id', 'lag_form_id','fix_product_id', 'product', 'partner_gr_product']]\
        .merge(walkin_bhc[walkin_bhc.rn_prio_grt.eq(1)], how='left', left_on=['lag_form_id','partner_gr_product'], \
               right_on=['form_id','partner_gr_product'], suffixes=('', '_init_product'))
    fix_type(w, ['crm_id','product_id'], int, 0)
    cols_product = ['product_id', 'product_ctg_id', 'his_service_type', 'product_category', 'product', 
                    'kpi_point', 'kpi_point_num', 'list_price', 'is_main_service', 'group_id', 'brand_etl']
    pdmerge = ['product_id', 'brand_etl']
    info_pd = dfc[['product_id', 'initial_product_id', 'brand_etl']].merge(stag_products[cols_product], how='left', on=pdmerge)
    info_pd = info_pd.merge(stag_products[cols_product].rename(columns={c: 'initial_'+c for c in cols_product if c != 'brand_etl'}), 
                                                                how='left', on=['initial_product_id', 'brand_etl'])
    dfc['services_date'] = pd.NaT    # Tạo cột tạm rỗng: Ngày làm dịch vụ trước
    add_col = []
    for c in cols_product:
        if c not in dfc.columns:
            add_col = add_col + [c, 'initial_'+ c]
    dfc[add_col] = pd.NA
    dfc.update(info_pd, overwrite=False) 

    # 3. UPDATE THÔNG TIN NHÂN SỰ CHỊU TRÁCH NHIỆM CHUYÊN MÔN
    dfcc = dfc[dfc.brand_etl.ne(0) & dfc.type_crm_id.eq(3)].copy() 
    # Thông tin Bác sĩ bị bảo hành - người dùng nhập trong phiếu PTTT/CK
    dfcc = dfcc.merge(erp_bh, how='left', on=['crm_id', 'product_id', 'brand_etl'], suffixes=('', '_erp_form'))
    # Thông tin Bác sĩ bị bảo hành - theo Phiếu khám của Booking ban đầu (Code tự tính toán).
    dfcc = dfcc.merge(w, how='left', on=['crm_id', 'product_id'], suffixes=('', '_erp_product'))            # dfcc = dfcc.merge(walkin_bh[walkin_bh.rn_prio_grt.eq(1)], how='left', on=['crm_id', 'product_id'], suffixes=('', '_erp_product'))
    # Thông tin Bác sĩ bị bảo hành theo Hồ sơ bệnh án CRM FPT cũ
    dfcc = dfcc.merge(ihc, how='left', on = ['initial_booking', 'initial_product_ctg_id'], suffixes=('', '_crm_fpt'))
    
    # Điền thông tin Nhân sự bị bảo hành, Ngày thực hiện trước theo thứ tự Vai trò người dùng nhập >> Phiếu thực hiện liền kề >> Hồ sơ bệnh án CRM FPT cũ
    pos_join = ['erp_form', 'erp_product', 'crm_fpt']  # 'erp_product_category',
    #suffixes = [('_'+c).replace('_erp_form', '') for c in pos_join]
    dfcc['pos_join'] = np.select([dfcc['employee_id' + '_' + suffix].notnull() for suffix in pos_join], pos_join, None)
    for p in pos_join: 
        for c in ['employee_id', 'employee_code', 'roleid', 'services_date']: 
            dfcc[c] = np.where(dfcc.pos_join.eq(p), dfcc[c].fillna(dfcc[c + '_' + p]), dfcc[c])
    dfcc['initial_ck_pttt'] = dfcc['initial_ck_pttt'].fillna(dfcc.initial_ck_pttt_erp_product) 
    dfcc['initial_total'] = dfcc['initial_total'].fillna(dfcc.initial_total_erp_product)

    info_emp = dfcc[['employee_code', 'roleid']].merge(se[se.employee_code.notnull()][list(c for c in col_fixnull if c in se.columns)], how='left', on ='employee_code')
    info_emp = info_emp.merge(roles[['roleid', 'role']], how='left', on='roleid')
    dfcc.update(info_emp, overwrite=False)
    dfcc['nhanvienphanquyen'] = dfcc.employee_id
    
    # 4. PHÂN LOẠI DỊCH VỤ TRÊN BOOKING BH: Bảo hành, nâng cấp, bán mới, chi phí
    is_guarantee_line = dfcc['pricelist_type'].eq('guarantee') \
                    | dfcc[['bh_ngoai_goi', 'reason_guarantee_id', 'initial_product_id']].fillna(0).astype(int).sum(axis=1).gt(0) \
                    | (dfcc.is_main_service.fillna(False) & dfcc.total.fillna(0).lt(100))
    crm_product_grt = dfcc[is_guarantee_line][['brand_etl','crm_id', 'initial_product_ctg_id']].rename(columns={'initial_product_ctg_id': 'product_ctg_id'})\
                        .drop_duplicates(['brand_etl','crm_id', 'product_ctg_id'])
    crm_product_grt['is_product_category_guarantee'] = True
    dfcc = dfcc.merge(crm_product_grt, how='left', on=['brand_etl','crm_id', 'product_ctg_id'])
    dfcc['is_product_category_guarantee'] = dfcc.is_product_category_guarantee | (dfcc.group_id.eq(dfcc.initial_group_id) & dfcc.group_id.fillna(0).ne(0))
    
    ls_thaochatlieu = [1333,1390,2045,2059,2089,2114,2135,2160,4531,4547,4576,4591,4634,4681,4735,4737,4738,4993,4994,
                       9689,9714,9756,9760,9837,9885,12058,12136,12200,12230,12704,12705,12706,12707,
                       17249,17321,17479,17492,17558,17586,17774,17776,17797,17806,17819,17822,17839,17842,17865,17892,17915,
                       18188,18229,18321,18550,18600,18657,18973,18980,18984,18992,19096,19105,19771,19772,
                       20099,20109,20357,20432,20570,20624,20638,20837,21219,21281,21584,22034,22048,22055,22132,25446,25857,27635,27637,27638]
    ls_nhomchatlieu = [4558,4559,4645,4646,4647,10674,10720,12240,12241,13851,13852,13853,13854,24976,24977]
    ls_dvdikem = [10216,10217,22752]         # DV đi kèm DV chính: cấy trung bì (nâng mũi)
    ls_chiphi = [4535,4536,4537,4538,4554,4555,4556,4557,4560,10120,10125,10127,10220,10221,10222,10223,10224,10587,10591,10644,10645,12062,12063,12064,12065,12237,12238,12239,12242,12950,12951,12952,12955,12956,14683,14737,14825,14826]
    ls_product_ctg_oth = [261,432,451,1652,1672,1729,1771]          # NHÓM PHỤ THU, CHI PHÍ KHÁC
    '''
    cd1: Bảo hành trong gói/liệu trình Laser/Spa/Trọn gói
    cd2: Bảo hành nâng gói chất liệu HOẶC nâng cấp dịch vụ chính (có giảm sâu) 
    cd3: Dịch vụ bảo hành
    cd4: Phát sinh thu thêm Chi phí khác (Phí sửa lại, Order bác sĩ, v.v. ...)
    cd5: Bán dịch vụ mới (Bán DV Khác nhóm hoặc Cùng nhóm DV bảo hành nhưng không Giảm giá sâu)
    '''
    total_valid = dfcc.total.fillna(0).gt(100000)
    cd1 = (dfcc.initial_his_service_type.isin(['Laser', 'Spa']) & dfcc.reason_guarantee_id.fillna(0).eq(20) ) \
        | (dfcc.initial_kpi_point_num.fillna(1).gt(1) & dfcc.reason_guarantee_id.fillna(0).isin([20, 31]) )
    cd2 = total_valid & ( dfcc.product_id.isin(ls_nhomchatlieu) \
        | (dfcc.is_product_category_guarantee & dfcc.is_main_service & dfcc.total_discount_review.fillna(0).gt(100000)) )
    cd3 = is_guarantee_line
    cd4 = total_valid & ( dfcc.product_ctg_id.fillna(0).isin(ls_product_ctg_oth) | dfcc.product_id.fillna(0).isin(ls_thaochatlieu + ls_chiphi + ls_dvdikem) )
    cd5 = dfcc.total.fillna(0).gt(1000000) & \
            (~dfcc.is_product_category_guarantee.fillna(False)  \
           | (dfcc.is_product_category_guarantee.fillna(False) & dfcc.total_discount_review.fillna(0).eq(0) ))
    dfcc['service_guarantee_type'] = np.select([cd1.astype(bool), cd2.astype(bool), cd3.astype(bool), cd4.astype(bool), cd5.astype(bool)], 
                                ['in_treatment', 'guarantee_upgrade', 'guarantee', 'chiphi', 'new_service'], None)

    dfcc.drop_duplicates(['brand_etl', 'product_id', 'crm_id', 'initial_product_id', 'employee_id'], inplace=True)
    dfcc['count_employee_grt'] = dfcc.groupby(['brand_etl', 'crm_id', 'initial_product_id'])['employee_id'].transform('nunique')
    dfcc.sort_values(['brand_etl', 'crm_id', 'product_id', 'order_stage', 'id'], ascending=[True,True,True,True,False], inplace=True)
    dfcc['rn_crm_product'] = dfcc.groupby(['brand_etl', 'crm_id', 'product_id'])['crm_id'].cumcount()+1
    ls_col_int = [c for c in dfcc.columns if check_substr(c, r'id$|phanquyen|^(count|rn|drk|)_|type_gua') and c not in ls_col_str]
    fix_type(dfcc, ls_col_int, 'Int64', np.nan)
    print('Hoàn thành Bảng tổng hợp thông tin Dịch vụ bảo hành!')
    return dfcc 

#dfcc = get_data_guarantee()


def write_stag_service_employee_guarantee():
    dfc = get_data_guarantee()       # get_data_guarantee()     dfcc.copy()
    TABLE = 'stag_service_employee_guarantee'

    replace_table_data(dfc, TABLE, where='brand_etl <> 0', conn=conn)
    print(f'HOÀN THÀNH! BẢNG THÔNG TIN DỊCH VỤ - NHÂN SỰ BỊ BẢO HÀNH')
    
    return TABLE

#write_stag_service_employee_guarantee()


#  BẢNG CHECK NGUỒN BOOKING LOYALTY

# Dữ liệu Booking có lịch sử cập nhật
query_loy = f'''
--- 1. Giới hạn các partner_id Khách hàng có Booking cập nhật trong 1 khoảng thời gian
WITH check_limit_partner AS (
SELECT DISTINCT ON (cl.brand_id, cl.partner_id)
	cl.brand_id, cl.partner_id, 
    ph.res_partner_id_org, COALESCE(ph.res_partner_id_org, cl.partner_id) AS fix_partner_id 
FROM crm_lead cl 
LEFT JOIN stag_res_partner_check_dupphone ph ON cl.partner_id = ph.id AND cl.brand_etl = ph.brand_etl AND ph.r = 1
WHERE COALESCE(partner_id, 0) <> 0 AND "type" = 'opportunity' AND write_date >= '{LIMIT_START_DATE_3M}' 
)
, limit_partner AS (
---- Lấy full Booking Khách hàng từ DS trên (trùng SĐT, khác Account)
SELECT brand_id, partner_id, fix_partner_id FROM check_limit_partner 
UNION
SELECT cl.brand_id, cl.partner_id, cl.partner_id AS fix_partner_id
FROM crm_lead cl WHERE EXISTS (SELECT 1 FROM check_limit_partner clp 
                                WHERE cl.partner_id = clp.res_partner_id_org AND cl.brand_id = clp.brand_id)
)
, bookings AS	(					
--- 2.1. Dữ liệu các Booking (lịch hẹn) của partner_id đã giới hạn
SELECT 
	CONCAT(lp.fix_partner_id, CASE WHEN lp.fix_partner_id > 786601 then '-' || cl.brand_etl::text else '' end)::text as char_partner_id, 
	null::int4 walkinid,
	cl.id crm_id,
	cl.type, cl.name booking, cl.type_crm_id, cl.contact_name, cl.code_customer, 
	cl.create_date, cl.booking_date + interval '7 hours' as booking_date, 
	cl.arrival_date + interval '7 hours' as arrival_date,
	--LAG(COALESCE(cl.arrival_date, CASE WHEN cl.stage_id IN (4,21,22,26) THEN cl.create_date END) + interval '7 hours') OVER w AS arrival_date_lag1,
	DATE(COALESCE(cl.arrival_date, cl.create_date) + interval '7 hours') "date",
	cs.name stage,
	--LAG(cs.name) OVER w AS stage_lag1,
	COALESCE(cl.reason_out_sold, COALESCE(cl.reason_cancel_booking, cl.reason_not_booking_date)) AS reason_close_booking,
	cl.source_id, --LAG(cl.source_id) OVER w AS source_id_lag1,
	cl.amount_total, cl.amount_paid,
	cl.note, cl.partner_id, cl.create_uid, 
	cl.company_id, cl.brand_id, 
	csb.id_old as crm_id_old, 
	cl.brand_etl 
FROM crm_lead cl
INNER JOIN limit_partner lp ON cl.partner_id = lp.partner_id AND cl.brand_id = lp.brand_id
LEFT JOIN crm_stage cs ON cl.stage_id = cs.id AND cl.brand_etl = cs.brand_etl 
LEFT JOIN crm_sync_brand csb ON cl.id = csb.crm_id and cl.brand_etl = csb.brand_etl 
WHERE cl.type = 'opportunity' AND cl.active
WINDOW w AS (PARTITION BY cl.partner_id, cl.brand_id ORDER BY COALESCE(arrival_date, cl.create_date))
--- ORDER BY cl.partner_id, arrival_date
),
walkins AS (						/* Phieu kham hoan thanh */
--- 2.2. Dữ liệu Phiếu khám (thực hiện dịch vụ) của partner_id đã giới hạn
SELECT 
	concat(lp.fix_partner_id, case when lp.fix_partner_id > 786601 then '-' || w.brand_etl::text else '' end)::text as char_partner_id, 
	w.id walkinid, 
	w.booking_id crm_id,
	'walkin' AS type, clw.name booking, clw.type_crm_id, clw.contact_name, clw.code_customer, 
	w.service_date_start create_date, clw.booking_date + interval '7 hours' booking_date, w.service_date_start arrival_date,
	(w.service_date_start::date) AS "date",
	w.state stage,
	null AS reason_close_booking,
	clw.source_id,
	so.amount_total,
	so.amount_total amount_paid,
	null AS note, clw.partner_id, clw.create_uid,
	w.company_id, smhc.brand brand_id, 
	csb.id_old crm_id_old, 
	w.brand_etl 
FROM sh_medical_appointment_register_walkin w
LEFT JOIN crm_lead clw ON w.booking_id = clw.id AND w.brand_etl = clw.brand_etl
INNER JOIN limit_partner lp ON clw.partner_id = lp.partner_id AND clw.brand_id = lp.brand_id
LEFT JOIN crm_sync_brand csb ON clw.id = csb.crm_id and clw.brand_etl = csb.brand_etl 
LEFT JOIN sale_order so ON w.sale_order_id = so.id AND w.brand_etl = so.brand_etl
LEFT JOIN sh_medical_health_center smhc ON w.institution = smhc.id AND w.brand_etl = smhc.brand_etl
WHERE w.state <> 'Cancelled' and w.service_date_start IS NOT NULL 
UNION ALL 
--- 2.2. Dữ liệu Phiếu khám (thực hiện dịch vụ) hệ thống Cũ
SELECT distinct on (w.walkinid)
	w.partner_id::text as char_partner_id, 
	w.walkinid, 
	w.crm_id,
	'walkin' AS type, w.booking, w.type_crm_id, w.contact_name, w.code_customer, 
	w.services_date create_date, NULL::timestamp booking_date, w.services_date arrival_date,
	(w.services_date::date) AS "date",
	w.state_walkin stage,
	null AS reason_close_booking,
	w.source_id,
	0 AS amount_total,
	0 AS amount_paid,
	null AS note, w.partner_id, NULL::int AS create_uid,
	w.company_id, w.brand_id, 
	NULL::INT crm_id_old, 
	w.brand_etl 
FROM report_kpi_team_service w
INNER JOIN limit_partner lp ON w.partner_id = lp.partner_id
WHERE w.state_walkin <> 'Cancelled' AND w.brand_etl = 0 --- AND lp.partner_id IS NOT NULL 
), bk_wk AS (						
--- 3. Hợp nhất dữ liệu Booking và Phiếu khám vào 1 Bảng
SELECT * FROM bookings
UNION ALL
SELECT * FROM walkins 
), bk_wk_lag AS	(					/* So sanh thoi gian quay lai, Nguon booking */
--- 4. Xác định Số dòng lùi đến: Phiếu khám (ngày làm DV) gần nhất, Booking thành công gần nhất, Booking Out-sold gần nhất 
SELECT 
	STRPOS(
		REVERSE(
			STRING_AGG(left("type", 1), '') OVER (PARTITION BY char_partner_id, brand_id order by "date", "type", arrival_date)
				),	
		'w') lag_to_wk
	--, SUM(CASE WHEN stage = 'Won' THEN 1 END) OVER (PARTITION BY char_partner_id, brand_id ORDER BY "date", "type", arrival_date) - 1 AS count_bk_won
	, STRPOS(
		REVERSE(
			STRING_AGG(left("stage", 1), '') OVER (PARTITION BY char_partner_id, company_id ORDER BY "date", "type", arrival_date)
				),	
		'O') lag_to_os
	, SUM(CASE WHEN stage = 'Out sold' THEN 1 END) OVER (PARTITION BY char_partner_id, company_id ORDER BY "date", "type", arrival_date) AS count_bk_outsold_company
	, *
FROM bk_wk 					/* WHERE code_customer in ('20003752', '21000728', '21006694', 'CUS307733')   */
ORDER BY code_customer, "date", bk_wk."type"  
)
SELECT b.*, 
		LAG("date", CASE WHEN lag_to_wk > 0 then lag_to_wk - 1 else null end) 
			OVER (PARTITION BY b.char_partner_id, b.brand_id order by "date", b."type", b.arrival_date) services_date_recent,
		LAG("arrival_date", CASE WHEN lag_to_os > 1 then lag_to_os - 1 else null end) 
			OVER (PARTITION BY b.char_partner_id, b.company_id order by "date", b."type", b.arrival_date) outsold_date_recent,
		LAG("reason_close_booking", CASE WHEN lag_to_os > 1 then lag_to_os - 1 else null end) 
			OVER (PARTITION BY b.char_partner_id, b.company_id order by "date", b."type", b.arrival_date) outsold_reason_recent,
		--ss.code source_code, ss.newcat_source category_source, ss.source,
		srpcd.phone, srpcd.mobile, srpcd.code_customer_org, srpcd.count_occur_phone,
		se.employee_code, se.employee, se.department, rc.name company, 
        CASE WHEN clp.partner_id IS NOT NULL AND b.brand_etl <> 0 THEN TRUE ELSE FALSE END AS is_record_update
FROM bk_wk_lag b
LEFT JOIN m_res_company rc ON rc.id = b.company_id AND rc.brand_etl = b.brand_etl
LEFT JOIN stag_res_partner_check_dupphone srpcd on srpcd.r = 1 AND b.partner_id = srpcd.id AND b.brand_etl = srpcd.brand_etl 
LEFT JOIN view_stag_employees_user se ON se.user_id = b.create_uid AND se.brand_etl = b.brand_etl
LEFT JOIN check_limit_partner clp ON b.partner_id = clp.partner_id AND b.brand_id = clp.brand_id
'''

#dfp = query_df(conn, query_loy)
#fix_type(dfp, list(c for c in dfp.columns if check_substr(c, r'id$|crm_id|brand_etl|^count') and c[:5] not in('amoun', 'char_')), 'Int64', np.nan)
#dfp[dfp.partner_id.eq(785696)]


def get_data_loy():
    dfc = query_df(conn, query_loy)     #  dfp.copy()
    df_reason = get_df_cache('reasons', dict_get_df, conn)
    df_source = get_df_cache('sources', dict_get_df, conn)
    fix_type(dfc, list(c for c in dfc.columns if check_substr(c, r'id$|crm_id|brand_etl|^count') and c[:5] not in('amoun', 'char_')), 'Int64', np.nan)
    
    # Thông tin các Booking "thành công" được đồng bộ từ ERP cũ - update dữ liệu ban đầu vào Booking trên hệ thống mới 
    dfo = dfc[dfc.crm_id.isin(dfc.crm_id_old.fillna(0).unique()) & dfc.type.eq('opportunity') & dfc.brand_etl.eq(0)]
    info_crm_old = dfc[['brand_etl','crm_id','crm_id_old']].merge(dfo, how='left', left_on='crm_id_old', right_on='crm_id', suffixes=('', '_o'))
    dfc.update(info_crm_old[['lag_to_wk','lag_to_os','count_bk_outsold_company','create_uid','employee_code','employee','department']])
    dfc['arrival_date'] = info_crm_old.arrival_date.fillna(dfc.arrival_date)
    is_crm_old_won = info_crm_old.stage.eq('Won')
    dfc['stage'] = np.where(is_crm_old_won, 'Won', dfc.stage)
    
    # df1: Tách các Dòng không phải Booking thành công cũ đồng bộ sang hệ thống mới.
    df1 = dfc[~is_crm_old_won].copy()
    df1 = df1.sort_values(['char_partner_id','brand_id','date','type','brand_etl']).reset_index(drop=True)
    df1['crm_id_lag1'] = df1.groupby(['char_partner_id', 'type'])['crm_id'].shift(periods=1)
    df1['crm_id_lag1'] = df1[['partner_id','crm_id','type','crm_id_lag1']].fillna(0)\
                        .groupby(['partner_id','crm_id','type'])['crm_id_lag1']\
                        .transform('first').replace(0, np.nan)
    
    rn_crm = df1.groupby(['char_partner_id','crm_id','type'])['crm_id'].cumcount()+1
    df1['value_crm_won'] = np.where(rn_crm.eq(1) & df1.stage.eq('Won'), 1, 0)
    df1['count_bk_won'] = df1.groupby(['brand_id','char_partner_id'])['value_crm_won'].cumsum()
    df1['count_bk_won'] = df1.count_bk_won.replace(0, np.nan) - 1          # Số Booking đã thành công trước Ngày đến cửa
    
    # df2: Dòng các Booking là Booking thành công cũ đồng bộ sang hệ thống mới.
    df2 = dfc[is_crm_old_won].copy()
    df2 = df2.merge(df1[df1.crm_id.isin(df2.crm_id_old.unique()) & df1.type.eq('opportunity') & df1.brand_etl.eq(0)].drop_duplicates('crm_id')
                    [['crm_id','crm_id_lag1','count_bk_won']].rename(columns={'crm_id':'crm_id_old'}), how='left', on='crm_id_old')
    
    # Nối 2 dữ liệu df1, df2, lấy thông tin tên Lý do đóng Booking, Nguồn Booking hiện tại và Booking liền kề trước.
    df = pd.concat([df1, df2])
    cols_lag = ['brand_id','crm_id_lag1', 'arrival_date', 'stage', 'source_id', 'reason_close_booking']
    df_crm_lag = df1[df1.crm_id.isin(df1.crm_id_lag1.unique()) & df1.type.eq('opportunity')]\
                .drop_duplicates(['brand_id','crm_id'], keep='last').rename(columns={'crm_id_lag1':'id_lag1', 'crm_id': 'crm_id_lag1'})
    
    df = df.merge(df_crm_lag[cols_lag], how='left', on=cols_lag[:2], suffixes=('', '_lag1'))
    df = df.merge(df_reason, how='left', on='reason_close_booking') 
    df = df.merge(df_reason.rename(columns={c: c+'_lag1' for c in df_reason.columns}), how='left', on='reason_close_booking_lag1')
    df = df.merge(df_source, how='left', on='source_id')
    df = df.merge(df_source.rename(columns={c: c+'_lag1' for c in df_source.columns}), how='left', on='source_id_lag1')
    
    # Số ngày KH quay lại so với Ngày KH đến gần nhất: Theo Ngày đến cửa/Booking hoặc Ngày đến làm dịch vụ
    df['date_diff_arrival'] = np.where(df.arrival_date_lag1.notnull(), pd.to_timedelta(pd.to_datetime(df.date).dt.date - df.arrival_date_lag1.dt.date).dt.days, np.nan)
    df['date_diff_service'] = np.where(df.services_date_recent.notnull(), pd.to_timedelta(pd.to_datetime(df.date).dt.date - pd.to_datetime(df.services_date_recent).dt.date).dt.days, np.nan)
    df['date_diff'] = np.where(df.date_diff_service.isnull(), df.date_diff_arrival, df.date_diff_service)
    
    ls_success = [94,95,123,127,209,283,291,531,565]       # Nguồn thành công quay lại
    ls_potential = [91,92,96,97,281,290]                   # Nguồn Tái khai khác
    src_notcheck = df.source.apply(lambda r: check_substr(r, r'ctv|thuê|đối ng|hợp tác'))
    src_success = df.source_id.fillna(0).isin(ls_success).astype(bool)    
    src_potential = df.source_id.fillna(0).isin(ls_potential).astype(bool)
    src_lag_success = df.source_id_lag1.fillna(0).isin(ls_success).astype(bool)    
    src_lag_potential = df.source_id_lag1.fillna(0).isin(ls_potential).astype(bool)
    src_eq_lag = df.source_id.fillna(0).ne(df.source_id_lag1.fillna(df.source_id.fillna(0))).astype(bool)
    
    df['check_source'] = np.select([src_notcheck | (src_success & df.count_bk_won.fillna(0).ge(1)),   # Nguồn ko cần rà soát HOẶC đúng KH quay lại
                                    src_success & df.code_customer_org.notnull(),                     # SĐT trùng account khác
                                    src_success & (df.date_diff.isnull() | df.date_diff.gt(365*2)),   # Sai nguồn KH cũ thành công quay lại trong 2 năm
                                    df.stage_lag1.isin(['Out sold', 'Cancel']) & df.fillna(100).date_diff_arrival.abs().le(30) & src_eq_lag], 
                                   [pd.NA, 'dup_account', 'src_not_return', 'src_out sold/cancel'], pd.NA)
    
    ls_col_int = [c for c in df.columns if check_substr(c, r'^(drk|rn_|rnk|lag|count)|id$|_id_|date_diff') and c[:5] not in('amoun', 'char_')]
    fix_type(df, ls_col_int, 'Int64', np.nan)
    df['id_etl'] = df[['type', 'partner_id', 'crm_id', 'walkinid', 'brand_etl']].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1)

    df = df[df.is_record_update]
    return df

#df = get_data_loy()

def write_stag_crm_booking_walkin_loyalty():
    starttime = datetime.now() 
    dfc = get_data_loy()     # query_df(conn, query_loy)  df.copy()  get_data_loy()
    dfc = dfc[dfc.brand_etl.ne(0)]
    TABLE = 'stag_crm_booking_walkin_loyalty'

    WHERE_UD = f'''brand_etl <> 0 AND (partner_id, brand_id) IN 
                (
                SELECT partner_id, brand_id FROM crm_lead WHERE COALESCE(partner_id, 0) <> 0 AND "type" = 'opportunity'
                AND write_date >= '{LIMIT_START_DATE_3M}' GROUP BY brand_id, partner_id 
                )'''
    replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    print(f'HOÀN THÀNH! BẢNG CHECK NGUỒN LOYALTY CỦA BOOKING')

    endtime = datetime.now() 
    print('Tiến trình xử lý: ', endtime - starttime)
    return TABLE

#write_stag_crm_booking_walkin_loyalty()


def clean_temp_table():
    temp_table = [TEMP_PATIENT_LIMIT, TEMP_PATIENT_AS]  #temp_table = set(v for k, v in globals().items() if isinstance(v, str) and v.startswith("temp") and not k.startswith("_"))
    qr_clean = ';\n'.join(f'''\t\tDROP TABLE IF EXISTS {tb};''' for tb in temp_table)
    query_df(conn, qr_clean, ret='exe')  
    print(f'''-------- ✷✷✷✷✷✷ -------- ĐÃ DỌN DẸP CÁC BẢNG TẠM -------- ✷✷✷✷✷✷ --------\n{qr_clean}''')

#clean_temp_table()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'task_1_stag_his_team_service': write_stag_his_team_service,
    'task_2_his_team_after_service': write_his_team_after_service,
    'task_3_stag_sale_order_services': write_stag_sale_order_services,
    'task_5_stag_walkin_evaluation_treatment': write_stag_walkin_evaluation_treatment,
    'task_6_stag_service_employee_guarantee': write_stag_service_employee_guarantee,
    'task_7_stag_crm_booking_walkin_loyalty': write_stag_crm_booking_walkin_loyalty,
    'task_End_cleaning_temp_table': clean_temp_table,
}


def main():
    parser = argparse.ArgumentParser(description='ETL stag_his')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
