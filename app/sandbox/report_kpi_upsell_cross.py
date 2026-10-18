# -*- coding: utf-8 -*-
"""
REPORT KPI UPSELL CROSS

Chuyen tu quentk_4_report_kpi_upsell_cross.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/report_kpi_upsell_cross_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python report_kpi_upsell_cross.py --task report_kpi_commission_upsell
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

DATE_SYS = dt.date(2026,6,1)            # Ngày chặn Cập nhật dữ liệu toàn hệ thống
DATE_SYS_STR = DATE_SYS.strftime("%Y-%m-%d")


conn = 'dev'   # Dev trên Jupyterlab -> kết nối với Sandbox, đồng bộ lên Airflow -> Tự động connect "new_product"
_CACHE = {}          # nhớ kết quả trong một tiến trình task, tránh truy vấn lặp

DATETRUNC = (dt.date.today() - relativedelta(months=1)).replace(day=1)
DATETRUNC = DATE_SYS if DATETRUNC < DATE_SYS else DATETRUNC
DATETRUNC_STR = DATETRUNC.strftime("%Y-%m-%d")
LIMIT_START_DATE = DATETRUNC_STR


# Khởi tạo các brand dữ liệu xử lý
DICT_BRAND_ETL = {1: 'kn', 3: 'pr', 4: 'hh', 8: 'thv', 7: 'sci'}
DICT_BR_ETL = {v: k for k, v in DICT_BRAND_ETL.items() if v}
LS_BRAND_ETL = list(DICT_BRAND_ETL.keys())
LS_BR = list(v for k, v in DICT_BRAND_ETL.items() if k > 0)
idb = -1 
idb_kn = 1
br = ''
br_sci = 7

CRMID = ['crm_id', 'brand_etl']
PCLINEID = ['clineid', 'product_id', 'brand_etl']
PARTNERID = ['partner_id', 'brand_etl']
CLINEID = ['brand_etl', 'clineid']


# Truy vắn dữ liệu thông tin dịch vụ ban đầu hủy - dịch vụ thay thế 

LS_RECEPT_ID = [2049, 2058, 2081, 2082, 2086, 2093, 2097, 2101, 2102, 2119, 2138, 2140, 2144, 2148, 2156, 2163, 2164, 2168, 2170, 2174, 
                2181, 2193, 2194, 2197, 2205, 2250, 2255, 2300, 2305, 2308, 2314, 2333, 2344, 2345, 2377, 2379, 2430, 2557, 2563, 2612]
CC_RECEPT = ','.join(str(e) for e in LS_RECEPT_ID)
query_line_view = 'SELECT * FROM view_crm_line_cancel_upsell' 
query_line = \
f'''
 SELECT clinec.crm_id,
    rlli.clid2 id,
    rlli.clid1 AS clineid_cancel,
    cline.id AS clineid,
    spc.product_category AS product_category_cancel,
    sp.product_category,
    spc.product AS product_cancel,
    sp.product,
    coalesce(scli.stage, cline.stage) as stage,
    clinec.total AS total_cancel,
    clinec.gia_truoc_huy,
    coalesce(scli.total, cline.total) as total,
    row_number() OVER (PARTITION BY cline.brand_etl, cline.id ORDER BY clinec.id) AS rn_clineid,
    row_number() OVER (PARTITION BY clinec.brand_etl, clinec.id) AS rn_clineid_cancel,
    sum(clinec.gia_truoc_huy) OVER (PARTITION BY cline.brand_etl, cline.id ORDER BY clinec.id) AS total_cancel_cumsum, 
    sec.employee AS employee_line_cancel,
    sec.department AS department_line_cancel, 
    clinec.reason_line_cancel,
    clinec.product_id AS product_id_cancel,
    cline.product_id,
    clinec.product_ctg_id, 
    spc.his_service_type his_service_type_cancel,
    spc.surgery_type surgery_type_cancel,
    cline.write_date, 
    cline.company_id, 
    clinec.consultants_1 consultants_1_cancel, clinec.consulting_role_1,  
    case when clinec.consultants_1 = clinec.consultants_2 then null else clinec.consultants_2 end consultants_2_cancel, clinec.consulting_role_2, 
    cline.consultants_3, cline.consulting_role_3, 
    clinec.brand_etl 
   FROM crm_line clinec
     LEFT JOIN res_line_line_ids rlli on clinec.id = rlli.clid1 and clinec.brand_etl = rlli.brand_etl 
     LEFT JOIN crm_line cline ON rlli.clid2 = cline.id and rlli.brand_etl = cline.brand_etl 
     LEFT JOIN stag_crm_line_info scli ON cline.id = scli.clineid and cline.brand_etl = scli.brand_etl
     LEFT JOIN view_stag_products spc ON clinec.product_id = spc.product_id and clinec.brand_etl = spc.brand_etl 
     LEFT JOIN view_stag_products sp ON cline.product_id = sp.product_id and cline.brand_etl = sp.brand_etl 
     LEFT JOIN view_stag_employees_user sec ON clinec.consultants_1 = sec.user_id and clinec.brand_etl = sec.brand_etl 
     LEFT JOIN view_stag_employees_user se ON cline.consultants_1 = se.user_id and cline.brand_etl = se.brand_etl 
  WHERE clinec.brand_etl = 1 
  	AND clinec.reason_line_cancel::text = 'change_service' 
  	AND rlli.clid1 IS NOT NULL 
  	AND sec.department_id IN ({CC_RECEPT}) 
  	AND (se.department_id NOT IN ({CC_RECEPT}) OR (se.company_id = 2 AND cline.consultants_3 IS NOT NULL))
    AND NOT ( (cline.consultants_1 = clinec.consultants_1 OR cline.consultants_1 = COALESCE(clinec.consultants_2, 0) 
            OR COALESCE(cline.consultants_2, -1) = clinec.consultants_1 OR COALESCE(cline.consultants_2, 0) = COALESCE(clinec.consultants_2, -1)
            ) 
            AND cline.company_id <> 2
            )  -- Loại trừ các Dịch vụ thay thế giữ nguyên NS tư vấn (ngoại trừ KNHN)
    AND EXISTS (SELECT 1 FROM stag_payment_transfer p
	              WHERE p.crm_id = clinec.crm_id AND p.brand_etl = clinec.brand_etl
	                AND p.payment_date >= '{LIMIT_START_DATE}')
'''

#df_line = query_df(conn, query_line)

# Dữ liệu các dòng dịch vụ thay thế phát sinh Doanh số trả về dịch vụ ban đầu
# id phiếu + crm_line_id: duy nhất

QR_CLID = '''
SELECT clid1 clineid FROM res_line_line_ids WHERE brand_etl = 1
UNION
SELECT clid2 clineid FROM res_line_line_ids WHERE brand_etl = 1
'''
query_pm = \
f'''
WITH clid AS ({QR_CLID})
SELECT id, ma_phieu, type_crm_id, crm_id, booking, code_customer, contact_name, arrival_date, 
	product_category, product, quantity, uom_price, number_used, stage_product, total_last, total_received_recal, total_received_cumsum, 
	payment, payment_date, pm.clineid, product_ctg_id, product_id, his_service_type, surgery_type, 
	source_id, category_source, "source", 
	org_brand_id, orgcompany_id, brand_id, company_id, org_company, company, 
	pm.brand_etl
FROM report_kpi_bonus_commission pm
LEFT JOIN clid ON pm.clineid = clid.clineid 
WHERE pm.brand_etl = 1
    AND pm.product_category <> 'SAN PHAM' 
    AND clid.clineid IS NOT NULL
    AND pm.payment_date >= '{LIMIT_START_DATE}'
    --- AND EXISTS (SELECT 1 FROM view_crm_line_cancel_upsell cline WHERE pm.clineid= cline.clineid)
'''
#df_pm = query_df(conn, query_pm)        # => Dùng chung cho 2 hàm

# Dữ liệu Tư vấn viên 1, 2 Booking bán mở rộng
query_consult = \
f'''
WITH clid AS ({QR_CLID})
SELECT pm.id, pm.clineid, rn_id_clineid, is_kpi_branch, 
	consultants, employee_code, employee, consulting_role, rto, ordinal_consult, job, department, company_employee, department_id, employee_company_id,
	commission_employee, commission_department, team_id, team, group_job_id, group_job, 
	cline.consultants_3, 
	nr_clone_permission, phongbanphanquyen_fix::INT, chinhanhphanquyen_fix::INT, thuonghieuphanquyen_fix::INT, id_etl, 
	pm.brand_etl 
FROM report_kpi_bonus_commission_transpose pm
LEFT JOIN clid ON pm.clineid = clid.clineid 
LEFT JOIN crm_line cline ON pm.clineid = cline.id AND cline.brand_etl = 1
WHERE pm.brand_etl = 1 
    AND ordinal_consult <> 'supporter' 
    AND product_category <> 'SAN PHAM' 
    AND clid.clineid IS NOT NULL
    AND pm.payment_date >= '{LIMIT_START_DATE}'
'''
#df_consult = query_df(conn, query_consult)

dict_get_df = {'stag_employees': 'SELECT * FROM view_stag_employees_user', 
               'dfline': query_line, 
               'df_pm': query_pm, 
               'df_consult': query_consult
              }


# XỬ LÝ THÔNG TIN DỊCH VỤ BAN ĐẦU - THAY THẾ VÀ TÍNH GIÁ TRỊ BÁN CHÊNH LỆCH THEO TIỀN PHẢI THU

ls_id = ['id', 'clineid', 'product_id', 'consultants', 'ordinal_consult', 'chinhanhphanquyen_fix']

total_replace_cumsum = 38200000
total_cancel_sum = 26250000
total = 26250000
gia_truoc_huy = 26250000  #1625000
total_cancel_cumsum = 26250000   #8475000

# Hàm tính giá trị dịch vụ bán tăng thêm
def cal_total_upsell(total, gia_truoc_huy, total_cancel_sum, total_cancel_cumsum, total_replace_cumsum):
    v1 = None      # Giá trị trả về dòng dịch vụ hủy
    v2 = None      # Giá trị ghi nhận bán tăng thêm
    if total_replace_cumsum <= total_cancel_sum:            # Lũy kế giá trị DV thay thế <= ∑ DV trước hủy
        # Giá trị trả về DV hủy = giá lũy kế DV thay thế nếu lũy kế này vẫn nhỏ hơn tổng đơn hủy
        v1 = total_replace_cumsum if total > total_replace_cumsum else total      
        v2 = None  
        #print('R1')
    else:                           
        diff = total_replace_cumsum - total_cancel_sum       # Chênh lệch giá trị lũy kế DV mới - ∑ DV trước hủy
        if diff < total: 
            v1 = gia_truoc_huy if total - diff > gia_truoc_huy else total - diff
            v2 = diff                                         # 0 < Giá trị chênh lệch < Giá trị DV mới => Số tiền tăng thêm = giá trị chênh lệch
            #print('R2')
        else: 
            v2 = total                                        # Giá trị DV mới đã vượt ∑ DV trước hủy
            #print('R3')
    ls_value = [v1, v2]
    return ls_value

def get_df_service_info(): 
    #df = df_line.copy()
    dfline = get_df_cache('dfline', dict_get_df, conn=conn)
    stag_employees = get_df_cache('stag_employees', dict_get_df, conn=conn)
    users = stag_employees[stag_employees.user_id.fillna(0).ne(0)].copy()
    
    ls_numeric = [c for c in dfline if check_substr(c, 'total|amount')]
    fix_type(dfline, ls_numeric, 'float', np.nan)
    ls_int = [c for c in dfline if check_substr(c, '_id|consultants')]
    fix_type(dfline, ls_int, 'Int64', np.nan)
    
    # Nối thông tin Tư vấn 3 
    cols_emp = ['brand_etl','user_id','employee_code','employee','job','department','department_id','company_id']
    df = dfline.merge(users[cols_emp], how='left', left_on=['brand_etl','consultants_3'], right_on=cols_emp[:2], suffixes=('','_3'))
    
    # Lọc dữ liệu: Phòng ban tư vấn đổi dịch vụ KHÁC Kinh doanh dịch vụ hoặc Có tư vấn 3 (KNHN) 
    df = df[~df.department_id.isin(LS_RECEPT_ID) | df.company_id_3.eq(2)]
    # Tách dataframe lọc trùng dòng dịch vụ hủy ban đầu 
    df_cc = df.drop_duplicates(['brand_etl','clineid_cancel'])
    # Tổng giá trị đơn hàng bị hủy ban đầu (Lễ tân tư vấn)
    
    df_cc = df_cc.groupby(CRMID).agg(total_cancel_sum=('gia_truoc_huy', sum))
    
    # Tách dataframe lọc trùng dòng dịch vụ thay thế 
    df_rp = df.drop_duplicates(CLINEID).copy()
    # Tính tổng giá trị đơn hàng của các dịch vụ thay thế
    df_rp['total_replace_cumsum'] = df_rp.sort_values('rn_clineid_cancel').groupby(CRMID)['total'].agg('cumsum')
    
    # Nối thông tin tổng giá trị đơn hàng thay thế
    df = df.merge(df_cc, how='left', on=CRMID).merge(df_rp[['brand_etl','clineid','total_replace_cumsum']], how='left', on = ['brand_etl','clineid'])

    cd1 = df.total_replace_cumsum <= df.total_cancel_sum       # Lũy kế giá trị bán mới thấp hơn tổng Đơn hủy của Lễ tân
    ls_numeric = [c for c in df.columns if check_substr(c, 'total|amount|gia_')]
    fix_type(df, ls_numeric, 'float', np.nan)
    df['total_upsell_sum'] = np.where(df.total_replace_cumsum > df.total_cancel_sum, df.total_replace_cumsum - df.total_cancel_sum, 0)
    df['total_cumsum_subs_before_cancel'] = np.where(cd1, 0, df.total_replace_cumsum - df.gia_truoc_huy)
    
    # Lấy list các tên cột theo tham số trong Hàm tính giá trị dịch vụ bán tăng thêm
    ls_cfunction = re.sub(r'[()]|=[\'|\w]+', '', str(inspect.signature(cal_total_upsell))).split(', ')
    ls_upsell = df[ls_cfunction].apply(lambda r: cal_total_upsell(*r), axis = 1)
    
    df['total_linec_back_recept'] = ls_upsell.str[0]              # Giá trị trả về dòng dịch vụ hủy
    df['total_upsell'] = ls_upsell.str[1]                         # Giá trị tăng thêm trên line mới
    df['total_line_back_recept'] = df.total - df.total_upsell     # Giá trị trả về BP ban đầu từ dịch vụ mới

    print('1. BẢNG TẠM: THÔNG TIN DỊCH VỤ HỦY - DỊCH VỤ THAY THẾ, TÍNH GIÁ TRỊ CHÊNH LỆCH' + '  ' + str(df.shape))
    return df

def get_df(): 
    if 'df' not in _CACHE: 
        _CACHE['df'] = get_df_service_info()
    return _CACHE['df']
#df = get_df_service_info()      
#print(df.shape)


# XỬ LÝ DỮ LIỆU THANH TOÁN _ V2

payment = -4500000
payment_new_order_cumsum = -3500000
total_cancel_sum = 35000000

def cal_pm_upsell(payment, payment_new_order_cumsum, total_cancel_sum):
    v = None      # Doanh số bán tăng thêm ghi nhận theo số tiền thu thực tế
    payment_new_order_cumsum = round(payment_new_order_cumsum)
    total_cancel_sum = round(total_cancel_sum)
    if payment < 0:
        dif = (payment_new_order_cumsum - total_cancel_sum) + payment
        if payment_new_order_cumsum >= total_cancel_sum: 
            v = payment
        else: 
            sub = - (payment_new_order_cumsum - payment - total_cancel_sum)
            v = sub if abs(sub) < abs(payment) else payment
    elif payment_new_order_cumsum <= total_cancel_sum:      # Lũy kế Số tiền thu dịch vụ mới chưa đạt Giá trị trả về BP tư vấn ban đầu
        v = 0
    elif payment_new_order_cumsum - total_cancel_sum > payment:           
        v = payment 
    else:
        v = payment_new_order_cumsum - total_cancel_sum
    return v 
    
def get_df_payment(): 
    df = get_df()
    pm = get_df_cache('df_pm', dict_get_df, conn=conn)   
    df_consult = get_df_cache('df_consult', dict_get_df, conn=conn)
    stag_employees = get_df_cache('stag_employees', dict_get_df, conn=conn)
    users = stag_employees[stag_employees.user_id.fillna(0).ne(0)].copy()

    df_info_cline = df[['brand_etl', 'clineid', 'gia_truoc_huy', 'total_cancel_sum']].drop_duplicates(CLINEID)
    pm = pm.merge(df_info_cline, how='inner', on=CLINEID)
    pm['is_payment_eq_giatruochuy'] = pm['payment'] == pm['gia_truoc_huy']
    pm['is_payment_cumsum_eq_total_cancel'] = pm['total_received_cumsum'] == pm['total_cancel_sum']
    pm['is_transfer_form'] = pm['ma_phieu'].str.startswith('PDC')
    # Sắp xếp thứ tự phiếu có doanh số trả về BP ban đầu
    lsc = ['crm_id', 'payment_date', 'is_payment_eq_giatruochuy', 'clineid', 'is_transfer_form', 'id']
    ascending = [False,True,False,True,False,True]
    # payment_new_order_cumsum: Lũy kế số tiền thu thực tế trên đơn hàng mới, sắp xếp ưu tiên thời gian TT, phiếu có số tiền bằng giá trước hủy...
    pm['payment_new_order_cumsum'] = pm.sort_values(lsc, ascending=ascending).groupby('crm_id')['payment'].agg('cumsum')
    pm.drop(columns='gia_truoc_huy', inplace=True)
    
    # df_cline: dữ liệu dịch vụ thay thế (lọc trùng dòng dịch vụ mới), tính trừ doanh số trả lại
    df_cline = df.drop_duplicates(['clineid', 'clineid_cancel'])[['clineid'] + [c for c in df.columns if c not in pm.columns]]
    pm = pm.merge(df_cline, how = 'inner', on = 'clineid')
    
    ls_cfunction = re.sub(r'[()]|=[\'|\w]+', '', str(inspect.signature(cal_pm_upsell))).split(', ')
    pm['payment_upsell'] = pm[ls_cfunction].fillna(0).apply(lambda r: cal_pm_upsell(*r), axis = 1)
    # payment_back_recept: Doanh số trả về BP ban đầu (theo số tiền thực thu)
    pm['payment_back_recept'] = ( pm.payment - pm.payment_upsell.fillna(0) )*1 #np.where(pm.payment.ge(0), 1, -1)   
    
    pm = pm.merge(df_consult, how='inner', on=['id', 'clineid'], suffixes=('_pm', ''))                # Nối dữ liệu Tư vấn viên 1, 2
    pm['rn_clineid'] = pm.groupby('clineid')['clineid'].cumcount()+1
    pm['rn_id_clineid_consult'] = pm.sort_values('nr_clone_permission', ascending=False).groupby(['id', 'clineid', 'consultants'])['clineid'].cumcount()+1
    pm.rename(columns={'commission_employee': 'commission_employee_crm_line', 'commission_department': 'commission_department_crm_line', 
                      'total_received_recal': 'total_received'}, inplace=True) 
    
    is_recept = (pm.consultants.eq(pm.consultants_1_cancel.fillna(0)) | pm.consultants.eq(pm.consultants_2_cancel.fillna(0))).astype(bool)
    pm_adjust = np.select([is_recept, pm.rn_id_clineid_consult.eq(1)], [pm.payment_back_recept, pm.payment_upsell], np.nan)
    pm['commission_employee'] = pm_adjust*pm.rto
    pm['commission_department'] = pm_adjust*(pm.commission_department_crm_line/pm.payment)    # Doanh số ghi nhận lại cho Bộ phận bán tăng thêm
    pm['is_recept_sell_org'] = is_recept.replace(False, np.nan)


    # Doanh số của nhân sự bán mở rộng các dòng dịch vụ nhập tại Tư vấn 3
    pm3 = pm[pm.consultants_3.fillna(0).ne(0) & pm.consultants.eq(pm.consultants_1_cancel.fillna(0)) & pm.rn_id_clineid_consult.eq(1) & pm.payment_upsell.fillna(0).ne(0)]\
            .reset_index().copy()
    pm3['consultants'] = pd.to_numeric(pm3.consultants_3, errors='coerce').astype('Int64')
    pm3['ordinal_consult'] = 'consult_3'
    pm3['is_recept_sell_org'] = np.nan
    pm3['commission_employee_crm_line'] = 0
    pm3['commission_department_crm_line'] = 0
    
    pm3_emp = pm3[['index','consultants','brand_etl']].merge(users, how='left', left_on=['consultants','brand_etl'], right_on=['user_id','brand_etl']).set_index('index')
    pm3_emp.rename(columns={'company':'company_employee', 'company_id': 'chinhanhphanquyen_fix'}, inplace=True)
    pm3 = pm3.set_index('index')
    for c in [c for c in pm3_emp.columns if c in pm3.columns and c != 'id']:
        pm3[c] = pm3_emp[c].where(pm3_emp[c].notna(), pm3[c])
    pm3['commission_employee'] = pm3.payment_upsell*pm3.rto        # Doanh số ghi nhận cho Nhân viên bán tăng thêm
    pm3['commission_department'] = pm3.payment_upsell
    if pm3.shape[0] > 0: 
        fix_type(pm3, ['clineid', 'product_id', 'consultants', 'chinhanhphanquyen_fix'], 'Int64', np.nan)
        pm3['id_etl'] = pm3[ls_id].fillna(0).astype(str).agg('-'.join, axis=1)

    pm = pd.concat([pm, pm3], axis=0)
    print('2. BẢNG DỮ LIỆU THANH TOÁN, TÍNH GIÁ TRỊ GHI NHẬN THEO SỐ TIỀN THU' + '  ' + str(pm.shape))
    return pm

def get_pm():
    if 'pm' not in _CACHE: 
        _CACHE['pm'] = get_df_payment()
    return _CACHE['pm']
#df = get_df_service_info()
#df_pm = query_df(conn, query_pm)
#pm = get_df_payment(df_pm, df)


# TÍNH TỶ LỆ KPI CHO 2 TƯ VẤN VIÊN BAN ĐẦU

def get_df_org_consult():
    df = get_df()
    df_pm = get_df_cache('df_pm', dict_get_df, conn=conn)
    # Bảng Dữ liệu nhân viên
    stag_employees = get_df_cache('stag_employees', dict_get_df, conn=conn)
    se = stag_employees[stag_employees.user_id.fillna(0).ne(0)]\
        [['brand_etl', 'user_id', 'employee_code', 'employee', 'job', 'department', 'old_department_id', 'phongbanphanquyen',
            'group_job_id', 'group_job', 'team_id', 'team', 'company_id', 'company']].copy()
    se_col_rename = {'company_id': 'employee_company_id', 'company': 'company_employee', 'old_department_id': 'department_id'}
    se.rename(columns=se_col_rename, inplace=True)
    
    # df_lc: dữ liệu dịch vụ đã hủy (lọc trùng)
    df_lc = df.drop_duplicates(['brand_etl','clineid_cancel'])\
            [CLINEID + [c for c in df.columns if c not in df_pm.columns.append(se.columns) or c == 'crm_id']].copy()
    ls_col_rename = [c for c in df_lc if c[-7:] == '_cancel']
    df_clinec_columns = df_lc.columns
    for c in ls_col_rename:                                               # Gán thông tin dịch vụ Hủy là dịch vụ chính trong bảng
        if c[:-7] in df_clinec_columns:
            df_lc[c[:-7]] = df_lc[c]
        else:
            df_lc.rename(columns={c: c[:-7]}, inplace=True)
    
    # Nối thông tin 2 tư vấn viên của DV ban đầu
    df_lc = df_lc.merge(se, how='left', left_on=['brand_etl','consultants_1'], right_on=['brand_etl','user_id'])
    df_lc = df_lc.merge(se, how='left', left_on=['brand_etl','consultants_2'], right_on=['brand_etl','user_id'], suffixes=('', '_2'))
    
    # Kiểm tra điều kiện tư vấn viên là 1 Lễ tân tư vấn với 1 Chuyên viên tư vấn ccjob = df_lc[['job','job_2']].apply(concat_str, axis = 1)
    rlt = r'lễ tân|nhân viên kinh doanh dịch'
    rcv = r'chuyên viên (tư vấn|kinh doanh dịch vụ)'
    ccjob = df_lc.job.map(str) + df_lc.job_2.map(str)
    is_ccjob_recept = ccjob.apply(lambda r: check_substr(r, rlt)) & ccjob.apply(lambda r: check_substr(r, rcv))
    
    # Các loại dịch vụ Lễ tân - Chuyên viên được đồng ghi nhận 100% doanh số 
    r1 = r'(surgery|laser|spa|chiphi|card)'     
    is_sell_recept = df_lc.apply(lambda r: check_substr(r['his_service_type'], r1), axis = 1)
    
    '''
    1: Không có tư vấn viên 1
    2: Không có tư vấn viên 2
    3: Lễ tân - CVTV cùng tư vấn dịch vụ Phẫu thuật/Laser/Spa/Chi phí khác
    4: Nhân viên 1 tư vấn chính
    5: Nhân viên 2 tư vấn chính
    6: Các trường hợp còn lại: chia đôi DS 50/50
    '''
    dict_conditions = {1: df_lc.consultants_1.fillna(0).eq(0).astype(bool), 
                       2: df_lc.consultants_2.fillna(0).eq(0).astype(bool), 
                       3: is_ccjob_recept & is_sell_recept, 
                       4: df_lc.consulting_role_1.astype(str) == '5', 
                       5: df_lc.consulting_role_2.astype(str) == '5'}
    ls_conditions = list(dict_conditions.values())
    dict_rto = {1: [0,1], 2: [1,0], 3: [1,1], 4: [0.7,0.3], 5: [0.3,0.7]}          # Cặp hệ số lương của tư vấn 1, 2 
    ls_rto = list(dict_rto.keys())
    rto_key = np.select(ls_conditions, ls_rto)                                     # Kết quả check các trường hợp 1-6
    
    # Gán kết quả hệ số lương tư vấn 1, 2 vào cột rto
    ls_rto_1 = []
    ls_rto_2 = []
    
    for r in rto_key:
        t1 = dict_rto[r][0] if r in ls_rto else 0.5
        t2 = dict_rto[r][1] if r in ls_rto else 0.5
        ls_rto_1.append(t1)
        ls_rto_2.append(t2)
    
    df_lc['rto_1'] = [round(e, 1) for e in ls_rto_1]
    df_lc['rto_2'] = [round(e, 1) for e in ls_rto_2]
    
    print('3. BẢNG TÍNH TỶ LỆ KPI CHO 2 TƯ VẤN VIÊN BAN ĐẦU' + '  ' + str(df_lc.shape))
    return df_lc

def get_df_lc():
    if 'df_lc' not in _CACHE: 
        _CACHE['df_lc'] = get_df_org_consult()
    return _CACHE['df_lc']
#df_lc = get_df_lc()   #get_df_org_consult()
#df_lc[df_lc.crm_id.eq(2270307)]


# DOANH SỐ BỘ PHẬN TƯ VẤN ĐẦU TIÊN

def get_df_org_consult_commission():
    pm = get_pm()   #get_df_cache('df_pm', dict_get_df, conn=conn)
    df_lc = get_df_lc()
    
    # Dữ liệu thanh toán 
    use_cols = ['brand_etl', 'clineid_cancel'] + [c for c in pm.columns if c not in df_lc.columns]
    pm_rc = pm.drop_duplicates(['brand_etl', 'id', 'clineid'])[use_cols].copy()
    pm_lc = pm_rc.merge(df_lc, how='outer', on = ['brand_etl', 'clineid_cancel'], suffixes=('', '_lc'))
    pm_lc = pm_lc[~(pm_lc.consultants_3.fillna(0).ne(0) & pm_lc.consultants.eq(pm_lc.consultants_1_cancel.fillna(0)))] # Loại trừ Dòng dịch vụ nhập Tư vấn mở rộng ở TV3
    pm_lc['stage_product'] = 'cancel'
    col_replace = {'consultants_1': 'consultants', 'rto_1': 'rto'}

    for k, v in col_replace.items(): 
        pm_lc[v] = pm_lc[k]
    
    # Chia doanh số cho Bộ phận tư vấn ban đầu theo Tư vấn 1, 2
    pm_lc['commission_department'] = np.where(pm_lc.rto_1*pm_lc.rto_2.eq(1), pm_lc.payment_back_recept/2, pm_lc.payment_back_recept*pm_lc.rto_1)
    pm_lc['commission_department_2'] = pm_lc.payment_back_recept - pm_lc.commission_department
    pm_lc['commission_department_crm_line'] = np.nan
    
    # Dữ liệu tính cho Tư vấn 1
    pm_lc1 = pm_lc[pm_lc.consultants_1.fillna(0).ne(0) | pm_lc.consultants_2.fillna(0).eq(0)][[c for c in pm_lc.columns if c[-2:] != '_2']]        
    pm_lc1['ordinal_consult'] = 'consult_1'
    # Dữ liệu tính cho Tư vấn 2
    pm_lc2 = pm_lc[pm_lc.consultants_2.fillna(0).ne(0)].copy()
    pm_lc2['ordinal_consult'] = 'consult_2'
    col_2 = [c for c in pm_lc2 if c[-2:] == '_2']
    
    for c in col_2:
        pm_lc2[c[:-2]] = pm_lc2[c]
    pm_lc2.drop(columns=col_2, inplace = True)       
    
    # Nối data Tư vấn 1, Tư vấn 2 về 1 bảng (2 dòng)
    pm_lccc = pd.concat([pm_lc1, pm_lc2[pm_lc1.columns]], axis=0)
    pm_lccc = pm_lccc[pm_lccc.payment_back_recept.fillna(0).ne(0)]              # Chỉ lấy các bản ghi có doanh số trả về
    pm_lccc['employee'] = pm_lccc['employee'].str.title()
    pm_lccc['commission_employee'] = pm_lccc.payment_back_recept*pm_lccc.rto
    pm_lccc['phongbanphanquyen_fix'] = pm_lccc['phongbanphanquyen']             # Gán lại id phongbanphanquyen theo nhân viên Tư vấn DV hủy
    pm_lccc['is_recept_sell_org'] = True                                        # Phân loại bản ghi tính doanh số về BP tư vấn đầu
    
    # Tạo id bảng (duy nhất)
    fix_type(pm_lccc, ['clineid', 'product_id', 'consultants', 'chinhanhphanquyen_fix'], 'Int64', np.nan)
    pm_lccc['id_etl'] = join_cols(pm_lccc, ls_id, sep='-', na='0')
    print('4. BẢNG DOANH SỐ KPI TRẢ VỀ BỘ PHẬN TƯ VẤN TRƯỚC' + '  ' + str(pm_lccc.shape))
    return pm_lccc

#pm_lccc = get_df_org_consult_commission()
def get_pm_lccc():
    if 'pm_lccc' not in _CACHE: 
        _CACHE['pm_lccc'] = get_df_org_consult_commission()
    return _CACHE['pm_lccc']
#pm_lccc = get_pm_lccc()


def write_report_kpi_commission_upsell(): 
    #df_pm = query_df(conn, query_pm)
    #df = get_df_service_info()
    pm = get_pm()
    pm_lccc = get_pm_lccc()
    # Nối kết quả 2 Data: Doanh số của BP tư vấn đầu tiên và BP bán tăng thêm
    dfc = pd.concat([pm, pm_lccc[pm_lccc.columns.intersection(pm.columns)]], ignore_index=True)
    dfc['is_recept_sell_org'] = dfc['is_recept_sell_org'].astype('boolean')          # bool: NaN=True, boolean: keep <NA>
    TABLE = 'report_kpi_commission_upsell'
    WHERE_UD = f"brand_etl <> 0 AND payment_date >= '{LIMIT_START_DATE}' "
    replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    
    return 'HOÀN THÀNH! DỮ LIỆU BÁN TĂNG THÊM/MỞ RỘNG PHÒNG DỊCH VỤ'

#write_report_kpi_commission_upsell()


# <center><b><font size=5> BOOKING BÁN CHÉO DA LIỄU </size></b></center>


# DÒNG DỊCH VỤ CÁC BOOKING BÁN CHÉO DA LIỄU + NGÀY THỰC HIỆN DỊCH VỤ PHẪU THUẬT
qr_cdl = \
f'''
WITH cline AS 
(  --- Các Dòng dịch vụ Phẫu thuật + da liễu có Số tiền đã thu > 0 và không ở trạng thái Hủy
SELECT cline.id clineid, cline.crm_id, cline.his_service_type, cline.total, cline.total_received, cline.stage
    , cline.product_id 
	, ROW_NUMBER() OVER (PARTITION BY cline.crm_id, cline.his_service_type ORDER BY cline.id) AS rn_cline_crm
	, MAX(cline.write_date) OVER (PARTITION BY cline.crm_id) AS write_date      ---max_write_date_crml
	, cline.create_uid, cline.brand_etl 
FROM crm_line cline 
WHERE brand_etl = 1
	AND type_pricelist = 'service' AND his_service_type IN ('Surgery', 'Laser', 'Spa')
	AND stage <> 'cancel' AND total_received > 0 
ORDER BY cline.id
)
, hst as (  
--- Nối chuỗi check trong 1 Booking có tồn tại cả Dịch vụ Phẫu thuật + Da có doanh số
SELECT crm_id, STRING_AGG(CASE WHEN rn_cline_crm = 1 THEN cline.his_service_type END, ',') AS hst_agg
FROM cline GROUP BY crm_id
) 
, walkin AS (
--- Phiếu khám: Ngày thực hiện đầu tiên/DV phẫu thuật/Booking
SELECT sol.crm_line_id, MIN(sol.order_id) AS order_id, MIN(so.name) AS sale_order
	, MIN(smarw.name) AS walkin, MIN(smarw.service_date_start + interval '7 hours')::date AS service_date_start
	, sol.brand_etl 
FROM sale_order_line sol 
LEFT JOIN sale_order so ON sol.order_id = so.id AND so.brand_etl = 1
LEFT JOIN sh_medical_appointment_register_walkin smarw ON so.id = smarw.sale_order_id AND smarw.brand_etl = 1
WHERE sol.state <> 'cancel' AND smarw.flag_surgery 
	AND sol.brand_etl = 1 
	AND smarw.service_date_start IS NOT NULL 
GROUP BY sol.crm_line_id, sol.brand_etl 
)
--- KẾT QUẢ: Ngày thực hiện đầu tiên/Dòng dịch vụ phẫu thuật trong Booking có bán chéo Da liễu
SELECT cline.*, hst.hst_agg
	, w.sale_order, w.walkin, w.service_date_start 
	, INITCAP(se.employee) creator, se.job creator_job, se.department creator_department
    , CASE WHEN cline.his_service_type IN ('Laser', 'Spa') THEN TRUE END AS is_cross_sell 
FROM cline
LEFT JOIN hst ON cline.crm_id = hst.crm_id
LEFT JOIN view_stag_employees_user se ON cline.create_uid = se.user_id AND cline.brand_etl = se.brand_etl 
LEFT JOIN walkin w ON cline.clineid = w.crm_line_id AND cline.brand_etl = w.brand_etl 
WHERE cline.write_date >= '20250701'
	AND hst.hst_agg ~* '^Surgery,(Laser|Spa)'
'''
#dfdl = query_df(conn, qr_cdl)
#dfdl.shape

# LỊCH SỬ KHÁCH HÀNG MUA (THANH TOÁN) DA LIỄU GẦN NHẤT
qr_pm_dl = f'''\
WITH limit_crm AS (
--- Giới hạn các KH mới đến cửa trong 1 khoảng thời gian 
SELECT code_customer FROM crm_lead WHERE type_crm_id = 2 AND arrival_date >= '20250701' AND brand_id = 1
GROUP BY code_customer
)
, pm AS (
--- Lịch sử thanh toán dịch vụ Da liễu (trừ hoàn tiền)
SELECT spt.crm_id, spt.booking, spt.code_customer, spt.contact_name, spt.arrival_date::date, 
	spt.his_service_type, spt.product, spt.ma_phieu, spt.payment_date, spt.payment, 
	ROW_NUMBER() OVER (PARTITION BY crm_id, payment_date ORDER BY spt.ma_phieu) AS rn_pm_crm, 
	ROW_NUMBER() OVER (PARTITION BY crm_id) AS rn_crm, 
	spt.brand_etl 
FROM report_kpi_bonus_commission spt
LEFT JOIN limit_crm lc ON spt.code_customer = lc.code_customer 
WHERE spt.brand_id = 1 
	AND spt.payment > 0 
	AND spt.his_service_type IN ('Laser', 'Spa')
	AND lc.code_customer IS NOT NULL
--ORDER BY code_customer, payment_date, ma_phieu
) 
SELECT pm.*, 
	-- lag_pdate: Lùi ngày giao dịch gần nhất so với ngày giao dịch mới 
	LAG(pm.payment_date) OVER (PARTITION BY pm.code_customer ORDER BY pm.payment_date, pm.ma_phieu) AS spt_lag_pdate, 
    LAG(pm.ma_phieu) OVER (PARTITION BY pm.code_customer ORDER BY pm.payment_date, pm.ma_phieu) AS lag_ma_phieu
FROM pm 
WHERE rn_pm_crm = 1    --- Lọc trùng các Booking có nhiều dòng phiếu thanh toán/ngày 
'''

#dfpmdl = query_df(conn, qr_pm_dl)
#print(dfpmdl.shape)

def get_data_cdl(): 
    dfdl = query_df(conn, qr_cdl)
    
    # Dữ liệu thanh toán, điểu chỉnh lại Ngày giao dịch bán dịch vụ Da liễu gần nhất theo Booking
    dfpmdl = query_df(conn, qr_pm_dl)
    pm = dfpmdl.copy() 
    for d in ['arrival_date', 'spt_lag_pdate']: 
        pm[d] = pd.to_datetime(pm[d], errors='coerce')

    cdt_shift = (pm.arrival_date <= pm.spt_lag_pdate) & pm.code_customer.eq(pm.code_customer.shift(1)) # 2 Booking liền kề trùng ngày đến cửa + Thanh toán
    pm['lag_pdate'] = np.where(cdt_shift, pm.spt_lag_pdate.shift(1), pm.spt_lag_pdate)
    pm['lag_ma_phieu'] = np.where(cdt_shift, pm.lag_ma_phieu.shift(1), pm.lag_ma_phieu)
    pm = pm[pm.rn_crm.eq(1)]  #.reset_index(drop=True)  
    
    # Nối dữ liệu Booking bán chéo da liễu với thông tin lịch sử thanh toán liền kề
    df = dfdl.merge(pm[['crm_id', 'brand_etl', 'arrival_date', 'lag_pdate', 'lag_ma_phieu']], how='left', on=['crm_id', 'brand_etl'])
    df['diff_days'] = (df.arrival_date - df.lag_pdate).dt.days
    
    return df

#dfc = get_data_cdl()


def write_stag_crm_line_cross_sell():
    dfc = get_data_cdl()       # query_df(conn, qr_cdl)     dfdl.copy() 
    TABLE = 'stag_crm_line_cross_sell'

    replace_table_data(dfc, TABLE, conn=conn)
    return TABLE
    
#write_stag_crm_line_cross_sell()


# <center><b><font size=5> DOANH BÁN TEAM TƯ VẤN 3 THÀNH VIÊN - KANGNAM SÀI GÒN 666 </size></b></center>


qr_team = f'''\
WITH mth AS (SELECT '{LIMIT_START_DATE}'::date as m
), crm_limit as   
(   --- Booking có thanh toán trong thời gian giới hạn
SELECT crm_id, brand_etl FROM stag_payment_transfer spt 
WHERE brand_etl = 1 AND company_id = 4 AND payment_date >= (SELECT m FROM mth) 
GROUP BY crm_id, brand_etl 
), crm_refund as  
(   --- Data có cọc trước 10/04/2025 phát sinh hoàn tiền, trừ DS theo hệ số KPI cũ
SELECT kr.crm_id refund_crm_id, kr.clineid refund_clineid, 
	kr.consultants_1, kr.rto_1, kr.consulting_role_1, kr.consultants_2, kr.rto_2, kr.consulting_role_2,  
	row_number() OVER (PARTITION BY kr.crm_id, kr.clineid order by payment_date desc) as rn_cline_consult_rf
FROM report_kpi_bonus_commission kr
INNER JOIN (SELECT DISTINCT crm_id FROM stag_payment_transfer WHERE payment_date >= (SELECT m FROM mth) AND payment < 0) 
	l ON kr.crm_id = l.crm_id          --- Booking có hoàn tiền/điều chuyển giảm trong tháng
WHERE kr.payment_date <= '20250409' AND kr.first_date_payment <= '20250409'
), agg_consult AS  
(   --- data check Team tư vấn (không bao gồm BS phẫu thuật)
SELECT c.crm_id, c.clineid, c.brand_etl, 
	STRING_AGG(CASE WHEN c.job !~* 'bacsi|giamdoc|noikhoa|phauthuat' AND right(c.ordinal_consult, 1) in ('1','2','3') 
		THEN c.ordinal_consult end, ',' order by c.ordinal_consult) as agg_ordinal_consult, 
	STRING_AGG(c.consulting_role, ',' order by c.ordinal_consult) as agg_consulting_role
FROM stag_crm_consult_rel c
INNER JOIN crm_limit ON c.crm_id = crm_limit.crm_id AND c.brand_etl = crm_limit.brand_etl
GROUP BY c.crm_id, c.clineid, c.brand_etl 
)  --- data Thông tin, thanh toán nối Bảng danh sách tư vấn viên/dòng dịch vụ
--, pm as (
SELECT k.brand_etl, 
	k.id || '-' || k.clineid as id_clineid, 
    k.id, k.clineid, k.crm_id, k.ma_phieu, k.payment_date, k.contact_name, k.booking, k.arrival_date, 
    k.category_source_id, k.source_id, k.category_source, k."source", 
    k.product_id, k.his_service_type, k.product_category, k.product, 
	k.code_customer, k.first_date_payment, k.total_received_last, 
	c.ordinal_consult, c.consulting_role, c.consultants consultants_team, 
	agg_consult.agg_ordinal_consult, agg_consult.agg_consulting_role, 
	INITCAP(se.employee) AS employee, 
	k.payment, k.org_company, k.company, 
	k.company_id, k.orgcompany_id, k.brand_id, k.org_brand_id, 
	se.employee_code, se.group_job, se.department, se.company AS company_employee, 
    COALESCE(dm."result", '') AS consulting_role_name, 
    se.job, se.job_id, se.group_job_id, se.department_id, 
    sp.is_no_kpi, 
	CASE WHEN se.job_id in (2058,2131,2134,2166,2211,3205,3208,3233,3234,3235,4067,4179,4181,4182,4183,4213,4214,4269,4314,4315,4416,4418,4456,4457) THEN true END AS is_job_doctor, 
	CASE WHEN se.department ~* 'kinh doanh dịch|lễ tân' THEN true END AS is_department_kddv, 
	MAX(CASE WHEN se.department ~* 'kinh doanh dịch|lễ tân' THEN 1 end) OVER (PARTITION BY k.crm_id) as is_crm_kddv, 
	MAX(CASE WHEN se.department ~* 'kinh doanh dịch|lễ tân|sale' THEN 1 end) OVER (PARTITION BY k.crm_id, k.clineid) as is_line_kddv, 
	cr.*
FROM report_kpi_bonus_commission k 
INNER JOIN crm_limit ON k.crm_id = crm_limit.crm_id AND k.brand_etl = crm_limit.brand_etl
LEFT JOIN stag_crm_consult_rel c ON (k.crm_id, k.clineid, k.brand_etl) = (c.crm_id, c.clineid, c.brand_etl)
LEFT JOIN agg_consult ON (k.crm_id, k.clineid, k.brand_etl) = (agg_consult.crm_id, agg_consult.clineid, agg_consult.brand_etl)
LEFT JOIN view_stag_employees_user se ON c.consultants = se.user_id AND c.brand_etl = se.brand_etl 
LEFT JOIN (SELECT * FROM crm_refund WHERE rn_cline_consult_rf = 1) cr ON (k.crm_id, k.clineid) = (cr.refund_crm_id, cr.refund_clineid)
LEFT JOIN view_stag_products sp ON k.product_id = sp.product_id AND k.brand_etl = sp.brand_etl 
LEFT JOIN dm_convert_case_WHEN dm ON dm.table_name = 'crm_line' AND dm.column_name = 'consulting_role_1' 
		and c.consulting_role = dm.value_case_when AND dm.brand_etl = 1
---WHERE c.ordinal_consult IS NOT NULL and k.payment_date < '20250601'   
/* -- CHUYỂN TỪ ĐOẠN DƯỚI XỬ LÝ TRÊN PANDAS
), pm_rto as 
(   --- Tính hệ số lương KPI mới team KDDV KN666: 60-30-10
SELECT kpi.*, 
	CASE WHEN kpi.first_date_payment <= '20250409' AND kpi.total_received_last <= 2000*1000 AND kpi.payment < 0 
		THEN 'refund' ELSE 'newrto' END AS check_refund, 
	ROW_NUMBER() OVER (PARTITION BY kpi.brand_etl, kpi.crm_id, kpi.clineid, kpi.ordinal_consult) as rn_cline_consult, 
	CASE WHEN agg_ordinal_consult not like '%' || ordinal_consult || '%' THEN 0 
		WHEN agg_ordinal_consult similar to 'consult_[1-3](,support)*' THEN CASE WHEN ordinal_consult like 'consult__' THEN 1 ELSE 0 end
		WHEN agg_ordinal_consult like 'consult__,consult__' THEN 
			CASE WHEN COALESCE(agg_consulting_role, '3,3') in ('3,3', '2,2', '1,1', '5,5') THEN 0.5 
				WHEN kpi.ordinal_consult in ('consult_2', 'consult_3') THEN CASE WHEN kpi.consulting_role = '5' THEN 0.7 ELSE 0.3 end 
			end
		WHEN agg_ordinal_consult in ('consult_1,consult_2,consult_3', 'consult_1,consult_2,support') THEN 
			CASE WHEN kpi.is_job_doctor THEN 0
			WHEN kpi.ordinal_consult='consult_2' THEN CASE WHEN kpi.consulting_role = '5' THEN 0.6 ELSE 0.3 end
			WHEN kpi.ordinal_consult in ('consult_3', 'support') THEN 0.1 
			end
	END AS rto_cal
FROM pm kpi
WHERE payment_date >= (SELECT m FROM mth) AND is_crm_kddv = 1
    AND ordinal_consult ~* '^(consult)|(support)$'
), pm_rto_fix as 
(   --- Tính lại hệ số lương KPI đối với Hoàn tiền dịch vụ đặt cọc trước 01/04/2025 theo hệ số KPI cũ
SELECT *, 
	CASE WHEN check_refund = 'refund' THEN consultants_1 ELSE consultants_team END AS consultants, 
	CASE WHEN check_refund = 'refund' THEN p.rto_1 ELSE 
		COALESCE(p.rto_cal, 1 - SUM(CASE WHEN p.rn_cline_consult = 1 THEN p.rto_cal end) OVER (PARTITION BY p.brand_etl, p.crm_id, p.clineid))
	END AS rto, 
	CASE WHEN check_refund = 'refund' THEN consulting_role_1 ELSE consulting_role END AS consulting_role_kpi
FROM pm_rto p
WHERE (CASE WHEN check_refund = 'refund' THEN ordinal_consult = 'consult_1' ELSE true end)  
UNION ALL 
SELECT *, 
	consultants_2 as consultants, 
	rto_2 as rto, 
	consulting_role_2 as consulting_role_kpi
FROM pm_rto WHERE check_refund = 'refund' AND coalesce(consultants_2,0) <> 0 AND ordinal_consult = 'consult_2'
)   --- Kết quả đầu ra cuối cùng
SELECT kpi.*, sp.is_no_kpi,
	u.payment_back_recept, 
	CASE WHEN u.ma_phieu is not null AND u.is_exist_pdc = 1 THEN 
		CASE WHEN is_department_kddv THEN kpi.payment + u.payment_back_recept 
		ELSE kpi.payment - u.payment_back_recept end 
	END AS payment_fix_upsell, 
	se.employee_code employee_code_kpi, INITCAP(se.employee) employee_kpi, se.job job_kpi, se.department department_kpi, se.group_job group_job_kpi, 
	se.job_id, se.group_job_id, se.department_id, se.company company_employee, 
	kpi.company_id as chinhanhphanquyen, 
	1 as thuonghieuphanquyen
FROM pm_rto_fix kpi
LEFT JOIN view_stag_employees_user se ON kpi.consultants = se.user_id AND kpi.brand_etl = se.brand_etl 
LEFT JOIN view_stag_products sp ON kpi.product_id = sp.product_id AND kpi.brand_etl = sp.brand_etl 
LEFT JOIN (SELECT ma_phieu, clineid, MAX(payment_back_recept) payment_back_recept, MAX(CASE WHEN ma_phieu like 'PDC%' THEN 1 ELSE 0 end) as is_exist_pdc 
		FROM report_kpi_commission_upsell WHERE brand_id = 1 group by ma_phieu, clineid
		) u ON (kpi.ma_phieu, kpi.clineid) = (u.ma_phieu, u.clineid)
*/
'''

#print(qr_team)
#df_team_qr = query_df(conn, qr_team)
#df_team_qr.shape


# Hàm tính hệ số Tư vấn team 3 thành viên

list_of_list_consult = [['consult_1', 1493, '1', None], ['supporter', 1318, None, True]]

def cal_rto_team(lsols):
    dict_rto = {}
    ls_new = []
    for ls in lsols: 
        if (ls[-1] == True and ls[0] in ['consult_3']) or ls[0]=='supporter':   # BS PTTM tư vấn chuyên môn: loại trừ không tính KPI
            dict_rto[ls[0]] = 0
        else:
            ls_new.append(ls)                                     # lọc sang list mới (không xoá khi đang duyệt)
           
    lenls = len(ls_new)
    if lenls == 0:                                            # Không còn tư vấn viên tính KPI
        return dict_rto
    ls_role = ','.join(str(e[2]) for e in ls_new)
    if lenls == 1:                                           # Chỉ có 1 Tư vấn viên: 100% KPI
        dict_rto[ls_new[0][0]] = 1
    elif lenls >= 3:                                         # Có 3 tư vấn chia thứ tự 60-30-10 %KPI
        dict_rto[ls_new[0][0]] = 0.6 
        dict_rto[ls_new[1][0]] = 0.3
        dict_rto[ls_new[2][0]] = 0.1
    else: 
        if ls_role in ['3,3', '2,2', '1,1', '5,5']:            # Có 2 tư vấn có cùng vai trò tư vấn
            dict_rto[ls_new[0][0]] = 0.5
        else: 
            dict_rto[ls_new[0][0]] = 0.7 if str(ls_new[-1][2]) != '5' else 0.3
        dict_rto[ls_new[-1][0]] = round(1 - dict_rto[ls_new[0][0]], 1) 
    return dict_rto

#cal_rto_team(list_of_list_consult)


def get_data_consult_team(): 
    df_team_qr = query_df(conn, qr_team)    # 'SELECT * FROM temp_crm_group_team'
    dfups = query_df(conn, f'''SELECT * FROM report_kpi_commission_upsell WHERE payment_date >= '{LIMIT_START_DATE}' ''')

    # Kiểm tra Có phải phiếu hoàn tiền đã cọc trước ngày 09/04/2025 (hệ số trừ KPI áp dụng hệ số KPI cũ)
    dft = df_team_qr[df_team_qr.payment_date.ge(DATETRUNC)].copy()
    dft = dft[~dft.id.str.contains('178982-1')].copy()   # => Tạm xử lý id thừa (đã xóa trên ERP gốc)
    dft['check_refund'] = np.where(dft.first_date_payment.le(dt.date(2025,4,9)) & dft.total_received_last.le(2000*1000) & dft.payment.lt(0), 
                                   'refund', 'newrto')

    # Tính lại hệ số tư vấn team 3 thành viên: 60-30-10
    dft['ordinal_consult'] = dft.ordinal_consult.replace({'support': 'supporter'})
    dft['consultants'] = dft.consultants_team
    dft['rn_cline_consult'] = dft.groupby(CLINEID + ['ordinal_consult']).cumcount()+1
    
    df_rto = dft[dft.rn_cline_consult.eq(1)].sort_values('ordinal_consult').groupby(CLINEID)\
    .apply(lambda r: r[['ordinal_consult', 'consultants_team', 'consulting_role', 'is_job_doctor']].to_numpy().tolist())\
    .reset_index(name='consult_lists')
    df_rto['rto_cal'] = df_rto.consult_lists.apply(cal_rto_team)
    
    
    dft = dft.merge(df_rto, how='left', on=CLINEID)
    #dft = dft[dft.rto_cal.notnull()].copy()
    dft['rto'] = dft.apply(lambda r: r['rto_cal'].get(r['ordinal_consult']) if isinstance(r['rto_cal'], dict) else np.nan, axis=1)
    dft['rto'] = np.select([dft.check_refund.eq('newrto'), dft.ordinal_consult.eq('consult_1'), dft.ordinal_consult.eq('consult_2')], 
                           [dft.rto, dft.rto_1, dft.rto_2])

    # Gán dữ liệu các cột thiếu ở bảng đầu ra
    lse = ['employee_code', 'employee', 'job', 'group_job', 'department', 'consulting_role']
    for c in lse: 
        dft[c+'_kpi'] = dft[c]
    dft['chinhanhphanquyen_fix'] = dft.company_id 
    dft['thuonghieuphanquyen_fix'] = dft.brand_id
    
    ls_id_etl = ['id', 'clineid', 'product_id', 'consultants', 'ordinal_consult', 'chinhanhphanquyen_fix']
    dft['id_etl'] = join_cols(dft, ls_id_etl, sep='-', na='0')

    # Nối dữ liệu Điều chỉnh Doanh số Upsell, tính lại Doanh số thực ghi nhận cho Tư vấn viên
    dfups_gr = dfups.groupby(['ma_phieu', 'consultants'], as_index=False)[['is_recept_sell_org','payment_back_recept']].max()
    dftf = dft.merge(dfups_gr, how='left', on = ['ma_phieu', 'consultants'])
    dftf['payment_fix_upsell'] = dftf.payment + dftf.payment_back_recept.fillna(0).astype(float)*\
                            np.select([dftf.is_recept_sell_org.fillna(False).astype(bool), dftf.payment_back_recept.notnull()], \
                                      [1, -1], 0)
    if dftf.id_etl.is_unique:
        return dftf
    else: 
        print('!!! KIỂM TRA LẠI "id_etl" CỦA DỮ LIỆU BỊ TRÙNG')

#dft = get_data_consult_team()


def write_report_kpi_commission_group_team():
    dfc = get_data_consult_team()       # dftf.copy()    get_data_consult_team()
    TABLE = 'report_kpi_commission_group_team'
    WHERE_UD = f"brand_etl <> 0 AND payment_date >= '{LIMIT_START_DATE}' "
    replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    return TABLE
    
#write_report_kpi_commission_group_team()


def write_stag_crm_line_ecosystem(): 
    starttime = datetime.now()
    query_df(conn, 'CALL func_stag_crm_line_ecosystem();', ret='exe')
    endtime = datetime.now()
    print('Xử lý Bảng Dòng dịch vụ xét trong Hệ sinh thái: ', endtime - starttime, 's')
    return 'Done'


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'report_kpi_commission_upsell': write_report_kpi_commission_upsell,
    'stag_crm_line_cross_sell': write_stag_crm_line_cross_sell,
    'report_kpi_commission_group_team': write_report_kpi_commission_group_team,
    'stag_crm_line_ecosystem': write_stag_crm_line_ecosystem,
}


def main():
    parser = argparse.ArgumentParser(description='ETL report_kpi_upsell_cross')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
