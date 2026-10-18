# -*- coding: utf-8 -*-
"""
REPORT KPI TEAM AFTER SERVICE

Chuyen tu quentk_4_report_kpi_team_after_service.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/report_kpi_team_after_service_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python report_kpi_team_after_service.py --task start_task_1_report_kpi_team_after_service
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

DATE_SYS = dt.date(2025,8,1)
DATE_SYS_STR = DATE_SYS.strftime("%Y-%m-%d")


conn = 'dev'   # Dev trên Jupyterlab -> kết nối với Sandbox, đồng bộ lên Airflow -> Tự động connect "new_product"
_CACHE = {}          # nhớ kết quả trong một tiến trình task, tránh truy vấn lặp

DATETRUNC = (dt.date.today() - relativedelta(days=15)).replace(day=1)
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


_CACHE = {}          # nhớ kết quả trong một tiến trình task, tránh truy vấn lặp
dict_get_df = {'stag_products': 'SELECT * FROM stag_products', 
              'stag_employees': 'SELECT * FROM view_stag_employees_user', 
              'df_sdate': '''\
            SELECT s.brand_etl, s.form_id, s.partner_id, s.product_id, 
                smhco.name surgery_room, CASE WHEN s.ck_pttt LIKE 'PTT%' THEN services_date END AS surgery_date,
                walkinid, sale_order_id, employee_id, roleid, 
                rn_walkin_product_fix, 
                ROW_NUMBER() OVER (PARTITION BY s.brand_etl, s.partner_id, s.product_id ORDER BY s.services_date desc, rn_walkin_product_fix) AS rn_partner_pd_br 
            FROM stag_his_team_service s
            LEFT JOIN sh_medical_health_center_ot smhco on s.room = smhco.id and s.brand_etl = smhco.brand_etl 
            WHERE state_ckpttt = 'Done' AND rn_walkin_product_fix = 1 --- or roleid in (1,2) '''}


# Thông tin BS mổ chính, Nhân sự thực hiện chuyên môn trong Ekip 
df_sdate = query_df(conn, '''
SELECT s.brand_etl, s.form_id, s.partner_id, s.product_id, 
	smhco.name surgery_room, CASE WHEN s.ck_pttt LIKE 'PTT%' THEN services_date END AS surgery_date,
	walkinid, sale_order_id, employee_id, roleid, 
	rn_walkin_product_fix, 
	ROW_NUMBER() OVER (PARTITION BY s.brand_etl, s.partner_id, s.product_id ORDER BY s.services_date desc, rn_walkin_product_fix) AS rn_partner_pd_br 
FROM stag_his_team_service s
LEFT JOIN sh_medical_health_center_ot smhco on s.room = smhco.id and s.brand_etl = smhco.brand_etl 
WHERE state_ckpttt = 'Done' AND rn_walkin_product_fix = 1 --- or roleid in (1,2)
''') 


query = f'''
WITH forms AS (
--- Dữ liệu Tái khám - Phiếu lưu giới hạn theo Ngày khám/nhập viện 
SELECT s.*
, 	CASE WHEN s.id LIKE 'hp%'
		THEN DENSE_RANK() over (partition by s.inpatient_id, s.company_id order by date(s.services_date)) 
		ELSE NULL 
		END days_inpatient
, 	CASE WHEN s.id LIKE 'hp%'
		THEN ROW_NUMBER() over (PARTITION BY s.inpatient_id, s.product_id, date(s.services_date) ORDER BY s.form_id) /* order by services_date */
		ELSE ROW_NUMBER() over (PARTITION BY s.partner_id, s.product_id, date(s.services_date) ORDER BY s.form_id)
		END rn_inpt_product_date
, 	ROW_NUMBER() OVER (PARTITION BY s.id, s.service) rn_form_service
,   CASE WHEN s.id LIKE 'hp%'
		THEN ROW_NUMBER() OVER (PARTITION BY s.inpatient_id, s.product_id ORDER BY date(services_date), s.form_id)
    END AS rn_inpt_product
FROM stag_his_team_after_service s
WHERE state_form = 'Completed' AND s.brand_etl <> 0 --LEAST(services_date, admission_date) >= '20250801' 
    AND NOT (type_form = 'postoperative' AND inpatient_id IS NULL)
)
select shtas.*
, 	DATE(shtas.discharge_date) - DATE(shtas.admission_date) + 1 hospital_days
, 	vssoh.hospital_days_purchased
, 	rc.name company
, 	rc.brand_id
, 	cl.type_crm_id, cl.name booking, cl.code_customer, cl.contact_name, cl.source_id
, 	sp.surgery_type, sp.product_ctg_id, sp.his_service_type, sp.product_category, sp.product
,   MAX(CASE WHEN sp.surgery_type = 'major' THEN 1 ELSE 0 END) OVER (PARTITION BY shtas.walkinid) AS is_surgery_type_walkin_major
, 	sp.kpi_point, COALESCE(sp.kpi_point_num, 1) kpi_point_num
, 	ssos.uom_price product_uom_qty, ssos.uom_price, ssos.revenue 
, 	smtr.name "role" 
,	dmd."result" doctor_attitude
, 	dm."result" satisfaction_level
, 	MAX(days_inpatient) over (partition by shtas.brand_etl, inpatient_id) max_days_inpatient 
FROM forms shtas 
LEFT JOIN m_res_company rc ON shtas.company_id = rc.id AND rc.brand_etl = shtas.brand_etl 
LEFT JOIN crm_lead cl ON shtas.crm_id = cl.id AND cl.brand_etl = shtas.brand_etl
LEFT JOIN view_stag_products sp ON shtas.product_id = sp.product_id AND sp.brand_etl = shtas.brand_etl
LEFT JOIN sh_medical_team_role smtr ON shtas.roleid = smtr.id AND smtr.brand_etl = shtas.brand_etl
LEFT JOIN stag_sale_order_services ssos ON (shtas.sale_order_id, shtas.product_id, shtas.brand_etl) = (ssos.order_id, ssos.product_id, ssos.brand_etl)
-- View: Lay so ngay KH mua them dich vu nam luu 
LEFT JOIN view_stag_sale_order_hospitalize vssoh ON shtas.crm_id = vssoh.booking_id AND shtas.brand_etl = vssoh.brand_etl
LEFT JOIN dm_convert_case_WHEN dm ON shtas.satisfaction_level_id = dm.value_case_WHEN AND dm.column_name = 'satisfaction_level' AND dm.brand_etl = shtas.brand_etl
LEFT JOIN dm_convert_case_WHEN dmd ON shtas.doctor_attitude_id = dmd.value_case_WHEN AND dmd.column_name = 'doctor_attitude' AND dmd.brand_etl = shtas.brand_etl
WHERE shtas.services_date >= '{LIMIT_START_DATE}'
'''
#df_query = query_df(conn, query)
#df_query.shape


#df[['rn_inpt_product_date', 'days_inpatient', 'date_diff_admission_service', 'is_new_request', 'is_vip_form', 'company_id', 'hospital_days_purchased', 'surgery_room_type', 'is_surgery_type_walkin_major', 'drk_form_walkin', 'kpiuom']]


# HÀM XỬ LÝ ĐIỂM PHÒNG LƯU - HẬU PHẪU
def kpi_ppt(rn_inpt_product_date, days_inpatient, date_diff_admission_service, is_new_request, is_vip_form, 
            company_id, hospital_days_purchased, surgery_room_type, is_major_in_regimen, kpiuom):
    k = None
    pct = None
    post_knhn = check_substr(surgery_room_type, r'minor|surg') and company_id == 2 and days_inpatient > 1   # KH tiểu phẫu nằm ngoài chế độ, chỉ áp dụng KNHN
    if rn_inpt_product_date > 1:       # Check trùng dịch vụ chăm sóc cùng ngày => Không tính điểm
        pass 
    # Phân loại ngày nằm viện của KH
    elif days_inpatient == 1:                            # NGÀY ĐẦU CHĂM SÓC
        if is_new_request: 
            pct = 'new_request_return'                    # Khách hàng được chỉ định nằm thêm
        elif is_major_in_regimen: 
                pct = 'major_postoperative_in_regimen' 
        elif (hospital_days_purchased or -1) >= days_inpatient -1: 
            pct = 'hospital_days_purchased'               # Khách hàng mua thêm Số ngày nằm lưu
        elif is_vip_form == 1 and company_id != 2: 
            pct = 'is_vip' 
    elif company_id == 2 and days_inpatient >= 4:         # KH NẰM NGOÀI CHẾ ĐÔ, chỉ áp dụng tại KNHN
        pct = 'major_postoperative_out_regimen_knhn' if surgery_room_type == 'major' else 'new_request_return_knhn' if is_new_request else 'minor_postoperative_knhn' 
    
    # Tính điểm KPI 
    if pct == 'major_postoperative_in_regimen': 
        k = kpiuom
    elif pct in ['new_request_return', 'new_request_return_knhn', 'hospital_days_purchased', 'is_vip', 'major_postoperative_out_regimen_knhn', 'minor_postoperative_knhn']: 
        k = kpiuom/2
                   
    return [k, pct]
    
# LƯỢT TÁI KHÁM
''' 
20, 27, 29: Bac si tai kham phau thuat, spa, laser
// 28, 30, 32: Bac si chiu trach nhiem Spa, Laser, Nha
// 33: Dieu duong ho tro BS tai kham
// 34: BS Hoi chan
// 21, 23, 24, 25, 26: Dieu duong cat chi, thay bang, tbcc, tiem thuoc, truyen khang sinh
// 35: Dieu duong hut dich/rut ong
'''
ls_doctor = [20,27,29,34]
ls_nursing = [21,23,24,25,26]
ls_suction = [35]
allr = ls_nursing + ls_suction
def kpi_evl(roleid, aggroleid, rn_form_team, type_form, rn_form, rn_inpt_product_date, days_inpatient, is_new_request, pcus):
    if type_form == 'postoperative':                   # Luot khach nam luu
        if rn_form > 1 or rn_inpt_product_date > 1:
            p  = None
        elif days_inpatient > 2 or is_new_request:     # Nằm lưu từ ngày thứ 2 hoặc Chỉ định thêm
            p = 0.5
        else:
            p = 1
    elif rn_form_team != 1 or aggroleid == None:
        p = None
    else:
        p = pcus
    return p

ls_col_char = ['id', 'aggroleid', 'order_product_id', 'team_service_id', 'id_etl']


def get_data_after_service():
    starttime = datetime.now()
    
    stag_products = get_df_cache('stag_products', dict_get_df, conn)
    se = get_df_cache('stag_employees', dict_get_df, conn)
    df_sdate = get_df_cache('df_sdate', dict_get_df, conn)
    
    df = query_df(conn, query)       # df_query.copy()
    df = df[df.company_id.notnull()].copy()
    ls_col_int = [c for c in df.columns if check_substr(c, r'(id|phanquyen)$') and c not in ls_col_char]
    fix_type(df, ls_col_int, 'Int64', np.nan)
    
    # Cập nhật Thông tin Bác sĩ Phẫu thuật/Nhân sự thực hiện chính nếu không nhập trên Phiếu khảo sát
    sdatew = df[['brand_etl','walkinid','product_id']].merge(df_sdate, how='left', on = ['brand_etl','walkinid','product_id'])
    sdatep = df[['partner_id', 'product_id']].merge(df_sdate[df_sdate.rn_partner_pd_br.eq(1) & df_sdate.brand_etl.eq(0)], how='left', on=['partner_id', 'product_id'])
    
    df['doctor_surgery_id'] = df.main_doctor_surgery_id.fillna(sdatew.employee_id).fillna(sdatep.employee_id) 
    df['surgery_date'] = sdatew.surgery_date.fillna(sdatep.surgery_date).fillna(df.service_date_start)
    df['surgery_room'] = sdatew.surgery_room.fillna(sdatep.surgery_room)
    df['surgery_room_type'] = np.select([df.surgery_room.str.contains('tiểu', case=False).astype(bool), df.surgery_room.str.contains('đại ph', case=False).astype(bool), 
                                         df.surgery_room.str.contains('tái kh', case=False).astype(bool), df.surgery_room.str.contains('mổ', case=False).astype(bool), df.surgery_room.notnull()], 
                                        ['minor', 'major', 'evaluation', 'surgery', 'other'])
    
    df['is_doctor_surgery_evaluation'] = df.doctor_surgery_id.eq(df.employee_id)
    df['is_vip'] = df['product_id'].map(stag_products[stag_products.product_id.fillna(0).ne(0)].set_index('product_id')['is_vip'])
    df['is_vip'] = df['is_vip'].map({True: 1, False: 0})
    df['is_vip_form'] = df.astype(str).fillna('0').groupby(['inpatient_id', 'id'])['is_vip'].transform(max)
    
    # Số ngày chênh lệch từ Ngày thực hiện dịch vụ đến Ngày vào khoa nằm lưu hoặc Tái khám
    df['care_date'] = pd.to_datetime(np.where(df.type_form.eq('postoperative'), df.admission_date.dt.date, df.services_date.dt.date), errors='coerce')
    df['date_diff_admission_service'] = (df.care_date.dt.date - df.surgery_date.dt.date) #.dt.days
    df['date_diff_admission_service'] = df.date_diff_admission_service.apply(lambda d: str(d).split(' day')[0].replace('0:00:00', '0')).replace('nan',np.nan).astype('Int64')
    
    # Lấy Tên, mã NV các thành viên: BS mổ chính, phụ, BS chỉ định bảo hành, Điều dưỡng viên, thành viên tái khám...
    dict_emp = {'physician_id': 'physician', 'main_doctor_surgery_id': 'main_doctor', 'sub_doctor_surgery_id': 'sub_doctor', 
                'doctor_bh_id': 'doctor_bh', 'doctor_id': 'doctor_attending', 'doctor_surgery_id': 'doctor_surgery'}
    cols_se = ['brand_etl', 'id', 'employee_code', 'employee', 'job']
    df = df.merge(se[cols_se + ['department', 'company', 'phongbanphanquyen']]
                  .rename(columns={c: 'employee_'+c for c in ['id','job','department','company']}).rename(columns={'phongbanphanquyen': 'phongbanphanquyen_fix'}), 
                  how='left', on=['brand_etl', 'employee_id'])
    for ide, e in dict_emp.items(): 
        cols = cols_se if ide in ['employee_id', 'doctor_surgery_id'] else cols_se[:-1]
        df = df.merge(se[cols].rename(columns={'id':ide, 'employee_code': ('doctor_employee' if ide=='doctor_id' else 'physician_employee' if ide=='physician_id' else e) + '_code', 
                                               'employee': e, 'job': e+'_job'}), 
                      how='left', on=['brand_etl', ide])

    df['service_removeaccent'] = df['service'].apply(remove_accent).fillna('null')
    ls_id = ['id', 'product_id', 'employee_id', 'roleid', 'doctor_id', 'doctor_surgery_id', 'service_removeaccent']
    df['id_etl'] = df[ls_id].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1)
    df.drop_duplicates('id_etl', keep='first', inplace=True)
    
    df['services_date_date'] = df.services_date.dt.date
    df['rn_form_product_fix'] = df.sort_values(['rn_inpt_product_date', 'inpatient_id', 'services_date_date']).groupby(['id', 'product_id'])['product_id'].cumcount()+1
    
    # 1. TÍNH ĐIỂM PHÒNG LƯU
    df['rn_form_team'] = df.groupby(['id', 'form_team_id']).cumcount()+1
    df['rn_form_service'] = df.groupby(['id', 'service']).cumcount()+1
    df['type_form'] = np.select([df['id'].str[:2].eq('tk'), df['id'].str[:2].eq('hp')], \
                                ['evaluation', 'postoperative'], np.nan)
    
    df['service_agg'] = df.groupby("id")['service'].transform(lambda r: ", ".join(r.unique()))
    df['kpiuom'] = df.kpi_point*df.product_uom_qty
    # is_major_in_regimen: Số ngày nằm viện Đại phẫu trong chế độ 3 ngày
    df['is_major_in_regimen'] = (df.surgery_room_type.eq('major') | df.is_surgery_type_walkin_major.eq(1)) & df.date_diff_admission_service.le(3) & df.drk_form_walkin.eq(1)
    df['is_major_in_regimen'] = df['is_major_in_regimen'].fillna(False).astype(bool)
    # Áp dụng hàm tính điểm
    ls_cfunction = re.sub(r'[()]', '', str(inspect.signature(kpi_ppt))).split(', ')
    kpippt = df[ls_cfunction].apply(lambda r: kpi_ppt(*r), axis=1)
    df['kpi_perf_point'] = kpippt.str[0].astype(float)
    df['postoperative_care_type'] = kpippt.str[1]
    
    
    # 2. TÍNH LƯỢT KHÁM - KPI BÁC SĨ, ĐIỀU DƯỠNG
    dict_id = {'doctor': ls_doctor, 'nursing':  ls_nursing, 'suction': ls_suction}
    df['pcus'] = np.nan
    for k, v in dict_id.items():
        df['roleid_f'] = np.where(df.rn_form_team.eq(1) & df.roleid.isin(v), df.roleid.astype(str), np.nan) 
        df['count_' + k] = df.groupby([df.id, df.rn_form_team])['roleid_f'].transform('count')
        df['pcus'] = np.where(df.roleid.isin(v), 1/df['count_' + k], df.pcus)
    
    ls_cfunction = re.sub(r'[()]', '', str(inspect.signature(kpi_evl))).split(', ')
    df['kpi_perf_cus'] = df[ls_cfunction].apply(lambda r: kpi_evl(*r), axis=1)
    
    df['thuonghieuphanquyen_fix'] = df.brand_id
    df['chinhanhphanquyen_fix'] = df.company_id
    ls_col_int = [c for c in df.columns if check_substr(c, r'^(rn|drk|dr_)|id$|phanquyen|(hospital|max)_day') and c not in ls_col_char]
    fix_type(df, ls_col_int, 'Int64', np.nan)
    endtime = datetime.now()
    print('Xử lý Bảng KPI Tái khám, Hậu phẫu: ', endtime - starttime)
    return df

def get_df(): 
    if 'df' not in _CACHE: 
        _CACHE['df'] = get_data_after_service()
    return _CACHE['df']


def step1_write_after_service():
    # lấy dữ liệu
    dfc = get_df()           #step1_transform_commission()    df.copy()  get_data_after_service() 
    table = 'report_kpi_team_after_service'

    WHERE_UD = f'''brand_etl <> 0 AND services_date >= '{LIMIT_START_DATE}' '''
    replace_table_data(dfc, table, where=WHERE_UD, conn=conn)
    print('Hoàn thành: Cập nhật Data Hậu phẫu từ ngày ', LIMIT_START_DATE, ' _ ', str(dfc.shape[0]), ' bản ghi.')

    return 'report_kpi_team_after_service'

#step1_write_after_service()


# DATA KHÁCH HÀNG ĐẠI PHẪU KHÔNG NẰM LƯU
query_major = \
f'''
SELECT kpi.*, 
ROW_NUMBER() OVER (PARTITION BY kpi.brand_etl, kpi.walkinid) AS rn_form,
SPLIT_PART(kpi.order_product_id, '-', 2)::integer AS sale_order_id,
left(kpi.team_service_id::text, 4) AS type_form
FROM report_kpi_team_service kpi
LEFT join   --- id khách hàng có phiếu Chăm sóc hậu phẫu hoàn thành
    (SELECT partner_id FROM stag_his_team_after_service shtas
    WHERE type_form = 'postoperative' AND state_form = 'Completed' AND shtas.brand_etl in (0,1)
    GROUP BY partner_id) 
    vhii ON kpi.partner_id = vhii.partner_id 
WHERE kpi.roleid = 18 AND LOWER(room) LIKE '%đại phẫu%' AND vhii.partner_id IS NULL AND kpi.ck_pttt::text !~~ 'TK%'
AND kpi.brand_id = 1 AND brand_etl <> 0 
AND kpi.services_date >= '{LIMIT_START_DATE}'
'''

def get_data_major():
    if 'df_major' not in _CACHE:
        df_major = query_df(conn, query_major)
        df_major['kpi_perf_point'] = np.where(df_major.company_id.eq(2), df_major.kpi_perf_point, np.nan)
    
        dict_rename = {'ck_pttt': 'form_name', 'state_walkin': 'state_form', 'drk_walkin_product': 'rn_form_product', 'job': 'employee_job', 
                   'rto_kpi': 'kpi_perf_cus', 'thuonghieuphanquyen': 'thuonghieuphanquyen_fix'}
        dict_eq = {'rn_form_product': 'rn_form_product_fix', 'product_uom_qty': 'uom_price', 
               'services_date': 'service_date_start', 'services_date': 'surgery_date', 'room': 'surgery_room', 'team_service_id': 'id_etl'}
        df_major.rename(columns=dict_rename, inplace=True)
        for k, v in dict_eq.items():
            df_major[v] = df_major[k]
        _CACHE['df_major'] = df_major
    return _CACHE['df_major']

#df2 = get_data_major()


def get_data_after_service_end():
    df = get_df()
    df2 = get_data_major()
    #df3 = get_data_complain()
    dfc = pd.concat([df, df2], ignore_index=True).copy()    
    
    is_surcharge_service = dfc['product'].apply(lambda r: check_substr(r, r'nhóm |phụ (phí|thu)|hi ph|dịch vụ sửa lại|túi ngực'))
    agg_product_crm = dfc[~is_surcharge_service & dfc.crm_id.notnull()].groupby('crm_id')['product'].apply(lambda r: '\n'.join(r.dropna().unique()))
    agg_product_id = dfc[~is_surcharge_service].groupby('id')['product'].apply(lambda r: '\n'.join(r.dropna().unique()))
    dfc['rn_product_toconcat'] = np.where(is_surcharge_service, np.nan, dfc.groupby(['id', 'product_id'])['product_id'].cumcount()+1)
    dfc['count_product'] = dfc.groupby('id')['product_id'].transform('nunique')
    dfc['str_agg_product'] = np.where(dfc.count_product == 1, dfc['product'], dfc['id'].map(agg_product_id))
    dfc['str_agg_product'] = np.where(dfc.str_agg_product.isnull(), dfc['crm_id'].map(agg_product_crm), dfc.str_agg_product)
    
    ls_col_int = [c for c in dfc.columns if check_substr(c, r'phanquyen|type_guarantee|date_diff|days|^(drk|rn_|rnk|dr)|id$') and c not in ls_col_char]
    fix_type(dfc, ls_col_int, 'Int64', np.nan)
    return dfc

#dfc = get_data_after_service_end()


def step2_write_after_service_end():
    # lấy dữ liệu
    dfc = get_data_after_service_end()       #get_data_after_service_end()
    table = 'report_kpi_team_after_service_end'

    WHERE_UD = f'''brand_etl <> 0 AND services_date >= '{LIMIT_START_DATE}' '''
    replace_table_data(dfc, table, where=WHERE_UD, conn=conn)
    print('Hoàn thành: Cập nhật Báo cáo Hậu phẫu từ ngày ', LIMIT_START_DATE, ' _ ', str(dfc.shape[0]), ' bản ghi.')

    return 'BÁO CÁO HẬU PHẪU + TÁI KHÁM: report_kpi_team_after_service_end'

#step2_write_after_service_end()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'start_task_1_report_kpi_team_after_service': step1_write_after_service,
    'start_task_2_report_kpi_team_after_service_end': step2_write_after_service_end,
}


def main():
    parser = argparse.ArgumentParser(description='ETL report_kpi_team_after_service')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
