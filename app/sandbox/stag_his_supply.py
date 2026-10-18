# -*- coding: utf-8 -*-
"""
STAG HIS SUPPLY

Chuyen tu quentk_2_stag_his_supply.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/stag_his_supply_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python stag_his_supply.py --task task_stag_his_form_supply
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

DATE_SYS = dt.date(2025,8,1)
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


# Danh sách vật tư Hàm máng duy trì (Kết thúc liệu trình)
lsid_maint = [6255, 10936]
lsid_maint_str = ', '.join(str(e) for e in lsid_maint)

LAST_PARTNER_ID = 786601         # Giá trị partner_id cuối cùng trên hệ thống ERP trước tách, sau giá trị này id trùng giữa các TH
LAST_PATIENT = 164363            # Giá trị id bệnh nhân cuối cùng trên hệ thống ERP trước tách, sau giá trị này id trùng giữa các TH
LAST_CRM_ID = 2174164            # Giá trị crm_id cuối cùng trên hệ thống ERP trước tách, sau giá trị này crm_id trùng giữa các TH
LAST_WALKIN = 646777

TABLE_TEMP = 'temp_update_walkin_supply'           # Bảng tạm các Phiếu sử dụng VTTH, giới hạn cập nhật theo id Phiếu khám
LS_FORM = ('sh_medical_specialty', 'sh_medical_surgery', 'sh_medical_evaluation', 'sh_medical_patient_rounding', 'sh_medical_lab_test')
QR_LIMIT_WALKIN = '\nUNION\n'.join(f'''SELECT walkin, brand_etl FROM {table} WHERE brand_etl <> 0 AND write_date >= '{LIMIT_START_DATE}' ''' for table in LS_FORM)

QR_CREATE_TEMP = f'''\
DROP TABLE IF EXISTS {TABLE_TEMP}; 
CREATE TABLE {TABLE_TEMP} AS (\n{QR_LIMIT_WALKIN}\n); 
CREATE INDEX ON {TABLE_TEMP} (walkin, brand_etl);
ANALYZE {TABLE_TEMP}; \
'''


TABLE_TEMP_ODO = 'temp_update_patient_odo'         # Bảng tạm Khách hàng có Phiếu khám gần đây => Giới hạn update thông tin Liệu trình chỉnh nha
QR_TEMP_ODO = f'''\
DROP TABLE IF EXISTS {TABLE_TEMP_ODO} ;
CREATE TABLE {TABLE_TEMP_ODO} AS (
SELECT DISTINCT brand_etl, patient
FROM stag_his_form_supply_2 shfs 
WHERE shfs.services_date >= '{LIMIT_START_DATE_3M}'
); '''
    

# Hàm tạo lại id duy nhất xuyên suốt lịch sử khách hàng từ Brand_etl = 0 trở đi, id lấy kiểu string
def newid_br(df, field='crm_id', last_value=0):
    val = pd.to_numeric(df[field], errors='coerce').astype('Int64')
    brand = pd.to_numeric(df['brand_etl'], errors='coerce').astype('Int64')
    is_new = val.gt(last_value).fillna(False) & ~brand.fillna(0).isin([0, 4, 7])
    char = val.astype('string')
    df['char_' + field] = char.where(~is_new, char + '-' + brand.astype('string'))
    return df

# Convert chênh lệch thời gian sang text hiển thị 'x năm, y tháng, z ngày'
def conv_dt_to_text(start_time, end_time):
    if pd.isna(start_time) or pd.isna(end_time):      # NaT/None: bỏ qua, không in cảnh báo cho từng dòng
        return None
    delt = relativedelta(end_time, start_time)
    parts = (('năm', delt.years), ('tháng', delt.months), ('ngày', delt.days))
    text = ', '.join(f'{v} {k}' for k, v in parts if v > 0)
    return text 


# CẬP NHẬT BẢNG VẬT TƯ TIÊU HAO ĐÃ HOÀN THÀNH
QUERY_SP = \
f'''
WITH supplies AS 
(
SELECT 'specialty' AS data_union, smss.id, smss.name, smss.qty, smss.uom_id, smss.supply, smss.notes, smss.qty_used, 
    smss.location_id, smss.medicament_type, smss.sequence, smss.create_uid, smss.create_date, smss.write_uid, smss.write_date, 
    smss.picking_id, smss.explanation_supply, sms.id AS form_id, sms.name AS form_name, sms.services_date, sms.patient, 
    ssssr.sh_medical_health_center_service_id AS service_id, sms.walkin AS walkinid, 
    sms.department AS ward_id, sms.perform_room AS room_id, sms.institution, sms.department_type, 
    ssssr.brand_etl
FROM sh_surgery_specialty_service_rel ssssr
 LEFT JOIN sh_medical_specialty_supply smss ON ssssr.sh_medical_specialty_supply_id = smss.id and ssssr.brand_etl = smss.brand_etl 
 LEFT JOIN sh_medical_specialty sms ON smss.name = sms.id and sms.brand_etl = smss.brand_etl 
 INNER JOIN {TABLE_TEMP} tmp ON (sms.walkin, sms.brand_etl) = (tmp.walkin, tmp.brand_etl) 
WHERE sms.state = 'Done' AND ssssr.brand_etl <> 0 
UNION ALL
SELECT 'surgery' AS data_union, smssurg.id, smssurg.name, smssurg.qty, smssurg.uom_id, smssurg.supply, smssurg.notes, smssurg.qty_used, 
    smssurg.location_id, smssurg.medicament_type, smssurg.sequence, smssurg.create_uid, smssurg.create_date, smssurg.write_uid, smssurg.write_date, 
    smssurg.picking_id, smssurg.explanation_supply, smsurg.id AS form_id, smsurg.name AS form_name, smsurg.surgery_date AS services_date, smsurg.patient, 
    ssurgssr.sh_medical_health_center_service_id AS service_id, smsurg.walkin AS walkinid, 
    smsurg.department AS ward_id, smsurg.operating_room AS room_id, smsurg.institution, smsurg.department_type, 
    ssurgssr.brand_etl 
FROM sh_surgery_supply_service_rel ssurgssr
 LEFT JOIN sh_medical_surgery_supply smssurg ON ssurgssr.sh_medical_surgery_supply_id = smssurg.id and ssurgssr.brand_etl = smssurg.brand_etl 
 LEFT JOIN sh_medical_surgery smsurg ON smssurg.name = smsurg.id and smssurg.brand_etl = smsurg.brand_etl 
 INNER JOIN {TABLE_TEMP} tmp ON (smsurg.walkin, smsurg.brand_etl) = (tmp.walkin, tmp.brand_etl) 
WHERE smsurg.state = 'Done' AND ssurgssr.brand_etl <> 0 
UNION ALL
 SELECT 'evaluation' AS data_union, smes.id, smes.name, smes.qty, smes.uom_id, smes.supply, smes.notes, smes.qty_used, 
     smes.location_id, smes.medicament_type, smes.sequence, smes.create_uid, smes.create_date, smes.write_uid, smes.write_date, 
     smes.picking_id, NULL AS explanation_supply, sme.id AS form_id, sme.name AS form_name, sme.evaluation_start_date AS services_date, sme.patient, 
     sessr.sh_medical_health_center_service_id AS service_id, sme.walkin AS walkinid, 
     sme.ward AS ward_id, sme.room AS room_id, sme.institution, 'Surgery'::character varying AS department_type, 
     sessr.brand_etl 
FROM sh_evaluation_supply_service_rel sessr
 LEFT JOIN sh_medical_evaluation_supply smes ON sessr.sh_medical_evaluation_supply_id = smes.id and sessr.brand_etl = smes.brand_etl 
 LEFT JOIN sh_medical_evaluation sme ON smes.name = sme.id and sme.brand_etl = smes.brand_etl 
 INNER JOIN {TABLE_TEMP} tmp ON (sme.walkin, sme.brand_etl) = (tmp.walkin, tmp.brand_etl) 
WHERE sme.state = 'Completed' AND sessr.brand_etl <> 0 
UNION ALL
SELECT 'inpatient' AS data_union, smprm.id, smprm.name, smprm.qty, smprm.uom_id, smprm.supply, smprm.notes, smprm.qty_used, 
    smprm.location_id, smprm.medicament_type, smprm.sequence, smprm.create_uid, smprm.create_date, smprm.write_uid, smprm.write_date, 
    smprm.picking_id, smprm.explanation_supply, smpr.id AS form_id, smpr.name AS form_name, smpr.evaluation_end_date AS services_date, smpr.patient, 
    srsr.sh_medical_health_center_service_id AS service_id, 
    smpr.walkin AS walkinid, smi.ward AS ward_id, smi.room AS room_id, smi.institution, 'Surgery'::character varying AS department_type, 
    smprm.brand_etl 
FROM ( SELECT id, name, qty, uom_id, medicine AS supply, notes, qty AS qty_used, 
    location_id, medicament_type, sequence, create_uid, create_date, write_uid, write_date, 
    picking_id, dosage AS explanation_supply, brand_etl 
    FROM sh_medical_patient_rounding_medicines WHERE brand_etl <> 0) smprm
 LEFT JOIN sh_medical_patient_rounding smpr ON smpr.id = smprm.name and smpr.brand_etl = smprm.brand_etl 
 INNER JOIN {TABLE_TEMP} tmp ON (smpr.walkin, smpr.brand_etl) = (tmp.walkin, tmp.brand_etl) 
 LEFT JOIN sh_rounding_service_rel srsr ON srsr.sh_medical_patient_rounding_id = smpr.id and srsr.brand_etl = smpr.brand_etl 
 LEFT JOIN sh_medical_inpatient smi ON smi.id = smpr.inpatient_id and smi.brand_etl = smpr.brand_etl 
WHERE smpr.state = 'Completed' 
UNION ALL
SELECT 'lab_test' AS data_union, smltm.id, smltm.name, smltm.qty, smltm.uom_id, smltm.supply, smltm.notes, smltm.qty_used, 
    smltm.location_id, smltm.medicament_type, smltm.sequence, smltm.create_uid, smltm.create_date, smltm.write_uid, smltm.write_date, 
    smltm.picking_id, smltm.explanation_supply, smlt.id AS form_id, smlt.name AS form_name, smlt.date_done AS services_date, smlt.patient, 
    NULL::integer AS service_id, smlt.walkin AS walkinid, 
    smlt.department AS ward_id, smlt.perform_room AS room_id, smlt.institution, 'Surgery'::character varying AS department_type, 
    smltm.brand_etl 
FROM ( SELECT id, lab_test_id AS name, init_quantity AS qty, uom_id, product_id AS supply, notes, quantity AS qty_used, 
    location_id, medicament_type, sequence, create_uid, create_date, write_uid, write_date, 
    picking_id, '' AS explanation_supply, brand_etl 
    FROM sh_medical_lab_test_material WHERE brand_etl <> 0) smltm
 LEFT JOIN sh_medical_lab_test smlt ON smlt.id = smltm.name and smlt.brand_etl = smltm.brand_etl 
 INNER JOIN {TABLE_TEMP} tmp ON (smlt.walkin, smlt.brand_etl) = (tmp.walkin, tmp.brand_etl) 
WHERE smlt.state = 'Completed' 
)
SELECT  s.brand_etl, s.data_union, s.id form_supply_id, 
    row_number() over (partition by s.brand_etl, s.data_union, s.id) as rn_form_supply, 
    s.name form_id, cl.id crm_id, cl.name booking, smarw.name walkin, cl.code_customer , cl.contact_name , s.form_name, s.services_date, 
    case when smm.medicament_type = 'Medicine' then 'THUỐC' else 'VẬT TƯ' end supply_category,  
    smm.name_use supply, uu.name uom , s.uom_id, s.qty_used, s.qty, s.explanation_supply, s.notes, sl.name "location", 
    slw.name location_department, s.department_type, smhc.company_name company,  
    s.service_id, smhcs.product_id , s.supply supply_id, smm.product_id supply_product_id,  
    s.patient, s.walkinid, s.ward_id, s.room_id, s.location_id, s.picking_id, sp3."name" picking, smhc.his_company company_id
FROM supplies s
LEFT JOIN sh_medical_medicines smm ON s.supply = smm.id and smm.brand_etl = s.brand_etl 
LEFT JOIN uom_uom uu ON s.uom_id = uu.id and s.brand_etl = uu.brand_etl 
LEFT JOIN sh_medical_appointment_register_walkin smarw ON s.walkinid = smarw.id and smarw.brand_etl = s.brand_etl
LEFT JOIN sh_medical_health_center smhc ON s.institution = smhc.id and smhc.brand_etl = s.brand_etl 
LEFT JOIN sh_medical_health_center_ward smhcw ON s.ward_id = smhcw.id and smhcw.brand_etl = s.brand_etl
LEFT JOIN sh_medical_health_center_ot smhco ON s.room_id = smhco.id and smhco.brand_etl = s.brand_etl
LEFT JOIN stock_location sl ON s.location_id = sl.id and sl.brand_etl = s.brand_etl
LEFT JOIN stock_location slw ON sl.location_id = slw.id and slw.brand_etl = sl.brand_etl 
LEFT JOIN crm_lead cl ON smarw.booking_id = cl.id and smarw.brand_etl = cl.brand_etl 
LEFT JOIN sh_medical_health_center_service smhcs ON s.service_id = smhcs.id and smhcs.brand_etl = s.brand_etl
LEFT JOIN stock_picking sp3 ON sp3.id = s.picking_id and sp3.brand_etl = s.brand_etl  
'''

#dfsp = query_df(conn, QUERY_SP)

def write_stag_his_form_supply():
    query_df(conn, QR_CREATE_TEMP, ret='exe')   # Tạo bảng tạm giới hạn dữ liệu Update: các Phiếu khám có phiếu con dùng VTTH hoàn thành trong LIMIT_DATE
    
    dfc = query_df(conn, QUERY_SP)     #query_df(conn, QUERY_SP)     dfsp.copy()
    #dfc['services_date'] = dfc['services_date'] + pd.Timedelta(hours=7)
    ls_col_int = [c for c in dfc.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|id$|patient|_id_') and c != 'id']
    fix_type(dfc, ls_col_int, 'Int64', np.nan)
    
    lsid = ['data_union', 'form_supply_id', 'product_id', 'supply_product_id', 'brand_etl']
    dfc['id_etl'] = dfc[lsid].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1)
    
    TABLE = 'stag_his_form_supply_2'
    WHERE_UD = f'''(walkinid, brand_etl) IN (SELECT walkin, brand_etl FROM {TABLE_TEMP})'''
    replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    print(f'HOÀN THÀNH! CẬP NHẬT BẢNG VẬT TƯ TIÊU HAO SỬ DỤNG TỪ NGÀY {LIMIT_START_DATE}')
    
    return TABLE

#write_stag_his_form_supply()
#print(QUERY_SP)


def get_lsid_brackets_str():
    if 'brackets' not in _CACHE:
        brakets = query_df(conn, 'SELECT product_id FROM import_product_supply_braket WHERE is_braket')   # Bảng lưu trữ vật tư mắc cài
        _CACHE['brackets'] = ', '.join(str(int(e)) for e in brakets['product_id'].dropna())               # Nối các id mắc cài thành string cách nhau = dấu ","
    return _CACHE['brackets']


# DATA CHECK NGÀY BẮT ĐẦU LIỆU TRÌNH CHỈNH NHA
# List danh sách cột sử dụng lấy từ Bảng vật tư tiêu hao
LS_COL_SUPPLY = ['brand_etl', 'data_union', 'patient', 'walkinid', 'form_id', 'supply', 'qty_used', 'uom', 
                 'crm_id', 'product_id', 'supply_product_id', 'services_date', 'rn_form_supply']
LS_COL_SUPPLY_STR = ', s.'.join(str(c) for c in LS_COL_SUPPLY)

start_treat_fm = f'''
WITH sups AS (
--- Check các Phiếu Chuyên khoa sử dụng vật tư là Mắc cài (bắt đầu Chỉnh nha)
SELECT s.{LS_COL_SUPPLY_STR} 
FROM stag_his_form_supply_2 s
INNER JOIN {TABLE_TEMP_ODO} tmp ON (s.patient, s.brand_etl) = (tmp.patient, tmp.brand_etl)
WHERE s.data_union = 'specialty' AND s.brand_etl <> 0  
    AND s.supply_product_id IN ({get_lsid_brackets_str()})  
)
SELECT s.*, 
    smarw.partner_id, 
    COALESCE(smarw.info_diagnosis, smarw.note) AS info_diagnosis, 
    sp.product_category, sp.product
FROM sups s
LEFT JOIN sh_medical_appointment_register_walkin smarw ON s.walkinid = smarw.id AND s.brand_etl = smarw.brand_etl 
LEFT JOIN view_stag_products sp ON s.product_id = sp.product_id AND s.brand_etl = sp.brand_etl 
'''

def get_df_start(): 
    dfbr0 = query_df(conn, f'''\
                    SELECT 'specialty' as data_union, s.* 
                    FROM stag_walkin_booking_treatment_odo s
                    INNER JOIN {TABLE_TEMP_ODO} tmp ON (s.patient, s.brand_etl) = (tmp.patient, tmp.brand_etl)
                    WHERE s.brand_etl = 0''')
    dfbr0['rn_form_supply'] = pd.NA
    for charcol in ['patient','partner_id','crm_id','walkinid']: 
        dfbr0['char_'+charcol] = pd.to_numeric(dfbr0[charcol], errors='coerce').astype('Int64').astype('string')

    df_braces = query_df(conn, start_treat_fm)
    df_braces.rename(columns={'supply_product_id': 'bracket_product_id', 'supply': 'bracket'}, inplace=True)
    # Tạo lại các id không trùng giữa các thương hiệu
    df_braces = newid_br(df_braces, 'patient', LAST_PATIENT)
    df_braces = newid_br(df_braces, 'partner_id', LAST_PARTNER_ID)
    df_braces = newid_br(df_braces, 'crm_id', LAST_CRM_ID)
    df_braces = newid_br(df_braces, 'walkinid', LAST_WALKIN)

    # Xác định Ngày gắn mắc cài đầu tiên và Lần sử dụng VT mắc cài
    df_start = pd.concat([dfbr0[df_braces.columns], df_braces])
    df_start.sort_values(['crm_id','services_date'], inplace=True)
    df_start['services_date_treat_start'] = df_start.groupby('char_crm_id')['services_date'].transform(min)
    df_start['drk_crm_braket'] = df_start.groupby(['char_partner_id', 'product_id'])['services_date'].transform('rank', method='dense')
    df_start['rn_walkin'] = df_start.groupby('char_walkinid').cumcount()+1

    return df_start
    
#df_start = get_df_start()


dict_get_df = {'hdt': f'''\
--- Check các Phiếu Chuyên khoa sử dụng vật tư Hàm duy trì (Áp dụng phiếu thực hiện trước 01/06/2024)
SELECT brand_etl, data_union, s.patient, walkinid, form_id, supply, qty_used, uom, crm_id, product_id, supply_product_id, 
    services_date, rn_form_supply, 2 as order_check_end_treat
FROM stag_his_form_supply_2 s
INNER JOIN (SELECT DISTINCT patient FROM {TABLE_TEMP_ODO}) p ON s.patient = p.patient
WHERE data_union = 'specialty' AND supply_product_id IN ({lsid_maint_str}) AND services_date < '20240601'
--- Từ data Hàm duy trì, đánh số dòng để lọc trùng bệnh nhân + Đánh số ngày sử dụng đầu tiên
''', 
               'walkin_end': f'''\
SELECT DISTINCT ON (sms.brand_etl, sms.walkin) sms.brand_etl, sms.walkin walkinid, smarw.booking_id AS crm_id, 
	sms.patient, smarw.partner_id, sms.booking_id, smarw.service_date_start AS services_date, 
	COALESCE(smarw.info_diagnosis, smarw.note) as info_diagnosis, 
	smarw.have_line_done, 
	smarw.state, 
	sp.product_id,  
	--ROW_NUMBER() OVER (PARTITION BY smarw.booking_id, sp.product_id ORDER BY smarw.service_date_start, smarw.walkinid) AS rn_booking_product, 
	CASE WHEN have_line_done = 'yes' THEN 1 ELSE 3 END AS order_check_end_treat
FROM sh_medical_specialty sms 
INNER JOIN {TABLE_TEMP_ODO} tmp ON (sms.patient, sms.brand_etl) = (tmp.patient, tmp.brand_etl)
LEFT JOIN sh_medical_appointment_register_walkin smarw ON sms.walkin = smarw.id and sms.brand_etl = smarw.brand_etl 
LEFT JOIN sh_walkin_service_rel swsr ON smarw.id = swsr.walkin_id and swsr.brand_etl = smarw.brand_etl
LEFT JOIN sh_medical_health_center_service smhcs ON swsr.service_id = smhcs.id AND swsr.brand_etl = smhcs.brand_etl
LEFT JOIN view_stag_products sp ON smhcs.product_id = sp.product_id AND smhcs.brand_etl = sp.brand_etl 
LEFT JOIN crm_sync_brand csb ON smarw.booking_id = csb.crm_id and smarw.brand_etl = csb.brand_etl 
WHERE smarw.state = 'Completed' AND sp.product_ctg_id IN (263,274,280,1741,1742,1743)
 	AND sms.department_type = 'Odontology' AND sms.state = 'Done' 
    AND (smarw.have_line_done = 'yes' OR (smarw.info_diagnosis ~* '^(tháo|thó|thao) (mc|mắc cài|mac cai)' and smarw.service_date_start <= '20240508' ))
'''
}


# Lấy dữ liệu phiếu khám, Ngày bắt đầu - kết thúc liệu trình Dịch vụ/Booking
def proc_data_treatment():
    query_df(conn, QR_TEMP_ODO, ret='exe')   # Tạo bảng tạm giới hạn dữ liệu Update: Khách hàng có Phiếu khám gần đây
    
    # Data Phiếu khám có vật tư mắc cài (Bắt đầu liệu trình) 
    df_start = get_df_start()
    df_walkin_end = get_df_cache('walkin_end', dict_get_df, conn)
    newid_br(df_walkin_end, 'patient', LAST_PATIENT)
    
    df_hdt = get_df_cache('hdt', dict_get_df, conn)                  # Dữ liệu Ngày kết thúc liệu trình theo Vật tư sử dụng Hàm duy trì trước 01/06/2024
    newid_br(df_hdt, 'patient', LAST_PATIENT)
    df_hdt['rn_patient'] = df_hdt.sort_values('services_date').groupby('patient').cumcount()+1
    
    # Nối Data thông tin Ngày kết thúc liệu trình đầu tiên/Dịch vụ theo Phiếu khám hoặc Vật tư Hàm duy trì
    df_end = df_walkin_end.merge(df_hdt[df_hdt.rn_patient.eq(1)], how='left', on='char_patient', suffixes=('','_hdt')).copy()
    df_end = newid_br(df_end, 'crm_id', LAST_CRM_ID)
    df_end = newid_br(df_end, 'patient', LAST_PATIENT)
    df_end.sort_values(['char_patient', 'char_crm_id', 'product_id', 'services_date', 'walkinid'], inplace=True) 
    df_end['rn_patient'] = df_end.groupby('char_patient').cumcount()+1 
    df_end['rn_booking_product'] = df_end.groupby(['char_crm_id', 'product_id']).cumcount()+1 
    df_end = df_end[df_end.rn_booking_product.eq(1)]
    for field in list(c for c in df_end if c[-4:] == '_hdt'): 
        df_end[field[:-4]] = df_end[field[:-4]].fillna(df_end[field])

    dict_ord = {1: 'have_line_done', 2: 'ham_duy_tri', 3: 'info_diagnosis'}
    df = df_start.merge(df_end[df_end.rn_patient.eq(1)].rename(columns={'services_date':'services_date_treat_end'}), 
                        how='outer', on='char_patient', suffixes=('', '_end'))

    cols_fillna = ['brand_etl', 'partner_id', 'walkinid', 'form_id', 'info_diagnosis', 'crm_id', 'product_id']       # Điền các cột null sau merge
    for c in cols_fillna: 
        df[c] = df[c].fillna(df[c + '_end'])
    df['services_date'] = df.services_date.fillna(df.services_date_treat_end)
    df['type_form'] = np.select([df.data_union.notnull(), df.order_check_end_treat.eq(2)], [df.data_union, 'specialty'], 'walkin')
    df['crm_id'] = df.crm_id.fillna(df.booking_id)

    # Xác định Ngày bắt đầu và kết thúc liệu trình Chỉnh nha
    # Ngày bắt đầu: ngày đầu tiên sử dụng vật tư Mắc cài
    # Ngày kết thúc: ngày đầu tiên phiếu khám thực hiện dịch vụ/Booking "Có" Dịch vụ kết thúc HOẶC giao hàm duy trì
    df['services_date_treat_start'] = df.groupby('char_crm_id')['services_date_treat_start'].transform('min')
    df['services_date_treat_end'] = df.groupby('char_patient')['services_date_treat_end'].transform('min')
    df['check_end_treat'] = df.groupby('patient')['order_check_end_treat'].transform('min')
    df['check_end_treat'] = df.check_end_treat.replace(dict_ord)                    # Loại data check kết thúc LT
    for m in ['min', 'max']: 
        df['services_date_'+m] = df.groupby('crm_id')['services_date'].transform(m)
    
    # Khoảng thời gian thực hiện liệu trình, theo Số ngày và Thông tin dạng text
    df['treatment_time_text'] = df[['services_date_treat_start', 'services_date_treat_end']].apply(lambda r: conv_dt_to_text(*r), axis=1)
    df['treatment_time_days'] = (df['services_date_treat_end'] - df['services_date_treat_start']).dt.days
    
    # UTC +7h
    for c in [c for c in df.columns if '_date' in c]:
        df[c] = df[c] + timedelta(hours=7)

    df.sort_values(['char_crm_id', 'product_id', 'bracket_product_id', 'services_date_treat_start', 'services_date_treat_end'], inplace=True)
    dict_rn = {'rn_crm_product': ['crm_id', 'product_id'], 'rn_crm_id': 'crm_id', 'rn_partner_product ':['patient', 'product_id']}
    for c, gr in dict_rn.items():
        df[c] = df.groupby(gr).cumcount()+1
    ls_col_int = [c for c in df.columns if check_substr(c, r'^(drk|rn_|rnk|order)|id|days|brand_etl')] + ['patient']

    df = df[df.brand_etl.gt(0) & df.services_date.dt.date.ge(dt.date(2025,12,1))]
    fix_type(df, ls_col_int, 'Int64', np.nan)
    df['id_etl'] = df[['type_form', 'walkinid', 'form_id', 'product_id', 'bracket_product_id', 'rn_walkin', 'brand_etl']].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1)

    print('Xử lý Dữ liệu thông tin Ngày bắt đầu - Kết thúc liệu trình: ', df.shape, ' bản ghi')
    return df

#dfc = proc_data_treatment()


#cur_dev = connect_database('dev_staging_sci').cursor()
def write_stag_walkin_booking_treatment_odo():
    dfc = proc_data_treatment()             #  proc_data_treatment()     df.copy()
    TABLE = 'stag_walkin_booking_treatment_odo'

    WHERE_UD = f'''brand_etl <> 0 AND (patient, brand_etl) IN (SELECT patient, brand_etl FROM {TABLE_TEMP_ODO})'''
    #query_df(conn, f'''DELETE FROM {table} WHERE brand_etl <> 0 AND services_date >= '20251201' ''', ret='exe')
    replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    print(f'HOÀN THÀNH! CẬP NHẬT BẢNG NGÀY BẮT ĐẦU - KẾT THÚC LIỆU TRÌNH CHỈNH NHA')

    return TABLE

#write_stag_walkin_booking_treatment_odo()


def clean_temp_table():
    temp_table = [TABLE_TEMP, TABLE_TEMP_ODO]  #temp_table = set(v for k, v in globals().items() if isinstance(v, str) and v.startswith("temp") and not k.startswith("_"))
    qr_clean = ';\n'.join(f'''\t\tDROP TABLE IF EXISTS {tb};''' for tb in temp_table)
    query_df(conn, qr_clean, ret='exe')  
    print(f'''-------- ✷✷✷✷✷✷ -------- ĐÃ DỌN DẸP CÁC BẢNG TẠM -------- ✷✷✷✷✷✷ --------\n{qr_clean}''')


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'task_stag_his_form_supply': write_stag_his_form_supply,
    'task_stag_walkin_booking_treatment_odo': write_stag_walkin_booking_treatment_odo,
    'task_clean_temp_table_supply': clean_temp_table,
}


def main():
    parser = argparse.ArgumentParser(description='ETL stag_his_supply')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
