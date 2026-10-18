# -*- coding: utf-8 -*-
"""
REPORT SCI SPECIALTY

Chuyen tu quentk_4_report_sci_specialty.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/report_sci_specialty_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python report_sci_specialty.py --task write_report_sci_specialty_v2
"""

import argparse
import datetime as dt
import gc
import json
import os
import re
import sys
import tempfile
import time
import unicodedata
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta
from loguru import logger

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

LS_COL_STR = ['id', 'team_service_id', 'order_product_id', 'lag_form_id', 'aggroleid']
CRMID = ['crm_id', 'brand_etl']
PCLINEID = ['clineid', 'product_id', 'brand_etl']
PARTNERID = ['partner_id', 'brand_etl']
CLINEID = ['brand_etl', 'clineid']


# ĐỌC DỮ LIỆU DATAFRAME ĐẦU VÀO 
# Mẫu truy vấn các thông tin Booking (crm_lead) cập nhật chung cho các DataFrame

query_crm_lead = f'''\
WITH reason_close AS                
(  --- Danh mục lý do Đóng/Hủy booking
SELECT value_case_when, MIN("result") "result" 
FROM dm_convert_case_when
WHERE column_name IN ('reason_out_sold', 'reason_cancel_booking', 'reason_not_booking_date')
GROUP BY value_case_when
), limit_update AS 
(   --- Giới hạn các Booking, KH trong khoảng thời gian chạy cập nhật
SELECT spt.crm_id, cl.partner_id, spt.brand_etl 
FROM stag_payment_transfer spt 
LEFT JOIN crm_lead cl on spt.crm_id = cl.id and spt.brand_etl = cl.brand_etl
WHERE spt.payment_date >= '{LIMIT_START_DATE}'
UNION 
SELECT smarw.booking_id AS crm_id, smarw.partner_id, smarw.brand_etl 
FROM sh_medical_appointment_register_walkin smarw 
WHERE smarw.state IN ('Completed', 'WaitPayment', 'InProgress') 
	AND COALESCE(smarw.service_date_start, smarw.service_date) >= '{LIMIT_START_DATE}'
UNION 
SELECT id AS crm_id, cl.partner_id, cl.brand_etl
FROM crm_lead cl 
WHERE cl.arrival_date >= '{LIMIT_START_DATE}'
), crm AS 
(  --- Phân loại Booking theo dịch vụ
SELECT cl.id AS crm_id, 
	MAX(CASE WHEN sp.his_service_type IN ('Laser', 'Spa', 'Card') and sp.product !~* '^(gội|phụ )' THEN 1 ELSE 0 END) AS is_book_derm,
	MAX(CASE WHEN sp.his_service_type IN ('Surgery', 'ChiPhi') THEN 1 ELSE 0 END) AS is_book_surg, 
	MAX(CASE WHEN sp.his_service_type IN ('Odontology') THEN 1 ELSE 0 END) AS is_book_odo, 
	MAX(CASE WHEN sp.his_service_type IN ('wellness', 'weight_loss') THEN 1 ELSE 0 END) AS is_book_other, 
    MAX(COALESCE(COALESCE(cl.reason_out_sold, cl.reason_cancel_booking), cl.reason_not_booking_date)) AS reason_close_booking, 
	cl.brand_etl 
FROM crm_lead cl
INNER JOIN (SELECT partner_id, brand_etl FROM limit_update GROUP BY partner_id, brand_etl) lm ON (cl.partner_id, cl.brand_id) = (lm.partner_id, lm.brand_etl)
LEFT JOIN crm_line cline ON cl.id = cline.crm_id AND cl.brand_etl = cline.brand_etl 
LEFT JOIN view_stag_products sp ON cline.product_id = sp.product_id AND cline.brand_etl = sp.brand_etl 
WHERE cl."type" = 'opportunity' AND cl.stage_id <> 34
GROUP BY cl.id, cl.brand_etl 
), fwalkin AS
(  --- Ngày đầu tiên hoành thành dịch vụ/Book
SELECT w.booking_id, w.brand_etl, MIN(service_date_start) + interval '7 hours' AS service_date_start
FROM sh_medical_appointment_register_walkin w
INNER JOIN (SELECT crm_id, brand_etl FROM limit_update GROUP BY crm_id, brand_etl) crm ON (w.booking_id, w.brand_etl) = (crm.crm_id, crm.brand_etl)
GROUP BY w.booking_id, w.brand_etl
)
   --- THÔNG TIN CHUNG VỀ BOOKING
SELECT crm.*, 
	cl.lead_id, cl.code_customer, 
	cl.arrival_date + interval '7 hours' AS arrival_date, 
    cl.stage_id, cs.name stage_booking, 
	cl.amount_total, cl.amount_paid, cl.type_data_partner, cl.note, 
	cl.source_id, ss.category_id category_source_id, ss.code source_code, ss.category_source, ss."source", 
	cl.create_uid, se.employee_code creator_employee_code, INITCAP(se.employee) creator_employee, 
	COALESCE(create_by_department, se.department) AS creator_department,
    rc.result AS reason_close_booking_name, 
	CASE WHEN cl.reason_out_sold IS NOT NULL THEN 'reason_out_sold' 
		WHEN cl.reason_cancel_booking IS NOT NULL THEN 'reason_cancel_booking' 
		WHEN cl.reason_not_booking_date IS NOT NULL THEN 'reason_not_booking_date' 
		END reason_close_field, 
    MAX(crm.is_book_derm) OVER (PARTITION BY cl.partner_id, cl.brand_id) is_customer_dermatology, 
    MAX(GREATEST(crm.is_book_surg, crm.is_book_odo, crm.is_book_other)) OVER (PARTITION BY cl.partner_id, cl.brand_id) AS is_customer_not_derm, 
    fwalkin.service_date_start
FROM crm
LEFT JOIN crm_lead cl ON (crm.crm_id, crm.brand_etl) = (cl.id, cl.brand_etl)
LEFT JOIN crm_stage cs ON (cl.stage_id, cl.brand_etl) = (cs.id, cs.brand_etl)
LEFT JOIN view_stag_employees_user se ON (cl.create_uid, cl.brand_etl) = (se.user_id, se.brand_etl)
LEFT JOIN stag_sources ss ON cl.source_id = ss.old_id 
LEFT JOIN fwalkin ON (cl.id, cl.brand_etl) = (fwalkin.booking_id, fwalkin.brand_etl)
LEFT JOIN reason_close rc ON crm.reason_close_booking = rc.value_case_when
'''

# Dữ liệu Doanh số / Tiền thu
query_payment = f'''\
SELECT 'payments' data_union
    , sale.*
FROM report_kpi_bonus_commission_transpose sale
WHERE sale.brand_etl <> 0
    AND COALESCE(nr_clone_permission, 0) <> 1
    AND payment_date >= '{LIMIT_START_DATE}' AND payment_date < '5000-01-01' 
    AND ( sale.his_service_type IN ('Laser', 'Spa', 'Card') 
        OR sale.product_category = 'SAN PHAM'
        OR COALESCE(sale.job, '') || COALESCE(sale.department, '') ~* 'thẩm mỹ|da liễu|vận hành dịch vụ'
        )
'''

# Dữ liệu Dịch vụ tư vấn (Bao gồm các DV Out-sold)
query_os = f'''\
WITH bk_out AS 
(
SELECT DISTINCT ON (os.clineid, os.consultants)
    'line_outsold' data_union
    , os.*
    , MAX(CASE WHEN os.stage_product IN ('done', 'sold') or COALESCE(os.total_received, 0) > 0 OR os.number_used > 0 THEN 1 ELSE 0 END) 
        OVER (PARTITION BY os.crm_id, os.product_ctg_id) 
        AS stage_sold
    , pp.name pricelist
    , cline.type_guarantee::int type_guarantee
    , cline.reason_guarantee_id
FROM report_crm_lead_customer_out_sold os 
LEFT JOIN crm_line cline on os.clineid = cline.id AND os.brand_etl = cline.brand_etl
LEFT JOIN product_pricelist pp on cline.price_list_id = pp.id AND cline.brand_etl = pp.brand_etl
LEFT JOIN 
    (SELECT DISTINCT crm_id, brand_etl FROM report_kpi_bonus_commission spt
    WHERE spt.brand_etl <> 0 AND spt.payment_date = date(arrival_date) AND spt.product_category <> 'SAN PHAM'
  	) pm ON os.crm_id = pm.crm_id AND os.brand_etl = pm.brand_etl
WHERE os.brand_etl <> 0
    AND os.customer_come = 'yes' --and stage_id <> 34   --- Khong lay Booking KH khong den cua
    AND arrival_date >= '{LIMIT_START_DATE}'
	AND pm.crm_id IS NULL
) 
SELECT *
FROM bk_out 
WHERE (stage_sold = 0 OR stage_id = 22)
'''

query_walkin = f'''\
SELECT 'walkin' data_union
    , s.*
    , ssos.crm_line_id clineid
    , s.revenue total
    , s.revenue total_received
    , 'done' stage_product
    , 1 as stage_sold
    , s.company_id orgcompany_id
    , s.company org_company
    , s.brand_id org_brand_id
    , 1 AS rn_walkin_product   --row_number() over (partition by s.walkinid, s.product_id order by s.services_date, s.order_priorole) 
    , s.team_service_id AS id_etl
    , s.thuonghieuphanquyen AS thuonghieuphanquyen_fix
FROM report_kpi_team_service s
LEFT JOIN stag_sale_order_services ssos ON s.order_product_id = ssos.order_product_id
WHERE s.brand_etl <> 0
    AND state_walkin <> 'Cancelled' 
	AND (s.ck_pttt ~ 'LASER|SPA' OR s.his_service_type IN ('Laser', 'Spa'))
	AND rn_order_product_id = 1             -- Mỗi dịch vụ/đơn hàng lấy 1 dòng duy nhất
	AND s.services_date >= '{LIMIT_START_DATE}'
'''


# Dữ liệu Vật tư Dịch vụ Nội khoa da liễu

ls_botox = [5326,7359,10713]      #[5326,10713]  ,32748
ls_juvederm = [6340,6341,6342,6343,10618,27463]         #[6340,6341,6342,6343,10618]
ls_xcelen = [10744,10745,10746]           #[10744,10745,10746,12973]
ls_eptq = [10328,10329,10330,10643]          #[10328,10329,10330,10643]
ls_genyal = [10743,22795,23009]
ls_restylane = [27514,27515,27516,27517,28267,28495]
ls_trehoa = [6145,6150,6151,6152,6154,6156,6157,6158,6159,6597,7355,10346,10874,12312,12973,22773,24877,25621,27616,28460,28474,28604,29449,30351]
ls_fill_oth = [5436]    # ,37626,37577,37578,37583,37585,37663,37683,37691

ls_botox_code = ['TH000986']
ls_trehoa_code = ['VT003538']
ls_fill_oth_code = ['VT003972', 'VT003782', 'VT003775', 'VT003776', 'VT003778', 'VT003779', 'VT003793', 'VT003795', 'VT003797']
lssup_code = ls_botox_code + ls_trehoa_code + ls_fill_oth_code
lssup_code_str = "'" + "','".join(str(e) for e in lssup_code) + "'"

dict_ord = {1: ls_botox, 2: ls_juvederm, 3: ls_xcelen, 4: ls_eptq, 5: ls_genyal, 6: ls_restylane, 7: ls_trehoa, 8: ls_fill_oth}
dict_ord_code = {1: ls_botox_code, 7: ls_trehoa_code, 99: ls_fill_oth}
dict_ord_code_rev = {v: k for k, value in dict_ord_code.items() for v in value}

dict_type = {1: 'Botox', 2: 'Juvederm', 3: 'Xcelens', 4: 'EPTQ', 5: 'Genyal', 6: 'Restylane', 7: 'Trehoa', 99: 'Others'}
lssls = list(dict_ord.values())
lssup = list(set(chain(*lssls)))    # ls_botox + ls_juvederm + ls_xcelen + ls_eptq + ls_genyal + ls_restylane + ls_fill_oth)
lssup_str = ','.join(str(e) for e in lssup)
lssup_str

dict_lssls = {i+1: lssls[i] for i in range(len(lssls))}    # Gán thứ tự tương ứng từng loại vật tư dưới dạng dictionary
df_sup = pd.Series(dict_lssls).to_frame().rename(columns={0: 'supply_id'}) 
df_sup = df_sup.apply(pd.Series.explode).reset_index()
df_sup = df_sup.groupby('supply_id')['index'].min()                        # Set nhóm vật tư theo thứ tự index ưu tiên

# Dữ liệu Vật tư Botox/Filler
query_supply = f'''\
SELECT s.*
    , smarw.partner_id 
    , smarw.type_crm_id 
    , smarw.state state_walkin
    , smhco.name room, smhcw.name ward
    , sp.his_service_type
    , sp.his_service_type initial_his_service_type
    , sp.product_ctg_id initial_product_ctg_id
    , sp.product_id initial_product_id
    , sp.product initial_product
    , sp.product_category initial_product_category
    , sp.kpi_point initial_kpi_point
    , sps.default_code
    , rc.name company_last
    , rc.brand_id thuonghieuphanquyen_fix
    , rc.brand_id org_brand_id
FROM stag_his_form_supply_2 s 
LEFT JOIN sh_medical_appointment_register_walkin smarw ON s.walkinid = smarw.id AND s.brand_etl = smarw.brand_etl
LEFT JOIN sh_medical_health_center_ot smhco ON s.room_id = smhco.id AND s.brand_etl = smhco.brand_etl 
LEFT JOIN sh_medical_health_center_ward smhcw ON s.ward_id = smhcw.id AND s.brand_etl = smhcw.brand_etl 
LEFT JOIN view_stag_products sp ON s.product_id = sp.product_id AND s.brand_etl = sp.brand_etl 
LEFT JOIN view_stag_products sps ON s.supply_product_id = sps.product_id AND s.brand_etl = sps.brand_etl 
LEFT JOIN m_res_company rc ON s.company_id = rc.id AND s.brand_etl = rc.brand_etl 
WHERE s.brand_etl <> 0
    AND sp.product_ctg_id in (404,449,1353,1393,1337,1558,1768)
	AND services_date >= '{LIMIT_START_DATE}'
    AND ( supply_product_id IN ({lssup_str}) 
        OR 
        sps.default_code IN ({lssup_code_str}) 
        )
'''
#dfsup = query_df(conn, query_supply)


r_dept_derm = r'lase|spa|thẩm mỹ da|^(phòng |)vận hành dịch vụ'
def get_od(department, job, user_id):
    is_dept_surg = check_substr(department, 'phẫu th') 
    is_dept_derm = check_substr(department, r_dept_derm) 
    if check_substr(department, 'lễ tân|kinh doanh dịch vụ'):
        r = 2 if check_substr(job, 'chuyên viên') else 1
    elif check_substr(job, r'bác s|trưởng ph|giám đốc'):
        r = 3 if is_dept_surg else 4 if is_dept_derm else 5
    elif check_substr(department, 'phẫu|điều d'):
        r = 6
    elif is_dept_derm: 
        r = 7
    elif user_id in [1,2,3,4,5,6284]:   #  _system_, root,... 
        r = 999
    elif check_substr(department, 'sale.*market'):
        r = 99
    else:
        r = 9
    return r

query_line_cancel = f'''\
SELECT id clineid, product_id, reason_line_cancel, brand_etl
FROM crm_line 
WHERE reason_line_cancel is not null
	AND brand_etl <> 0
	AND (status_cus_come = 'come' OR write_date >= '{LIMIT_START_DATE}') \
'''
#df_line_cancel = query_df(conn, query_line_cancel)


dict_get_df = {'crm_lead': query_crm_lead, 
               'payment': query_payment, 
               'outsold': query_os, 
               'walkin': query_walkin,
               'supply': query_supply, 
               'line_cancel': query_line_cancel, 
               'stag_employees': 'SELECT * FROM stag_employees'
              }


def get_data_payment():
    pm = get_df_cache('payment', dict_get_df, conn=conn).copy()      # query_df(conn, query_payment)
    ls_col_int =[c for c in pm.columns if check_substr(c, r'id$|^(rn_|dr_|rnk|drk)|order_|consultant') if c not in LS_COL_STR]
    fix_type(pm, ls_col_int, 'Int64', np.nan)
    
    pm['walkinid'] = pm.id.apply(lambda v: v.split('-')[1] or None)
    pm['order_product_id'] = pm['id'].str[:-2] + '-' + pm[['clineid', 'brand_etl']].fillna(0).astype(str).agg('-'.join, axis=1)
    pm['product_uom_qty'] = pm.quantity*pm.uom_price.fillna(1)
    pm['dr_order_role'] = np.select([pm.ordinal_consult.eq('consult_1'), pm.ordinal_consult.eq('consult_2')], [1,2], 3)
    pm['stage_sold'] = np.where((pm.total_received + pm.payment).eq(0) & pm.total.gt(0) & ~pm.stage_product.isin(['done', 'sold']), -1, 1)
    pm['is_book_product_dermatology'] = pm.his_service_type.isin(['Laser', 'Spa', 'Card']) & ~pm['product'].str.contains('gội đầu|phụ ', case=False, na=False)
    
    dict_ref = {
        'form_id': 'walkinid', 
        'team_service_id': 'id', 
        'agg_surgery_type': 'surgery_type', 
        'total': 'total_last', 
        'total_received': 'total_received_last', 
        'nhanvienphanquyen_fix': 'consultants', 
        'chinhanhphanquyen_fix': 'company_id'
    }
    for k, v in dict_ref.items():
        pm[k] = pm[v]
    
    dict_rename = {'ma_phieu': 'ck_pttt', 
                   'payment_date': 'services_date', 
                   'payment': 'revenue', 
                   'price_list': 'pricelist',
                   'rnk_crm_pmdate': 'drk_walkin_product', 
                   'rnk_clineid_pmdate': 'drk_crm_product', 
                   'consulting_role': 'role', 
                   'consultants': 'employee_id', 
                   'rto': 'rto_kpi', 
                   'commission_department': 'payment_dept_last',
                   'is_kpi_branch': 'is_kpi_sale_branch', 
                   'commission_employee': 'kpi_perf_revenue'}   # , 'thuonghieuphanquyen_fix': 'thuonghieuphanquyen'
    pm.rename(columns = dict_rename, inplace=True)
    print('1. Xử lý Data Doanh số! - ', str(pm.shape[0]), ' bản ghi')
    return pm

#pm = get_data_payment()


def get_data_outsold():
    os = get_df_cache('outsold', dict_get_df, conn=conn).copy()   #  query_df(conn, query_os)
    ls_col_int =[c for c in os.columns if check_substr(c, r'id$|^(rn_|dr_|rnk|drk)|order_|consultant') if c not in LS_COL_STR]
    fix_type(os, ls_col_int, 'Int64', np.nan)
    
    os['product_uom_qty'] = os.quantity*os.uom_price.fillna(1)
    os['dr_order_role'] = np.select([os.ordinal_consult.eq('consult_1'), os.ordinal_consult.eq('consult_2')], [1,2], 3)
    dict_cc = {'id': ['crm_id', 'clineid', 'consultants'], 
               'team_service_id': ['consultants', 'product_id', 'brand_etl'], 
               'order_product_id': ['crm_id', 'product_id', 'brand_etl'] 
              }
    for i, lsid in dict_cc.items():
        os[i] = os[lsid].fillna(0).astype(str).agg('-'.join, axis=1)
    
    dict_ref = {'report_date': 'services_date', 
                'booking': 'ck_pttt', 
                'clineid': 'form_id', 
                'surgery_type': 'agg_surgery_type', 
                'consultants': 'nhanvienphanquyen_fix'}
    for k, v in dict_ref.items():
        os[v] = os[k]
    
    dict_rename = {
                   'consultants': 'employee_id', 'consulting_role': 'role', 
                   'company_id': 'orgcompany_id', 'company': 'org_company', 'brand_id': 'org_brand_id', 
                   'report_company_id': 'company_id', 'report_company': 'company', 'report_brand_id': 'brand_id', 
                   'chinhanhphanquyen': 'chinhanhphanquyen_fix', 'thuonghieuphanquyen': 'thuonghieuphanquyen_fix'
    }
    os.rename(columns=dict_rename, inplace=True)
    
    if not os.id.is_unique: 
        print('Số lượng id "line_outsold" trùng: ', str(os[os.id.duplicated()].shape[0]))
        os[os.id.duplicated()][['id', 'ck_pttt', 'services_date', 'booking', 'contact_name', 'product', 'employee']]
        
    print('2. Xử lý Data tư vấn Out-sold: Done! - ', str(os.shape[0]), ' bản ghi')
    return os
#dfos = get_data_outsold()


def get_data_supply():
    dfsup = get_df_cache('supply', dict_get_df, conn=conn).copy()       #query_df(conn, query_supply)
    if dfsup.shape[0] > 0:
        dfsup['data_union'] = 'supply'
        for c in ['initial_kpi_perf', 'stage_sold']:
            dfsup[c] = 1
        fix_type(dfsup, ['supply_product_id', 'service_id', 'form_supply_id', 'company_id'], int, 0)
        dfsup['id'] = 'supp-' + dfsup[['supply_product_id', 'service_id', 'form_supply_id', 'company_id']].astype(str).fillna('').agg('-'.join, axis=1)
        dfsup['order_product_id'] = 'dfsup-' + dfsup[['form_supply_id', 'supply_product_id']].astype(str).fillna('').agg('-'.join, axis=1) 
        dfsup['is_new_request'] = dfsup.qty_used.ne(dfsup.qty.fillna(0))
        dfsup['stage_product'] = 'done'
        
        dfsup['order_priorole'] = dfsup['supply_product_id'].map(df_sup).replace(list(dict_lssls)[-1], 99)
        dfsup['order_priorole'] = dfsup['order_priorole'].fillna(dfsup.default_code.map(dict_ord_code_rev)).fillna(99).astype('Int64')
        dfsup['type_kpi_ekip'] = dfsup.order_priorole.map(dict_type).fillna('Others')
        
        dict_ref = {'ck_pttt': 'form_name', 'services_end_date': 'services_date', 'initial_booking': 'booking', 'product_id': 'supply_product_id', 
                    'orgcompany_id': 'company_id', 'org_company': 'company', 'company': 'company_last', 'chinhanhphanquyen_fix': 'company_id'}
        for k, v in dict_ref.items():
            dfsup[k] = dfsup[v]
        dict_rename = {'supply_product_id':'team_service_id', 'supply_category': 'product_category', 'supply':'product', 
                       'qty_used': 'product_uom_qty', 'department_type': 'department', 'uom': 'role', 'uom_id': 'roleid'}
        dfsup.rename(columns=dict_rename, inplace=True)
        if not dfsup.id.is_unique: 
            print('Số lượng id "supply" trùng: ', str(dfsup[dfsup.id.duplicated()].shape[0]))
            dfsup[dfsup.id.duplicated()][['id', 'ck_pttt', 'services_date', 'booking', 'contact_name', 'product', 'initial_product']]
    print('4. Xử lý Data Vật tư Botox, Filler: Done! - ', str(dfsup.shape[0]), ' bản ghi')
    return dfsup
#dfsup = get_data_supply()


def concat_payment_outsold():
    start = time.time()

    # Lấy Mẫu Bảng đầu ra
    columns =  query_df(conn, f'''SELECT * from report_sci_specialty_v2 limit 0''', ret='cols')
    df_sample = pd.DataFrame(columns=columns)
    c_date = [c for c in df_sample.columns if 'date' in c]
    df_sample[c_date] = df_sample[c_date].apply(pd.to_datetime)

    df_line_cancel = get_df_cache('line_cancel', dict_get_df, conn=conn)  #query_df(conn, query_line_cancel)
    dfpm = get_data_payment()
    dfos = get_data_outsold()    
    df = pd.concat([df_sample, dfpm, dfos], ignore_index=True)[columns]

    df['order_priorole'] = df[['department', 'job', 'employee_id']].fillna(0).apply(lambda r: get_od(*r), axis = 1)
    df['is_new_request'] = False
    for c in ['drk_walkin_product', 'drk_crm_product', 'overtime', 'kpi_point_num']:
        df[c] = df[c].fillna(1)
    dict_c = {'rn_order_product_id' : 'dr_order_role', 'roleid': 'order_priorole'}
    for c, v in dict_c.items():
        df[c] = df[v]

    # Lý do Hủy dòng dịch vụ
    col_cc = ['clineid', 'product_id', 'brand_etl']
    df_linecc = df[col_cc].fillna(0).astype(int).merge(df_line_cancel, how = 'left', on = col_cc)
    df['type_kpi_ekip'] = df_linecc.reason_line_cancel.fillna(df.reason_close_booking).fillna(df.type_kpi_ekip)
    
    dict_reason = {'not_enough_money': ['not_enough_finance', 'not_money', '14', 14], 
                   'think_more': ['consider_more'], 
                   'due_to_illness': ['cus_are_being_treated']
                     }
    dict_reason_inv = {v: k for k, values in dict_reason.items() for v in values}
    df['type_kpi_ekip'] = df.type_kpi_ekip.map(dict_reason_inv).fillna(df.type_kpi_ekip)
    
    end = time.time()
    print('3. Nối Bảng Payment, tư vấn Out-sold: Done!  (', round((end-start), 4), 's)')
    return df

#df_pmos = concat_payment_outsold()


def get_data_report_sci_specialty():
    start = time.time()
    columns =  query_df(conn, f'''SELECT * from report_sci_specialty_v2 limit 0''', ret='cols')
    
    # Các bảng dữ liệu chính: Payment/Consult, Walkin, Supply
    df_pmos = concat_payment_outsold()
    dfwk = get_df_cache('walkin', dict_get_df, conn=conn)         #query_df(conn, query_walkin)
    dfsup = get_data_supply()

    # Các bảng dữ liệu update bổ sung thông tin
    df_crm_lead = get_df_cache('crm_lead', dict_get_df, conn=conn)   #query_df(conn, query_crm_lead)
    df_se = get_df_cache('stag_employees', dict_get_df, conn=conn)\
            .rename(columns={'company_id': 'company_employee_id', 'company': 'company_employee'})
    
    # Nổi các bộ dữ liệu Thanh toán, Phiếu khám, Vật tư về 1 DataFrame
    df = pd.concat([df_pmos, dfwk, dfsup], ignore_index=True)[columns]
    
    # Update thông tin Booking (crm_lead), nhân sự
    df_update_crm = df[CRMID].reset_index().merge(df_crm_lead, how='left', on=CRMID).set_index('index')
    df_update_se = df[['employee_code']].reset_index().merge(df_se[df_se.employee_code.notnull()], how='left', on='employee_code').set_index('index')
    for c in [c for c in df_update_crm.columns if c in df.columns]:       # = update(overwrite=True): ưu tiên thông tin Booking
        df[c] = df_update_crm[c].where(df_update_crm[c].notna(), df[c])
    for c in [c for c in df_update_se.columns if c in df.columns]:        # = update(overwrite=False): chỉ điền ô trống
        df[c] = df[c].where(df[c].notna(), df_update_se[c])
    ls_col_insert = [c for c in df_update_crm.columns if c not in df.columns]
    df[ls_col_insert] = df_update_crm[ls_col_insert]
    df['service_date_start'] = pd.to_datetime(df.service_date_start)

    df = df.copy()    # Sắp xếp lại Khối dữ liệu 
    
    # Xác định Booking theo loại dịch vụ 
    re_cos_surg = 'áo ngực|quần|định hình|băng hàm|(?:gen|nịt) bụng|dehema|lce ba|súc m|chườm|silicone gel|xeragel|vô trùng|phẫu thuật|keo dán da|tăm nước|dr.*paris|dịch truyền|(?:đại|tiểu) ph.u'  # Từ khóa sản phẩm bán KH phẫu thuật
    is_cos_surg = df['product'].str.contains(re_cos_surg, na=False, case=False, regex=True)      # Sản phẩm KH phẫu thuật
    re_cos_derm = 'image|iluma|helio.*care|serum|SPF|diego|diago|ormedic|the max|decaar|endocare|malu.*wil|neo.*tra|anti.*age|biotrade.*mụn|laser|hush&hu|rửa mặt|srm|cleansing|time out|vital.* c '
    is_cos_derm = df['product'].str.contains(re_cos_derm, na=False, case=False, regex=True) 
    
    df['is_book_surg'] =  df.is_book_surg.fillna(0).astype(bool) | is_cos_surg
    df['is_book_surg'] = df.groupby(CRMID)['is_book_surg'].transform('max')       # Book Bán dịch vụ/sản phẩm KH phẫu thuật
    
    df['is_book_cosmetic'] =  df.product_category.ne('SAN PHAM') | df.service_date_start.notnull()
    df['is_book_cosmetic'] = df.groupby(CRMID)['is_book_cosmetic'].transform('max').ne(1)
    df['product_group_id'] = df.product_group_id.fillna(df.is_book_cosmetic.map({True: 0, False: None}))    # product_group_id = 0: Book chỉ bán Sản phẩm
    
    df['is_book_consult_dermatology'] = df.is_book_derm.fillna(0).astype(bool) | is_cos_derm | ( df.is_book_cosmetic & df.is_customer_dermatology.eq(1) )
    df['is_book_consult_dermatology'] = df.groupby(CRMID)['is_book_consult_dermatology'].transform('max')
    is_dept_derm = check_substr(df['department'], r_dept_derm, vect=True)         # Phòng chuyên môn Da
    
    df['is_sale_dermatology'] =  is_dept_derm | df.is_book_consult_dermatology | df['his_service_type'].isin(['Laser', 'Spa', 'Card'])  # Book Bán dịch vụ/sản phẩm KH da liễu
    
    df['stage_sold_derm'] = np.where(df.his_service_type.isin(['Laser', 'Spa', 'Card']) | df.is_book_cosmetic, df.stage_sold, np.nan)
    df['stage_sold_dermatology_booking'] = df['stage_sold_derm'].fillna(0).groupby([df[k].fillna(0) for k in CRMID]).transform('max')

    end = time.time()
    print('5. Nối thêm Bảng phiếu khám, Vật tư: Done! - Tổng: ', str(df.shape[0]), ' bản ghi \n    -------------- Thời gian xử lý: ', round((end-start), 4), 's --------------')
    return df

#dfc = get_data_report_sci_specialty()


def write_report_sci_specialty():
    # lấy dữ liệu
    dfc = get_data_report_sci_specialty()                 #  df.copy()      get_data_report_sci_specialty()  
    TABLE = 'report_sci_specialty_v2'
    dfc['ingestion_timestamp'] = datetime.now() + timedelta(hours=7)

    # Kiểm tra id Bảng "report_kn_kpi_bonus_commission" là duy nhất?
    if not dfc.id_etl.is_unique:
        df_checkid = dfc[dfc.id_etl.duplicated()][['data_union', 'crm_id', 'id_etl', 'brand_etl']].copy()
        #print(f'Kiểm tra lại id_etl Bảng {TABLE} trùng: ', df_checkid.shape[0])
        raise ValueError(f'Kiểm tra lại id Bảng {TABLE}: {dfc.id_etl.duplicated().sum()} id trùng')
    else:
        WHERE_UD = f"services_date >= '{LIMIT_START_DATE}' "
        replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)

    return f'''BÁO CÁO CHUYÊN MÔN: {TABLE}'''

#write_report_sci_specialty()
#get_shared_conn().close() if get_shared_conn() else None
#hook.get_conn().close()
#hook_test.get_conn().close()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'write_report_sci_specialty_v2': write_report_sci_specialty,
}


def main():
    parser = argparse.ArgumentParser(description='ETL report_sci_specialty')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
