# -*- coding: utf-8 -*-
"""
REPORT KPI BONUS COMMISSION

Chuyen tu quentk_3_report_kpi_bonus_commission_New_sys.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/report_kpi_bonus_commission_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python report_kpi_bonus_commission.py --task report_kpi_bonus_commission
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
DICT_BRAND_ETL = {1: 'kn', 3: 'pr', 4: 'hh', 8: 'thv', 9: 'sunwoo', 7: 'sci'}   # them brand: 9=sunwoo
DICT_BR_ETL = {v: k for k, v in DICT_BRAND_ETL.items() if v}
LS_BRAND_ETL = list(DICT_BRAND_ETL.keys())
LS_BR = list(v for k, v in DICT_BRAND_ETL.items() if k > 0)
idb = -1 
idb_kn = 1
br = ''
br_sci = 7


# Truy vấn stag_payment_transfer
# Giới hạn payment_date xử lý Báo cáo. Không nhỏ hơn 01/05/2024, dữ liệu trở về trước lưu tại bảng report_kn_kpi_sales_ext_backup_240430_prod
query = \
f'''
WITH limit_crm_id AS (      --- Giới hạn các Booking phát sinh Doanh số trong khoảng thời gian muốn cập nhật dữ liệu
SELECT DISTINCT crm_id, brand_etl FROM stag_payment_transfer 
WHERE payment_date >= '{LIMIT_START_DATE}' AND payment_date < '3000-01-01'
)
, consults AS (          	--- Thông tin Lễ tân đầu tiên tư vấn -> điền vào những dòng dịch vụ có Tư vấn viên = NULL
SELECT id, lm.crm_id, crm_line_id clineid, user_id, "role", data_union, lm.brand_etl,  
    ROW_NUMBER() OVER (PARTITION BY scic.crm_id, scic.crm_line_id order by CASE WHEN "role" = 'recept' THEN 1 ELSE 99 END) rn_cline_consult,
    COUNT(id) OVER (PARTITION BY scic.crm_id, scic.crm_line_id, "role") count_consult_in_role
FROM stag_crm_information_consultant scic 
INNER JOIN limit_crm_id lm ON scic.crm_id = lm.crm_id AND scic.brand_etl= lm.brand_etl
) 
, crm_lead_crop AS ( 		--- Thông tin khách hàng, Booking
SELECT cl.id AS crm_id_join, cl.type_crm_id, cl.name AS booking, cl.type_data_partner, 
	cl.code_customer, cl.contact_name, cl.arrival_date + interval '7 hours' AS arrival_date, cl.create_by, 
	cl.partner_id, cl.note, cl.source_id, ru.login, cl.brand_etl 
FROM crm_lead cl
--LEFT JOIN crm_sync_brand csb ON cl.id = csb.crm_id AND cl.brand_etl = csb.brand_etl 
--LEFT JOIN crm_lead clo ON csb.id_old = clo.id AND clo.brand_etl = 0
LEFT JOIN res_users ru ON cl.create_by = ru.id AND cl.brand_etl = ru.brand_etl 
INNER JOIN limit_crm_id lm ON cl.id = lm.crm_id AND cl.brand_etl = lm.brand_etl
), 
payments AS (  			--- Doanh số. Đánh thứ tự lần thanh toán Booking/Dịch vụ
SELECT spt.*, 
    CASE WHEN ROUND(spt.total_received_cumsum) = ROUND(spt.total) THEN true END is_bonus_commission,    --- Kiểm tra lũy kế số tiền đã thu đã bằng tổng phải thu? Cơ chế thưởng chênh lệch
    LAG(spt.total_received_cumsum, 1) OVER (partition by spt.clineid, spt.product_id order by spt.payment_date, spt.ma_phieu) total_received_cumsum_lag1,
    COALESCE(spt.consultants_1, c.user_id) AS consultants,
    CASE WHEN spt.consultants_1 = spt.consultants_2 THEN NULL ELSE spt.consultants_2 END consultants_2_fix
FROM public.stag_payment_transfer spt
LEFT JOIN (SELECT * FROM consults where rn_cline_consult = 1) c ON (spt.crm_id, spt.clineid) = (c.crm_id, c.clineid)
INNER JOIN limit_crm_id lm ON spt.crm_id = lm.crm_id AND spt.brand_etl = lm.brand_etl
)
SELECT pm.*, 
		cl.*,
		sp.product_ctg_id,
        sp.his_service_type,
        sp.surgery_type,
        sp.product_category,
        sp.product,
        sp.kpi_point, sp.kpi_point_num, 
        sp.is_no_kpi, 
        CASE WHEN cline.type_guarantee_2 = 'totality' THEN '11' 
            when cline.type_guarantee_2 = 'not_totality' THEN '10' 
            when cline.type_guarantee IS NOT NULL THEN cline.type_guarantee 
            when cline.initial_product_id IS NOT NULL THEN '99'    				--- Khong xac dinh loai bao hanh
            END type_guarantee,
        cline.discount_percent, 		--- Chiet khau MKT %
        cline.discount_cash, 			--- Chieu khau MKT tien mat
        cline.other_discount, 			--- Chiet khau MKT khac
        cline.sale_to, 					--- Chiet khau MKT con
        cline.discount_cs_percent,  	--- Chiet khau co so %
        cline.discount_cs_amount, 		--- Chiet khau co so tien mat
        cline.total_discount_review, 	--- Chiet khau co so giam sau
        cline.total_before_discount, 
        cline.total_discount,
        COALESCE(cline.total, clinep.total) AS total_last, 
        COALESCE(cline.total_received, clinep.total_received) AS total_received_last, 
        CASE WHEN COALESCE(cline.total_before_discount, 0) > 0 THEN cline.total_discount/cline.total_before_discount END AS total_discount_percent, 
        COALESCE(cline.price_min, ppi.price_min) AS price_min, 
		COALESCE(cline.source_extend_id, clinep.source_extend_id) as source_extend_id,
        CASE WHEN clinep.id IS NULL THEN cline.extensive_source_classification END extensive_source_classification,
        CASE WHEN clinep.id IS NULL THEN cline.reason_line_cancel END reason_line_cancel,
        ss.category_id category_source_id, ss.category_source, ss."source",
		COALESCE(sse."source", ssep."source") source_extend,
        pp."name" price_list, 
        CASE WHEN pp.id in (115,108,111,126) AND COALESCE(cline.price_min, 0) > 0 THEN true END as is_price_list_flexible_discount, 
        cline.price_min*pm.quantity*pm.uom_price total_price_min,
        SUM(CASE WHEN is_bonus_commission THEN 1 ELSE 0 END) OVER (partition by pm.crm_id, pm.clineid, pm.product_id order by payment_date, pm.ma_phieu, pm.total_received_cumsum) is_bonus_commission_back,
        MIN(CASE WHEN is_bonus_commission THEN payment_date END) OVER (partition by pm.crm_id, pm.clineid, pm.product_id) payment_date_complete_debt,
        rc.brand_id, orc.brand_id org_brand_id, rc."name" AS company, orc."name" AS org_company,
        NULL::int4 AS org_group_clineid,
        NULL::float8  AS total_recept_sell
FROM payments pm
LEFT JOIN crm_lead_crop cl ON pm.crm_id = cl.crm_id_join AND cl.brand_etl = pm.brand_etl
LEFT JOIN public.crm_line cline ON (pm.clineid, pm.product_id, pm.brand_etl) = (cline.id, cline.product_id, cline.brand_etl) 
LEFT JOIN public.crm_line_product clinep ON (pm.clineid, pm.product_id, pm.brand_etl) = (clinep.id, clinep.product_id, clinep.brand_etl)
LEFT JOIN (SELECT product_id, pricelist_id, max(price_min) price_min FROM public.product_pricelist_item group by product_id, pricelist_id) 
        ppi ON (cline.product_id, cline.price_list_id) = (ppi.product_id, ppi.pricelist_id)
LEFT JOIN public.view_stag_products sp ON pm.product_id = sp.product_id AND pm.brand_etl = sp.brand_etl
LEFT JOIN public.m_res_company rc ON rc.id = pm.company_id AND rc.brand_etl = pm.brand_etl
LEFT JOIN public.m_res_company orc ON orc.id = pm.orgcompany_id AND orc.brand_etl = pm.brand_etl
LEFT JOIN public.stag_sources ss ON cl.source_id = ss.old_id 
LEFT JOIN public.stag_sources sse ON cline.source_extend_id = sse.old_id 
LEFT JOIN public.stag_sources ssep ON clinep.source_extend_id = ssep.old_id 
LEFT JOIN public.product_pricelist pp ON pm.price_list_id = pp.id AND pp.brand_etl = pm.brand_etl 
'''


dict_get_df = {'payment': query,
                'users': 'SELECT * FROM view_stag_employees_user WHERE COALESCE(user_id, 0) <> 0', 
                'stag_employees': 'SELECT * FROM stag_employees', 
                'department_company': '''\
WITH cte as 
(
SELECT dc.hr_department_id department_id, dc.res_company_id company_id, hd.name, hd.company_id hd_company_id, dc.brand_etl, 
	CASE WHEN hd."name" ILIKE '%kinh doanh dịch vụ%' THEN 1         --- Ưu tiên sử dụng phân quyền Phòng lễ tân (trường hợp xử lý lại phongbanphanquyen)
		WHEN hd."name" ILIKE '%lễ tân%' THEN 2
		WHEN hd."name" ILIKE '%nha kh%' THEN 3                      --- Sử dụng phân quyền phòng Nha khoa với Chi nhánh Paris không có BP.Lễ tân (trường hợp xử lý lại phongbanphanquyen)
	END ordinal_dept 
FROM hr_department_res_company_rel dc 
LEFT JOIN hr_department hd ON dc.hr_department_id = hd.id AND dc.brand_etl = hd.brand_etl 
)
SELECT *, 
	ROW_NUMBER() OVER (PARTITION BY cte.company_id ORDER BY ordinal_dept) rn_dept_cpn, 
	ROW_NUMBER() OVER (PARTITION BY cte.company_id, cte.brand_etl ORDER BY ordinal_dept) rn_dept_cpn_br
FROM cte
ORDER BY cte.company_id, cte.brand_etl, cte.name, cte.department_id '''
               }

def get_df_se(): 
    stag_employees = get_df_cache('users', dict_get_df, conn)
    use_col_se = ['user_id','brand_etl','employee_code','employee','job_id','job','job_lv2',
              'team_id','team','group_job_id','group_job','old_department_id','department','company','company_id','brand_id']
    rename_cols= {'old_department_id': 'department_id', 'company_id': 'employee_company_id', 'brand_id': 'employee_brand_id', 'company': 'company_employee'}
    se = stag_employees[use_col_se].rename(columns=rename_cols)
    return se


# Từ khóa, id nhận diện các phòng ban dịch vụ
r_service = r'surgery|laser|spa|odo|card'
r4 = r'thẩm mỹ da|laser|spa|vận hành dịch vụ'
ls_id_derm = [2617,2601,2562,2553,2348,2315,2309,2301,2251,2215,2199,2198,2153,2141,2120,2108,2103,2084,2070,2066,2054,2053,2048,2046,2045]   # id các phòng ban laser/spa
r_surg = r'phẫu thu|gây mê|thay b|dịch vụ khách|quan h|chuyên môn|đại phẫu|tiểu phẫu|điều dưỡng'
ls_id_surg = [2607,2605,2604,2603,2602,2599,2598,2047]    # id các phòng ban thuộc Phòng mổ
r_odo = r'nha|răng'
ls_id_odo = [2600,2547,2373,2327,2304,2254,2219,2208,2202,2182,2169,2161,2151,2135,2117,2113,2109,2100,2092,2089,2079,2072,2062]    # id các phòng Nha

dict_spec = {'Laser_Spa_Card': [r4, ls_id_derm], 'Surgery': [r_surg, ls_id_surg], 'Odontology': [r_odo, ls_id_odo]}

ls_col_int_add = ['clineid', 'org_group_clineid', 'consultants_1', 'consultants_2', 'ho_tro', 'create_by',
                  'consultants', 'clineid', 'org_group_clineid', 'quantity', 'number_used', 'diff_day_payment',
                  'quantity', 'number_used', 'type_guarantee', 
                 'phongbanphanquyen_fix', 'chinhanhphanquyen_fix', 'thuonghieuphanquyen_fix', 'nr_clone_permission']


# BƯỚC 1: XỬ LÝ BẢNG THÔNG TIN THANH TOÁN, TIỀN CHÊNH LỆCH DỊCH VỤ, THƯỞNG
ls_col_int_add = ['clineid', 'org_group_clineid', 'consultants_1', 'consultants_2', 'ho_tro', 'create_by',
                  'consultants', 'clineid', 'org_group_clineid', 'quantity', 'number_used', 'diff_day_payment',
                  'quantity', 'number_used', 'type_guarantee', 'ho_tro', 
                 'phongbanphanquyen_fix', 'chinhanhphanquyen_fix', 'thuonghieuphanquyen_fix', 'nr_clone_permission']

def step1_transform_commission():
    start = time.time()
    df_payment = get_df_cache('payment', dict_get_df, conn)
    se = get_df_se()            #get_df_cache('users', dict_get_df, conn=conn)
    
    # Drop duplicate columns, keeping the first occurrence
    dfcd = df_payment.loc[:, ~df_payment.columns.duplicated()][(df_payment.payment_date >= DATETRUNC)].copy()
    #dfcd = dfcd[dfcd.id.ne('ti-165729-1') & dfcd.source_id.notnull()].copy()

    # Convert kiểu dữ liệu các cột là giá trị Số thập phân/Số nguyên
    ls_col_numeric = ['payment', 'total', 'total_price_min', 'total_last', 'total_received_last', 'price_min', 
                       'total_received', 'total_received_cumsum', 'discount_cs_amount', 'total_before_discount', 'total_discount'] 
    ls_col_int = [c for c in dfcd.columns if '_id' in c or c[:3] in ['drk', 'rn_', 'rnk', 'nr_'] or c in ls_col_int_add]
    dfcd[ls_col_numeric] = dfcd[ls_col_numeric].apply(pd.to_numeric)
    fix_type(dfcd, ls_col_int, 'Int64', np.nan)

    # Nối thông tin Tư vấn viên 1, 2, người tạo Booking 
    dict_se = {'consultants': '', 'consultants_2_fix': '2', 'ho_tro': 'sup', 'create_by': 'creator'}
    ls_dftemp = []
    for idb in dfcd['brand_etl'].unique():    # Tách dữ liệu theo từng thương hiệu
        dftemp = dfcd[dfcd.brand_etl.eq(idb)].copy()
        for col, suff in dict_se.items():     # Vòng lặp cột User_id và ký tự bổ sung các cột trùng tên sau merge từng TH
            dftemp[col] = dftemp[col].fillna(-9999).astype(int)
            dftemp=dftemp.merge(se, how='left', left_on=['brand_etl', col], right_on=['brand_etl', 'user_id'], suffixes=('','_'+suff))
            dftemp[col] = dftemp[col].replace(-9999, np.nan)
        ls_dftemp.append(dftemp)
    dfc = pd.concat(ls_dftemp, axis=0, ignore_index=True) 
    dfc.rename(columns={c: 'creator_' + c[:-8] for c in dfc.columns if c[-7:] == 'creator'}, inplace=True)
    dfc['is_recept_sell_org'] = (dfc.department.str.contains('lễ tân|kinh doanh dịch vụ', regex=True, case=False) & ~dfc.job.str.contains('chuyên viên', regex=True, case=False, na=False) ) \
                            | (dfc.department_2.str.contains('lễ tân|kinh doanh dịch vụ', regex=True, case=False) & ~dfc.job_2.str.contains('chuyên viên', regex=True, case=False, na=False) )
    
    dfc['ccjob'] = dfc['job'].map(str) + dfc['job_2'].map(str)                         # Nối chuỗi chức danh 2 Tư vấn viên
    dfc['ccdept'] = join_cols(dfc, ['department','department_2'], sep='-', na='')      # Nối chuỗi tên phòng ban 2 Tư vấn viên

    for c in ['payment_date','write_date','first_date_payment','arrival_date']: 
        dfc[c] = pd.to_datetime(dfc[c])
    dfc['diff_day_payment'] = (dfc.payment_date - dfc.first_date_payment)/np.timedelta64(1, 'D')       # Thời gian từ Ngày thanh toán đầu tiên đến ngày TT hiện tại: số ngày
    dfc['total_owed'] = dfc.total.astype(float) - dfc.total_received_recal.astype(float)               # Số tiền phải thu còn lại
    is_received = dfc.total_received_recal.fillna(0).gt(0).astype(bool)                                # Số tiền đã thu > 0
    is_owed =  dfc.total_owed.fillna(0).gt(10) & (dfc.stage_product.eq('done') | is_received)          # Trạng thái Chưa thanh toán hết, DV đã kết thúc
    is_paid_unful = is_received & dfc.number_used.fillna(0).eq(0).astype(bool)                         # Trạng thái đặt cọc, chưa thực hiện
    is_paid_ful =  is_received & dfc.total_owed.eq(0).astype(bool)   
    
    
    # Loại đặt cọc: cancel (DV đã hủy), sell_cosmetic (Bán sản phẩm), owed (Chưa thanh toán hết, DV đã kết thúc), paid_unfulfilled (Đặt cọc, chưa thực hiện), full_payment (Thanh toán đủ), wrong_transfer (Điều chuyển sai tiền: Tiền đã thu > Tiền phải thu)
    dfc['deposit_type'] = np.select([dfc.stage_product.eq('cancel'), dfc.product_category.eq('SAN PHAM'), is_owed, is_paid_unful, is_paid_ful, dfc.total_received_recal.lt(0).astype(bool)  ], \
                                    ['cancel', 'sell_cosmetic', 'owed', 'paid_unfulfilled', 'full_payment', 'wrong_transfer'], \
                                        'unpaid')
    
    # Các loại dịch vụ Lễ tân - Chuyên viên được đồng ghi nhận 100% doanh số 
    r1 = r'(?:surgery|laser|spa|chiphi|card)'    
    is_sell_recept =  check_substr(dfc['his_service_type'], r1, vect=True)
    
    # Tồn tại Phòng lễ tân tư vấn KH (tư vấn 1 hoặc 2)
    r2 = r'lễ tân|chuyên viên tư v|kinh doanh dịch vụ'
    rlt = r'lễ tân|nhân viên kinh doanh dịch'
    rcv = r'chuyên viên (tư vấn|kinh doanh dịch vụ)'
    is_dept_recept = check_substr(dfc['ccjob'], r2, vect=True) | check_substr(dfc['ccdept'], r2, vect=True) \
                    | dfc.department_id.fillna(0).eq(2612) | dfc.department_id_2.fillna(0).eq(2612) 
    # 2 Tư vấn gồm 1 Nhân viên Lễ tân/Kinh doanh dịch vụ/Phan Yến Nhi  VÀ  1 Chuyên viên tư vấn/kddv
    set_job_id = dfc[['job_id', 'job_id_2']].fillna(0).astype(int).astype(str).apply(lambda r: set(r), axis=1)
    dfc['id1'] = np.where(dfc.consultants.fillna(0).eq(6596), dfc.consultants, dfc.job_id)         # user_id = 6596: Phan Yến Nhi _ S220044\
    dfc['id2'] = np.where(dfc.consultants_2.fillna(0).eq(6596), dfc.consultants_2, dfc.job_id_2)
    set_user_job_id = dfc[['id1', 'id2']].fillna(0).astype(int).apply(lambda r: set(r), axis=1)
    is_ccjob_recept = ( dfc['ccjob'].apply(lambda r: check_substr(r, rlt)) & dfc['ccjob'].apply(lambda r: check_substr(r, rcv)) ) \
                    | set_job_id.eq({4233,4192}) \
                    | set_user_job_id.eq({6596,4192})
    
    # Dịch vụ da liễu
    r3 = r'laser|spa|card'
    is_sell_dermatology = check_substr(dfc['his_service_type'], r3, vect=True) | (dfc['product_category'] == 'SAN PHAM')
    # Cả 2 Tư vấn viên thuộc phòng da liễu
    
    is_dept_dermatology = ( check_substr(dfc['department'], r4, vect=True) | dfc.department_id.fillna(0).isin(ls_id_derm) ) \
                        & ( check_substr(dfc['department_2'], r4, vect=True)  | dfc.department_id_2.fillna(0).isin(ls_id_derm) )
    
    # Bác da liễu cùng tư vấn
    r5 = r'(bác sĩ|phòng|giám|trưởng).*(lase|spa|thẩm mỹ da|da liễu)'
    is_ccjob_dermatology_doctor = dfc[['job','job_2']].apply(lambda r: check_substr(r, r5), axis=1) | \
                                    dfc[['consultants', 'consultants_2']].eq(1231).any(axis=1)   # user_id = 1231: Lê Tiến Dũng (CV thẩm mỹ da)
    # Tư vấn viên có Nhân viên BP Chăm sóc sau dịch vụ (thuộc phòng KDDV)
    is_group_job_cssdv = dfc[['group_job_id', 'group_job_id_2']].fillna(0).apply(tuple, axis=1).apply(lambda r: 601 in r)   
    
    
    #print(dfc.sort_values(by='payment_date',ascending=[False]))
    # Các trường hợp ghi nhận tỷ lệ tính Doanh số
    '''
    1: Không có tư vấn viên 2
    2: Không có tư vấn viên 1
    3: Lễ tân / Phan Yến Nhi - CVTV cùng tư vấn dịch vụ Phẫu thuật/Laser/Spa/Chi phí khác
    4: Có Bác sĩ da liễu tư vấn bán dịch vụ da Laser/Spa hoặc Sản phẩm (không có lễ tân tư vấn)
    5: Lễ tân - BS da liễu tại TMV (trừ 3 BV)  - Team KSTC đề xuất chỉnh ngày 18/07/2024
    6: Tư vấn viên 1 là "Tư vấn chính"
    7: Tư vấn viên 2 là "Tư vấn chính"
    8: Các trường hợp còn lại: chia đôi DS 50/50
    '''
    company_hospital = dfc['company_id'].fillna(0).isin([2,3,4,12])    # Chi nhánh nhận Doanh số là các Bệnh viện
    dict_conditions = {1: dfc.consultants_2.fillna(0).eq(0) | dfc.consultants_2.fillna(0).eq(dfc.consultants_1), 2: dfc.consultants_1.fillna(0).eq(0), \
                        3: is_ccjob_recept & is_sell_recept, \
                            4: is_sell_dermatology & is_ccjob_dermatology_doctor & (is_group_job_cssdv | ~is_dept_recept), \
                               5: is_sell_dermatology & is_ccjob_dermatology_doctor & is_dept_recept & ~company_hospital, \
                                    6: dfc.consulting_role_1.astype(str) == '5', \
                                        7: dfc.consulting_role_2.astype(str) == '5'}
    ls_conditions = list(dict_conditions.values())
    dict_rto = {1: [1,0], 2: [0,1], 3: [1,1], 4: [1,1], 5: [1,1], 6: [0.7,0.3], 7: [0.3,0.7]}
    
    # Hệ số lương kinh doanh Tư vấn 1, Tư vấn 2 và phòng ban   !!!
    bool_conditions = [cdt.fillna(False).astype(bool) for cdt in dict_conditions.values()]
    dfc['rto_1'] = np.select(bool_conditions, [dict_rto[k][0] for k in dict_conditions], 0.5)
    dfc['rto_2'] = np.select(bool_conditions, [dict_rto[k][1] for k in dict_conditions], 0.5)

    dfc['commission_employee'] = dfc.rto_1*dfc.payment
    dfc['commission_employee_2'] = dfc.rto_2*dfc.payment
    dfc['commission_department'] = np.where(dfc.rto_1*dfc.rto_2.eq(1), dfc.payment/2, dfc.commission_employee)
    dfc['commission_department_2'] = dfc.payment - dfc.commission_department.astype(float)
    
    
    # Phân loại Bán chéo - Bán mở rộng dịch vụ trong phòng ban
    is_his_service = check_substr(dfc['his_service_type'], r_service, vect=True)       # Là dịch vụ: phẫu thuật, laser, spa, nha
    for i in ['', '_2']:
        # dept: convert tên phòng ban sang loại dịch vụ chuyên môn tương ứng
        dfc['dept' + i] = np.select([check_substr(dfc['department' + i], ls[0], vect=True)
                                 | dfc['department_id' + i].fillna(0).isin(ls[1]).astype(bool)
                                 for ls in dict_spec.values()], \
                                list(dict_spec.keys()), dfc['department' + i]).astype(str)
    
        # Kiểm tra xem dịch vụ phòng ban bán tăng thêm có cùng chức năng chuyên môn không? (Khớp từ khóa sau convert)
        is_same_service_type = pd.Series([check_substr(d, s) for d, s in zip(dfc['dept' + i], dfc['his_service_type'])], index=dfc.index)
        dfc['ext' + i] = np.select([~check_substr(dfc['dept' + i], r_service, vect=True) | ~is_his_service, \
                                            ~is_same_service_type], \
                                        ['', 'ext02'], 'ext01')
                                        # ext02 = bán chéo, ext01 = bán mở rộng
    dfc['extensive_cross_upsell'] = dfc['ext'] + ',' + dfc['ext_2']
    
    # Điều kiện tính doanh số KPI: Loại trừ DV Covid, phụ thu hóa đơn, DS Văn phòng TH và các Nguồn CBNV, Đối ngoại, Thuê phòng mổ
    dfc['is_kpi_branch'] = ~(dfc.is_no_kpi.fillna(False) 
                            | dfc.company_id.fillna(0).isin([54,55,56])
                            | dfc.source_id.isin([215,216,217,219,80,81,82,83,84,87,88,89,90,124,278,279,280,287,288,289,292,535,569,570]))
                            
    
    
    # DOANH SỐ THƯỞNG CHÊNH LỆCH
    
    # pay_bonus = Điều kiện tính thưởng: bảng giá mới 2024 + số tiền lũy kế trong quá khứ từng đạt 100% tiền phải thu
    pay_bonus_refund = dfc.is_bonus_commission_back.gt(0) & dfc.payment.lt(0)                   # Hoàn tiền dịch vụ đã thanh toán đủ 100%
    pay_bonus = (dfc.is_bonus_commission_back.gt(0) & dfc.is_price_list_flexible_discount & dfc.is_kpi_branch & dfc.type_guarantee.fillna(0).eq(0)).astype(bool)    # | pay_bonus_refund)
    cdt1 = pay_bonus & dfc.payment.gt(0)                                                        # Khách đã đóng 100% tiền phải thu
    cdt2 = pay_bonus & dfc.payment.lt(0) & (dfc.total_received_cumsum >= dfc.total_price_min)   # Hoàn tiền, tiền đã thu sau hoàn >= giá trị bán tối thiểu
    cdt3 = pay_bonus & dfc.payment.lt(0) & (dfc.total_received_cumsum < dfc.total_price_min)    # Hoàn tiền, tiền thu sau hoàn < giá trị bán tối thiểu
    
    dfc['commission_bonus'] = np.select([cdt1, cdt2, cdt3], 
                            [dfc.total_received_cumsum - dfc.total_price_min, \
                                dfc.payment, \
                                    dfc.payment + (dfc.total_price_min - dfc.total_received_cumsum)])
    dfc['commission_bonus_shift1'] = dfc.groupby(['brand_etl', 'clineid', 'product_id'])['commission_bonus'].shift(periods=1)
    dfc['commission_bonus'] = np.select([dfc.commission_bonus.gt(0) & dfc.payment.lt(0), cdt1 & dfc.commission_bonus_shift1.gt(0)],
                                        [0, dfc.commission_bonus - dfc.commission_bonus_shift1], dfc.commission_bonus)
    #dfc['commission_bonus_cumsum'] = dfc.groupby(['clineid', 'product_id'])['commission_bonus_cumsum'].cumsum()
    dfc['commission_bonus_employee'] = dfc.commission_bonus*dfc.rto_1
    dfc['commission_bonus_employee_2'] = dfc.commission_bonus*dfc.rto_2
    dfc['commission_bonus_department'] = np.where(dfc.rto_1*dfc.rto_2 == 1, dfc.commission_bonus_employee/2, dfc.commission_bonus_employee)
    dfc['commission_bonus_department_2'] = dfc.commission_bonus - dfc.commission_bonus_department.astype(float)
    
    
    # Lũy kế Doanh số đạt của Chi nhánh theo tuần trong tháng
    d_mth = pd.to_datetime(dfc.payment_date).dt.day
    mth = pd.to_datetime(dfc.payment_date).dt.month
    
    dict_w = {1: 7, 2: 14, 3: 21}
    week_in_mth = np.select([d_mth <= 7, d_mth <= 14, d_mth <= 21], ['W1', 'W2', 'W3'], 'W4') 
    ym = pd.to_datetime(dfc['payment_date']).dt.strftime('%Y%m') 
    dfc['week_in_mth'] = ym + week_in_mth
    
    
    # Doanh số KPI lũy kế đạt được theo tuần trong tháng (Các mốc ngày 7, 14, 21, còn lại)
    for w in range(1, 5):
        cdt_date = d_mth <= 7*w
        dfc['payment_kpi_accumulated_w' + str(w)] = dfc['payment'].where(cdt_date & dfc['is_kpi_branch']) \
                                                        .groupby([mth, dfc['company_id'], dfc['brand_etl']]).transform('sum')
    
    ## không cần sử dụng globalss
    dfc['payment_kpi_accumulated_w'] = np.select([d_mth <= 7, d_mth <= 14, d_mth <= 21], \
                                                [dfc['payment_kpi_accumulated_w' + str(i+1)] for i in range(3)], \
                                                dfc['payment_kpi_accumulated_w4']) 
    fix_type(dfc, ls_col_int, 'Int64', np.nan)
    end = time.time() 
    print('Xử lý Bảng chung (tiền thanh toán): ', round((end-start), 4), "s  (size: ", str(dfc.shape[0]), ')')
    return dfc

# Bảng chung sau bước 1: tính 1 lần trong mỗi tiến trình task, các bước sau gọi get_df()
def get_df():
    if 'df' not in _CACHE:
        _CACHE['df'] = step1_transform_commission()
    return _CACHE['df']
    
#df = get_df()


# Transpose Bảng Doanh số/thưởng từng nhân viên theo dòng (record) thương hiệu khác Paris
# report_kpi_bonus_commission_transpose
def step2_transform_kangnam():
    
    start = time.time()
    df = get_df()
    users = get_df_cache('users', dict_get_df, conn)
    stag_employees = get_df_cache('stag_employees', dict_get_df, conn)
    
    dfcut = df[df['brand_id'].fillna(0).ne(3)].drop(columns=['consultants_1', 'consultants_2']).rename(columns={'consultants_2_fix': 'consultants_2'}).copy()
    dfcut.columns = [c[:-2] if c[-2:] == '_1' else c for c in dfcut.columns]    # Bỏ đuôi '_1' ở các cột trong Bảng. VD: consultants_1
    # dft: danh sách các cột chung để nối 3 dataframe tư vấn 1, tư vấn 2, hỗ trợ tư vấn
    dft = dfcut[list(c for c in dfcut.columns if not check_substr(c, r'_2|sup|ho_tro|dept|cross_upsell'))]
    col_emp_sale = ['ext', 'rto', 'commission_employee', 'commission_bonus_employee', 'commission_department', 'commission_bonus_department', 'consulting_role', 'consultants', 
                    'employee_code', 'employee', 'job', 'department', 'department_id', 'team_id', 'team', 'group_job_id', 'group_job', 
                    'employee_company_id', 'employee_brand_id','company_employee']
    col_emp_sale_2 = [e + '_2' for e in col_emp_sale]
    col_emp_sup = ['ho_tro'] + [e + '_sup' for e in col_emp_sale[-12:]]
    col_common = [c for c in dft.columns if c not in col_emp_sale]
    
    # Các bảng tạm theo doanh số của Tư vấn 1, Tư vấn 2 và Hỗ trợ tư vấn
    temp1 = pd.concat([dft[col_common], dfcut[col_emp_sale].set_axis(col_emp_sale, axis=1)], axis=1)
    temp2 = pd.concat([dft[col_common], dfcut[col_emp_sale_2].set_axis(col_emp_sale, axis=1)], axis=1)
    temp3 = pd.concat([dft[col_common], dfcut[dfcut['ho_tro'].fillna(0) > 0][col_emp_sup].set_axis(col_emp_sale[-13:], axis=1)], axis=1)
    temp3['consulting_role'] = '99'     # role = 99: Hỗ trợ tư vấn
    
    temp_dict = {'1': temp1, '2': temp2, '3': temp3}
    # Field phân biệt dữ liệu từ 3 bảng tạm ở trên
    for k, v in {'1': 'consult_1', '2': 'consult_2', '3': 'supporter'}.items():
         temp_dict[k]['ordinal_consult'] = v
    
    
    # Nối data 3 bảng tạm    
    kpi_concat = pd.concat([temp1, temp2[temp2['consultants'].fillna(0) > 0], temp3[temp3['consultants'].fillna(0) > 0]], ignore_index=True)
    kpi_concat = kpi_concat.sort_values(['booking', 'clineid', 'ordinal_consult']).rename(columns={'ext': 'extensive_cross_upsell'})

    
    # XỬ LÝ ID PHÂN QUYỀN

    ls_special_user = [1,2,3,4,5,6284]          # Các user đặc biệt: admin, bot, Giang - IT, v.v. ...
    #kpi_concat['consultants'] = kpi_concat['consultants'].astype('Int64')  
    kpi_concat['consultants'] = kpi_concat.consultants.fillna(0)
    kpi_concat.merge(users[['brand_etl','user_id', 'department_id', 'phongbanphanquyen']], how='left', 
                    left_on=['brand_etl','consultants'], right_on=['brand_etl','user_id'], suffixes=('', '_se'))
    kpi_concat = kpi_concat.merge(stag_employees[['kn_user_id', 'department_id', 'phongbanphanquyen']], how='left', left_on='consultants', right_on='kn_user_id', suffixes=('', '_se'))
    
    # Mỗi công ty lấy 1 id phongbanphanquyen đại diện => xử lý trong trường hợp nhân sự trong Báo cáo không có department_id tương ứng company cần hiển thị báo cáo 
    hr_department_company = get_df_cache('department_company', dict_get_df, conn)
    pbpq_getone = hr_department_company[hr_department_company.rn_dept_cpn_br.eq(1) & hr_department_company.name.notnull()].iloc[:, :5]
    
    kpi_concat = kpi_concat.merge(pbpq_getone, how='left', on = ['company_id', 'brand_etl'], suffixes=('', '_pbpq'))
    # Merge hr_department_company để kiểm tra cặp [department_id, company_id] sẵn trong Data có hợp lệ không?
    kpi_concat = kpi_concat.merge(hr_department_company, how='left', left_on=['company_id', 'phongbanphanquyen', 'brand_etl'], 
                                  right_on=['company_id', 'department_id', 'brand_etl'], suffixes=('', '_check_pq'))       
                
    kpi = kpi_concat.copy()
    kpi['department_id_std_target'] = kpi.department_id_se.fillna(kpi.department_id)
    kpi['thuonghieuphanquyen_fix'] = kpi['brand_id']
    kpi['chinhanhphanquyen_fix'] = np.where((kpi.company_id.eq(12) & kpi.payment_date.dt.date.ge(dt.date(2026,8,1)).fillna(False)).astype(bool), 
                                            4, kpi['company_id'])
    # Trường hợp phòng ban của Tư vấn viên không được cấu hình theo Công ty được tính DS thì phân quyền theo id phongbanphanquyen đại diện tại chi nhánh đó
    kpi['phongbanphanquyen_fix'] = np.select([kpi.rn_dept_cpn.fillna(0).eq(0) & kpi.department_id_pbpq.fillna(0).ne(0), kpi.phongbanphanquyen.fillna(0).ne(0).astype(bool)], \
                                                [kpi.department_id_pbpq, kpi.phongbanphanquyen], kpi.department_id)

    # to_clone: điều kiện nhân bản bản ghi có Công ty ghi nhận doanh số và Công ty của nhân sự bán khác nhau
    to_clone =  (kpi['employee_company_id'].fillna(0).gt(1)\
                & (kpi['employee_company_id'] != kpi['company_id']) \
                & kpi['brand_id'].isin([1,2,3,4]) \
                & ~kpi['consultants'].isin(ls_special_user) \
                & ~kpi['ordinal_consult'].isin(['supporter']) \
                & ~(kpi.employee_company_id.eq(4) & kpi.company_id.eq(12)) \
                ).fillna(False).astype(bool)
    
    kpi['nr_clone_permission'] = np.where(to_clone, 1, 0)
    
    # Nhân bản dữ liệu cho Chi nhánh của nhân sự công tác
    kpi3 = kpi.iloc[to_clone.values, :].copy()     
    if kpi.shape[0] > 0: 
        kpi3['nr_clone_permission'] = 2
        kpi3 = kpi3.merge(pbpq_getone, how='left', on = ['company_id', 'brand_etl'], suffixes=('', '_pbpq_ct'))
        kpi3['phongbanphanquyen_fix'] = kpi3['phongbanphanquyen'].fillna(kpi3.department_id_pbpq_ct)
        kpi3['chinhanhphanquyen_fix'] = kpi3['employee_company_id']
        kpi3['thuonghieuphanquyen_fix'] = kpi3['employee_brand_id']
    
    # Tư vấn viên tại Chi nhánh khác bán cho CN được tính doanh số: phân quyền tại CN được tính DS không có Doanh số cá nhân và phòng ban của nhân viên này
    for c in ['commission_employee', 'commission_bonus_employee', 'commission_department', 'commission_bonus_department']:
        kpi[c] = np.where(kpi.nr_clone_permission.eq(1), np.nan, kpi[c])
        
    # Nối các bảng sau xử lý id phân quyền
    kpi_trans = pd.concat([kpi, kpi3])
    kpi_trans['rn_id_clineid'] = kpi_trans.groupby(['id', 'clineid', 'product_id']).cumcount().add(1)
    
    
    # Xử lý kiểu dữ liệu integer các cột id   
    ls_col_int = [c for c in kpi_trans.columns if '_id' in c or c[:3] in ['drk', 'rn_', 'rnk'] or c in ls_col_int_add]                          
    fix_type(kpi_trans, ls_col_int, 'Int64', np.nan)
    end = time.time()
    print('Bảng Doanh số - Thưởng Kangnam: ', round((end-start), 4), "s")
    return kpi_trans

#t2 = step2_transform_kangnam()


# BÁO CÁO DOANH SỐ, THƯỞNG - THƯỞNG THƯƠNG HIỆU PARIS

def step3_transform_paris():
	# Bảng tư vấn viên Paris
	start = time.time()
	df = get_df()

	query_consult = '''
    SELECT DISTINCT ON (crm_id, crm_line_id, user_id, "role") id, crm_id, crm_line_id clineid, user_id, "role", data_union, 
        ROW_NUMBER() OVER (PARTITION BY crm_id, crm_line_id ORDER BY CASE WHEN "role" = 'recept' THEN 1 ELSE 99 END) rn_cline_consult,
        ROW_NUMBER() OVER (PARTITION BY data_union, crm_line_id ORDER BY id) rn_consult, 
        COUNT(id) OVER (PARTITION BY crm_id, crm_line_id, "role") count_consult_in_role,
        COUNT(id) OVER (PARTITION BY crm_id, crm_line_id) count_consult_in_cline
    FROM stag_crm_information_consultant scic WHERE brand_etl = 3
    '''
	consults = query_df(conn, query_consult)
	stag_employees = get_df_cache('stag_employees', dict_get_df, conn)
    
	dfcpr = df[df['brand_id'].eq(3)].copy()

    # LẤY THÔNG TIN THÀNH VIÊN TƯ VẤN THƯƠNG HIỆU PARIS
	ls_col_consult = ['data_union', 'crm_id', 'clineid', 'user_id', 'role','rn_consult', 'count_consult_in_role', 'count_consult_in_cline']
	dfcpr = dfcpr[list(c for c in dfcpr.columns if c != 'user_id')].merge(consults[ls_col_consult], how = 'left', on = ['crm_id', 'clineid'])

    # Trường hợp không nhập Thành viên tư vấn Paris, lấy theo Tư vấn 1, 2 trên crm_line (nếu có)
	check_cdt = dfcpr.user_id.isnull() & dfcpr.consultants_2.fillna(0).ne(0)
	dfcpr2 = dfcpr[check_cdt].copy()
	dfcpr2['consultants'] = dfcpr2['consultants_2'].fillna(0).astype(int)
	dfcpr2['rto'] = dfcpr2.rto_2
	dfcpr2['ordinal_consult'] = 'consult_2'
	dfcpr2['commission_department'] = dfcpr2['commission_department_2']
	
	dfcpr['ordinal_consult'] = np.where(dfcpr['user_id'].isnull(), 'consult_1', dfcpr.role)
	dfcpr['consultants'] = np.where(dfcpr['user_id'].isnull(), dfcpr['consultants_1'], dfcpr['user_id'])
	dfcpr['consultants'] = dfcpr['consultants'].fillna(0).astype(int)  
	# Doanh số, thưởng nhân viên, phòng ban
    # Bán Sản phẩm: mặc định tính 100% cho 1 dòng Nhân sự tư vấn đầu tiên. Bán dịch vụ: Doanh số / số người có cùng vai trò tư vấn (Lễ tân, Bác sĩ, Trợ thủ)
	dfcpr['rto'] = np.select([dfcpr.data_union.eq('cosmetic') & dfcpr.rn_consult.eq(1), dfcpr.product_category.ne('SAN PHAM')], \
                                 [1, 1/dfcpr.count_consult_in_role], np.nan)
	dfcpr['rto'] = dfcpr['rto'].fillna(dfcpr.rto_1)

	dfcpr = pd.concat([dfcpr, dfcpr2])[list(c for c in dfcpr.columns if re.search('_(2|sup)$',c) == None)].copy()
	dfcpr['commission_employee'] = dfcpr['payment']*dfcpr['rto']
	dfcpr['consulting_role'] = dfcpr['role']  #.fillna('recept')
	dfcpr.sort_values(['clineid', 'rn_consult'], inplace=True)
	is_exists_consult = dfcpr['consulting_role'].eq('recept').groupby(dfcpr['clineid']).transform('any')     # Dòng DV, Sản phẩm có lễ tân tư vấn ko?
	dfcpr['rn_id_clineid'] = dfcpr.groupby(['id', 'clineid', 'product_id']).cumcount().add(1)
	dfcpr['rn_id_clineid_max'] = dfcpr.groupby(['id', 'clineid', 'product_id'])['rn_id_clineid'].transform('max')
    
	''' Doanh số Phòng: 
	1. Trường hợp chỉ có 1 Tư vấn viên: Tính 100% doanh số cho Phòng ban của Tư vấn viên "đầu tiên"
	2. Bán Sản phẩm: Tính Doanh số Phòng theo Doanh số Nhân viên tư vấn được tính DS.
    3. Ko có vai trò "Lễ tân" tư vấn trên dòng dịch vụ/Sản phẩm: 
	4. Còn lại: chia cho các phòng ban của Nhân sự có vai trò tư vấn là "Lễ tân" ''' 
	dfcpr['commission_department'] = np.select([dfcpr.rn_id_clineid_max.eq(1), 
                                                dfcpr.data_union.eq('cosmetic'), 
                                                dfcpr.ordinal_consult.str.startswith('consult').astype(bool), 
                                                dfcpr.rn_id_clineid.eq(1) & ~is_exists_consult, 
                                                dfcpr['consulting_role'].fillna('').eq('recept')], 
                                               [dfcpr.payment,
                                                dfcpr.commission_employee, 
                                                dfcpr.commission_department, 
                                                dfcpr.payment, 
                                                dfcpr.payment*dfcpr.rto]
                                               , np.NaN)
	for c in ['commission_bonus_employee', 'commission_bonus_department']:
		dfcpr[c] = dfcpr['commission_bonus']/dfcpr['count_consult_in_cline']
	# list cột thông tin từ bảng nhân viên cần lấy
	col_emp_pr = ['pr_user_id', 'old_department_id', 'employee_code', 'employee', 'job', 'department', 'group_job_id', 'group_job', 'team_id', 'team', 
                  'company_id', 'brand_id', 'department_id', 'phongbanphanquyen', 'company']
	dfcpr = dfcpr.merge(stag_employees[col_emp_pr].rename(columns={'department_id':'department_id_std_target', 'pr_user_id': 'consultants', 'company': 'company_employee'}), 
                        how='left', on='consultants', suffixes=('', '_pr'))
	# Gán lại các cột thông tin Tư vấn viên theo dữ liệu nhân viên đã merge ở trên (VD: job = job_pr)
	dfcpr[['department_id', 'employee_company_id', 'employee_brand_id']] = dfcpr[['old_department_id', 'company_id_pr', 'brand_id_pr']]
	for col in [c for c in dfcpr.columns if c[-3:] == '_pr' and c[:-3] not in ['department_id', 'company_id', 'brand_id']]:
		dfcpr[col[:-3]] = dfcpr[col]
	dfcpr['employee'] = dfcpr['employee'].str.title()       # Viết hoa chữ đầu tiên trong tên Nhân viên

    # Phân loại Bán chéo - Bán mở rộng dịch vụ trong phòng ban
	is_his_service = dfcpr.apply(lambda r: check_substr(r['his_service_type'], r_service), axis = 1)       # Là dịch vụ: phẫu thuật, laser, spa, nha
	dfcpr['dept'] = np.select([dfcpr.apply(lambda r: check_substr(r['department'], ls[0] ), axis = 1) 
                                 | dfcpr['department_id'].fillna(0).isin(ls[1]).astype(bool) 
                                 for ls in list(ls for ls in dict_spec.values())], \
                                list(dict_spec.keys()), dfcpr['department']).astype(str)

    # Kiểm tra xem dịch vụ phòng ban bán tăng thêm có cùng chức năng chuyên môn không? (Khớp từ khóa sau convert)
	dfcpr['extensive_cross_upsell'] = np.select([~dfcpr.apply(lambda r: check_substr(r['dept'], r_service), axis = 1) | ~is_his_service, \
                                        ~dfcpr.apply(lambda r: check_substr(r['dept'], r['his_service_type']), axis = 1)], \
                                    ['', 'ext02'], 'ext01')
                                    # ext02 = bán chéo, ext01 = bán mở rộng

    # XỬ LÝ ID PHÂN QUYỀN
    # pbpq_getone: id phongbanphanquyen đại diện tại 1 chi nhánh => xử lý trường hợp department_id của Nhân viên không tương ứng với Chi nhánh được phân quyền
	hr_department_company = get_df_cache('department_company', dict_get_df, conn)
	pbpq_getone = hr_department_company[hr_department_company.rn_dept_cpn_br.eq(1) & hr_department_company.name.notnull()].iloc[:, :5]
    
	dfcpr = dfcpr.merge(pbpq_getone, how='left', on = ['company_id', 'brand_etl'], suffixes=('', '_pbpq'))
	dfcpr['phongbanphanquyen_fix'] = np.select([(dfcpr.employee_company_id.fillna(0).eq(dfcpr.company_id) & dfcpr.department_id.fillna(0).ne(0)).astype(bool), 
                                                dfcpr.employee_company_id.ne(dfcpr.company_id).fillna(True).astype(bool)], \
                                                [dfcpr.phongbanphanquyen, dfcpr.department_id_pbpq], dfcpr.department_id)
	dfcpr['chinhanhphanquyen_fix'] = dfcpr['company_id']
	dfcpr['thuonghieuphanquyen_fix'] = dfcpr['brand_id']
	dfcpr['nr_clone_permission'] = 0
    
    
    # Xử lý kiểu dữ liệu, tên cột
	ls_col_int = [c for c in dfcpr.columns if '_id' in c or c[:3] in ['drk', 'rn_', 'rnk'] or c in ls_col_int_add]                          
	fix_type(dfcpr, ls_col_int, 'Int64', np.nan)

	end = time.time()
	print('Bảng Doanh số - Thưởng Paris: ', round((end-start), 4), "s")
	return dfcpr

#t3 = step3_transform_paris()


#cur_product = connect_database('test_staging_sci').cursor()

def step_report_kpi_commission():
    # lấy dữ liệu
    dfc = get_df().copy()            #step1_transform_commission()
    TABLE = 'report_kpi_bonus_commission'

    # id trùng: dừng task để Airflow báo lỗi 
    if not dfc['id'].is_unique:
        raise ValueError(f'Kiểm tra lại id Bảng {TABLE}: {dfc["id"].duplicated().sum()} id trùng')
    
    WHERE_UD = f'''brand_etl > 0 AND payment_date >= '{LIMIT_START_DATE}' '''
    replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    print('HOÀN THÀNH BẢNG DOANH SỐ PAYMENT!')

    return TABLE

#step_report_kpi_commission()


def step_report_kpi_commission_transpose():
    # lấy dữ liệu
    start = time.time()
    dfckn = step2_transform_kangnam()   #  step2_transform_kangnam()   t2.copy()
    dfcpr = step3_transform_paris()   #step3_transform_paris()       t3.copy()
    TABLE_TRANS = 'report_kpi_bonus_commission_transpose'
    
    existing_columns = query_df(conn, f'''SELECT * from {TABLE_TRANS} limit 0 ''', ret = 'cols')
    existing_columns_pop = [c for c in existing_columns if c != 'id_etl']

    # Nối kết quả Báo cáo doanh số KPI Kangnam, Paris vào 1 Dataframe
    result_trans = pd.concat([dfckn[existing_columns_pop], dfcpr[existing_columns_pop]])
    ls_id_etl = ['id', 'clineid', 'product_id', 'consultants', 'ordinal_consult', 'chinhanhphanquyen_fix']
    result_trans['id_etl'] = join_cols(result_trans, ls_id_etl, sep='-')
   
    # Kiểm tra id Bảng "report_kpi_bonus_commission_transpose" là duy nhất? 
    if not result_trans['id_etl'].is_unique:
        raise ValueError(f'Kiểm tra lại id_etl Bảng {TABLE_TRANS}: {result_trans["id_etl"].duplicated().sum()} id trùng')
        
    WHERE_UD = f'''brand_etl > 0 AND payment_date >= '{LIMIT_START_DATE}' '''
    replace_table_data(result_trans, TABLE_TRANS, where=WHERE_UD, conn=conn)
    end = time.time()
    print('THÀNH CÔNG! \nCập nhật dữ liệu Doanh số KPI Nhân viên từ ngày ',  LIMIT_START_DATE, '  (', str(result_trans.shape[0]), ' dòng: ', round((end-start), 4), "s )", )

    return TABLE_TRANS

#step_report_kpi_commission_transpose()


# Cập nhật thông tin dịch vụ, thứ tự Ngày thanh toán giảm dần 
def step_update_info():
    ls_table = ['report_kpi_bonus_commission', 'report_kpi_bonus_commission_transpose']
    for table in ls_table:
        qr_update = f'''
    WITH cline AS 
    ( --- Giới hạn các crm_line có cập nhật giao dịch tiền trong n ngày gần nhất 
    SELECT clineid, product_id, brand_etl, MAX(number_used) number_used, MIN(stage_product) stage_product 
    FROM stag_payment_transfer spt 
    WHERE brand_etl > 0 AND write_date > (now()::date - interval '5 days') 
    GROUP BY clineid, product_id, brand_etl
    ), spt as 
    ( --- Đánh thứ tự ngày thanh toán giảm dần 
    SELECT pm.id, pm.clineid, cline.number_used, cline.stage_product, 
    DENSE_RANK() OVER (PARTITION BY pm.clineid, pm.product_id, pm.brand_etl ORDER BY payment_date DESC) AS rnk_clineid_pmdate_desc
    FROM stag_payment_transfer pm
    LEFT JOIN cline ON pm.clineid = cline.clineid AND pm.product_id = cline.product_id AND pm.brand_etl = cline.brand_etl
    WHERE cline.clineid IS NOT NULL
    ) --- Update giá trị
    UPDATE {table} kpi
        SET number_used = spt.number_used, stage_product = spt.stage_product, 
        	rnk_clineid_pmdate_desc = spt.rnk_clineid_pmdate_desc
        FROM spt
        WHERE kpi.id = spt.id AND kpi.clineid = spt.clineid
    '''
        query_df(conn, qr_update, ret='exe')
    return 'Done!'

#step_update_info()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'report_kpi_bonus_commission': step_report_kpi_commission,
    'report_kpi_bonus_commission_transpose': step_report_kpi_commission_transpose,
    'update_info': step_update_info,
}


def main():
    parser = argparse.ArgumentParser(description='ETL report_kpi_bonus_commission')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
