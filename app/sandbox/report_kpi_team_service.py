# -*- coding: utf-8 -*-
"""
REPORT KPI TEAM SERVICE

Chuyen tu quentk_3_report_kpi_team_service_New_sys.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/report_kpi_team_service_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python report_kpi_team_service.py --task report_kpi_team_service
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


# <center><b><font size=5> 1. BÁO CÁO KPI PHÒNG DỊCH VỤ - ĐIỂM THỰC HIỆN </size></b></center>


# Lấy dữ liệu nhân sự thực hiện

query = \
f''' 
WITH limit_walkin AS 
(SELECT walkinid, brand_etl 
FROM stag_his_team_service 
WHERE state_ckpttt = 'Done' AND brand_etl <> 0 AND services_date >= '{LIMIT_START_DATE}'
GROUP BY walkinid, brand_etl 
), done_service as 
(
SELECT
CASE WHEN sht.ck_pttt like 'PTTT%' THEN 'surgery'
	WHEN sht.ck_pttt like 'NHA%' THEN 'odontology'
    ELSE REPLACE(LOWER(SPLIT_PART(sht.ck_pttt, '-', 1)), ' ', '_')
END type_form
, team_service_id, form_id, ck_pttt, sht.is_new_request
, sht.services_date, sht.services_end_date
, ROUND(EXTRACT(epoch FROM 
        sht.services_end_date - sht.services_date)/60, 0) services_interval_minutes
, product_id
, rn_walkin_product drk_crm_product
, DENSE_RANK() OVER (PARTITION BY sht.brand_etl, sht.walkinid, product_id ORDER BY sht.services_date::date, form_id) drk_walkin_product
, state_ckpttt
, sht.walkinid, walkin, state_walkin, employee_id, roleid
, string_agg(cast(roleid as varchar), ',') over (partition by sht.brand_etl, sht.id, sht.product_id) aggroleid
, ROW_NUMBER() OVER (PARTITION BY sht.brand_etl, sht.id, sht.product_id, sht.roleid) countid	
, rg.role_rename "role"
, rg.group_id role_group_id, rg."type" type_form_role, rg.div_rto  
, COUNT(CASE WHEN rg.group_div_id <> 0 THEN rg.group_div_id END) OVER (PARTITION BY sht.brand_etl, sht.id, sht.product_id, rg.group_div_id) count_role_group
, sht.order_priorole
, min_order_priorole
, CASE WHEN min_order_priorole = 0 THEN dr_order_role - 1 ELSE dr_order_role END dr_order_role
, sht.room room_id
, sht.sale_order_id
, concat(sht.sale_order_id, '-', product_id, '-', sht.brand_etl) order_product_id
, sht.partner_id
, sht.crm_id
, sht.company_id
, sht.brand_etl
, cl.name as booking, cl.type_crm_id, cl.lead_id, cl.code_customer, cl.contact_name, cl.source_id, cl.category_source_id
, rc.brand_id, rc.name as Company, d.name as room
FROM "public".stag_his_team_service sht
LEFT JOIN "public".import_team_role_group rg ON sht.roleid = rg.id
INNER JOIN limit_walkin lw ON (sht.walkinid, sht.brand_etl) = (lw.walkinid, lw.brand_etl)
LEFT JOIN "public".crm_lead cl ON sht.crm_id = cl.id AND sht.brand_etl = cl.brand_etl 
LEFT JOIN "public".m_res_company rc ON rc.id = sht.company_id AND rc.brand_etl = sht.brand_etl
LEFT JOIN "public".sh_medical_health_center_ot d ON d.id = sht.room AND d.brand_etl = sht.brand_etl 
WHERE state_ckpttt = 'Done' 
)
SELECT ds.*
, 	sms.anesthetist_type, dccw."result" anesthetist_type_name
,	sp.his_service_type, sp.product_category, COALESCE(sp.product, '') product,	sp.surgery_type
, 	null::text AS agg_surgery_type 		---- string_agg(surgery_type, ',') over (partition by walkinid) 
, 	MAX(CASE sp.surgery_type WHEN 'major' THEN 2 WHEN 'minor' THEN 1 ELSE 0 END) OVER (PARTITION BY walkinid, ck_pttt, ds.brand_etl) max_surgery_type_form
, 	MAX(CASE sp.surgery_type WHEN 'major' THEN 2 WHEN 'minor' THEN 1 ELSE 0 END) OVER (PARTITION BY walkinid, ds.brand_etl) max_surgery_type_walkin
,  	sp.kpi_point
, 	COALESCE(CASE WHEN sp.product LIKE '%xăm có sẹo%' THEN 6 ELSE sp.kpi_point_num END, 1) kpi_point_num
, 	sp.product_ctg_id
, 	sp.group_id product_group_id
,   sp.kpi_first_time 
, 	CASE WHEN sp.group_id IS NOT NULL THEN 
		DENSE_RANK() OVER (PARTITION BY ds.brand_etl, walkinid, sp.group_id ORDER BY revenue desc, sp.list_price DESC, sp.product_id) 
		ELSE 1 END AS drk_product_group_id            --- Thứ hạng dịch vụ cùng loại theo Giá trị doanh thu và giá gốc giảm dần
, 	CASE WHEN sp.group_id is not null then 
		DENSE_RANK() OVER (PARTITION BY ds.brand_etl, walkinid, sp.group_id ORDER BY revenue asc, sp.list_price ASC, sp.product_id) 
		ELSE 1 END AS drk_product_group_id_asc        --- Thứ hạng dịch vụ cùng loại theo Giá trị doanh thu và giá gốc tăng dần
, 	ssos.product_uom_qty, ssos.uom_price, ssos.price_unit, ssos.revenue, ssos.pricelist, ssos.pricelist_id  
, 	ssos.type_guarantee
,   (1 - ssos.total/NULLIF(ssos.total_before_discount, 0)) AS perc_discount
, 	max(cast(ssos.type_guarantee as numeric)) over (partition by ds.brand_etl, ds.crm_id) max_type_guarantee
, 	ssos.bh_ngoai_goi 
, 	se.employee_code, initcap(se.employee) employee, se.job, se.department, se.company company_employee
, 	CASE WHEN se.id = 3337 OR se.company_id = 12 then 1 ELSE se.brand_id END thuonghieuphanquyen
, 	CASE WHEN se.id = 3337 then 2 ELSE se.company_id END chinhanhphanquyen_fix
, 	CASE WHEN se.id = 3337 then 2047 ELSE se.phongbanphanquyen END phongbanphanquyen_fix
, 	se.id nhanvienphanquyen_fix
FROM done_service ds
LEFT JOIN view_stag_products sp ON ds.product_id = sp.product_id and ds.brand_etl = sp.brand_etl 
LEFT JOIN stag_sale_order_services ssos on ssos.order_product_id = ds.order_product_id and ssos.brand_etl = ds.brand_etl
LEFT JOIN view_stag_employees_user se on se.id = ds.employee_id and se.brand_etl = ds.brand_etl
LEFT JOIN sh_medical_surgery sms on ds.form_id = sms.id and sms.brand_etl = ds.brand_etl
LEFT JOIN dm_convert_case_when dccw ON sms.anesthetist_type = dccw.value_case_when 
    AND dccw.column_name = 'anesthetist_type' AND ds.team_service_id like 'surg%'
    AND dccw.table_name = 'sh_medical_surgery' AND dccw.brand_etl = sms.brand_etl 
---WHERE ds.services_date >= '{LIMIT_START_DATE}'
'''


# Dịch vụ Giảm giá sâu 0 đồng
query_dd = \
'''SELECT crm_id, product_id, MAX(reason_discount_id) reason_discount_id 
FROM ggs_discount_review_doctor 
WHERE is_approved_discount = 1
GROUP BY crm_id, product_id'''


# Bảng tên Cộng tác viên nội bộ trên Booking
query_collaborator = \
f'''
SELECT DISTINCT (shts.brand_etl, crm_id), 
    shts.brand_etl, crm_id, cl.name booking, cl.contact_name, gce.collaborator_id, gce.employee_id collaborator_employee_id, gce.collaborator 
FROM stag_his_team_service shts 
LEFT JOIN crm_lead cl ON shts.crm_id = cl.id and shts.brand_etl = cl.brand_etl 
LEFT JOIN ggs_collaborator_employee gce ON cl.collaborator_id = gce.collaborator_id 
WHERE gce.collaborator_id IS NOT NULL AND cl.active AND shts.brand_etl <> 0 
---AND shts.services_date >= '{LIMIT_START_DATE}'
'''

roleidgrt = [46,48,49,50]

# DỮ LIỆU LỊCH SỬ BẢO HÀNH DỊCH VỤ PHẪU THUẬT/BOOKING

query_grt = f'''
WITH v_crm_id AS (         --- Cac crm_id trong lich su cua KH Bao hanh
SELECT DISTINCT UNNEST(ARRAY[crm_id, lead_id]) crm_id_grt FROM stag_service_employee_guarantee
), grt AS (                --- Thong tin Booking, dich vu Bao hanh
SELECT
  	sseg.brand_etl, sseg.id, crm_id, booking, initial_booking
	, sseg.product_ctg_id, sseg.product_id, unit_price, total, sseg.initial_product_ctg_id, sseg.initial_product_id, type_guarantee, bh_ngoai_goi 
	, reason_guarantee_id, cgr.name reason_guarantee, cgr.perc
	, lead_id, stage, sseg.product_category, sseg.product, sp.kpi_point, initial_his_service_type, initial_product_category, initial_product
	, spi.kpi_point initial_kpi_point, spi.kpi_first_time, initial_total
	, initial_ck_pttt, initial_services_date, employee_id, roleid, count_employee_grt, sseg.employee_code, INITCAP(sseg.employee) employee, role
    , service_guarantee_type
FROM "public".stag_service_employee_guarantee sseg
LEFT JOIN view_stag_products sp on sseg.product_id = sp.product_id and sp.brand_etl = sseg.brand_etl 
LEFT JOIN view_stag_products spi on sseg.initial_product_id = spi.product_id and spi.brand_etl = sseg.brand_etl 
LEFT JOIN "public".crm_guarantee_reason cgr on sseg.reason_guarantee_id = cgr.id and cgr.brand_etl = sseg.brand_etl 
WHERE sseg.stage <> 'cancel' ---and sseg.brand_etl <> 0
---order by crm_id, product_id
), treatment as (          
--- Lich su So lan Khach hang thuc hien cung 1 Dich vu, sap xep theo Ngay thuc hien dau tien cua crm_line_id
select swet.brand_etl, sol.crm_line_id clineid, min(drk_grt_times) as drk_partner_product 
from stag_walkin_evaluation_treatment swet 
left join sale_order so on swet.sale_order_id = so.id and swet.brand_etl = so.brand_etl 
left join sale_order_line sol on sol.order_id = so.id and sol.brand_etl = so.brand_etl 
where exists (select 1 from stag_service_employee_guarantee sseg2 where swet.crm_id = sseg2.crm_id) 
group by swet.brand_etl, sol.crm_line_id
)
SELECT grt.*, 
	treatment.drk_partner_product,  								--- Lan thuc hien DV LT theo Ngay den cua
    se.job, 
    se.company AS company_employee, 
    se.id nhanvienphanquyen_fix, 
    se.phongbanphanquyen AS phongbanphanquyen_fix, 
    se.company_id chinhanhphanquyen_fix
FROM grt
LEFT JOIN treatment on (treatment.clineid, treatment.brand_etl) = (grt.id, grt.brand_etl)
LEFT JOIN stag_employees se ON grt.employee_code = se.employee_code
'''


# DỮ LIỆU LỊCH SỬ KHÁCH BẢO HÀNH DA LIỄU
# Data khách hàng có mua Gói bảo dưỡng
query_gbd = f'''\
SELECT l.brand_etl, l.id AS gbd_clineid, l.product_id, sp.product, l.quantity, l.total_received, cl.partner_id, 
	sp.group_id, 
    TRUE AS is_have_gbd, 
	ROW_NUMBER() OVER (PARTITION BY cl.partner_id, sp.group_id ORDER BY l.id DESC) AS rn_partner
FROM crm_line l 
LEFT JOIN view_stag_products sp ON l.product_id = sp.product_id and l.brand_etl = sp.brand_etl 
LEFT JOIN crm_lead cl ON l.crm_id = cl.id and l.brand_etl = cl.brand_etl 
WHERE sp.gr_bh_id = 5 AND l.total_received >= 2000000 
	AND (sp.product ~* 'gói bảo dưỡng')
'''

query_treat_grt = f'''\
SELECT w.brand_etl, w.partner_id, w.crm_id, w.form_id as char_form_id, w.walkinid, w.product_id, 
	w.walkin, w.form_name, sp.product, w.services_date, 
	w.drk_customer_product,  
	w.drk_grt_times, 
	w.lag_form_id, 
    COALESCE(sp.group_id::int, w.product_id) AS group_fix_product_id
FROM stag_walkin_evaluation_treatment w 
LEFT JOIN view_stag_products sp ON w.product_id = sp.product_id AND w.brand_etl = sp.brand_etl 
LEFT JOIN sh_medical_appointment_register_walkin smarw ON w.walkinid = smarw.id AND w.brand_etl = smarw.brand_etl 
WHERE smarw.type_crm_id = 3
	AND w.form_name NOT like 'TK%'
	AND w.services_date >= '{LIMIT_START_DATE}' 
    AND w.brand_etl <> 0
'''

query_his_derm = f'''\
SELECT k.partner_id, k.crm_id,  
    case when ck_pttt like 'PTT%' then 'pt-' else 'ck-' end || k.form_id 
	|| (case when k.brand_etl <> 0 then '-' || k.brand_etl else '' end) as lag_form_id, 
    k.services_date,
	k.product_group_id, k.product_id, k.ck_pttt, coalesce(NULLIF(k.product_group_id, '0')::int, k.product_id) as group_fix_product_id, 
	k.product, k.employee_id, k.employee_code, k.employee, k.role, k.roleid, k.aggroleid, 
	k.order_priorole, se.job, se.company AS company_employee, 
	k.nhanvienphanquyen_fix, COALESCE(se.phongbanphanquyen, k.phongbanphanquyen_fix) AS phongbanphanquyen_fix, 
    se.company_id AS chinhanhphanquyen_fix
FROM report_kpi_team_service k
LEFT JOIN stag_employees se ON k.employee_code = se.employee_code
LEFT JOIN (SELECT partner_id FROM stag_walkin_evaluation_treatment w 
	WHERE drk_grt_times is not null AND w.brand_etl <> 0 AND services_date >= '{LIMIT_START_DATE}'
	GROUP BY partner_id) 
	t ON (k.partner_id) = (t.partner_id)
where t.partner_id IS NOT NULL AND k.roleid IN (1,9,10,11,12,13,14,15,40) 
'''

qr_product_pricelist =  '''
SELECT sp.brand_etl,
    sp.product_id, sp.default_code, sp.product_category, sp.product, ppi.fixed_price, ppi.active,ppi.create_date, pp.name pricelist, ppi.pricelist_id, 
	ROW_NUMBER() OVER (PARTITION BY sp.brand_etl, sp.product_id, ppi.pricelist_id ORDER BY ppi.active DESC, ppi.create_date DESC) AS rn_pd_pp, 
	ROW_NUMBER() OVER (PARTITION BY sp.brand_etl, sp.product_id ORDER BY ppi.active DESC, ppi.create_date DESC) AS rn_pd
FROM view_stag_products sp 
LEFT JOIN product_pricelist_item ppi ON sp.product_id = ppi.product_id AND sp.brand_etl = ppi.brand_etl
LEFT JOIN product_pricelist pp ON ppi.pricelist_id = pp.id AND pp.brand_etl = ppi.brand_etl
WHERE pp.id IS NOT NULL and ppi.fixed_price > 0
'''

qr_role = '''\
SELECT *
FROM import_team_role_group itrg 
WHERE COALESCE(itrg.group_id, 0) <> 0 
	AND (
		itrg."type" IN ('laser', 'spa', 'wellness', 'weight_loss') 
		OR 
		(itrg.position = 'nursing' AND itrg.type = 'surgery')
		)
'''

# Bảng check giá trị dịch vụ bị BH trong lịch sử
qr_his_derm = f''' \
WITH partner_grt AS 
(   --- Id khách hàng có Bảo hành
SELECT partner_id 
FROM sh_medical_appointment_register_walkin
WHERE type_crm_id = 3 AND service_date_start >= '{LIMIT_START_DATE}'
GROUP BY partner_id 
), cline_total AS 
(   --- Dòng dịch vụ đã Kết thúc của KH có phát sinh BH
SELECT cl.brand_etl, cl.partner_id, cline.product_id, cline.unit_price unit_price_grt, cline.total, cline.total_received, cline.uom_price, cline.quantity, 
    (1 - cline.total/cline.total_before_discount) AS perc_discount_grt, 
    CASE WHEN sp.group_id::int >= 100 THEN sp.group_id::int ELSE cline.product_id END AS grt_product_group_id, 
	ROW_NUMBER() OVER (partition by cl.partner_id, 
		 CASE WHEN sp.group_id::int >= 100 THEN sp.group_id::int ELSE cline.product_id END   --- Gán lại các dịch vụ cùng loại nhưng khác id 
		ORDER BY CASE WHEN cline.total_received > 0 THEN 1 ELSE 2 END, cline.id DESC) AS rn_partner_pd
FROM partner_grt g
LEFT JOIN crm_lead cl ON g.partner_id = cl.partner_id 
LEFT JOIN crm_line cline ON cl.id = cline.crm_id and cline.brand_etl = cl.brand_etl 
LEFT JOIN view_stag_products sp ON cline.product_id = sp.product_id and cline.brand_etl = sp.brand_etl 
WHERE g.partner_id IS NOT NULL AND cline.stage = 'done' AND cline.total > 100000
) SELECT c.*, ROUND(c.total/c.quantity, 1) AS his_revenue_pd
FROM cline_total c
'''

dict_get_df = {'df_service': query, 
               'df_grt': query_grt, 
               'df_coll': query_collaborator, 
               'df_dd': query_dd,
               'df_products': 'SELECT * FROM view_stag_products', 
               'stag_employee': 'SELECT * FROM view_stag_employees_user', 
               'df_gbd': query_gbd,                        
               'df_treat_grt': query_treat_grt, 
               'df_his_derm': query_his_derm, 
               'df_his_derm_revenue': qr_his_derm, 
               'df_grt_times': 'SELECT * FROM import_guarantee_times WHERE active', 
               'product_pricelist': qr_product_pricelist, 
               'dfrole': qr_role, 
               'df_grt_treat': f'''SELECT * FROM stag_walkin_evaluation_treatment WHERE drk_grt_times IS NOT NULL AND services_date >= '{LIMIT_START_DATE}' '''
              }
#df_service = query_df(conn, query)
#df_service.shape


def get_df_gbd(): 
    if 'df_gbd' not in _CACHE: 
        gbd = get_df_cache('df_gbd', dict_get_df, conn=conn)
        frames = []
        for idb in list(v for v in gbd.brand_etl.unique() if v != 0):
            ls_partner = gbd[gbd.brand_etl.eq(idb)]['partner_id'].unique().tolist()
            temp = gbd[gbd.brand_etl.isin([0, idb]) & gbd.partner_id.isin(ls_partner)].copy()
            temp['brand_etl'] = idb
            temp['rn_gbd'] = temp.sort_values('gbd_clineid').groupby(['partner_id', 'group_id']).cumcount() + 1
            frames.append(temp)
        _CACHE['df_gbd'] = pd.concat(frames, ignore_index=True)
        fix_type(_CACHE['df_gbd'], ['brand_etl','partner_id','group_id','gbd_clineid'], int, 0)
    return _CACHE['df_gbd']
#df_gbd =  get_df_gbd()


LAST_PARTNER_ID = 786601         # Giá trị partner_id cuối cùng trên hệ thống ERP trước tách, sau giá trị này id trùng giữa các TH

# Id khách hàng duy nhất xuyên thương hiệu: id <= LAST (chung từ hệ thống cũ) giữ nguyên '123'; id mới sau tách -> '123-<brand_etl>'
def get_char_id(df, field='partner_id', last_value=LAST_PARTNER_ID):
    val = pd.to_numeric(df[field], errors='coerce').astype('Int64')
    brand = pd.to_numeric(df['brand_etl'], errors='coerce').astype('Int64')
    is_new = val.gt(last_value).fillna(False) & ~brand.fillna(0).isin([0, 4, 7])
    char = val.astype('string')
    return char.where(~is_new, char + '-' + brand.astype('string'))

def get_df_grt_derm(): 
    df_his_derm = get_df_cache('df_his_derm', dict_get_df, conn=conn)
    his_derm = df_his_derm.drop_duplicates(['lag_form_id', 'group_fix_product_id', 'employee_code'])

    df_treat_grt = get_df_cache('df_treat_grt', dict_get_df, conn=conn)         # Lịch sử phiếu điều trị/làm dịch vụ có thông tin Phiếu thực hiện liền trước      
    df_gbd = get_df_cache('df_gbd', dict_get_df, conn=conn)                     # Data khách hàng có mua Gói bảo dưỡng

    print('df_treat_grt: ', df_treat_grt.shape, '\n', 'df_his_derm: ', df_his_derm.shape) 
    # Xử lý Nối dữ liệu các phiếu thực hiện trước của Phiếu khám bảo hành
    df_grt_derm = df_treat_grt.merge(his_derm, how='left', on=['lag_form_id', 'group_fix_product_id'], suffixes=('', '_his'))
    df_grt_derm['id_grt_derm'] = join_cols(df_grt_derm, ['char_form_id', 'group_fix_product_id'], sep='-', na='0')
    #df_grt_derm['is_have_gbd'] = pd.NA
    df_grt_derm['char_partner_id'] = get_char_id(df_grt_derm, 'partner_id')
    gbd = df_gbd[df_gbd.rn_partner.eq(1)].assign(char_partner_id=lambda d: get_char_id(d, 'partner_id'))
    gbd = gbd[['char_partner_id', 'is_have_gbd']].dropna(subset=['char_partner_id']).drop_duplicates('char_partner_id')
    cdf_grt_derm = df_grt_derm.merge(gbd, how='left', on='char_partner_id')
    return df_grt_derm


# HÀM TÍNH HỆ SỐ ĐIỂM THỰC HIỆN 
# countd_role_group: đếm xem có tồn tại nhiều hơn 1 loại (tên) vai trò khác nhau cùng nhóm (group_id, ngoại trừ nhóm 0) được chia tổng 100% KPI DV với nhau
# count_role_group: số thành viên chia đều tỷ lệ KPI nếu tồn tại cùng nhóm (group_div_id, ngoại trừ nhóm 0)
# is_oms: Dịch vụ nhóm HÀM MẶT, điều chỉnh chia KPI giữa BS PTTM - BS làm kế hoạch hàm
# have_plan_oms: Dịch vụ Chỉnh hàm hô/móm nhóm HÀM MẶT có kế hoạch, điều chỉnh chia KPI giữa BS PTTM - BS làm kế hoạch hàm
# is_incorrect_role_form: Nhập sai role quy tắc trên hệ thống (role không khớp loại DV chuyên môn, phát sinh role ngoài Logic tính KPI chưa thông báo team DL...)
# is_collaborator_surgeon: Dịch vụ được duyệt 0 đồng (quyền Bác sĩ) => không tính điểm KPI toàn bộ ekip

columns = ['is_oms', 'have_plan_oms', 'bs_chinh', 'bs_phu', 'bs_kehoach'] 
kpi_doctor = [
    [False, False, 0.8, 0.2, 0], 
    [True, False, 0.6, 0.4, 0], 
    [True, True, 0.4, 0.3, 0.3]
]
dfk = pd.DataFrame(kpi_doctor, columns=columns)   # Data Hệ số điểm KPI BS mổ chính - BS mổ phụ - BS kế hoạch theo Dịch vụ thường - Hàm mặt - Chỉnh hàm hô/móm (làm 3D)

def cal_rto_kpi(employee_id=0, roleid=0, aggroleid='0', countd_role_group=1, count_role_group=1, product_id=0, product_ctg_id=0, 
                is_oms=False, have_plan_oms=False, team_service_id='', is_incorrect_role_form=False, is_guarantee=False, 
                is_collaborator_surgeon=False):
    lsroleid = list(int(e) for e in re.split(r'{|}|,', str(aggroleid)) if e.isdigit()) 
    roleid = int(roleid) if str(roleid).isdigit() else 0
    is_surg_doctor = roleid in [1,2,37,38,57,58]
    ls_role_oms = [51,37,38,39,43,57,58,60,61]                             # Danh sách Vai trò về hàm mặt
    ls_endo_dry = [2138,4673,9898,12177,17590,17792,17940,17942,22140,37469]     # Dịch vụ Hút tuyến (Điều dưỡng thực hiện không có BS: 200%) 
    
    if is_surg_doctor:              # BS phau thuat
        getr = dfk.loc[dfk.is_oms.eq(is_oms) & dfk.have_plan_oms.eq(have_plan_oms)]
        rto_plan = getr.bs_kehoach.values[0] \
                    if any(i for i in [37,38,57,58] if i in lsroleid) \
                    else 0
        rto_bsphu = getr.bs_phu.values[0] if 2 in lsroleid else 0
        rto_bschinh = 1 - rto_bsphu - rto_plan
        rto = rto_bschinh if roleid == 1 else rto_bsphu if roleid == 2 else rto_plan 
        
    elif roleid in [5,6]:       # Điều dưỡng vòng trong/vòng ngoài: 80/20  (Update: 10/09/2024)
        # Dịch vụ Hút tuyến - Điều dưỡng thực hiện không có BS: 200%  (Update: 07/11/2024)
        endo_dry = 2 if 1 not in lsroleid and roleid == 5 and product_id in ls_endo_dry and not is_guarantee else 1  
        rto = endo_dry*(0.2 if roleid == 6 else 0.8 if 6 in lsroleid else 1) 
        count_role_group = lsroleid.count(roleid)
    elif roleid == 51 and employee_id in [2775,2789]:     # KNSG: ĐD Võ Hồng Phúc, Đặng Văn Tuấn vai trò "chuyên viên hàm mặt" 100%  
        rto = lsroleid.count(roleid)
    else:                       # Cac Vai tro thuc hien khac
        lsdd = [6, 7, 21, 22, 23, 24]
        if roleid in lsdd:
            roleid = 5          # Dieu duong vong trong
            for i in range(len(lsroleid)):
                if lsroleid[i] in lsdd:  # Dieu duong
                    lsroleid[i] = 5  
        rto = 1

    # BS mổ chính là CTV nội bộ hoặc Nhập vai trò Hàm mặt, Hút mỡ không đúng Dịch vụ hoặc không đúng loại phiếu thủ thuật/chuyên khoa
    incorrect_role = is_incorrect_role_form \
                    or (roleid in ls_role_oms and not is_oms) \
                    or (roleid == 45 and product_ctg_id not in [384,442,1402,1655,1815,2019])  \
                    or is_collaborator_surgeon   # (employee_id == collaborator_employee_id and roleid == 1 and is_guarantee != True)
    result = None if count_role_group == 0 or incorrect_role else rto/count_role_group # if lsroleid.count(roleid) > 0 else 1
    return result

# ----------------------------------------------- ₍^. .^₎⟆  ------------------------------------------

# HÀM TÍNH HỆ SỐ ĐIỂM BẢO HÀNH - TRỪ BẢO HÀNHàm tính hệ số điểm Bảo hành - Trừ bảo hành

columns_bh = ['label_guarantee_type', 'rto_bh', 'rto_tbh', 'note']
perc = 0.2
kpi_bh = [
    ['not_guarantee', None, None, 'Không phải Dịch vụ bảo hành'], 
    ['bh_phunxam', 0.3, -0.3, 'bảo hành Phun xăm'], 
    ['bh_lt_tronggoi', 1, 0, 'Bảo hành DV liệu trình/Cấy dặm trong gói'], 
    ['bh_lt_ngoaigoi', 0.5, -0.5, 'Bảo hành liệu trình vượt số lần QĐ'], 
    ['bh_derm_for_surgery', 1, 0, 'Thực hiện DV Laser/Spa cho BH phẫu thuật'], 
    ['bh_hair_removal', 0.2, 0, 'Bảo hành triệt lông'], 
    ['bh_derm_other', 0, 0, 'Bảo hành DV Da, không cộng/trừ điểm'], 
    ['bh_not_npi', 0.5, -0.5, 'BS khác bảo hành lại lỗi không chuyên môn HOẶC BH DV Cấy mỡ'], 
    ['bh_not_npi_null_emp', 0, 0, 'Bảo hành không lỗi CM nhưng không có tên nhân sự bị BH'], 
    ['bh_totality', 1.5, -2, 'Bảo hành toàn phần'], 
    ['bh_not_totality', None, None, 'Bảo hành một phần còn lại']
]
dfkpi_bh = pd.DataFrame(kpi_bh, columns=columns_bh)

def cal_rto_bh(type_kpi_ekip, is_guarantee, type_guarantee, reason_guarantee_id, rto_kpi, perc, is_hair_removal, drk_partner_product, \
               his_service_type, product_id, product_ctg_id, initial_his_service_type, initial_product_id, initial_product_ctg_id, \
               kpi_point, initial_kpi_point, kpi_point_num, employee_id_grt, ck_pttt):
    rto_bh = None
    rto_tbh = None
    # is_grt_stol: dịch vụ bảo hành thay thế là BH bổ trợ 1 phần cho DV chính: trị sẹo, tháo chất liệu...
    is_grt_stol = initial_product_id is not None and (initial_product_id != product_id) and (kpi_point <= initial_kpi_point/2)
    if not is_guarantee | (rto_kpi == 0) | (type_kpi_ekip == 'source_rent_room'):
        lb = 'not_guarantee'
    elif initial_product_ctg_id in [424,1361,485]:   # Bảo hành PHUN XĂM
        lb = 'bh_phunxam'
    elif reason_guarantee_id in [20,31] and kpi_point_num > 1:              # DV trong gói/liệu trình/Cấy dặm
        if drk_partner_product in range(1, kpi_point_num): 
            lb = 'bh_lt_tronggoi'                                           # Số lượt thực hiện (theo Booking) nằm trong Số lần quy định
        else:                                                               
            lb = 'bh_lt_ngoaigoi'                                           # Số lần thực hiện vượt QĐ
    elif check_substr(his_service_type, 'laser|spa'):
        if is_hair_removal: 
            lb = 'bh_hair_removal' 
        elif is_grt_stol or initial_his_service_type=='Surgery': 
            lb = 'bh_derm_for_surgery'                                       
        else: 
            lb = 'bh_derm_other'
    elif (type_guarantee == 9) | (reason_guarantee_id in [19,31]) | (product_ctg_id in [399,439,1376,2022]):    # BS khác bảo hành lại lỗi không chuyên môn HOẶC BH DV Cấy mỡ
        lb = 'bh_not_npi' if employee_id_grt is not None else 'bh_not_npi_null_emp'
    elif (type_guarantee in [5,6,7,8,11]) | (reason_guarantee_id == 21) | ((reason_guarantee_id == 20) & ~(his_service_type in ('Laser','Spa'))):    # Bảo hành toàn phần, chỉ định BH do chuyên môn
        #(type_guarantee in [5,6,7,8,11]) | (reason_guarantee_id != 21) | ((reason_guarantee_id == 20) & (his_service_type=='Surgery'))
        lb = 'bh_totality'
    else: 
        lb = 'bh_not_totality'
    #print(lb)
    
    getr_bh = dfkpi_bh[dfkpi_bh.label_guarantee_type.eq(lb)]
    if lb == 'bh_not_totality' or getr_bh.shape[0]==0: 
        rto_bh = perc if 0 < perc < 0.5 else 0.5
        rto_tbh = -perc if reason_guarantee_id > 0 else -1
    else: 
        rto_bh = getr_bh.rto_bh.values[0]
        rto_tbh = getr_bh.rto_tbh.values[0]
        
    ls_rto = [rto_bh, rto_tbh]
    return ls_rto

ls_col_text = ['order_product_id', 'team_service_id', 'aggroleid', 'id']


# XỬ LÝ DATA THỰC HIỆN CHUNG
def kpi_service_cal():
    # Nối các dữ liệu thông tin: dịch vụ bảo hành/Booking, cộng tác viên nội bộ (nếu có)
    df_service = get_df_cache('df_service', dict_get_df, conn=conn)     # Dữ liệu thực hiện dịch vụ
    df_dd = get_df_cache('df_dd', dict_get_df, conn=conn)               # Dịch vụ được quyền bác sĩ duyệt giảm giá sâu 0 đồng
    df_coll = get_df_cache('df_coll', dict_get_df, conn=conn)           # Thông tin Cộng tác viên/Booking
    df_grt = get_df_cache('df_grt', dict_get_df, conn=conn)
    
    df = df_service[df_service.services_date.dt.date.ge(DATETRUNC)].merge(df_grt, how='left', on=['crm_id', 'product_id'], suffixes=('', '_grt'))
    df = df.merge(df_dd, how='left', on=['crm_id', 'product_id'], suffixes=('', '_dd'))
    df = df.merge(df_coll[['brand_etl', 'crm_id', 'collaborator_id', 'collaborator_employee_id']], how='left', on=['brand_etl','crm_id'])

    df['group_fix_product_id'] = df['product_group_id'].replace(0,np.nan).fillna(df['product_id'])
    df['id'] = np.where(df.ck_pttt.str.startswith('PTT'), 'pt-', 'ck-') + df.form_id.astype(str) \
                            + np.where(df.brand_etl.eq(0), '', '-'+df.brand_etl.astype(str))
    df['rn_team_service_id'] = df.groupby('team_service_id').cumcount().add(1)
    
    df['agg_surgery_type'] = df.groupby(['brand_etl','walkinid'], dropna=False)['surgery_type'].transform(lambda x: ','.join(x.map(str).unique()))
    df['product_uom_qty'] = np.where(df['drk_product_group_id'].gt(1), 0, df['product_uom_qty']*df['uom_price'])
    df['kpi_point_num'] = np.where(df['kpi_point_num'].fillna(0).lt(1), 1, df['kpi_point_num'])
    fix_type(df, ['product_uom_qty', 'kpi_point', 'initial_kpi_point', 'revenue'], float, np.nan)

    # Xử lý trường hợp Dịch vụ bị BH cùng mã đổi tên => đồng bộ theo tên mới nhất
    for ci in ['product', 'product_category']: 
        df['initial_' + ci] = np.where(df.product_id.eq(df.initial_product_id).fillna(False).astype(bool), df[ci], df['initial_' + ci])
    
    # Dịch vụ hút tuyến do Điều dưỡng thực hiện (không có BS mổ chính)
    is_sweat_gland = df['product_id'].isin([4673,2138,12177]) & ~df['aggroleid'].str.contains('1') & df['roleid'].eq(5)
    is_spaodo = df['ck_pttt'].str.contains('SPA')
    # Dịch vụ bảo hành triệt lông
    df['is_hair_removal'] = (df['initial_product_ctg_id'].fillna(0).isin([428,484,1350]) | df.initial_product_category.str.contains('TRIỆT LÔ', case=False, na=False))\
    .fillna(False).astype(bool)  
    
    df['is_oms'] = df['product_ctg_id'].fillna(0).isin([436,276,387,1813,1374,2020])       # Dịch vụ nhóm HÀM MẶT
    df['have_plan_oms'] = df['product'].apply(lambda r: check_substr(r, 'hình hàm h|hàm (.*)(hô|móm)'))
    df['is_roleid_bh'] = df['roleid'].fillna(0).isin(roleidgrt)                       # Các vai trò là người bị Bảo hành
    # is_incorrect_role_form: Kiểm tra nhập vai trò không đúng loại phiếu
    df['is_incorrect_role_form'] = np.where(df.is_roleid_bh | (df.type_form.isin(['laser', 'spa']) & df.type_form_role.isin(['laser', 'spa'])), 
                                             False, df.type_form.ne(df.type_form_role))
    
    # is_guarantee: xác định dịch vụ là dv bị bảo hành 
    df['is_guarantee'] = (df['initial_product_id'].fillna(0).ne(0) | df['type_guarantee'].fillna(0).ne(0) | df['pricelist'].str.contains('bảo hành', case=False, na=False) \
                            | df['drk_product_group_id'].ne(df['drk_product_group_id_asc'])).astype(bool)
    # is_collaborator_surgeon: là BS mổ chính làm cộng tác viên nội bộ đưa KH làm dịch vụ
    df['is_collaborator_surgeon'] = df.roleid.eq(1) & df.employee_id.fillna(0).eq(df.collaborator_employee_id.fillna(-99999)) & ~df.is_guarantee.fillna(False)
    ls_source_rent = [87,88,89,90,219,278,279,280,535,570]          # Nguồn Thuê phòng mổ id
    ls_source = [80,81,82,83,84,87,88,89,90,124,215,216,217,219,278,279,280,287,288,289,292,535]    # CBNV, Đối ngoại, thuê phòng mổ
    
    df['type_kpi_ekip'] = np.select([df['source_id'].isin(ls_source_rent).astype(bool), df.is_collaborator_surgeon.astype(bool), df.reason_discount_id.fillna(0).ne(0).astype(bool)], 
                                     ['source_rent_room', 'collaborator_surgeon', 'discount_doctor_approved'], None)
    df['is_no_kpi'] = df.type_kpi_ekip.isin(['discount_doctor_approved', 'collaborator_surgeon'])   #'collaborator_surgeon', 
    
    # Áp dụng hàm tính hệ số và điểm cho các Thành viên thực hiện
    # rto_lt: hệ số tính cho các Dịch vụ liệu trình số lần > 1
    df['countd_role_group'] = df.groupby('order_product_id')['role_group_id'].transform('nunique')
    ls_cfunction = re.sub(r'[()]|=[\'|\w]+', '', str(inspect.signature(cal_rto_kpi))).split(', ')
    df_args = df[ls_cfunction].astype(object).where(df[ls_cfunction].notna(), np.nan)    # pd.NA -> NaN: tránh lỗi 'boolean value of NA is ambiguous'
    df['rto'] = df_args.apply(lambda r: cal_rto_kpi(*r), axis = 1)
    df['rto_lt'] = np.select([df.kpi_point_num.le(1) | df.kpi_first_time.isnull() | df.is_hair_removal, 
                              ~df.is_guarantee, 
                              df.drk_partner_product.lt(df.kpi_point_num).fillna(False).astype(bool)],
                             [1, df.kpi_first_time, (1-df.kpi_first_time)/(df.kpi_point_num.replace(1,2)-1)],
                             0) 
    df['rto_kpi'] = np.where(df['drk_walkin_product'].gt(1) | df['drk_product_group_id'].gt(1) | df.is_roleid_bh | df.is_no_kpi, 0, df['rto'])
    df['rto_kpi_fix'] = np.where(df.rto_lt.eq(1), df.rto_kpi/df.kpi_point_num, df.rto_lt)
    df['kpi_perf_point'] = df['kpi_point']*df['product_uom_qty']*df['rto_kpi_fix']
    df['kpi_perf_revenue'] = df['revenue']*df['rto_kpi']
    
    # TÍNH HỆ SỐ ĐIỂM BẢO HÀNH LẠI DỊCH VỤ - TRỪ BẢO HÀNH
    
    ls_cfunction = re.sub(r'[()]', '', str(inspect.signature(cal_rto_bh))).split(', ')
    # Giả sử rằng ls_rto chứa tuple hoặc list
    df_args = df[ls_cfunction].astype(object).where(df[ls_cfunction].notna(), np.nan)
    ls_rto =  df_args.apply(lambda r: cal_rto_bh(*r), axis=1)
    
    df['rto_bh'] = ls_rto.str[0]
    df['rto_tbh'] = ls_rto.str[1]
    ls_col_int = [c for c in df.columns if check_substr(c, r'id$|phanquyen|^(count|rn|drk|)_|type_gua') and c not in ls_col_text]
    fix_type(df, ls_col_int, 'Int64', np.nan) 

    print('Bước 2: Xử lý hệ số tính điểm, doanh thu. Done!')
    return df
    

def get_df(): 
    if 'df' not in _CACHE:
        _CACHE['df'] = kpi_service_cal()
    return _CACHE['df']

#df = get_df()


# TRANSFORM DATA THỰC HIỆN: MỚI - BẢO HÀNH - TRỪ BẢO HÀNH

def kpi_service_concat():
    df = get_df()
    df = df[df.crm_id.fillna(0).ne(0)].copy()      # 17/06/2026: Xử lý lỗi hệ thống gắn thiếu mã Booking => Ko có ko tính điểm
    df_grt_derm = get_df_grt_derm()
    
    print('Bước 1: Truy vấn các Bảng dữ liệu sử dụng. Done!')
    cdt_grt = (df['type_crm_id'].eq(3) & (df['dr_order_role'].eq(1) | df['is_hair_removal']) ) \
        | (df.is_guarantee & df.type_form.isin(['laser', 'spa'])) \
        | df['roleid'].isin(roleidgrt)                                            

    # df1: Data nhân sự thực hiện DV mới (loại trừ BH)
    df1 = df[~cdt_grt & df['rn_team_service_id'].eq(1)][[c for c in df.columns if '_grt' not in c]].copy()
    df1['rto_bh'] = None
    
    # df2_bh: Data Nhân sự thực hiện dịch vụ Bảo hành
    df2 = df[cdt_grt].copy()
    is_grt_derm = df2.initial_his_service_type.isin(['Laser', 'Spa']) & ~df2.department.str.contains(r'phẫu|gây mê|điều dưỡng', regex=True, na=False)
    
    # df2_surg: DATA BẢO HÀNH DỊCH VỤ KHÁC DA LIỄU (Phẫu thuật...)
    df2_surg = df2[~is_grt_derm].copy()
    
    col_rename = ['employee_id','employee_code','employee', 'job', 'role','company_employee', 'nhanvienphanquyen_fix', 'phongbanphanquyen_fix','chinhanhphanquyen_fix']
    df2_surg.rename(columns={c + '_grt': 'initial_' + c for c in col_rename}, inplace=True)
    
    df2_surg_bh = df2_surg[df2_surg['rn_team_service_id'].eq(1) & ~df2_surg['roleid'].isin(roleidgrt)].copy()

    # df2_derm: DATA BẢO HÀNH DỊCH VỤ DA LIỄU (Laser, Spa)
    df2_derm = df2[is_grt_derm].copy()
    # Lấy thông tin Nhân sự thực hiện trước (trừ BH)
    df2_derm['id_grt_derm'] = df2_derm.id + '-' + df2_derm['group_fix_product_id'].astype(str)
    df2_derm = df2_derm.merge(df_grt_derm, how='left', on = 'id_grt_derm', suffixes=('', '_grt_derm'))
    df2_derm.rename(columns={c + '_grt_derm': 'initial_' + c for c in col_rename}, inplace=True)
    df2_derm['initial_services_date'] = df2_derm['services_date_his'].fillna(df2_derm.initial_services_date)
    
    # NỐI DATA NHÂN SỰ THỰC HIỆN BẢO HÀNH PHẪU THUẬT + DA
    df2_bh = pd.concat([df2_surg_bh, df2_derm], ignore_index=True)
    df2_bh = df2_bh[~df2_bh.roleid.fillna(0).isin(roleidgrt)].drop_duplicates('team_service_id')
    df2_bh['rto_bh'] = df2_bh['rto_bh'].fillna(1)
    df2_bh['kpi_perf_point'] = df2_bh['kpi_perf_point']*df2_bh['rto_bh']
    

    # --------------------------------------------------     (ㅅ` ˘ `)   --------------------------------------------
    #df2_tbh: DATA NHÂN SỰ BỊ (TRỪ) BẢO HÀNH
    df2_surg_tbh = df2_surg[df2_surg['initial_employee_id'].fillna(0).ne(0)].copy()
    df2_surg_tbh['roleid'] = 46
    
    df2_derm_tbh = df2_derm[df2_derm['initial_employee_id'].fillna(0).ne(0)].copy()
    df2_derm_tbh['roleid'] = np.where(df2_derm_tbh.initial_his_service_type.eq('Laser'), 49, 48)
    # Nối các dữ liệu Bị bảo hành Phẫu thuật + Da liễu
    df2_tbh = pd.concat([df2_derm_tbh, df2_surg_tbh], ignore_index=True)
     
    df2_tbh = df2_tbh[~df2_tbh.initial_product_id.fillna(0).eq(0)]
    df2_tbh['is_samep'] = df2_tbh.product_id.eq(df2_tbh.initial_product_id.fillna(0)) 
    
    col_replace = ['initial_product_ctg_id', 'initial_product_id','initial_his_service_type','initial_product_category','initial_product',
                'initial_employee_id','initial_employee_code','initial_employee','initial_job','initial_company_employee',
                'initial_nhanvienphanquyen_fix', 'initial_phongbanphanquyen_fix', 'initial_chinhanhphanquyen_fix']
    for c in col_replace:
        df2_tbh[c[8:]] = np.where(df2_tbh[c].isnull() | df2_tbh[c].eq(0), df2_tbh[c[8:]], df2_tbh[c])
    id_grt = ['team_service_id', 'initial_product_id', 'initial_employee_id']
    df2_tbh['rn_team_service_grt'] = df2_tbh.sort_values(['is_samep','order_priorole','roleid'],ascending=[False,True,True]).groupby(['form_id','product_id','employee_id']).cumcount()+1
    df2_tbh = df2_tbh[df2_tbh.rn_team_service_grt.eq(1)]    #.drop_duplicates(subset=['form_id', 'product_id', 'employee_id'], inplace=True)      # Bỏ trùng dòng nhân sự bị bảo hành
    fix_type(df2_tbh, ['initial_product_id', 'initial_employee_id'], int, 0)
    df2_tbh['team_service_id'] = df2_tbh[id_grt].apply(lambda r: '-'.join(r.values.astype(str)), axis=1)

    df2_tbh['kpi_point'] = np.where(( df2_tbh.initial_his_service_type.isin(['Surgery', 'ChiPhi']) 
                                    & df2_tbh.kpi_point.lt(df2_tbh.initial_kpi_point) 
                                    & (df2_tbh.type_guarantee.fillna(0).eq(9) | df2_tbh.reason_guarantee_id.fillna(0).eq(19)) )
                                    | df2_tbh.initial_kpi_point.isnull(), 
                                    df2_tbh.kpi_point, df2_tbh.initial_kpi_point)
    
    
    df2_tbh['product_uom_qty'] = np.where(df2_tbh.product_uom_qty.eq(0), 1, df2_tbh.product_uom_qty)
    df2_tbh['initial_kpi_point'] = np.where(df2_tbh.product_id.eq(df2_tbh.initial_product_id.fillna(0)), df2_tbh.kpi_point, df2_tbh.initial_kpi_point)
    df2_tbh['role'] = np.select([df2_tbh.roleid.eq(46), df2_tbh.roleid.eq(48), df2_tbh.roleid.eq(49)], 
                                 ['BS bị bảo hành', 'Bảo hành Spa', 'Bảo hành Laser'], df2_tbh.role)
    df2_tbh['type_kpi_ekip'] = 'Trừ bảo hành'
    df2_tbh['rto_bh'] = df2_tbh['rto_tbh']
    df2_tbh['roleid_grt_fix'] = df2_tbh['roleid_grt_derm'].fillna(df2_tbh.roleid_grt).fillna(df2_tbh.roleid)
    df2_tbh['count_employee_grt'] = df2_tbh.groupby(['form_id', 'product_id', 'roleid_grt_fix'])['employee_id'].transform('count')
    df2_tbh['rto_kpi'] = 1/df2_tbh['count_employee_grt']
    df2_tbh['kpi_perf_point'] = df2_tbh.kpi_point*df2_tbh.product_uom_qty*df2_tbh.rto_kpi*df2_tbh.rto_bh/df2_tbh.kpi_point_num

    # --------------------------------------------------  (˶^▾^˶ )   --------------------------------------------
    # Nối Data các bảng tạm df1, df2, df2_tbh
    df_out = pd.concat([df1, df2_bh, df2_tbh], ignore_index=True).copy()
    
    df_out['overtime'] = 1
    df_out['initial_kpi_perf'] = np.nan
    
    
    # ĐIỂM PHÒNG DỊCH VỤ
    df_out = df_out.copy()
    df_out['roleid_is_guarantee'] = df_out.roleid.isin([46,48,49,50]).astype(bool)
    df_out = df_out.sort_values(['roleid_is_guarantee', 'dr_order_role', 'roleid'], ascending=[True, True, True], na_position='first').copy()
    df_out['is_KN_branch_work'] = df_out.company_id != df_out.chinhanhphanquyen_fix
    col_sort = ['order_product_id', 'drk_walkin_product', 'roleid_is_guarantee', 'is_incorrect_role_form', 'is_KN_branch_work', 'dr_order_role']
    df_out['rn_order_product_id'] = df_out.sort_values(col_sort).groupby('order_product_id').cumcount().add(1)
    df_out['rn_order_product_id'] = np.where(df_out.drk_walkin_product.gt(1), 99, df_out.rn_order_product_id)
    
    ''' 
    1. Dịch vụ không tính KPI (BS duyệt free): điểm phòng = 0
    2. Vai trò bị bảo hành HOẶC Vai trò BS mổ chính, mổ phụ HOẶC bảo hành DV da liễu: điểm phòng = điểm Nhân viên
    3. Các DV Xóa xăm có sẹo: điểm phòng = điểm DV/6  (chia điểm thực hiện 6 lần).
    4. Các DV mới thực hiện còn lại: điểm phòng tính 1 lần nguyên điểm 
    '''
    is_rto_guarantee = (df_out.is_guarantee & (df_out.ck_pttt.str.startswith(('LASE','SPA')) & df_out.rto_bh.fillna(0).le(1))).astype(bool)
    kpi_perf_point_std = df_out.kpi_point*df_out.product_uom_qty/df_out.kpi_point_num
    rto_treat = np.where(df_out.type_crm_id.eq(3), (1-df_out.kpi_first_time)/(df_out.kpi_point_num-1).replace(0, np.nan), df_out.kpi_first_time)
    df_out['kpi_perf_point_department'] = np.select([df_out.is_no_kpi | df_out.rn_order_product_id.ne(1), \
                                                     is_rto_guarantee, \
                                                         df_out.kpi_first_time.fillna(0).ne(0), \
                                                             df_out.product_id.between(4881,4890)], \
                                                [0, \
                                                     kpi_perf_point_std*df_out.rto_bh, \
                                                         kpi_perf_point_std*rto_treat*df_out.kpi_point_num, 
                                                            df_out.kpi_point*df_out.product_uom_qty/6], \
                                                kpi_perf_point_std)
    ls_col_int = [c for c in df_out.columns if check_substr(c, r'id$|phanquyen|^(count|rn|drk|)_|type_gua') and c not in ls_col_text]
    fix_type(df_out, ls_col_int, 'Int64', np.nan) 

    df_cut = df_out[df_out.services_date.dt.date.ge(DATETRUNC)].copy()
    print('Bước 3: Transform và nối dữ liệu thực hiện mới, bảo hành, bị bảo hành. Done!')
    return df_cut

def get_df_out(): 
    if 'df_out' not in _CACHE:
        _CACHE['df_out'] = kpi_service_concat()
    return _CACHE['df_out']
#df_out = get_df_out()


# cols = ['brand_etl','partner_id','crm_id','form_id','id','product_id','product','initial_product','employee_grt','role','id','rto_bh','kpi_perf_point']
# df[df.crm_id.eq(2174419) & df.product_id.eq(4639) & df.roleid.isin([1,46])][cols]

# cols = ['crm_id','form_id','id','product_id','product','initial_product','employee','role','id','rto_bh','kpi_perf_point_department']
# df_out[df_out.order_product_id.eq('865707-28321-1')][cols]


# Kết nối đến cơ sở dữ liệu Dev

def call_kpi_team_service():
    # lấy dữ liệu
    #df = kpi_service_cal(df_service)   #kpi_service_cal(df_service)
    dfc = get_df_out()        # df_out.copy()   kpi_service_concat(df)  
    TABLE = 'report_kpi_team_service'
    # Kiểm tra id Bảng "report_kpi_team_service" là duy nhất?
    if dfc['team_service_id'].is_unique:
        WHERE_UD = f"brand_etl <> 0 AND services_date >= '{LIMIT_START_DATE}' "
        replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
    else:
        raise ValueError(f'Kiểm tra lại id Bảng {TABLE}: {dfc["team_service_id"].duplicated().sum()} id trùng')
    del dfc
    gc.collect() 
    return 'BÁO CÁO DOANH THU - ĐIỂM THỰC HIỆN DỊCH VỤ'

#call_kpi_team_service()


# <center><b><font size=5> 2. BÁO CÁO KPI PHÒNG DỊCH VỤ - THEO CƠ CHẾ TÍNH DOANH THU (GIÁ TRỊ DỊCH VỤ) </size></b></center>


def get_df_his_total(): 
    if 'df_his_total' not in _CACHE: 
        df_his_derm_revenue = get_df_cache('df_his_derm_revenue', dict_get_df, conn=conn)
        frames = []
        for idb in list(i for i in df_his_derm_revenue.brand_etl.unique() if i != 0): 
            ls_partner = df_his_derm_revenue[df_his_derm_revenue.brand_etl.eq(idb)]['partner_id'].unique()
            temp = df_his_derm_revenue[df_his_derm_revenue.partner_id.isin(ls_partner)].copy()
            temp['brand_etl'] = idb
            frames.append(temp)
        _CACHE['df_his_total'] = pd.concat(frames, ignore_index=True)
    return _CACHE['df_his_total']

#df_his_total = get_df_his_derm() 


def data_team_service_revenue(): 
    df_out = get_df_out()
    df_gbd = get_df_gbd()
    df_his_total = get_df_his_total() 
    dfrole = get_df_cache('dfrole', dict_get_df, conn=conn)
    df_products = get_df_cache('df_products', dict_get_df, conn=conn)
    df_grt_times = get_df_cache('df_grt_times', dict_get_df, conn=conn)
    product_pricelist = get_df_cache('product_pricelist', dict_get_df, conn=conn)
    product_pricelist = product_pricelist[product_pricelist.rn_pd_pp.eq(1)]
    
    # Cắt dữ liệu thực hiện đã xử lý, lấy các Vai trò Chuyên khoa da tính Doanh thu
    ls_role_derm = [4,5,6,7,8,9,10,11,12,13,14,15,21,22,23,24,25,26,27,28,29,30,35,40,48,49]
    ls_role_wellness = [88,89,90,91,92,93,94,102,103]
    ls_role_weightloss = [110]
    ls_role_use = dfrole.id.to_list()
    df_derm = df_out[(df_out.drk_walkin_product.eq(1) & ~df_out.ck_pttt.str.contains('NHA'))
                    & (df_out.roleid.isin(ls_role_use) | df_out.job.str.match('(bác s|thẩm m).*da|laser|spa|phun xăm', na=False, case=False))].copy()
    # Nối dữ liệu Kiểm tra giá trị dịch vụ cũ thực hiện và Gói bảo dưỡng đã mua nếu có
    df_derm['grt_product_group_id'] = np.where(df_derm.product_group_id.fillna(0).gt(99), df_derm.product_group_id, df_derm.product_id)
    ls_cols_left = ['brand_etl','partner_id','group_fix_product_id']
    ls_cols_right = ['brand_etl','partner_id','group_id','gbd_clineid']
    dfd = df_derm.copy().merge(df_his_total[df_his_total.rn_partner_pd.eq(1)], how='left', on=['brand_etl','partner_id','grt_product_group_id'], suffixes=('', '_histotal'))
    dfd = dfd.merge(df_gbd[df_gbd.rn_gbd.eq(1)][ls_cols_right], how='left', left_on=ls_cols_left, right_on=ls_cols_right[:-1])
    
    # Tạo id bảng, Check Đơn giá cố định theo Bảng giá hoặc lịch sử set giá gần nhất
    dfd['id_etl'] = dfd['id'] + '-' + dfd['order_product_id']
    dfd = dfd.merge(df_products[['brand_etl', 'product_id', 'gr_bh_id']], how='left', on=['brand_etl', 'product_id'])
    
    p1 = dfd[['brand_etl','product_id','pricelist_id']].merge(product_pricelist, how='left', on=['brand_etl','product_id','pricelist_id'])
    p2 = dfd[['brand_etl','product_id']].merge(product_pricelist[product_pricelist.rn_pd.eq(1)], how='left', on=['brand_etl','product_id'])
    dfd['fixed_price'] = p1['fixed_price'].fillna(p2['fixed_price'])
    
    # Loại bỏ Vai trò trừ Doanh thu BH thừa trên phiếu Chuyên khoa
    ls_role_grt = [46,48,49,50]
    dfd['is_role_grt'] = dfd.roleid.fillna(0).isin(ls_role_grt).astype(bool)                     # Vai trò bị trừ bảo hành
    dfd['is_grt_derm'] = dfd['roleid'].isin([48,49]).groupby(dfd['order_product_id']).transform('any')
    dfd['rn_grt_team_init'] = dfd.groupby(['form_id', 'product_id', 'employee_id', 'roleid'])['form_id'].cumcount()+1
    dfd = dfd[~( (dfd.roleid.eq(46) & dfd.is_grt_derm) | (dfd.is_role_grt & dfd.rn_grt_team_init.gt(1)) )].copy()
    
    dfd['type_form'] = np.select([dfd['ck_pttt'].str.startswith('PTT'), dfd['ck_pttt'].str.contains('LASE|SPA|NHA|WELL',regex=True)], 
                                ['surgery', 'specialty'], None)   # numpy 2.x: default phai cung dtype voi choicelist (None ~ np.nan)
    # Xử lý Null lần liệu trình và Số lần tối đa thay đổi hệ số bảo hành
    dfd['drk_grt_times'] = dfd['drk_grt_times'].fillna(dfd.groupby(['brand_etl', 'crm_id', 'form_id', 'group_fix_product_id'])['drk_grt_times'].transform('max'))
    dfd['times'] = np.where(dfd.drk_grt_times.fillna(0).gt(6), 6, dfd.drk_grt_times)     
    
    dfd = dfd.merge(df_grt_times, how='left', on=['gr_bh_id', 'times'], suffixes=('', '_grt'))
    dfd.sort_values(['form_id', 'fixed_price', 'product_id', 'roleid'], ascending=[True,False,True,True], inplace=True)
    
    ls_col_float = ['uom_price','price_unit','unit_price','total','initial_total','unit_price_grt','total_histotal','total_received','his_revenue_pd','fixed_price']
    fix_type(dfd, ls_col_float, float, np.nan)
    
    # Đơn giá gốc = Đơn giá Dòng dịch vụ nếu KHÔNG PHẢI: Book bảo hành hoặc Book CRM cũ hoặc Đơn giá cũ <= Đơn giá hiện tại x 2 
    dfd['fixed_pricef'] = np.where(dfd.price_unit.fillna(0).ne(0) & (dfd.price_unit.le(dfd.fixed_price*2) | ~dfd.booking.str.contains(r'^[PKD]',regex=True) ) ,
                                 dfd.price_unit, dfd.fixed_price)
    
    dfd['is_grt_service'] = dfd.pricelist.str.contains('bảo hành', case=False, na=False) | dfd.initial_product.notnull() | dfd.is_role_grt
    dfd['revenue_div_price'] = (dfd.revenue/dfd.product_uom_qty)/dfd.fixed_pricef      # Mức khuyến mại so với Giá gốc
    dfd['is_rent_room'] = dfd['product_category'].str.contains('TPM',na=False) | dfd['product'].str.contains('thuê phòng',case=False,na=False)
    dfd['check_type_kpi'] = np.where(dfd.is_rent_room, 'rent_room', 
                                    np.where(dfd.revenue.fillna(0).eq(0), 
                                    np.where(dfd.is_grt_service, 'guarantee_0_dong', 'service_free'), 
                                    np.where(dfd.revenue_div_price.fillna(1).lt(0.25), 'service_deep_discount', 'service_org_revenue')
                                   ) )
    
    
    # Điều kiện Bảo hành DV theo lần hoặc Đổi sang DV khác
    '''
    ** GIÁ TRỊ GỐC TÍNH DOANH THU
    I. DỊCH VỤ DOANH THU THỰC 0 ĐỒNG HOẶC KHUYẾN MẠI GIẢM > 75% 
        •	Dịch vụ bảo hành
        - Nếu là Phiếu chuyên khoa Laser/Spa: 
            (I.1.a) Giá bán cũ KM <= 75% hoặc là BH PHUN XĂM => Doanh thu 1 buổi (Tổng phải thu cao nhất/Dòng dịch vụ có giá trị cao nhất CHIA số buổi) 
            Giá bán cũ Giảm sâu > 75%   
            (I.1.b1) => 25% Đơn giá gốc tại thời điểm bán cũ 
            (I.1.b2) => Khử null nếu không có giá trị = 25% Đơn giá gốc của Bảng giá mới nhất
        - Nếu là Phiếu thủ thuật phẫu thuật: 
            Bảo hành thay thế dịch vụ giá trị thấp (Chi phí, tháo chất liệu, cấy trung bì, trị sẹo cho DV phẫu thuật...)
            => (I.2.a): Ghi nhận Doanh thu theo Dịch vụ thực tế: 25% Đơn giá gốc DV thực hiện
            (I.2.b): Doanh thu dịch vụ bị Bảo hành nếu có giá trị > 0
            (I.2.c): Doanh thu dịch vụ bị Bảo hành (quét lịch sử Booking của KH có giá trị) 
         Đặc biệt với BH Nâng gói phẫu thuật 
            => (I.2.d) Doanh thu KPI = Giá trị DV nâng cấp thu thêm + 50% Doanh thu dịch vụ ban đầu.
        •	Dịch vụ thực hiện mới
        - Nếu là Phiếu thủ thuật phẫu thuật  => (I.3.1): Doanh thu thực tế (SO)
        - Nếu là Phiếu chuyên khoa Laser/Spa => (I.3.2): 25% theo Đơn giá gốc
    II. DỊCH VỤ THỰC HIỆN MỚI DOANH THU >= 25% GIÁ GỐC HOẶC NHÓM PHUN XĂM
        (II) Dịch vụ Thuê phòng = 0  (Chưa có số liệu xử lý trên hệ thống - người dùng theo dõi ngoài) 
             Còn lại: Doanh thu tính hiệu suất = Doanh thu thực tế (SO)
        
    '''
    
    ls_thaochatlieu = [1333,1390,2045,2059,2089,2114,2135,2160,4531,4547,4576,4591,4634,4681,4735,4737,4738,4993,4994,9689,9714,9756,9760,9837,9885,12058,12136,12200,12230,12704,12705,12706,12707,17249,17321,17479,17492,17558,17586,17774,17776,17797,17806,17819,17822,17839,17842,17865,17892,17915,18188,18229,18321,18550,18600,18657,18973,18980,18984,18992,19096,19105,19771,19772,20099,20109,20357,20432,20570,20624,20638,20837,21219,21281,21584,22034,22048,22055,22132,25446,25857,27635,27637,27638]
    ls_nhomchatlieu = [4558,4559,4645,4646,4647,10674,10720,12240,12241,13851,13852,13853,13854,24976,24977]
    ls_dvdikem = [10216,10217,22752]         # DV đi kèm DV chính: cấy trung bì (nâng mũi)
    ls_chiphi = [4535,4536,4537,4538,4554,4555,4556,4557,4560,10120,10125,10127,10220,10221,10222,10223,10224,10587,10591,10644,10645,12062,12063,12064,12065,12237,12238,12239,12242,12950,12951,12952,12955,12956,14683,14737,14825,14826]
    ls_product_ctg_oth = [261,432,451,1652,1672,1729,1771]          # NHÓM PHỤ THU, CHI PHÍ KHÁCd
        
    cdt_grt_fprice =  ( (dfd.fixed_pricef*dfd.product_uom_qty < dfd.initial_total) & dfd.initial_total.lt(1000) ) \
                    | ( dfd.initial_product_id.fillna(0).ne(dfd.product_id) & dfd.revenue.lt(1))  
    
    dfd['is_service_tattoo'] = dfd.product_ctg_id.fillna(0).isin([424,1361,485])
    dfd['is_grt_down'] = (dfd.initial_product_id.fillna(0).ne(0) & dfd.initial_product_id.fillna(0).ne(dfd.product_id) \
                        & dfd.revenue.eq(0) \
                        & (dfd.fixed_pricef.lt(dfd.initial_total.fillna(dfd.his_revenue_pd)*0.5).fillna(False) | dfd.product_id.fillna(0).isin(ls_thaochatlieu + ls_chiphi + ls_dvdikem) )
                        ).astype(bool)                   # Bảo hành thay thế bằng Dịch vụ giá trị thấp
    
    revenue_low_limit = dfd['fixed_pricef']*dfd.product_uom_qty*0.25     # Giá trị tối thiểu tính DOanh thu: 25% giá gốc
    is_form_derm = dfd.ck_pttt.str.contains(r'LASE|SPA|WELL', regex=True)
    dfd['revenue_recalc_perf'] = np.where(dfd.check_type_kpi.isin(['guarantee_0_dong', 'service_free', 'service_deep_discount']), 
                                         np.where(dfd.is_grt_service, 
                                                  np.where(is_form_derm, 
                                                           np.where(dfd.his_revenue_pd.fillna(0).gt(1000) & (dfd.perc_discount_grt.le(0.75) | dfd.is_service_tattoo),  
                                                                    dfd.his_revenue_pd,                                                      # (I.1.a)
                                                                    (dfd.unit_price_grt.fillna(dfd.fixed_pricef))*0.25*dfd.product_uom_qty),   # (I.1.b 1+2)
                                                      np.select([dfd.is_grt_down, dfd.initial_total.gt(0).fillna(False).astype(bool)], 
                                                                [dfd.fixed_pricef*dfd.product_uom_qty , dfd.initial_total],                    # [(I.2.a), (I.2.b)]
                                                                dfd.his_revenue_pd) 
                                                          ), 
                                                  np.where(dfd.ck_pttt.str.contains('PTT') | dfd.is_service_tattoo, dfd.revenue, revenue_low_limit)   # (I.3.1+2)
                                                 ),
                                         np.where(dfd.check_type_kpi.eq('rent_room'), 0, dfd.revenue)             # (II)
                                        )
    
    # Tính hệ số KPI hiệu suất cá nhân: Chia đều các Nhóm, mỗi nhóm chia đều số Nhân sự cùng vai trò
    dfd['is_doctor'] = dfd.roleid.isin([1,2,3])
    dfd['count_position'] = dfd.groupby(['form_id','product_id','is_doctor','is_role_grt'])['roleid'].transform('nunique')
    dfd['count_role'] = dfd.groupby(['form_id','product_id','is_role_grt', 'roleid'])['roleid'].transform('count')
    dfd['rto_kpi'] = np.where(dfd.ck_pttt.str.contains(r'PTT', regex=True) & ~dfd.is_role_grt,    
                             1/dfd.count_position/dfd.count_role, 
                             dfd.rto)
    
    # Tính hệ số bảo hành (theo Cơ chế cộng/trừ Doanh thu bảo hành chi nhánh)
    is_removehair_allin = dfd.is_role_grt & dfd.is_hair_removal & ( dfd.gbd_clineid.notnull() | dfd.reason_guarantee_id.fillna(0).eq(20).astype(bool) )
    is_service_not_valid_grt = dfd.is_grt_service & dfd.type_form.isin(['wellness', 'weight_loss'])
    dfd['rto_bh'] = np.select([ is_removehair_allin | is_service_not_valid_grt, dfd.is_role_grt, dfd.is_grt_service], 
                              [0, (dfd.perc_grt*(-1)).fillna(dfd.rto_bh).fillna(0), 0.5], np.nan).astype(float)  
    
    # KẾT QUẢ DOANH THU HIỆU SUẤT CÁ NHÂN
    # !!! Đặc biệt với DV Bảo hành nâng gói phẫu thuật = 50% Doanh thu DV cũ (BH) + 100% Doanh thu mở rộng
    dfd['is_upgrade_expense'] = dfd.service_guarantee_type.fillna('').eq('guarantee_upgrade') & dfd.product_id.ne(dfd.initial_product_id) \
                                & dfd.revenue.gt(0) & dfd.perc_discount.gt(0.3)            # Nâng cấp dịch vụ khác DV ban đầu và Giảm sâu > 30% 
    dfd['rn_upgrade_service'] = dfd.groupby(['form_id', 'initial_product_id', 'employee_id', 'roleid', 'is_upgrade_expense'])['product_id'].cumcount()+1
    dfd['kpi_perf_revenue_recalc'] = np.where(dfd.is_upgrade_expense & dfd.rn_upgrade_service.eq(1),    # Nâng cấp Dịch vụ đóng thêm Phí chất liệu hoặc DV chính giảm sâu
                                         (dfd.initial_total*dfd.rto_kpi*dfd.rto_bh).fillna(0) + dfd.revenue*dfd.rto_kpi,    # (I.2.d)
                                         dfd.revenue_recalc_perf*dfd.rto_kpi*dfd.rto_bh.fillna(1) )
    dfd['rn_initial_product_id'] = dfd.groupby(['form_id', 'initial_product_id', 'employee_id', 'roleid', 'is_grt_service','revenue_recalc_perf'])['product_id'].cumcount()+1

    #dfd['revenue'] = dfd.so_revenue 
    lscol_int = [c for c in dfd.columns if check_substr(c, r'(_|cline)id$|^(drk|rn_|dr_)') and not check_substr(c, 'order_product|team_se')]
    fix_type(dfd, lscol_int, 'Int64', np.nan)
    return dfd
#dfd = data_team_service_revenue()


#dfd[dfd.order_product_id.eq('831705-4839-1')]         # 'grt_product_group_id','revenue_recalc_perf','kpi_perf_revenue_recalc','total_histotal','his_revenue_pd','perc_discount_grt'
#dfd[dfd.ck_pttt.str.contains('WELL') & dfd.type_crm_id.eq(3)][['team_service_id','walkinid','crm_id','booking','product','revenue','fixed_price','count_position','count_role','rto_kpi','rto_bh','employee','role','kpi_perf_revenue_recalc','perc','perc_grt','rto_bh']]
#dfd[dfd.order_product_id.eq('840451-22707-1')][['team_service_id','walkinid','crm_id','booking','product','revenue','fixed_pricef','revenue_div_price','count_position','count_role','rto_kpi','rto_bh','employee','role','kpi_perf_revenue_recalc']]
#dfd[dfd.booking.eq('BH-260817-00005')][['team_service_id','walkinid','crm_id','booking','product','revenue','fixed_pricef','revenue_div_price','count_position','count_role_group','count_role','rto_kpi','rto_bh','employee','role','kpi_perf_revenue_recalc']]


# Kết nối đến cơ sở dữ liệu Dev

def call_kpi_team_service_revenue():
    dfc = data_team_service_revenue()         #  dfd.copy()    data_team_service_revenue()      
    TABLE = 'report_kpi_team_service_revenue'
    
    # Kiểm tra id bảng có là duy nhất?
    if dfc['team_service_id'].is_unique: 
        dfc['fixed_price'] = dfc['fixed_pricef']
        WHERE_UD = f"brand_etl <> 0  AND services_date >= '{LIMIT_START_DATE}' "
        replace_table_data(dfc, TABLE, where=WHERE_UD, conn=conn)
        print(f'''CẬP NHẬT: BÁO CÁO KPI PHÒNG DỊCH VỤ - CƠ CHẾ DOANH THU TỪ NGÀY {LIMIT_START_DATE}''')
    else: 
        raise ValueError(f'Kiểm tra lại id Bảng {TABLE}: {dfc["team_service_id"].duplicated().sum()} id trùng')
    return TABLE

# call_kpi_team_service_revenue()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'report_kpi_team_service': call_kpi_team_service,
    'report_kpi_team_service_revenue': call_kpi_team_service_revenue,
}


def main():
    parser = argparse.ArgumentParser(description='ETL report_kpi_team_service')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
