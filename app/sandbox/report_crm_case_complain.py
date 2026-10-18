# -*- coding: utf-8 -*-
"""
REPORT CRM CASE COMPLAIN

Chuyen tu quentk_3_report_crm_case_complain.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/report_crm_case_complain_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python report_crm_case_complain.py --task start_task_1_stag_crm_case
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


#!/usr/bin/env python
# coding: utf-8


#import airflow

from datetime import datetime,timedelta


import pandas as pd
import numpy as np
import re
import datetime as dt
import os
import tempfile
import warnings
import time
import unicodedata
from dateutil.relativedelta import relativedelta

pd.set_option('display.max_columns', None)
pd.options.display.float_format = '{:,}'.format

# khai báo DAG

date_sys = dt.date(2025,8,1)
date_sys_str = date_sys.strftime("%Y-%m-%d")


# connect id = sci -> bản dev
# connect id = sci_test -> bản test

# Sử dụng hook để lấy một kết nối đến cơ sở dữ liệu
# conn = hook.get_conn()
# # Tạo một con trỏ để thực thi các lệnh SQL
# cur = conn.cursor()


#quentk 
# Hàm lấy Dataframe hoặc list tên cột từ DB
conn = 'dev_staging_sci'
cur_dev = connect_database('dev_staging_sci_test').cursor()
def query_df(conn: 'dev_staging_sci', query: str, ret = 'df'):
    cur = connect_database(conn).cursor()
    cur.execute(query)
    if ret == 'df':
        rows = cur.fetchall()
        result = pd.DataFrame(rows, columns=[str(desc[0]) for desc in cur.description])
    else:
        result = [desc[0] for desc in cur.description]
    return result


def truncate(table):
    drop_query = f""" DELETE FROM {table}; """
    return drop_query


# Hàm kiểm tra chuỗi có chứa ký tự regex (regex: không nhập ký tự viết Hoa)
def check_substr(input_str, re_str):
    input_str = str(input_str).lower()
    re_str = '' if re_str is None else re_str.lower()
    result = True if re.search(re_str, input_str) else False
    return result


# Hàm nối chuỗi từ list các chuỗi con
def concat_str(ls_str = [], sep = ''):
    if len(ls_str) < 2:
        return 'List không đủ 2 phần tử trở lên'
    else:
        ccs = str(ls_str[0])
        for s in ls_str[1:]: 
            ccs = str(ccs) + sep + str(s)
        return ccs
        
# Hàm xử lý kiểu dữ liệu trong Dataframe
def fix_type(df, ls_col_name, as_type: str, fill_na: 0):
    ls_col_name = list(set(ls_col_name) & set(df.columns))
    for c in ls_col_name:
        c = str(c)
        try:
            if as_type == 'Int64': 
                df[c] = np.floor(pd.to_numeric(df[c]))             # df[c].astype(pd.Int64Dtype())
            df[c] = df[c].fillna(fill_na).astype(as_type)
        except ValueError:
             print(f'Không thể chuyển đổi cột {c} sang kiểu {as_type}')

# Tìm kiếm chuỗi ký tự đầu tiên khớp điều kiện (nếu có)
def get_fmatch(input_str, re_str):
    s_match = re.search(re_str, str(input_str), re.I)
    if s_match != None:
        return s_match[0]

def remove_accent(str_ip):
    str_ip = str(str_ip).lower().replace('đ', 'd') 
    chuoi_normalized = unicodedata.normalize('NFKD', str_ip).encode('ASCII', 'ignore').decode('utf-8') 
    chuoi_convert = re.sub('\W+', '', chuoi_normalized)
    return chuoi_convert


datetrunc = (dt.date.today() - relativedelta(days=7)).replace(day=1)
datetrunc = date_sys if datetrunc < date_sys else datetrunc
datetrunc_str = datetrunc.strftime("%Y-%m-%d")
limit_start_date = datetrunc_str

datetrunc_1m = (dt.date.today() - relativedelta(months=1)).replace(day=1)
datetrunc_1m = date_sys if datetrunc_1m < date_sys else datetrunc_1m
datetrunc_str_1m = datetrunc_1m.strftime("%Y-%m-01")
limit_start_date_1m = datetrunc_str_1m

datetrunc_3m = (dt.date.today() - relativedelta(months=3)).replace(day=1)
datetrunc_3m = date_sys if datetrunc_3m < date_sys else datetrunc_3m
datetrunc_str_3m = datetrunc_3m.strftime("%Y-%m-01")
limit_start_date_3m = datetrunc_str_3m


# Khởi tạo các brand dữ liệu xử lý
dict_brand_etl = {0: '', 1: 'kn', 3: 'pr', 4: 'hh', 7: 'sci'}
ls_brand_etl = list(dict_brand_etl.keys())
idb = -1 
idb_kn = 1
br = ''
br_sci = 7


query_crm_case = \
'''
SELECT 
	ccc.id, 
	ccc.code_crm_case crm_case,
	cc.type_case, 
	cl.id crm_id, cl.name booking,
	rp.code_customer, rp.name contact_name,
	ccc.phone_partner, ccc.stage,
	COALESCE(COALESCE(ccg.id, ccg2.id), sccmp.id) as complain_group_id,
	COALESCE(COALESCE(ccg.name, ccg2.name), sccmp.complain_group) as complain_group,
	ccp.id complain_name_id, ccp.name complain_name,
	COALESCE(ccc.priority, '1') as priority,
	ccc.receive_source, ccc.desc, ccc.solution, ccc.note, 
	cccppr.product_product_id,
	cccs.product_id,
	ccchdr.hr_department_id, COALESCE(hd.name, sccmp.department_complain) AS department, hd.root_code, cccs.hr_employee_id,
	ccc.processing_results, ccc.support_cost, 
	(cc.start_date + interval '7 hours') start_date, 
	ccc.create_by, 
	se.employee_code create_by_employee_code, 
	INITCAP(COALESCE(rpc.name, se.employee)) AS create_by_employee, 
    COALESCE(hdc.name, se.department) AS create_by_department, 
	(ccc.create_date + interval '7 hours') create_date, 
	(ccc.write_date + interval '7 hours') write_date, 
    ROUND(CASE WHEN ccc.stage IN ('complete', 'need_to_track') 
            THEN EXTRACT(epoch FROM age(ccc.write_date, ccc.create_date)/86400) 
            ELSE EXTRACT(epoch FROM age(NOW() + '12 hour', ccc.create_date)/86400) END 
        , 1) as time_solved, 
	ccc.partner_id, 
	ccc.company_id,
    rc.name AS company, 
	ccc.brand_id, 
	ccc.brand_etl
FROM crm_content_complain ccc 
LEFT JOIN crm_case cc ON ccc.crm_case = cc.id AND ccc.brand_etl = cc.brand_etl 
LEFT JOIN crm_complain_group ccg ON ccc.complain_group_id = ccg.id AND ccg.brand_etl = ccc.brand_etl 
LEFT JOIN crm_complain ccp ON ccc.complain_id = ccp.id AND ccc.brand_etl = ccp.brand_etl 
LEFT JOIN crm_complain_group ccg2 ON ccp.complain_group_id = ccg2.id AND ccg2.brand_etl = ccp.brand_etl 
LEFT JOIN res_partner rp ON ccc.partner_id = rp.id AND ccc.brand_etl = rp.brand_etl 
LEFT JOIN crm_lead cl ON ccc.booking_id = cl.id AND ccc.brand_etl = cl.brand_etl 
LEFT JOIN crm_content_complain_product_product_rel cccppr ON ccc.id = cccppr.crm_content_complain_id AND ccc.brand_etl = cccppr.brand_etl 
LEFT JOIN 
	(SELECT DISTINCT ON (cccs.brand_etl, content_complain, product_id) *, cccs.brand_etl cccs_brand_etl
	FROM crm_content_complain_service cccs 
	LEFT JOIN crm_content_complain_service_hr_employee_rel cccsher ON cccs.id = cccsher.crm_content_complain_service_id AND cccs.brand_etl = cccsher.brand_etl
	WHERE NOT (COALESCE(cccs.product_id,0) <> 0 AND COALESCE(cccsher.hr_employee_id,0) <> 0)
	) cccs 
ON cccs.content_complain = ccc.id and cccs.cccs_brand_etl = ccc.brand_etl 
LEFT JOIN crm_content_complain_hr_department_rel ccchdr ON ccc.id = ccchdr.crm_content_complain_id AND ccc.brand_etl = ccchdr.brand_etl 
LEFT JOIN hr_department hd ON ccchdr.hr_department_id = hd.id AND ccchdr.brand_etl = hd.brand_etl 
LEFT JOIN stag_crm_complain_map_department sccmp ON ccp.id = sccmp.id
LEFT JOIN res_users ru ON ccc.create_by = ru.id and ccc.brand_etl = ru.brand_etl 
LEFT JOIN res_partner rpc ON ru.partner_id = rpc.id and ru.brand_etl = rpc.brand_etl 
LEFT JOIN view_stag_employees_user se ON ccc.create_by = se.user_id and ccc.brand_etl = se.brand_etl 
LEFT JOIN hr_department hdc ON cc.department_create_by = hdc.id and cc.brand_etl = hdc.brand_etl
LEFT JOIN m_res_company rc ON ccc.company_id = rc.id AND ccc.brand_etl = rc.brand_etl
WHERE ccc.brand_etl <> 0
'''


# Danh mục Dịch vụ Sản phẩm và Nhân sự 
products = query_df(conn, 'SELECT brand_etl, product_id, his_service_type, surgery_type, product_ctg_id, product_category, product FROM view_stag_products')
employees = query_df(conn, 'SELECT DISTINCT employee_code, id, INITCAP(employee) AS employee, job, department FROM stag_employees WHERE employee_code IS NOT NULL')

query_bk = '''SELECT MIN(id) crm_id, name book_desc, brand_etl FROM crm_lead 
            WHERE type = 'opportunity' and stage_id <> 34 AND name ilike '%book%' GROUP BY (name), brand_etl '''


# DATA XỬ LÝ LẤY THÔNG TIN CASE KHIẾU NẠI, CHECK MÃ BOOKING

def get_data_crm_case():
    dfbk = query_df(conn, query_bk)
    dfcc = query_df(conn, query_crm_case)
    
    # Khử null Booking người dùng nhập mã tại "Nội dung phản ánh"
    re_bk = r'(K|D|P)?-?(BOOK|BH)-(\d+|-)*'
    dfcc['book_desc'] = dfcc['desc'].apply(lambda r: get_fmatch(r, re_bk))
    map_crm_id = dfcc[['brand_etl','crm_id','book_desc']]
    for idb in dfcc.brand_etl.unique():
        br = dict_brand_etl[idb]
        map_crm_id = map_crm_id.merge(dfbk[dfbk.brand_etl.isin([0, idb])].drop_duplicates('book_desc'), 
                                      how='left', on='book_desc', suffixes=('', '_' + br))
        dfcc.update(map_crm_id[['crm_id_'+br, 'book_desc']].rename(columns={'crm_id_'+br: 'crm_id', 'book_desc': 'booking'}), overwrite=False)

    # Xử lý kiểu dữ liệu, kiểm tra id bảng có là duy nhất?
    ls_col_int = [c for c in dfcc.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|(create_by|id)$|priority')]
    fix_type(dfcc, ls_col_int, 'Int64', np.nan)
    dfcc['id_etl'] = dfcc[['id', 'product_product_id', 'product_id', 'hr_department_id', 'brand_etl']].fillna(0).apply(lambda r: concat_str(r, '-'), axis=1)
    if not dfcc['id_etl'].is_unique:
        print('!!! Kiểm tra lại giá trị trùng id_etl Bảng stag_crm_case')
    return dfcc
dfcc = get_data_crm_case()


cur_dev = connect_database('dev_staging_sci').cursor()

def step1_stag_crm_case():
    # lấy dữ liệu
    dfc = dfcc.copy()         #   dfcc.copy()    get_data_crm_case()  
    table = 'stag_crm_case'

    try:
        #cur_dev.execute(truncate(table))
        cur_dev.execute(f'''DELETE FROM {table} WHERE brand_etl <> 0 ''')
        cur_dev.connection.commit()
    except Exception as err:
        print(str(err))
    # Kiểm tra các cột tồn tại ở Bảng đầu ra
    existing_columns = query_df(conn, f'''SELECT * from {table} limit 0 ''', ret = 'column')
    existing_columns_fix = ['"desc"' if e == 'desc' else e for e in existing_columns]
    insert_query = "COPY {} ({}) FROM STDIN WITH CSV HEADER DELIMITER ','".format(table,', '.join(existing_columns_fix))
    # Ghi dữ liệu vào tệp tạm
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    result = dfc[existing_columns]
    result.to_csv(temp_file.name, index=False, header=True)
    
    # Chèn dữ liệu vào bảng của cơ sở dữ liệu từ tệp tạm
    try:
        with open(temp_file.name, 'r', encoding='utf-8') as f:
            cur_dev.copy_expert(insert_query, f)
        cur_dev.connection.commit()
        print('BẢNG 1: Bảng Case khiếu nại sau làm sạch. Hoàn thành!')
    except Exception as e:
        print(str(e))

    return 'stag_crm_case'

#step1_stag_crm_case()


# Lịch sử thực hiện dịch vụ của Khách hàng khiếu nại
sdate = ''' 
SELECT DISTINCT ON (s.partner_id, s.crm_id, s.product_id) 
	s.brand_etl, s.partner_id, s.crm_id, s.walkinid, s.walkin, s.ck_pttt, s.services_date, 
	s.product_id, sp.product, s.roleid, itrg.role, s.employee_code, s.employee_id 
FROM stag_his_team_service s 
LEFT JOIN view_stag_products sp ON (s.product_id, s.brand_etl) = (sp.product_id, sp.brand_etl)
LEFT JOIN import_team_role_group itrg ON s.roleid = itrg.id
WHERE (rn_walkin_product_fix = 1 OR roleid = 1)
	AND EXISTS (SELECT 1 FROM stag_crm_case scc WHERE scc.partner_id = s.partner_id AND scc.brand_etl <> 0)
ORDER BY s.partner_id, s.crm_id, s.product_id, s.services_date 
'''
#df_sdate = query_df(conn, sdate)

# Dịch vụ trên Booking tư vấn của Khách hàng khiếu nại
cline_case = '''
SELECT cline.brand_etl, cline.crm_id, cline.id clineid, cline.product_id, 
	sp.product, cline.stage, cline.total_received, sp.product_category 
FROM crm_line cline 
LEFT JOIN view_stag_products sp ON (cline.brand_etl, cline.product_id) = (sp.brand_etl, sp.product_id)
WHERE EXISTS (SELECT 1 FROM stag_crm_case scc WHERE (cline.brand_etl, cline.crm_id) = (scc.brand_etl, scc.crm_id) AND scc.brand_etl <> 0)
	AND sp.product_category !~* 'phụ |chi phí'
'''
#df_cline = query_df(conn, cline_case)


def get_data_report(): 
    # Dịch vụ trên Booking tư vấn của Khách hàng khiếu nại
    df_cline = query_df(conn, cline_case)
    dict_st = {'done': 1, 'processing': 2, 'waiting': 3, 'new': 4, 'chotuvan': 5, 'cancel': 6}
    df_cline['order_stage'] = df_cline.stage.replace(dict_st)
    df_cline.sort_values(['brand_etl','crm_id','product_category','product_id','order_stage','total_received'], 
                              ascending=[True,True,True,True,True,False], inplace=True) 
    df_cline['rn_booking_product'] = df_cline.groupby(['brand_etl','crm_id','product_id']).cumcount()+1
    df_cline['rn_booking'] = df_cline.groupby(['brand_etl','crm_id']).cumcount()+1
    df_cline['count_product_dupinbk'] = df_cline.groupby(['brand_etl','crm_id'])['product_id'].transform('nunique')
    df_cline['count_product_ctg_dupinbk'] = df_cline.groupby(['brand_etl','crm_id'])['product_category'].transform('nunique')
    
    case_pd = dfcc[['brand_etl','partner_id','crm_id']].merge(df_cline[df_cline.count_product_ctg_dupinbk.eq(1) & df_cline.rn_booking.eq(1)].iloc[:,:7], \
                                                               how='left', on=['brand_etl','crm_id'])
    # Lịch sử thực hiện dịch vụ của Khách hàng khiếu nại
    df_sdate = query_df(conn, sdate)

    # Nối thông tin dịch vụ, ngày làm DV, Nhân sự chính thực hiện với Data khiếu nại tổng hợp
    df = dfcc.copy()
    dict_eq = {'product_id_complain': 'product_id', 'employee_id_complain': 'hr_employee_id', 'chinhanhphanquyen_fix': 'company_id', 'thuonghieuphanquyen_fix': 'brand_id'}
    for k, v in dict_eq.items(): 
        df[k] = df[v]
    df.update(case_pd[['product_id']], overwrite=False)
    
    df = df.merge(products.iloc[:,:-1], how='left', on=['brand_etl','product_id'], suffixes=('','_pd'))
    df = df.merge(df_sdate, how='left', on=['partner_id','crm_id','product_id'], suffixes=('','_sdate'))\
            .merge(employees, how='left', on='employee_code', suffixes=('','_emp'))
    #df = df.merge(employees.rename(columns={c: 'create_by_'+c for c in employees.columns}), how='left', on='create_by_employee_code', suffixes=('','_cb'))
    df['department'] = df.department.fillna(df.department_emp)

    fix_type(df, [c for c in df.columns if c[-2:]=='id'], 'Int64', np.nan)
    return df

#df = get_data_report()


def write_report_crm_case_complain():
    # lấy dữ liệu
    dfc = get_data_report()         #df.copy()  
    table = 'report_crm_case_complain'

    try:
        cur_dev.execute(f'''DELETE FROM {table} WHERE brand_etl <> 0 ''')
        cur_dev.connection.commit()
    except Exception as err:
        print(str(err))
    # Kiểm tra các cột tồn tại ở Bảng đầu ra
    existing_columns = query_df(conn, f'''SELECT * from {table} limit 0 ''', ret = 'column')
    existing_columns_fix = ['"desc"' if e == 'desc' else e for e in existing_columns]
    insert_query = "COPY {} ({}) FROM STDIN WITH CSV HEADER DELIMITER ','".format(table,', '.join(existing_columns_fix))
    # Ghi dữ liệu vào tệp tạm
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    result = dfc[existing_columns]
    result.to_csv(temp_file.name, index=False, header=True)
    
    # Chèn dữ liệu vào bảng của cơ sở dữ liệu từ tệp tạm
    try:
        with open(temp_file.name, 'r', encoding='utf-8') as f:
            cur_dev.copy_expert(insert_query, f)
        cur_dev.connection.commit()
        print('BẢNG 2: Bảng Case Khiếu nại. Hoàn thành!')
    except Exception as e:
        print(str(e))

    return 'report_crm_case_complain'

#write_report_crm_case_complain()


# BẢNG TỔNG HỢP DỮ LIỆU BẢO HÀNH + KHIẾU NẠI

datetrunc = dt.date.today() - relativedelta(months=6)
datetrunc_str = datetrunc.strftime("%Y-%m-01")

limit_start_date = datetrunc_str   #'2024-01-01'

query_report = \
f'''
WITH guarantee_db AS
(
SELECT
 'guarantee' as type_form
, ck_pttt form_name, is_new_request, services_date, walkinid, walkin
, crm_id, booking, rkts.code_customer, rkts.contact_name, room, rn_order_product_id
, product_ctg_id, product_id, drk_crm_product rn_walkin_product, his_service_type
, max_surgery_type_form, max_surgery_type_walkin
, product_category, product, product_uom_qty, revenue
, rkts.employee_id, rkts.employee_code, initcap(rkts.employee) employee, rkts.job, rkts.department, "role", roleid
, pricelist, services_date start_date
, null as type_case 
, state_walkin stage
, 0 as complain_group_id, null as complain_group, 0 as complain_name_id, null as complain_name
, -1 as priority
, cast(cl.source_id as varchar) source
, null as "desc", null as solution, cl.note, null as processing_results, 0 as support_cost
, initial_booking, initial_services_date, initial_product_ctg_id, initial_product_id
, initial_product_category, initial_product, initial_total
, cast(type_guarantee as int) type_guarantee, max_type_guarantee 
, cast(bh_ngoai_goi as int) bh_ngoai_goi, reason_guarantee_id, reason_guarantee
, rkts.lead_id
, initial_employee_id, initial_employee_code, initcap(initial_employee) initial_employee, initial_job, initial_role
, (cl.create_date + interval '7 hours') as create_date
, (cl.write_date + interval '7 hours') as write_date
, 0 as time_solved 
, cl.create_by, se.employee_code create_by_employee_code, se.employee create_by_employee, se.department create_by_department
, rkts.company_id, rkts.company
, rc.brand_id thuonghieuphanquyen
, rkts.company_id chinhanhphanquyen
, rkts.team_service_id AS id_etl
, rkts.brand_etl
FROM "public".report_kpi_team_service rkts
LEFT JOIN crm_lead cl ON rkts.crm_id = cl.id and rkts.brand_etl = cl.brand_etl 
LEFT JOIN view_stag_employees_user se ON se.user_id = cl.create_by AND se.brand_etl = cl.brand_etl 
LEFT JOIN m_res_company rc ON rkts.company_id = rc.id AND rkts.brand_etl = rc.brand_etl 
WHERE rkts.type_crm_id = 3 AND (dr_order_role = 1 OR aggroleid NOT LIKE '%,%')
    ---AND rkts.brand_etl <> 0
	---AND rkts.services_date >= '{limit_start_date}'
),
complain_db as
(
SELECT
 'complain' as type_form
, crm_case form_name, false as is_new_request, create_date services_date, id as walkinid, null as walkin
, crm_id, booking, code_customer, contact_name, null as room, 0 as rn_order_product_id
, product_ctg_id, product_id, 0 as rn_walkin_product, his_service_type
, 0 as max_surgery_type_form, 0 as max_surgery_type_walkin
, product_category, product, 1 as product_uom_qty, 0 as revenue
, employee_id_complain employee_id, employee_code, initcap(employee) employee, job, department, "role", roleid
, null as pricelist
, start_date
, type_case 
, stage
, complain_group_id, complain_group, complain_name_id, complain_name
, priority
, receive_source source
, "desc", solution, note, processing_results, support_cost
, booking as initial_booking, services_date as initial_services_date, product_ctg_id as initial_product_ctg_id, product_id as initial_product_id
, product_category as initial_product_category, product as initial_product, 0 as initial_total
, 0 as type_guarantee, 0 as max_type_guarantee
, 0 as bh_ngoai_goi, 0 as reason_guarantee_id, null as reason_guarantee 
, crm_id as lead_id
, employee_id_complain as initial_employee_id, employee_code as initial_employee_code, initcap(employee) initial_employee, job as initial_job, role as initial_role
, create_date
, write_date 
, time_solved 
, create_by, create_by_employee_code, create_by_employee, create_by_department
, company_id, company
, thuonghieuphanquyen_fix
, chinhanhphanquyen_fix
, id_etl
, brand_etl 
FROM "public".report_crm_case_complain rccc 
---WHERE brand_etl <> 0
)
SELECT db.*, 
		CASE WHEN sp.is_main_service = TRUE THEN TRUE ELSE FALSE END initial_product_main 
FROM 
	(SELECT * from complain_db
	UNION ALL
	SELECT * from guarantee_db) db
LEFT JOIN view_stag_products sp on db.initial_product_id = sp.product_id AND db.brand_etl = sp.brand_etl
'''

cur_dev = connect_database('dev_staging_sci').cursor()

def write_report_case_complain_guarantee():
    # lấy dữ liệu
    dfc = query_df(conn, query_report)         #df.copy()  
    ls_col_int = [c for c in dfc.columns if check_substr(c, r'phanquyen|type_guarantee|bh_ngoai|priority|max_surger|_id_|^(drk|rn_|rnk|dr|count)|(create_by|id)$')]
    fix_type(dfc, ls_col_int, 'Int64', np.nan)
    table = 'report_case_complain_guarantee'

    try:
        cur_dev.execute(truncate(table))
        #cur_dev.execute(f'''DELETE FROM {table} WHERE brand_etl <> 0 ''')   #  type_form = 'complain' OR (type_form = 'guarantee' AND services_date >= '{limit_start_date}')
        cur_dev.connection.commit()
    except Exception as err:
        print(str(err))
    # Kiểm tra các cột tồn tại ở Bảng đầu ra
    existing_columns = query_df(conn, f'''SELECT * from {table} limit 0 ''', ret = 'column')
    existing_columns_fix = ['"desc"' if e == 'desc' else e for e in existing_columns]
    insert_query = "COPY {} ({}) FROM STDIN WITH CSV HEADER DELIMITER ','".format(table,', '.join(existing_columns_fix))
    # Ghi dữ liệu vào tệp tạm
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    result = dfc[existing_columns]
    result.to_csv(temp_file.name, index=False, header=True)
    
    # Chèn dữ liệu vào bảng của cơ sở dữ liệu từ tệp tạm
    try:
        with open(temp_file.name, 'r', encoding='utf-8') as f:
            cur_dev.copy_expert(insert_query, f)
        cur_dev.connection.commit()
        print('BẢNG 3: Bảng tổng hợp Case Khiếu nại + Bảo hành. Hoàn thành!')
    except Exception as e:
        print(str(e))

    return table

#write_report_case_complain_guarantee()


cur_dev.connection.commit()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'start_task_1_stag_crm_case': step1_stag_crm_case,
    'start_task_2_report_crm_case_complain': write_report_crm_case_complain,
    'start_task_3_report_case_complain_guarantee': write_report_case_complain_guarantee,
}


def main():
    parser = argparse.ArgumentParser(description='ETL report_crm_case_complain')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
