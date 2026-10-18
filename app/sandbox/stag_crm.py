# -*- coding: utf-8 -*-
"""
STAG CRM

Chuyen tu quentk_1_stag_crm_New_sys.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/stag_crm_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python stag_crm.py --task task_1_stag_employees
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

DATETRUNC_1m = (dt.date.today() - relativedelta(months=1)).replace(day=1)
DATETRUNC_1m = DATE_SYS if DATETRUNC_1m < DATE_SYS else DATETRUNC_1m
DATETRUNC_STR_1m = DATETRUNC_1m.strftime("%Y-%m-01")
LIMIT_START_DATE_1M = DATETRUNC_STR_1m


# Khởi tạo các brand dữ liệu xử lý
DICT_BRAND_ETL = {1: 'kn', 2: 'da', 3: 'pr', 4: 'hh', 8: 'thv', 7: 'sci'}
DICT_BR_ETL = {v: k for k, v in DICT_BRAND_ETL.items() if v}
LS_BRAND_ETL = list(DICT_BRAND_ETL.keys())
LS_BR = list(v for k, v in DICT_BRAND_ETL.items() if k > 0)
idb = -1 
idb_kn = 1
br = ''
br_sci = 7


# CẬP NHẬT BẢNG DANH MỤC DỊCH VỤ, SẢN PHẨM SAU XỬ LÝ

# Code mẫu truy vấn thông tin Bảng dịch vụ, sản phẩm
br = ''
idb = '-1'
query_pd = f''' 
SELECT
	smhcs.id smhcsid
,	COALESCE(smhcs.his_service_type, CASE WHEN pp.default_code LIKE 'TDL%' THEN 'Card' end) AS his_service_type
,	smhcs.surgery_type,	smhcsc.product_cat_id product_ctg_id
,	smhcs.kpi_first_time 
,	TRIM(COALESCE(
        REGEXP_REPLACE(pc.name, '\s+', ' ', 'g')
        , CASE WHEN smhcs.his_service_type IS NOT NULL THEN 'PTTM KHÁC' 
            WHEN pp.default_code LIKE 'TDL%' THEN 'THẺ DA LIỄU' 
            ELSE 'SAN PHAM' END)) AS product_category
, 	pimp.product_category_paris, pimp.product_category_new
,	pp.id AS product_id, pp.default_code, TRIM(pt.name) product, pt.list_price, pp.product_tmpl_id 
,	pimp.is_main_service, pimp.group_id::int AS group_id,pimp.product_id_kn, pimp.is_vip, pimp.qty_service
,	CASE WHEN smhcs.kpi_point = 0 THEN null else smhcs.kpi_point end kpi_point
,	CASE WHEN smhcs.kpi_point_num = 0 THEN 1 ELSE COALESCE(smhcs.kpi_point_num, 1) END kpi_point_num 
, 	pt.uom_id, 	uu.name uom
,   pp.create_date, GREATEST(pp.write_date, smhcs.write_date) AS write_date
, 	CASE WHEN pp.default_code LIKE 'TDL%' THEN 1 
        WHEN COALESCE(pc.name, smhcs.his_service_type) IS null or pc.name ~* 'TPM' THEN NULL 
        WHEN pc.name ~* 'pttm.*mắt' THEN 3 
		WHEN pc.name ~* 'pttm.*mũi' THEN 8
		WHEN pc.name ~* 'ngực|mông' THEN 5
		WHEN pc.name ~* '(pttm.*mỡ)|hút mỡ' THEN 6
		WHEN pc.name ~* 'hàm mặt' THEN 7
		WHEN pc.name ~* 'gội đầu|dưỡng sinh|phun xăm|triệt lô|tắm trắ' OR pc.name = 'ĐIỀU TRỊ MỤN' THEN 2
        WHEN pc.name ~* 'căng (chỉ|da)' THEN 4
		WHEN smhcs.his_service_type in ('Laser', 'Spa') THEN 1
		ELSE 4
	END AS kdtc_product_ctg_id , pp.brand_etl
FROM sh_medical_health_center_service smhcs
LEFT JOIN sh_medical_health_center_service_category smhcsc ON smhcs.service_category = smhcsc.id AND smhcs.brand_etl = smhcsc.brand_etl 
LEFT JOIN product_category pc ON smhcsc.product_cat_id = pc.id AND smhcs.brand_etl = pc.brand_etl 
RIGHT JOIN product_product pp ON smhcs.product_id = pp.id AND smhcs.brand_etl = pp.brand_etl 
LEFT JOIN product_template pt ON pp.product_tmpl_id = pt.id AND pp.brand_etl = pt.brand_etl 
LEFT JOIN uom_uom uu ON pt.uom_id = uu.id AND uu.brand_etl = pt.brand_etl 
LEFT JOIN
(
SELECT
	*
FROM import_product_service_analyst
WHERE product_category <> 'SAN PHAM'
) pimp ON (CASE WHEN pp.brand_etl < 2 THEN pp.id END) = pimp.product_id 
WHERE pp.brand_etl = 1 
'''

#dfpd = query_df(conn, query_pd)

def get_products_by_brand():
    # Lấy danh mục Product từng brand.
    dfs = {}
    for br, idb in DICT_BR_ETL.items():
        dfs[br] = query_df(conn, query_pd.replace('brand_etl = 1', f'brand_etl = {idb}'))
        print(f'df{br}: {dfs[br].shape}')
    return dfs


def get_data_product():
    # GỘP DANH MỤC, XỬ LÝ ĐIỂM DỊCH VỤ NULL, GÁN ID NHÓM BẢO HÀNH (DA LIỄU)
    dfs = get_products_by_brand()
    frames = []
    for br in LS_BR:
        dft = dfs[br].copy()
        dft.insert(0, 'br', br)
        frames.append(dft)
    df = pd.concat(frames, ignore_index=True)

    df['cvl_unique'] = df.groupby('default_code')['product'].transform('nunique')
    df['cvl_iunique'] = df.groupby(['default_code', 'br'])['product_id'].transform('nunique')
    df['cpname'] = df.groupby(['default_code', 'product'])['product_id'].transform('size')
    df.sort_values(['default_code', 'brand_etl'], inplace=True)

    # Các dịch vụ không tính Doanh số KPI
    # 'covid_fee': 8207,8194,10252,10254,10255,10716,16836,17097,20306,20881,20890,20090,20290,20279,20280,20282
    # 'invoice_fee': 10801
    ls_nokpi = [10801,8207,8194,10252,10254,10255,10716,16836,17097,20306,20881,20890,20090,20290,20279,20280,20282]
    df['is_no_kpi'] = np.where(df.product_id.isin(ls_nokpi), 'True', '')

    # Tính điểm các dịch vụ phẫu thuật ngoài Bảng giá Kangnam
    dffkpi = df[df.kpi_point.isnull() & ~df.product_id_kn.fillna('').isin(['','0.0', '0'])].drop_duplicates(['product_id'])[['product_id', 'product', 'product_id_kn']]
    dffkpi = dffkpi.set_index(['product_id','product']).apply(lambda x: x.str.split(',').explode()).reset_index()
    # regex=False: nếu thiếu, pandas 1.x hiểu '.0' là biểu thức chính quy và cắt cụt id
    dffkpi['product_id_kn'] = dffkpi['product_id_kn'].str.replace('.0', '', regex=False).fillna(0).astype(int)
    dffkpi = dffkpi.merge(df[df.product_id.notnull()].drop_duplicates('product_id')[['product_id', 'kpi_point']]\
                          .rename(columns={'product_id': 'product_id_kn'}), how='left', on='product_id_kn')
    dfgr = dffkpi.groupby('product_id')['kpi_point'].sum()
    df['kpi_point'] = np.where(df.kpi_point.fillna(0).eq(0), df.product_id.map(dfgr), df.kpi_point)

    # Gắn id nhóm Bảo hành DV laser-Spa theo cơ chế Doanh thu
    dict_bh = {2: [[411,454,1334,1336,1394], 'tàn nhang| nám |ban vàng| chàm |u tuyến bã|trị.*đồi mồi|bớt (sùi)?'], \
               3: [[419,466,1345,1396], 'mụn (thịt|cơm)|tẩy nốt ruồi'], \
               4: [[424,425,475,485,1344,1361,426,1302,1351], 'xóa (hình )?xăm|phun (môi|mày|mí|thêu)|điêu khắc|hair stro'], \
               5: [[428,484,1350], 'triệt (lông|râu|ria)']}
    df['gr_bh_id'] = np.nan
    for i, ls in dict_bh.items():
        df['gr_bh_id'] = np.select([df.gr_bh_id.notnull(), df.product_ctg_id.isin(ls[0]) | df['product'].str.match('.*(' + ls[1] + ').*', case=False)],
                                   [df.gr_bh_id, i], np.nan)
    df['gr_bh_id'] = np.select([df.gr_bh_id.notnull(),
                                df['product'].str.match('.*(botox|botul|(nhăn|mặt).*aller).*', case=False).astype(bool),
                                df.his_service_type.isin(['Laser', 'Spa'])],
                               [df.gr_bh_id, 6, 1], df.gr_bh_id)

    df.reset_index(inplace=True)

    # MERGE DEFAULT_CODE VỚI CÁC PRODUCT CÙNG LOẠI TRONG DANH MỤC, BỔ SUNG CÁC FIELD ID TỪNG BRAND

    # List các Product cùng default_code nhưng khác DV
    ls_pid = [13083,13077,13096,13106,25446,30368,30369]
    ls_dc = ['TH001002', 'TH001004', 'VT003539', 'VT003758', 'VT003769', 'VT003770'] \
            + ['EHC-NSTHTNT0025', 'EHC-NSTHTNT0033', 'EHC-NSTHTNT0038'] \
            + list('TH0010' + f"{i:02d}" for i in range(5,22)) \
            + list('VT00376' + str(i) for i in range(1,7)) \
            + list('VT0038' + str(i) for i in range(32,76)) \
            + list('VT0039' + str(i) for i in range(15,23))
    is_difn = df.product_id.isin(ls_pid) | df.default_code.fillna('').isin(ls_dc)

    # dfmap1: product_id = 31842 là id cuối cùg trước khi tách hệ thống theo thương hiệu => cùng product_id -> cùng Product
    # dfmap2: Các bản ghi merge về 1 dòng theo default_code
    # dfmap3: Các bản ghi còn lại (Có thể trùng hoặc Null default_code), khác loại product giữa các thương hiệu >> Không merge dòng
    dfmap1 = df[df.product_id.le(31842)].copy()
    # cpname.gt(1): gom các dòng trùng nhau cả default_code lẫn tên product.
    dfmap2 = df[df.product_id.gt(31842) & df.cpname.gt(1) & ~is_difn & df.default_code.notnull() & df.cvl_iunique.eq(1)]
    dfmap3 = df.drop(dfmap1.index.union(dfmap2.index))

    dfo = dfmap1[dfmap1.br.eq('kn')].copy()
    dfgr_code = dfmap2.sort_values('brand_etl').drop_duplicates('default_code',keep='first')
    dfmerge_code = dfgr_code.set_index('index')

    lsid = ['product_id', 'product_ctg_id', 'product_tmpl_id', 'smhcsid']
    for br in LS_BR:
        ls_newid = [br + '_' + i for i in lsid]
        for i in lsid:
            col = br + '_' + i
            dfmap3[col] = np.where(dfmap3.br.eq(br), dfmap3[i], np.nan)       # Product cùng default_code tên khác nhau trong danh mục
        dfo[ls_newid] = dfo[lsid]                                             # Product trước 01/08/2025
        dfcbr = dfgr_code[['index','default_code']]\
                .merge(dfmap2[dfmap2.br.eq(br)].groupby('default_code')[lsid].min(), how='left', on='default_code')\
                .set_index('index')
        dfmerge_code[ls_newid] = dfcbr[lsid]                                   # Product merge theo default_code giữa các thương hiệu

    dfnew = pd.concat([dfo, dfmerge_code, dfmap3], ignore_index=True)
    dfnew[lsid] = dfnew[list('kn_' + c for c in lsid)]
    dfnew['group_id'] = dfnew['group_id'].astype('Int64')
    return dfnew

#dfpd = get_data_product()

def write_stag_products():
    dfc = get_data_product()     #    dfpd.copy()      get_data_product()
    TABLE = 'stag_products'

    for br in LS_BR:
        if not dfc[br+'_product_id'].dropna().is_unique: 
            raise ValueError(f'DỪNG! {br}_product_id bị trùng, kiểm tra lại luồng xử lý')
        
    # CẬP NHẬT GIÁ TRỊ BẢNG 
    replace_table_data(dfc, TABLE, conn=conn)
    print('DANH MỤC DỊCH VỤ, VẬT TƯ, THUỐC, SẢN PHẨM!')

    return TABLE
    
#write_stag_products()


# Truy vấn dữ liệu các bảng Nhân sự gốc từ ERP
ls_table_etl = ['ru', 'rp', 'hds', 'ht', 'hj', 'hgj', 'hd']

#' AND '.join(f"({e}.brand_etl = {br_sci} OR {e}.brand_etl IS NULL)" for e in ls_table_etl)
PART_GET_ID = '\n'.join(f", he{br}.id AS {br}_id, he{br}.user_id AS {br}_user_id "
                        for i, br in DICT_BRAND_ETL.items() if i != 7)
PART_JOIN_HE = '\n'.join(f"\
LEFT JOIN (select MIN(id) id, MIN(user_id) user_id, employee_code from hr_employee he{br} where he{br}.brand_etl = {i} \
GROUP BY employee_code) he{br} ON he.employee_code = he{br}.employee_code" 
                         for i, br in DICT_BRAND_ETL.items() if i != 7)

query_emp = \
f'''\
SELECT 
  he.id, he.user_id, he.employee_code, he.name employee, he.gender 
, he.job_id, hj.name job, he.group_job group_job_id, hgj.name group_job, hd.name department
, he.team_id, ht.name team, he.sector_id, hds.name sector
, COALESCE(hdc.id, he.department_id) department_id, he.department_id old_department_id, he.company_id    --- , he.department_id phongbanphanquyen
, he.staff_level, he.area, he.birthday, he.place_of_birth, he.identification_id, he.barcode, he.mobile_phone, he.work_email
, he.create_date + interval '7 hours' as create_date
, he.write_date + interval '7 hours' as write_date
, he.active, he.resign_date, he.reason_resign 
, rc.name company, he.brand_id, rb.name brand
{PART_GET_ID}
FROM hr_employee he 
LEFT JOIN m_res_company rc ON he.company_id = rc.id AND rc.brand_etl = he.brand_etl 
LEFT JOIN res_brand rb ON rc.brand_id = rb.id AND rb.brand_etl = rc.brand_etl 
LEFT JOIN res_users ru ON ru.id = he.user_id AND he.brand_etl = ru.brand_etl 
LEFT JOIN res_partner rp ON rp.id = ru.partner_id AND ru.brand_etl = rp.brand_etl 
LEFT JOIN hr_department_sector hds ON he.sector_id = hds.id AND he.brand_etl = hds.brand_etl 
LEFT JOIN hr_team ht ON he.team_id = ht.id AND ht.brand_etl = he.brand_etl 
LEFT JOIN hr_job hj ON he.job_id = hj.id AND he.brand_etl = hj.brand_etl 
LEFT JOIN hr_group_job hgj ON he.group_job = hgj.id AND he.brand_etl = hgj.brand_etl 
LEFT JOIN stag_phongbanphanquyen_id spi ON he.department_id = spi.department_id 
LEFT JOIN (select max(id) id, name from hr_department where brand_etl in (0,7) group by name) hdc ON spi.new_name = hdc.name
LEFT JOIN hr_department hd ON COALESCE(hdc.id, he.department_id) = hd.id AND hd.brand_etl = 7
{PART_JOIN_HE}
--LEFT join lateral (select id, user_id, employee_code from hr_employee hehh where hehh.brand_etl = 4 and he.employee_code = hehh.employee_code order by active desc limit 1) hehh ON true
WHERE rc.brand_etl = 7 AND rb.brand_etl = 7 
    AND he.brand_etl = 7 --AND 
'''           # + ' AND '.join(f"({e}.brand_etl = {br_sci} OR {e}.brand_etl IS NULL)" for e in ls_table_etl)

# Truy vấn dữ liệu các user_id nhưng không có thông tin Nhân viên hr_employee
user_ot = '''
SELECT (ROW_NUMBER() OVER (ORDER BY ru.id) + 1) AS id, 
    rp.name AS employee, ru.id user_id, ru.login work_email, ru.partner_id, 
    ru.create_date, ru.write_date, ru.company_id
FROM res_users ru 
LEFT JOIN hr_employee he ON ru.id = he.user_id 
LEFT JOIN res_partner rp ON ru.partner_id = rp.id
WHERE COALESCE(he.user_id, 0) = 0  
'''

# CẬP NHẬT DANH MỤC CÁN BỘ NHÂN VIÊN
def get_data_employee():
    # Truy vấn dữ liệu các bảng Nhân sự gốc từ ERP
    df = query_df(conn, query_emp)
    # Xử lý convert các cột phải là số nguyên sang kiểu Int  
    ls_col_int = [c for c in df.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|id$|create_by|priority|phanquyen|_id_') and not check_substr(c, 'identification')]
    fix_type(df, ls_col_int, 'Int64', np.nan)

    # company_id=4 & department_id=2599: xử lý tạm phân quyền phòng ban BS Đoàn Hùng Thịnh (BV KNSG - Chuyên khoa PTTM)
    df['phongbanphanquyen'] = np.where(df.department_id.fillna(0).isin([2635,2636]) | df.id.eq(3337) | (df.company_id.fillna(0).eq(4) & df.department_id.fillna(0).eq(2599))
                                       , 2602, df.department_id) 
    
    for b in list(DICT_BRAND_ETL.values())[1:-1]:
        user_admin = df.user_id.fillna(0).lt(10)
        df[b+'_user_id'] = np.where(df[b+'_user_id'].isnull() & user_admin, df.user_id, df[b+'_user_id']) 
        df[b+'_id'] = np.where(df[b+'_id'].isnull() & user_admin, df.id, df[b+'_id']) 
    
        
    # Job_lv2: tùy chỉnh theo Order nhóm chức danh bác sĩ của KNHN
    dict_job = {1: 'Bác sĩ Tiểu phẫu chính', 2: 'Bác sĩ Đại phẫu chính', 3: 'Bác sĩ Tiểu phẫu thực hành', 4: 'Bác sĩ Đại phẫu thực hành', 5: 'Bác sĩ Phẫu thuật Hàm mặt'}
    dict_emp = {
        1: {
            2479: 'NGUYỄN TẤN VÀNG',
            2349: 'TRẦN VĂN ANH',
            2350: 'NGUYỄN ĐÌNH CHIẾN',
            2981: 'TẠ THANH HẢI',
            2982: 'NGUYỄN XUÂN TRƯỜNG',
            2420: 'NGUYỄN TIẾN MẠNH',
            2446: 'NGUYỄN VĂN HUYÊN',
            2463: 'DƯƠNG VIỆT HÙNG',
            2825: 'HỒ NGỌC TRUNG',
            2342: 'TRẦN THẾ ANH',
        },
        2: {
            2464: 'NGUYỄN THÀNH LUÂN',
            2365: 'HOÀNG ĐỨC TRỌNG',
            2983: 'NGUYỄN MẠNH CƯỜNG',
        },
        3: {
            2532: 'NGUYỄN ĐỨC QUỲNH',
        },
        4: {
            2558: 'PHẠM HỒNG SƠN',
            4146: 'NGUYỄN TIẾN MINH',
            4268: 'TÔ NGỌC HIẾU',
        },
        5: {
            2442: 'LÊ THỊ THÙY',
            2549: 'HÁN ĐỨC ANH',
            2554: 'VŨ HỮU CHỈNH',
        },
    }
    
    joblv2 = pd.DataFrame(dict_job.items(), columns=['job_lv2_id', 'job_lv2'])
    # Tạo bảng tạm gán các cặp id nhân viên + Tên nhân viên - id danh mục nhóm chức danh mới theo key các dictionary dict_job, dict_emp
    emp_job2 = pd.DataFrame()
    for i in range(1,6):
        dft = pd.DataFrame(list(dict_emp[i].items()), columns=['employee_id', 'employee'])
        dft.insert(2, "job_lv2_id", i, True)
        emp_job2 = pd.concat([emp_job2, dft])
    emp_job2 = emp_job2.merge(joblv2, how='left', on='job_lv2_id')
    # Map job_lv2 vào bảng kết quả từ bảng tạm trên
    df['job_lv2'] = np.where(df.id.isin(emp_job2.employee_id), df['id'].map(emp_job2.set_index('employee_id')['job_lv2']), df.job)

    df.sort_values(['active', 'create_date'], ascending=[True,True], inplace=True)
    df.drop_duplicates('employee_code', keep='first', inplace=True)
    ls_col_int = [c for c in df.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|id$|create_by|priority|phanquyen|_id_') and not check_substr(c, 'identification')]
    fix_type(df, ls_col_int, 'Int64', np.nan)
    return df

#df = get_data_employee()

def write_stag_employees():
    dfc = get_data_employee()     #  get_data_product()   dfpd.copy()
    TABLE = 'stag_employees'

    # CẬP NHẬT GIÁ TRỊ BẢNG 
    # Lưu ý nếu không truyền tham số "where=...", hàm replace sẽ DELETE cả bảng trước khi đổ dữ liệu mới vào
    replace_table_data(dfc, TABLE, conn=conn)
    print('''DANH MỤC CÁN BỘ NHÂN VIÊN TẬP ĐOÀN (HR_EMPLOYEE)''')
        
    return TABLE

#write_stag_employees()


# Dữ liệu check Ngày thanh toán lần đầu của các Dịch vụ/Booking được đồng bộ từ ERP cũ

def get_df_fdate():
    """Truy vấn nằm trong hàm để không chạy mỗi lần scheduler parse file DAG."""
    if 'fdate' in _CACHE:
        return _CACHE['fdate']
    df_fdate = query_df(conn, f'''\
SELECT DISTINCT ON (spt.crm_id, spt.clineid, spt.product_type)
	spt.crm_id as crm_id_old, clineid, product_id,    --- spt.clineid AS clineid_old
    spt.crm_id,
    first_date_payment, rnk_clineid_pmdate,
    rnk_crm_pmdate,
    total,
    total_received_recal AS total_received_old,
    quantity,
    number_used,
    stage_product AS stage,
    spt.brand_etl
FROM stag_payment_transfer spt
INNER JOIN (SELECT DISTINCT crm_id FROM stag_payment_transfer WHERE crm_id < 2174165 AND payment_date >= '{LIMIT_START_DATE_1M}')
	new_transaction ON spt.crm_id = new_transaction.crm_id
WHERE spt.brand_etl = 0 AND spt.product_type = 'service'
ORDER BY spt.crm_id, spt.clineid, spt.product_type, spt.rnk_clineid_pmdate DESC
''')
    fix_type(df_fdate, ['brand_etl', 'crm_id', 'product_id'], int, 0)
    df_fdate['rn_product'] = df_fdate.sort_values('first_date_payment').groupby(['brand_etl', 'crm_id', 'product_id']).cumcount()+1
    df_fdate['max_rnk_crm_date'] = df_fdate.groupby('crm_id')['rnk_crm_pmdate'].transform('max')
    _CACHE['fdate'] = df_fdate
    return df_fdate


# STAG_CRM_LINE_INFO: DỪNG UPDATE (12/08/2025)


def get_fix_product_id():
    if 'fixpid' not in _CACHE:
        _CACHE['fixpid'] = query_df(conn, f'''SELECT clineid, brand_id, MAX(product_id) AS product_id  \
            FROM stag_payment_transfer WHERE GREATEST(payment_date, write_date) >= '{LIMIT_START_DATE_1M}'\
            GROUP BY clineid, brand_id''')
    return _CACHE['fixpid']

def write_temp_crm_id_update():
    query = f'''
    WITH crm_limit AS (
    SELECT crm_id, payment_date, brand_etl FROM account_payment WHERE GREATEST(payment_date, write_date) >= '{LIMIT_START_DATE_1M}'
    UNION ALL 
    SELECT crm_id, payment_date, brand_etl FROM crm_transfer_payment WHERE GREATEST(payment_date, write_date) >= '{LIMIT_START_DATE_1M}' 
    ) SELECT c.crm_id, max(c.payment_date) payment_date, c.brand_etl 
    FROM crm_limit c
    INNER JOIN crm_lead cl ON c.crm_id = cl.id AND c.brand_etl = cl.brand_etl AND cl.active 
    WHERE COALESCE(c.crm_id, 0) <> 0 AND c.brand_etl <> 0 GROUP BY c.crm_id, c.brand_etl '''
    dfc = query_df(conn, query)   
    table = 'temp_crm_id_update'

   
    # temp_crm_id_update: giới hạn các Booking phát sinh Doanh số trong 3 tháng trở lại để Delete - Insert trở lại vào "stag_payment_transfer'
    # CẬP NHẬT GIÁ TRỊ BẢNG temp_crm_id_update'
    qr_create_table_temp = f'''DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (crm_id INT NOT NULL, payment_date DATE, brand_etl INT);  '''
    query_df(conn, qr_create_table_temp, ret='exe')
    insert_data_to_db(dfc, table)

    return table
    
#write_temp_crm_id_update()


# Phiếu thanh toán phát sinh của crm_id trên bảng tạm temp_crm_id_update
query_pm = \
f'''
SELECT concat('ps-', capd.id) AS id, 'service' as product_type, 
    ap.name AS ma_phieu, ap.crm_id, ap.payment_date, ap.payment_method, ap.communication, 
    cline.id AS clineid, cline.product_id, cline.quantity, cline.uom_price, cline.number_used,
    CASE WHEN cline.gia_truoc_huy > 0 THEN cline.gia_truoc_huy ELSE cline.total END AS total,
    capd.total_received, cline.total_received total_received_cline, 
    CASE WHEN ap.payment_type = 'outbound' THEN - capd.prepayment_amount ELSE capd.prepayment_amount END AS payment,
    ap.payment_type,
    cline.stage AS stage_product, cline.consultants_1, cline.consulting_role_1, cline.consultants_2, cline.consulting_role_2, cline.ho_tro,
    cline.price_list_id,
    ap.company_id AS orgcompany_id,
        CASE
            WHEN capd.company_id IS NOT NULL THEN capd.company_id
            WHEN ap.x_receive_company_id IS NULL OR ap.x_receive_company_id = 0 THEN ap.company_id
            ELSE ap.x_receive_company_id
        END AS company_id,
    (ap.write_date + interval '7 hours') write_date, 
    ap.brand_id, ap.brand_etl
   FROM crm_account_payment_detail capd
     LEFT JOIN account_payment ap ON capd.account_payment_id = ap.id AND ap.brand_etl = capd.brand_etl
     LEFT JOIN crm_line cline ON capd.crm_line_id = cline.id AND cline.brand_etl = capd.brand_etl
  WHERE capd.brand_etl = {idb_kn} AND 
      ap.state = 'posted' AND capd.prepayment_amount <> 0 
      AND ap.crm_id IN (SELECT crm_id FROM temp_crm_id_update WHERE brand_etl = {idb_kn})
      AND NOT COALESCE(capd.is_delete, False)
UNION ALL
 SELECT concat('pc-', cappd.id) AS id, 'cosmetic' as product_type, 
    ap.name AS ma_phieu, ap.crm_id, ap.payment_date, ap.payment_method, ap.communication, 
    clinep.id AS clineid, clinep.product_id, clinep.product_uom_qty AS quantity, 1 AS uom_price,
    CASE WHEN clinep.stage_line_product = 'sold' THEN clinep.product_uom_qty ELSE 0 END AS number_used,
    clinep.total,
    cappd.total_received, clinep.total_received total_received_cline, 
    CASE WHEN ap.payment_type = 'outbound' THEN - cappd.prepayment_amount ELSE cappd.prepayment_amount END AS payment,
    ap.payment_type,
    clinep.stage_line_product AS stage_product, clinep.consultants_1, clinep.consulting_role_1, clinep.consultants_2, clinep.consulting_role_2,
    NULL::integer AS ho_tro,
    clinep.product_pricelist_id AS price_list_id,
    ap.company_id AS orgcompany_id,
        CASE
            WHEN cappd.company_id IS NOT NULL THEN cappd.company_id
            WHEN ap.x_receive_company_id IS NULL OR ap.x_receive_company_id = 0 THEN ap.company_id
            ELSE ap.x_receive_company_id
        END AS company_id,
    (ap.write_date + interval '7 hours') write_date,
    ap.brand_id, ap.brand_etl
   FROM crm_account_payment_product_detail cappd
     LEFT JOIN account_payment ap ON cappd.account_payment_id = ap.id AND ap.brand_etl = cappd.brand_etl
     LEFT JOIN crm_line_product clinep ON cappd.crm_line_product_id = clinep.id AND clinep.brand_etl = cappd.brand_etl
  WHERE cappd.brand_etl = {idb_kn} AND 
      ap.state = 'posted' AND cappd.prepayment_amount <> 0 
      AND ap.crm_id IN (SELECT crm_id FROM temp_crm_id_update WHERE brand_etl = {idb_kn})
      AND NOT COALESCE(cappd.is_delete, False)
'''


# Phiếu điều chuyển phát sinh của crm_id trên bảng tạm temp_crm_id_update
query_tf = \
f'''
SELECT ctpl.id,
    ctp.name AS ma_phieu,
    ctp.crm_id,
    CASE WHEN COALESCE(ctpl.from_crm_line_id, 0) = 0 THEN 'cosmetic' ELSE 'service' END product_typef,
    CASE WHEN COALESCE(ctpl.to_crm_line_id, 0) = 0 THEN 'cosmetic' ELSE 'service' END product_typet,
    COALESCE(NULLIF(ctpl.from_crm_line_id, 0), ctpl.from_crm_line_product_id) clineidf,
    COALESCE(NULLIF(ctpl.to_crm_line_id, 0), ctpl.to_crm_line_product_id) clineidt,
    ctp.payment_date,
    ctp.company_id AS orgcompany_id,
    COALESCE(NULLIF(clinef.product_id, 0), clinepf.product_id) AS product_idf,
    COALESCE(NULLIF(clinet.product_id, 0), clinept.product_id) AS product_idt,
    ctpl.from_company_id company_idf,
    COALESCE(NULLIF(ctpl.to_company_id, 0), ctpl.from_company_id) AS company_idt,
    COALESCE(NULLIF(clinef.quantity, 0), clinepf.product_uom_qty) quantityf,
    clinef.uom_price AS uom_pricef,
    COALESCE(NULLIF(clinef.number_used, 0), CASE WHEN clinepf.stage_line_product = 'sold' THEN clinepf.product_uom_qty ELSE 0 END) 
    AS number_usedf,
    COALESCE(clinef.stage, clinepf.stage_line_product) AS stage_productf,
    COALESCE(NULLIF(clinef.consultants_1, 0), clinepf.consultants_1) AS consultants_1f,
    COALESCE(clinef.consulting_role_1, clinepf.consulting_role_1) AS consulting_role_1f,
    COALESCE(NULLIF(clinef.consultants_2, 0), clinepf.consultants_2) AS consultants_2f,
    COALESCE(clinef.consulting_role_2, clinepf.consulting_role_2) AS consulting_role_2f,
    clinef.ho_tro AS ho_trof,
    COALESCE(NULLIF(clinet.quantity, 0), clinept.product_uom_qty) AS quantityt,
    clinet.uom_price AS uom_pricet,
    COALESCE(NULLIF(clinet.number_used, 0), CASE WHEN clinept.stage_line_product = 'sold' THEN clinept.product_uom_qty ELSE 0 END)
    AS number_usedt,
    COALESCE(clinet.stage, clinept.stage_line_product) AS stage_productt,
    COALESCE(NULLIF(clinet.consultants_1, 0), clinept.consultants_1) AS consultants_1t,
    COALESCE(clinet.consulting_role_1, clinept.consulting_role_1) AS consulting_role_1t,
    COALESCE(NULLIF(clinet.consultants_2, 0), clinept.consultants_2) AS consultants_2t,
    COALESCE(clinet.consulting_role_2, clinept.consulting_role_2) AS consulting_role_2t,
    clinet.ho_tro AS ho_trot,
    CASE WHEN clinef.gia_truoc_huy > 0 THEN clinef.gia_truoc_huy ELSE COALESCE(clinef.total, clinepf.total) END AS totalf,
    CASE WHEN clinet.gia_truoc_huy > 0 THEN clinet.gia_truoc_huy
        WHEN clinet.total IS NULL THEN clinept.total ELSE clinet.total
    END AS totalt,
    ctpl.transfer_amount,
    NULL AS payment_type,
    ctpl.total_received AS total_receivedf,
    ctplt.total_received AS total_receivedt,
    clinef.total_received AS total_received_clinef,
    clinet.total_received AS total_received_clinet,
    COALESCE(NULLIF(clinef.price_list_id, 0), clinepf.product_pricelist_id) AS price_list_idf,
    COALESCE(NULLIF(clinet.price_list_id, 0), clinept.product_pricelist_id) AS price_list_idt,
    'pdc' AS payment_method,
    ctp.communication, 
    (ctp.write_date + interval '7 hours') write_date, 
    ctp.brand_id, ctp.brand_etl
   FROM crm_transfer_payment_line ctpl
     LEFT JOIN crm_transfer_payment ctp ON ctpl.crm_transfer_payment_id = ctp.id AND ctp.brand_etl = {idb_kn} 
     LEFT JOIN crm_line clinef ON ctpl.from_crm_line_id = clinef.id AND clinef.brand_etl = {idb_kn} 
     LEFT JOIN crm_line clinet ON ctpl.to_crm_line_id = clinet.id AND clinet.brand_etl = {idb_kn} 
     LEFT JOIN crm_line_product clinepf ON ctpl.from_crm_line_product_id = clinepf.id AND clinepf.brand_etl = {idb_kn} 
     LEFT JOIN crm_line_product clinept ON ctpl.to_crm_line_product_id = clinept.id AND clinept.brand_etl = {idb_kn} 
     LEFT JOIN crm_transfer_payment_line ctplt ON ctpl.crm_transfer_payment_id = ctplt.crm_transfer_payment_id 
                                                 AND ctpl.to_crm_line_id = ctplt.from_crm_line_id AND ctplt.brand_etl = {idb_kn} 
  WHERE ctpl.brand_etl = {idb_kn} 
      AND ctpl.transfer_amount <> 0 AND ctp.state = 'done' 
      AND ctp.crm_id IN (SELECT crm_id FROM temp_crm_id_update WHERE brand_etl = {idb_kn})
      AND NOT COALESCE(ctpl.is_delete, False) = True
'''

def get_data_payment():
    fix_product_id = get_fix_product_id()
    df_fdate = get_df_fdate()
    df_spt = pd.DataFrame()
    for idb in [i for i in LS_BRAND_ETL if i != 7]:      # Lấy Data từng thương hiệu, loại trừ ERP chung HCI
        query_pm_repl = query_pm.replace('brand_etl = 1', f'brand_etl = {idb}')
        query_tf_repl = query_tf.replace('brand_etl = 1', f'brand_etl = {idb}')
    
        dfpm = query_df(conn, query_pm_repl)          # Truy vấn Dữ liệu Thanh toán 
        dftf = query_df(conn, query_tf_repl)          # Truy vấn Dữ liệu Điều chuyển tiền
    
        # Xử lý Bảng điều chuyển tiền
        colft = sorted([c for c in dftf.columns if c[-1] in ['t', 'f'] and c != 'transfer_amount'])
        col = list(set(c[:-1] for c in colft))
        
        dfo = dftf.drop_duplicates('id').copy()       # Dữ liệu điều chuyển Outbound 
        dfo['id'] = 'to-' + dfo['id'].astype(str)
        dfo['payment_type'] = 'outbound'
        dfo['payment'] = -dfo['transfer_amount']
        dfo.rename(columns={c: c[:-1] for c in colft if c[-1] == 'f'}, inplace=True)
        
        dfi = dftf.drop_duplicates('id').copy()       # Dữ liệu điều chuyển Inbound 
        dfi['id'] = 'ti-' + dfi['id'].astype(str)
        dfi['payment_type'] = 'inbound'
        dfi['payment'] = dfi['transfer_amount']
        dfi.rename(columns={c: c[:-1] for c in colft if c[-1] == 't'}, inplace=True)

        # Nối tiếp dữ liệu vào mẫu stag_payment_transfer từ các Data xử lý trên
        df_spt = pd.concat([df_spt, dfpm, dfo[dfpm.columns], dfi[dfpm.columns]], axis=0)
        print(f'Cập nhật dữ liệu Payment {DICT_BRAND_ETL[idb].upper()}:     {dfpm.shape[0]} \nCập nhật dữ liệu Điều chuyển {DICT_BRAND_ETL[idb].upper()}: {dfi.shape[0]}*2')

    # Xử lý product_id = 0 nếu từng tồn tại.
    map_pid = (fix_product_id.dropna(subset=['clineid'])
               .drop_duplicates(['brand_id', 'clineid'])
               .set_index(['brand_id', 'clineid'])['product_id'])
    need_fix = df_spt['product_id'].fillna(0).eq(0)
    if need_fix.any():
        keys = pd.MultiIndex.from_arrays([df_spt.loc[need_fix, 'brand_etl'],
                                          df_spt.loc[need_fix, 'clineid']])
        df_spt.loc[need_fix, 'product_id'] = keys.map(map_pid)

    # XỬ LÝ SỐ TIỀN ĐÃ THU LŨY KẾ THEO PHIẾU GIAO DỊCH VÀ THỜI GIAN 
    df_spt.reset_index(drop=False, inplace=True)
    fix_type(df_spt, [c for c in df_spt.columns if check_substr(c, r'total|uom_price|^payment$')], float, np.nan)
    df_spt['uom_price'] = np.where(df_spt.uom_price.isnull() & df_spt.product_type.eq('cosmetic'), 1, df_spt.uom_price)
    
    # Update lại thông tin ban đầu của DV: Tổng tiền phải thu, Tổng tiền đã thu, Số buổi, Số lượng đã sử dụng
    col_crm = ['crm_id', 'clineid']
    col_cline = ['brand_etl','clineid','product_type']
    cline_info = df_spt[col_crm].reset_index().merge(df_fdate, how='left', on=col_crm, suffixes=('', '_old')).set_index('index')
    crm_info = df_spt[['crm_id']].reset_index()\
            .merge(df_fdate.groupby('crm_id').agg({'first_date_payment': min, 'rnk_crm_pmdate': max}), how='left', on='crm_id')\
            .set_index('index')
    
    # Thứ tự Ngày thanh toán / Dòng dịch vụ hoặc / Booking, lũy kế Số tiền đã thu  
    df_spt['first_date_payment'] = df_spt.groupby(col_cline)['payment_date'].transform(min)
    df_spt['rnk_crm_pmdate'] = df_spt.groupby(['brand_etl','crm_id'])['payment_date'].transform('rank', method='dense', ascending=True)
    df_spt['rnk_clineid_pmdate'] = df_spt.groupby(col_cline)['payment_date'].transform('rank', method='dense', ascending=True)    # Thanh toán ngày thứ n / Dịch vụ / Booking
    df_spt['rnk_clineid_pmdate_desc'] = df_spt.groupby(col_cline)['payment_date'].transform('rank', method='dense', ascending=False)
    df_spt['total_received_cumsum'] = df_spt.groupby(['crm_id', 'clineid', 'product_id'])['payment'].cumsum()
    
    df_spt['first_date_payment'] = crm_info['first_date_payment'].fillna(df_spt['first_date_payment'])
    df_spt['rnk_crm_pmdate'] = df_spt.rnk_crm_pmdate + crm_info.rnk_crm_pmdate.fillna(0)
        
    df_spt['total_received_old'] = cline_info['total_received_old']
    dict_col_cline = {'gt': ['total', 'quantity'], 'add': ['number_used', 'rnk_clineid_pmdate'], 'eq': ['total_received_old']}
    for cdt, lscol in dict_col_cline.items(): 
        for col in lscol:
            if cdt == 'gt': 
                df_spt[col] = np.where(cline_info[col].fillna(0).gt(df_spt[col]), cline_info[col], df_spt[col])
            elif cdt == 'add': 
                df_spt[col] = df_spt[col] + cline_info[col].fillna(0)
            elif cdt == 'eq': 
                df_spt[col] = cline_info[col]
    
    # Sắp xếp lại dữ liệu theo Ngày giao dịch tăng dần/Dòng dịch vụ
    df_spt['is_priority'] = np.where(df_spt.ma_phieu.str.startswith('PDC') & df_spt.total_received_cline.gt(0), True, False)
    df_spt = df_spt.sort_values(['brand_id','crm_id', 'clineid', 'product_id', 'payment_date', 'is_priority', 'write_date'], 
                                ascending=[True, True, True, True, True, False, True])
    
    fix_type(df_spt, [c for c in df_spt.columns if check_substr(c, r'total|uom_price|^payment$')], float, np.nan)
    df_spt['total_received_cumsum'] = df_spt.groupby(['crm_id', 'clineid', 'product_id'])['payment'].cumsum() + df_spt.total_received_old.fillna(0)  
    # Điều kiện kiểm tra lại tổng Số tiền đã thu dựa trên Tiền đã thu hệ thống cũ hoặc Giá trị Số tiền đã thu đẩy lên hệ thống mới
    keep_total = df_spt.total_received_cumsum.round().eq((df_spt.total + df_spt.total_received_old).round()) \
        | df_spt.total.round().eq((df_spt.total_received+df_spt.payment).round())
    keep_total_received_p = (df_spt.payment + df_spt.total_received).round() > df_spt.total.round()
    df_spt['total_received_cumsum'] = np.select([keep_total, keep_total_received_p], [df_spt.total, df_spt.payment+df_spt.total_received],\
                                                df_spt.total_received_cumsum)
    ls_rec = ['brand_id', 'crm_id', 'clineid', 'product_id', 'payment_date']
    df_spt['total_received_recal'] = df_spt.groupby(ls_rec)['total_received_cumsum'].transform('last')

    df_spt['id'] = df_spt.id + '-' + df_spt.brand_etl.astype(str)
    ls_col_int = [c for c in df_spt.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|id$|quantity|number_used|consultants|ho_tro|_id_') and c != 'id']
    fix_type(df_spt, ls_col_int, 'Int64', np.nan)
    return df_spt

#df = get_data_payment()


def write_stag_payment_transfer_grby_date(df):
    colgr = ['brand_id', 'crm_id', 'product_id', 'payment_date']
    dfjoin = df.groupby(colgr)[['id', 'ma_phieu', 'clineid', 'payment_type']].agg(lambda x: ','.join(x.values.astype(str)))
    dfsum = df.groupby(colgr)['payment'].agg(lambda x: sum(x.astype(float)))
    dflast = df.groupby(colgr)[['quantity', 'uom_price', 'number_used', 'total', 'total_received', 'total_received_recal', 'stage_product', 
                               'consultants_1', 'consulting_role_1', 'consultants_2', 'consulting_role_2', 'price_list_id', 
                                'orgcompany_id', 'company_id', 'brand_etl']].last()
    
    dfc = pd.concat([dfjoin, dfsum, dflast], axis=1).reset_index()
    TABLE = 'stag_payment_transfer_grby_date'

    # CẬP NHẬT GIÁ TRỊ BẢNG 
    where_ud = '''brand_etl <> 0 AND (crm_id, brand_etl) IN (SELECT crm_id, brand_etl FROM temp_crm_id_update)'''
    qr_del_to_update = f'''DELETE FROM {TABLE} pm 
                        USING temp_crm_id_update t WHERE pm.brand_etl = t.brand_etl AND pm.crm_id = t.crm_id AND pm.brand_etl <> 0 '''
    replace_table_data(dfc, TABLE, where=where_ud, conn=conn)
    print(f'CẬP NHẬT BẢNG THANH TOÁN DỊCH VỤ GROUPBY_DATE TỪ NGÀY {LIMIT_START_DATE_1M}')
      
    return TABLE
    
#write_stag_payment_transfer_grby_date(df)


def write_stag_payment_transfer():
    #write_temp_crm_id_update()
    check_temp = query_df(conn, "SELECT to_regclass(%s) IS NOT NULL AS ton_tai", params=('temp_crm_id_update',)).iloc[0, 0]
    if check_temp:
        dfc = get_data_payment()    #  get_data_payment()      df.copy()
        TABLE = 'stag_payment_transfer'
    
        # CẬP NHẬT GIÁ TRỊ BẢNG 
        where_ud = '''brand_etl <> 0 AND (crm_id, brand_etl) IN (SELECT crm_id, brand_etl FROM temp_crm_id_update)'''
        replace_table_data(dfc, TABLE, where=where_ud, conn=conn)
        print(f'CẬP NHẬT BẢNG THANH TOÁN GIAO DỊCH TỪ NGÀY {LIMIT_START_DATE_1M}')
    
        # Cập nhật dữ liệu Bảng: stag_payment_transfer_grby_date
        write_stag_payment_transfer_grby_date(dfc)    
        return TABLE
    else: 
        raise ValueError(warn_msg(f'DỪNG! KIỂM TRA BẢNG TẠM (DÙNG ĐỂ UPDATE) CHƯA TỒN TẠI: temp_crm_id_update'))

#write_stag_payment_transfer()
#print(query_tf)


# BẢNG XÁC ĐỊNH CHI NHÁNH ĐẾN CỬA BOOING SHARE

query_crm_share = \
f'''
WITH d AS  			--- Giới hạn Thời gian update thông tin (Ngày đến cửa trong N ngày trở lại)
(SELECT DATE_TRUNC('month', NOW() - INTERVAL '7 days') AS limit_date)
, cls AS 			--- Danh sách Booking có share company
(
SELECT cl.id crm_id, cl.contact_name, cl.name booking, cl.arrival_date, cl.company_id, ccsr.company share_company_id, cl.brand_etl 
FROM crm_lead cl 
LEFT JOIN company_crm_share_ref ccsr ON cl.id = ccsr.crm and ccsr.brand_etl = cl.brand_etl 
where cl.brand_etl > 0 ---AND cl.arrival_date >= (SELECT * FROM d)
AND cl.type_crm_id = 2 
AND cl.company_id <> ccsr.company 
AND cl.customer_come = 'yes'
AND coalesce(cl.arrival_date, '19000101') >= '{LIMIT_START_DATE_1M}'
), cline AS 
( 						--- Danh sách tư vấn viên
SELECT cls.*, 
	l.id clineid, l.product_id, 
	l.consultants_1 consultants, se.employee, se.job, 'consult_1' as ordinal_consult,
	se.company_id employee_company_id, se.company employee_company, 
	CASE WHEN se.company_id = cls.company_id or se.company_id = cls.share_company_id 
		THEN true ELSE false 
	END AS is_branch_consult, 
	CASE WHEN l.stage in ('cancel') THEN 0 ELSE 1 END is_done
FROM crm_line l 
INNER JOIN cls ON l.crm_id = cls.crm_id and l.brand_etl = cls.brand_etl
LEFT JOIN view_stag_employees_user se ON l.consultants_1 = se.user_id AND l.brand_etl = se.brand_etl
	---(case when l.brand_etl = 1 then se.kn_user_id when l.brand_etl = 3 then se.pr_user_id when l.brand_etl = 4 then se.hh_user_id end)
WHERE se.brand_id in (1,3,4)  									--- Tư vấn viên thuộc thương hiệu
--- AND c.ordinal_consult in ('consult_1', 'consult_2', 'recept')	--- Tư vấn 1, 2 hoặc Lễ tân
), cline_rn AS 
(
SELECT cline.*, 
	CASE WHEN employee_company_id = share_company_id THEN share_company_id ELSE company_id END AS arrival_company_id, 
	ROW_NUMBER() over (partition by cline.brand_etl, cline.crm_id ORDER BY is_done DESC, is_branch_consult DESC, clineid, ordinal_consult) AS rn_consult_crm
FROM cline
)
SELECT l.*, sp.product, rc."name" company, rcs.name share_company, rca.name arrival_company, rca.brand_id arrival_brand_id, rb.name arrival_brand
FROM cline_rn l 
LEFT JOIN stag_products sp on l.product_id = (case when l.brand_etl = 1 then sp.product_id when l.brand_etl = 3 then pr_product_id when l.brand_etl = 4 then sp.hh_product_id end)
LEFT JOIN m_res_company rc ON l.company_id = rc.id and rc.brand_etl = l.brand_etl 
LEFT JOIN m_res_company rcs ON l.share_company_id = rcs.id and rcs.brand_etl = l.brand_etl 
LEFT JOIN m_res_company rca ON l.arrival_company_id = rca.id and rca.brand_etl = l.brand_etl 
LEFT JOIN res_brand rb ON rca.brand_id = rb.id and rb.brand_etl = rca.brand_etl 
WHERE rn_consult_crm = 1
'''

def write_stag_crm_booking_arrival_company():
    dfc = query_df(conn, query_crm_share)
    ls_col_int = [c for c in dfc.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|(id|create_by)$|consultants|_id_')]
    fix_type(dfc, ls_col_int, 'Int64', np.nan)
    TABLE = 'stag_crm_booking_arrival_company'
    
    # CẬP NHẬT GIÁ TRỊ BẢNG 
    where_ud = f"""brand_etl <> 0 AND coalesce(arrival_date, '19000101') >= '{LIMIT_START_DATE_1M}'"""
    replace_table_data(dfc, TABLE, where=where_ud, conn=conn)
    print(f'''BẢNG THÔNG TIN CHI NHÁNH ĐẾN CỬA TỪ NGÀY {LIMIT_START_DATE_1M} CÁC BOOKING SHARE COMPANY''')

    return TABLE

#write_stag_crm_booking_arrival_company()
#print(query_crm_share)


# BẢNG ID BOOKING - DỊCH VỤ - TƯ VẤN VIÊN PARIS: stag_crm_information_consultant

query_consult_pr = \
f'''
WITH consults AS 
(             --- Tư vấn viên Dòng Dịch vụ
SELECT 	'service' as data_union, cic.brand_etl, 
		cast(cic.id as varchar) id,
		cic."role", cic.user_id, cline.crm_id, cic.crm_line_id,
        cline.stage, 
		cic.create_uid, cic.create_date + INTERVAL '7 hours' AS create_date, cic.write_uid, cic.write_date + INTERVAL '7 hours' AS write_date
FROM crm_information_consultant cic
LEFT join crm_line cline on cic.crm_line_id = cline.id and cline.brand_etl = cic.brand_etl 
WHERE cic.brand_etl = 3 AND cic.crm_line_id IS NOT NULL 
UNION ALL    --- Tư vấn viên Dòng Sản phẩm 
SELECT 	'cosmetic' as data_union, cicc.brand_etl, 
		cast(cicc.id as varchar) id,
		cicc."role", cicc.user_id, clp.booking_id crm_id, cicc.crm_line_product_id crm_line_id,
        clp.stage_line_product stage, 
		cicc.create_uid, cicc.create_date + INTERVAL '7 hours' AS create_date, cicc.write_uid, cicc.write_date + INTERVAL '7 hours' AS write_date
FROM crm_information_consultant cicc
LEFT join crm_line_product clp on cicc.crm_line_product_id = clp.id and clp.brand_etl = cicc.brand_etl 
WHERE cicc.brand_etl = 3 AND cicc.crm_line_product_id IS NOT NULL
UNION ALL    --- Tư vấn viên Dòng sản phẩm Người dùng không nhập Thành viên ở bảng Many-to-One mà nhập ở Tư vấn 1
SELECT 'cosmetic' as data_union, clp.brand_etl , 
		cast(clp.id as varchar) id, 
		'sold_cosmetic' "role", consultants_1 user_id, clp.booking_id crm_id, clp.id crm_line_id, 
        clp.stage_line_product stage, 
        clp.create_uid, clp.create_date + INTERVAL '7 hours' AS create_date, clp.write_uid, clp.write_date + INTERVAL '7 hours' AS write_date
FROM crm_line_product clp
LEFT JOIN m_res_company rc ON clp.company_id = rc.id 
WHERE clp.brand_etl = 3 and rc.brand_id = 3 
AND NOT clp.id IN
	(select distinct crm_line_product_id from crm_information_consultant cicl
	where crm_line_product_id is not null)
), crm_limit as 
(   --- Giới hạn các Booking có cập nhật Tư vấn viên
SELECT crm_id
FROM consults 
WHERE write_date >= '{LIMIT_START_DATE}'
GROUP BY crm_id
)
SELECT * 
FROM consults 
WHERE crm_id in (SELECT crm_id FROM crm_limit)
'''


def write_stag_crm_information_consultant():
    # TRUY VẤN, XỬ LÝ DATA 
    TABLE = 'stag_crm_information_consultant'
    
    df = query_df(conn, query_consult_pr)
    df = df[df.crm_id.fillna(0).ne(0) & df.crm_line_id.fillna(0).ne(0)].drop_duplicates(['crm_id', 'crm_line_id', 'user_id'])
    df['rn_role_line'] =  df.sort_values('create_date').groupby(['crm_id', 'crm_line_id', 'role']).cumcount()+1
    ls_col_int = [c for c in df.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|(id|create_by)$|phanquyen|_id_')]
    fix_type(df, ls_col_int, 'Int64', np.nan)
    df['id_etl'] = df[['crm_id', 'crm_line_id', 'role', 'rn_role_line']].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1) #.apply(lambda r: concat_str(r, '-'), axis=1)
    
    
    # Tạo Bảng tạm các Booking được Cập nhật Tư vấn viên trong thời gian giới hạn
    df_temp = df.groupby(['brand_etl','crm_id'], as_index=False)['write_date'].max()
    col_temp = ', '.join(df_temp.columns)
    create_temp_table = f"DROP TABLE IF EXISTS {TABLE}_temp_update; \
                    CREATE TABLE {TABLE}_temp_update AS SELECT {col_temp} FROM {TABLE} LIMIT 0; "
    query_df(conn, create_temp_table, ret='exe')
    insert_data_to_db(df_temp, f"{TABLE}_temp_update")

    
    # CẬP NHẬT GIÁ TRỊ BẢNG THÀNH VIÊN TƯ VẤN -  PARIS
    where_ud = f'(crm_id, brand_etl) IN (SELECT crm_id, brand_etl FROM {TABLE}_temp_update)'
    replace_table_data(df, TABLE, where=where_ud, conn=conn)
    query_df(conn, f"DROP TABLE IF EXISTS {TABLE}_temp_update;", ret='exe')
    print('DỮ LIỆU TRUNG GIAN ID TƯ VẤN VIÊN - PARIS')

    return TABLE

#write_stag_crm_information_consultant()


# BẢNG THỨ TỰ ƯU TIÊN TƯ VẤN VIÊN / BOOKING
query_consult = \
f'''
WITH linecc AS
(
---- Vi tri nhap Tu van vien 1, 2, 3
SELECT '1-service' AS data_union 
    ,   cline.crm_id, cline.id clineid, COALESCE(cline.consultants_1, 0) consultants_1, cline.consulting_role_1 
	, 	COALESCE(case when cline.consultants_2 = cline.consultants_1 and cline.consultants_2 = cline.consultants_3 
         THEN NULL ELSE cline.consultants_2 END, 0) consultants_2 
	,	cline.consulting_role_2 
	, 	COALESCE(CASE WHEN cline.consultants_1 = cline.consultants_2 AND cline.consultants_2 = cline.consultants_3 THEN 0 
        ELSE cline.consultants_3 END, 0) as consultants_3 
	,	cline.consulting_role_3  
	,	COALESCE(cline.ho_tro, 0) ho_tro
    ,   cline.stage 
	, 	cline.write_uid, 	cline.write_date
	, 	cline.brand_etl 
FROM crm_line cline
WHERE brand_etl > 0
UNION ALL
---- Tư vấn viên của Booking chỉ bán Sản phẩm (không có dòng dịch vụ)
SELECT '2-cosmetic' AS data_union 
    , CASE WHEN clinep.brand_etl = 1 AND clinep.id IN (253400, 253401, 253402) THEN 2325389 ELSE clinep.booking_id END AS crm_id
    , clinep.id clineid, COALESCE(clinep.consultants_1, 0), clinep.consulting_role_1 
	, COALESCE(case when clinep.consultants_2 = clinep.consultants_1 then null else clinep.consultants_2 end, 0) consultants_2 
	, clinep.consulting_role_2
	, 0 as consultants_3 
	, null as consulting_role_3 
	, 0 as ho_tro 
    , clinep.stage_line_product stage
	, clinep.write_uid, clinep.write_date 
	, clinep.brand_etl 
FROM crm_line_product clinep 
LEFT JOIN (SELECT crm_id, brand_etl FROM crm_line 
			WHERE crm_id IS NOT NULL AND (total_received > 0 OR stage = 'done') AND brand_etl > 0
			GROUP BY crm_id, brand_etl) l 
	ON l.crm_id = clinep.booking_id AND l.brand_etl = clinep.brand_etl 
WHERE clinep.brand_etl > 0 ----AND l.crm_id IS NULL 
), crm_limit as 
(SELECT crm_id, brand_etl 
FROM linecc 
WHERE write_date >= '{LIMIT_START_DATE_1M}'    --- '{LIMIT_START_DATE}'  '{DATE_SYS_STR}' 
GROUP BY crm_id, brand_etl)       
SELECT linecc.* FROM linecc  
LEFT JOIN crm_limit ON linecc.crm_id = crm_limit.crm_id AND linecc.brand_etl = crm_limit.brand_etl
WHERE crm_limit.crm_id IS NOT NULL              
'''

#dfqr = query_df(conn, query_consult)
def get_users():
    if 'users' not in _CACHE:
        _CACHE['users'] = query_df(conn, 'SELECT * FROM stag_employees')
    return _CACHE['users']

def get_data_crm_consult():
    users = get_users()
    dfpr = query_df(conn, f'''
    SELECT scic.crm_id, crm_line_id clineid, user_id consultants, role consulting_role, write_uid, write_date, 'role' as ordinal_consult, stage, id_etl, brand_etl
    FROM stag_crm_information_consultant scic 
    WHERE "role" = 'recept' AND write_date >= '{LIMIT_START_DATE_1M}'    ---'{LIMIT_START_DATE_1M}' '{DATE_SYS_STR}' ''')
    
    df = query_df(conn, query_consult)

    frames = []
    for i in [1,2,3]:
        i = str(i)
        dfcut = df[df['consultants_'+i].ne(0)].rename(columns={'consultants_'+i: 'consultants', 'consulting_role_'+i: 'consulting_role'})
        dfcut.drop(columns = list(c for c in dfcut.columns if c[-2:-1] == '_' or c == 'ho_tro'), inplace=True)
        dfcut['ordinal_consult'] = 'consult_'+i
        frames.append(dfcut.copy())
    df4 = df[df.ho_tro.ne(0)].drop(columns=list(c for c in df.columns if c[-2:-1] == '_')).rename(columns={'ho_tro': 'consultants'})
    df4['ordinal_consult'] = 'support'
 
    # Nối dữ liệu tư vấn 1, 2, hỗ trợ, tư vấn paris
    dfc = pd.concat(frames + [df4, dfpr], ignore_index=True)
    dfc = dfc[dfc.crm_id.ne(0) & dfc.clineid.ne(0)]
    dfc.drop_duplicates(['brand_etl', 'crm_id', 'clineid', 'consultants', 'ordinal_consult'], inplace=True)
    frames_br = []
    for idb in dfc['brand_etl'].unique():
        df_temp = dfc[dfc.brand_etl.eq(idb)]
        user = 'kn_user_id' if idb==1 else 'pr_user_id' if idb==3 else 'hh_user_id' if idb==4 else 'user_id' if idb==7 else None
        if user != None:
            cols_user = [user, 'job', 'department', 'job_id', 'department_id']
            df_temp = df_temp.merge(users[users[user].fillna(0).ne(0)][cols_user], how='left', left_on='consultants', right_on=user)
        frames_br.append(df_temp)
    dfcc = pd.concat(frames_br, ignore_index=True)
    
    # Nối thông tin chức danh, phòng ban nhân sự tư vấn
    for _c in ['job', 'department']:
        _val = dfcc[_c].astype(str)
        _tra = {v: remove_accent(v) for v in _val.unique()}
        dfcc[_c] = _val.map(_tra)
    dept_letan = dfcc.department.str.contains('letan') | dfcc.department_id.isin([2430,2557,2612])
    priority_job = np.select([dept_letan & ( dfcc.job.str.contains('chuyenvien') | dfcc.job_id.isin([4192, 2161]) ), 
                             dept_letan, 
                              dfcc.consultants.isin([1,2,3,4,5,6284]), # admin, bot, it...
                              dfcc.department.str.contains('sale')], 
                             [2, 1, 999, 99], 4)
    dfcc['priority_consult_crm'] = np.select([dfcc.consulting_role.eq('recept'), dfcc.ordinal_consult.eq('support')], 
                                             [1, 100], priority_job)
    dfcc['min_priority_consult_crm'] = dfcc.groupby(['brand_etl','crm_id'])['priority_consult_crm'].transform('min')
    dfcc['is_not_recept_role'] = dfcc.consulting_role.fillna('').ne('recept') 
    dfcc.sort_values(['brand_etl','crm_id','data_union','is_not_recept_role', 'priority_consult_crm'], inplace=True) 
    dfcc['rn_consult_crm'] = dfcc.groupby(['brand_etl','crm_id','priority_consult_crm'])['crm_id'].cumcount()+1
    dfcc['order_stage'] = np.where(dfcc.stage.fillna('').eq('cancel'), 1, 0)         # Gán giá trị để sắp xếp, ưu tiên xét các Dòng DV không bị Hủy trước
    dfcc['sort_priority_role'] = np.select([dfcc.min_priority_consult_crm.eq(dfcc.priority_consult_crm).astype(bool), dfcc.priority_consult_crm.isin([2,3]).astype(bool)], 
                                          [1, 2], 0)             # 1: NV tư vấn/đón tiếp chính, 2: Chuyên viên/Bác sĩ tư vấn
    dfcc.sort_values(['brand_etl', 'data_union', 'order_stage', 'clineid','sort_priority_role', 'priority_consult_crm'], inplace=True)
    dfcc['rn_fconsult_crm'] = np.where(dfcc.sort_priority_role.ne(0), dfcc.groupby(['brand_etl', 'crm_id', 'sort_priority_role']).cumcount()+1, np.nan)
    
    ls_col_int = [c for c in dfcc.columns if check_substr(c, r'^(drk|rn_|rnk|dr)|(id|create_by)$|consultants|priority|_id_')]
    fix_type(dfcc, ls_col_int, 'Int64', np.nan)
    dfcc['id_etl'] = np.where(dfcc.consulting_role.eq('recept'), dfcc.id_etl, dfcc[['brand_etl', 'crm_id', 'clineid', 'ordinal_consult']].fillna(0)
                              .apply(lambda r: '-'.join(r.values.astype(str)), axis=1))
    return dfcc
#dfcc = get_data_crm_consult()


def write_stag_crm_consult_rel():
    dfc = get_data_crm_consult()     #  get_data_crm_consult()   dfcc.copy()
    TABLE = 'stag_crm_consult_rel'

    col_temp = ['id_etl', 'crm_id', 'brand_etl', 'consulting_role']
    create_temp_table = (f"DROP TABLE IF EXISTS {TABLE}_temp_update; "
                         f"CREATE TABLE {TABLE}_temp_update AS "
                         f"SELECT {', '.join(col_temp)} FROM {TABLE} LIMIT 0;")
    query_df(conn, create_temp_table, ret='exe')
    insert_data_to_db(dfc[col_temp], f'{TABLE}_temp_update')
    
    # Bảng tạm vừa tạo chưa có index lẫn thống kê, planner sẽ đoán sai và chọn nested loop. Hai index + ANALYZE là đủ để auto dùng hash join.
    query_df(conn, f'CREATE INDEX ON {TABLE}_temp_update (id_etl); '
                   f'CREATE INDEX ON {TABLE}_temp_update (crm_id, brand_etl); '
                   f'ANALYZE {TABLE}_temp_update;', ret='exe')

    # CẬP NHẬT GIÁ TRỊ BẢNG
    # HAI lệnh DELETE riêng, KHÔNG gộp bằng OR: gộp OR khiến Postgres chạy subplan cho từng dòng của bảng đích, chậm gấp hàng trăm lần 
    # Lệnh xoá nhóm 'recept' chạy ở transaction sau), nếu bước ghi phía sau hỏng, các dòng 'recept' vừa xoá sẽ không được chèn lại.
    query_df(conn, f'''DELETE FROM {TABLE}
                       WHERE consulting_role = 'recept'
                         AND id_etl IN (SELECT id_etl FROM {TABLE}_temp_update)''', ret='exe')
    
    where_ud = f"""(crm_id, brand_etl) IN (SELECT crm_id, brand_etl FROM {TABLE}_temp_update
                                            WHERE COALESCE(consulting_role, '') <> 'recept')"""
    replace_table_data(dfc, TABLE, where=where_ud, conn=conn)
    
    query_df(conn, f"DROP TABLE IF EXISTS {TABLE}_temp_update;", ret='exe')
    return TABLE
    
#write_stag_crm_consult_rel()


# CẬP NHẬT BẢNG CHECK ACCOUNT TRÙNG SĐT

query_acc = \
'''
SELECT 
	rp.id, rp.name customer, rp.code_customer, rp.overseas_vietnamese, 
	phone, mobile, rp.create_date, rp.create_uid,
	se.employee_code, case when se.user_id is null then ru.login else se.employee end employee, 
	se.department, se.company, 
	rp.brand_etl 
FROM res_partner rp
LEFT JOIN res_users ru ON rp.create_uid = ru.id AND rp.brand_etl = ru.brand_etl 
LEFT JOIN view_stag_employees_user se ON rp.create_uid = se.user_id AND rp.brand_etl = se.brand_etl 
WHERE code_customer IS NOT NULL AND code_customer NOT SIMILAR TO '(NCC|S|CHC)%' AND name IS NOT NULL
    AND rp.brand_etl NOT IN (0, 7)
'''

#dfacc = query_df(conn, query_acc)
def check_phone(phoneno, contact_name, overseas):
    p = str(phoneno)
    check = None
    if not ((len(p) == 10 and p[0] == '0') or phoneno is None):
        if check_substr(str(contact_name), r'bs|siam|tpm'):
            check = 'operating_room_rental'
        elif overseas in ['branch', 'marketing']:
            check = 'overseas'
        elif p[:2] == '00' or len(p) <= 8 or len(p) >= 15:
            check = 'wrong_phone'
    return check

def get_data_account_phone():
    dfacc = query_df(conn, query_acc)
    df = pd.DataFrame()
    
    for idb in dfacc.brand_etl.unique():
        df1 = dfacc[dfacc.brand_etl.eq(idb)].copy()
        df1['phone_number'] = df1.phone
        df2 = df1[df1.mobile.ne(df1.phone) & df1.mobile.fillna('').ne('')].copy()
        df2['phone_number'] = df2.mobile
        df1['r'] = 1
        df2['r'] = 2
        dft = pd.concat([df1, df2], ignore_index=True)
        dft['phone_fix'] = dft.phone_number.apply(lambda p: re.sub('^(0+|\+*84(0)?)', '0', re.sub('\s+|^\+', '', str(p))) ).replace('None', np.nan)
    
        dft = dft.sort_values(['phone_fix', 'create_date'])
        dft['count_occur_phone'] = dft.groupby('phone_fix')['phone_fix'].transform('count')
        dft['rn_occur_phone'] = dft.groupby('phone_fix')['phone_fix'].cumcount()+1
        dft['check_phone'] = dft[['phone_number', 'customer', 'overseas_vietnamese']].apply(lambda r: check_phone(*r), axis=1)
    
        org_acc = dft[dft.count_occur_phone.gt(1) & dft.rn_occur_phone.eq(1) ][['phone_fix', 'id', 'code_customer']].rename(columns={'id': 'res_partner_id_org', 'code_customer': 'code_customer_org'})
        dft = dft.merge(org_acc, how='left', on='phone_fix')
        for c in ['res_partner_id_org', 'code_customer_org']:
            dft[c] = np.where(dft.rn_occur_phone.fillna(0).eq(1), np.nan, dft[c])
        fix_type(dft, ['id', 'create_uid', 'r', 'res_partner_id_org', 'count_occur_phone', 'rn_occur_phone', 'r'], 'Int64', np.nan)
    
        df = pd.concat([df, dft])
    return df


def write_stag_res_partner_check_dupphone():
    dfc = get_data_account_phone()
    TABLE = 'stag_res_partner_check_dupphone'

    replace_table_data(dfc, TABLE, conn=conn)
    
    return TABLE

#write_stag_res_partner_check_dupphone()


# BẢNG TÍNH TOÁN TỶ LỆ GIÁ TRỊ DOANH THU THỰC CÁC DÒNG DỊCH VỤ ÁP DỤNG THẺ DA LIỄU

qr_card = ''' 
SELECT cwt.id, pwt.id pwt_id, pcwtr.card_id, pwt.wallet_id, pw.partner_id, cw.code card_code, 
	pwt.line_id clineid, cline.product_id, sp.product, 
    cline.total, cline.quantity, cline.total/cline.quantity AS unit_total, 
    COALESCE(cwt.amount, pwt.amount) as pwt_amount, 
	case when pwt."type" = 'ht' then -cwt.amount else cwt.amount end as amount, 
    pwt."type" as pwt_type, cwt."type",  
	spc.product card, 
	cwv.value_face, cwv.bonus, cwv.value, cwv.gift, 
	cwv.value_face/cwv.value as rto_value, 
	cw.amount_done, cw.value_total, 
    pw.name wallet, pw.code_customer, 
	pwt.crm_id, pwt.brand_etl, pwt.create_date, pwt.write_date 
FROM partner_wallet_transaction pwt 
LEFT JOIN partner_wallet pw ON pwt.wallet_id = pw.id AND pwt.brand_etl = pw.brand_etl 
LEFT JOIN partner_card_wallet_transaction_rel pcwtr ON pcwtr.wallet_tran_id = pwt.id AND pcwtr.brand_etl = pwt.brand_etl 
LEFT JOIN card_wallet cw ON pcwtr.card_id = cw.id AND pcwtr.brand_etl = cw.brand_etl 
LEFT JOIN card_wallet_transaction cwt ON cw.id = cwt.card_id AND cw.brand_etl = cwt.brand_etl AND cwt.pwt_id = pwt.id
LEFT JOIN card_wallet_value cwv ON cw.card_value_id = cwv.id AND cw.brand_etl = cwv.brand_etl 
LEFT JOIN crm_line cline ON pwt.line_id = cline.id AND pwt.brand_etl = cline.brand_etl 
LEFT JOIN view_stag_products sp ON cline.product_id = sp.product_id AND cline.brand_etl = sp.brand_etl 
LEFT JOIN view_stag_products spc ON cwv.product_id = spc.product_id AND cwv.brand_etl = spc.brand_etl 
WHERE pwt.brand_etl <> 0
'''
#dfcard = query_df(conn, qr_card)

def get_data_card_wallet_line(): 
    dfcard = query_df(conn, qr_card)
    dfc = dfcard.sort_values(['clineid','brand_etl','id'])
    fix_type(dfc, ['pwt_amount','amount','value','value_face','value_total','bonus','gift','value','amount_done','total','unit_total','rto_value'], float, np.nan)
    # Cập nhật 
    info_pd = dfc[dfc.brand_etl.eq(0)].drop_duplicates('clineid')   #.set_index('clineid')
    dfmerge_pd = dfc[['clineid']].reset_index().merge(info_pd, how='left', on='clineid').set_index('index')
    col_ud = ['card','product_id','product','total','quantity','unit_total']
    dfc.update(dfmerge_pd[col_ud], overwrite=False)
    
    dfc['actual_value'] = dfc.amount*dfc.rto_value
    gr_cline = ['clineid','brand_etl']
    dfc.insert(dfc.columns.get_loc('clineid')+1, 'rn_cline_desc', dfc.sort_values('id', ascending=False).groupby(gr_cline).cumcount().add(1))
    dfc.insert(dfc.columns.get_loc('pwt_amount')+1, 'pwt_amount_cum', dfc.groupby(gr_cline)['pwt_amount'].cumsum())
    dfc.insert(dfc.columns.get_loc('amount')+2, 'pwt_amount_last', 
               dfc.groupby(gr_cline)['pwt_amount_cum'].transform(lambda r: r.where(dfc.rn_cline_desc.eq(1)).max()))
    dfc.insert(dfc.columns.get_loc('amount')+3, 'amount_cum', dfc.groupby(gr_cline)['amount'].cumsum(skipna=False))
    
    dfc.insert(dfc.columns.get_loc('amount')+3, 'actual_value_cum', dfc.fillna(0).groupby(gr_cline)['actual_value'].cumsum())
    dfc.insert(dfc.columns.get_loc('amount')+4, 'actual_value_last', 
               dfc.groupby(gr_cline)['actual_value_cum'].transform(lambda r: r.where(dfc.rn_cline_desc.eq(1)).max()))
    gr_card = ['clineid','brand_etl','card_id']
    dfc.insert(dfc.columns.get_loc('amount')+3, 'amount_card_cum', dfc.groupby(gr_card)['amount'].cumsum())
    dfc.insert(dfc.columns.get_loc('actual_value_last')+1, 'actual_value_card_cum', dfc.fillna(0).groupby(gr_card)['actual_value'].cumsum())
    dfc.insert(dfc.columns.get_loc('amount_card_cum')+1, 'rn_cline_card_desc', dfc.sort_values('id', ascending=False).groupby(gr_card).cumcount().add(1))
    dfc.insert(dfc.columns.get_loc('actual_value_card_cum')+1, 'rto_value_card', dfc.amount_card_cum.fillna(0)/dfc.pwt_amount_last.replace(0,np.nan))
    dfc.insert(dfc.columns.get_loc('rto_value_card')+1, 'rto_std', dfc.rto_value*dfc.rto_value_card) 
    dfc.insert(dfc.columns.get_loc('rto_std')+1, 'rto_value_actual', 
               dfc.groupby(gr_cline)['rto_std'].transform(lambda r: r.where(dfc.rn_cline_card_desc.eq(1)).sum()))
    
    dfc['is_nt'] = dfc.pwt_type.eq('nt').astype(int)
    dfc['is_line_use'] = dfc.sort_values(['is_nt', 'id'], ascending=[True,False]).groupby(gr_cline).cumcount().add(1)
    dfc['is_line_use'] = dfc.is_line_use.eq(1)

    fix_type(dfc, [c for c in dfc.columns if check_substr(c, r'id$|^(rn_|drk|rnk)')], 'Int64', np.nan)
    dfc['id_etl'] = dfc[['pwt_id','id','brand_etl']].fillna(0).apply(lambda r: '-'.join(r.values.astype(str)), axis=1)
    #dfc[dfc.wallet_id.eq(98) & dfc.brand_etl.isin([0,1])]
    return dfc
#dfc = get_data_card_wallet_line()


def write_stag_card_wallet_crm_line_value():
    dfc = get_data_card_wallet_line()
    TABLE = 'stag_card_wallet_crm_line_value'

    if int(dfc[['id','pwt_id','brand_etl']].duplicated().max()) == 0:
        replace_table_data(dfc, TABLE, where='brand_etl <> 0', conn=conn)
    else: 
        print(warn_msg('''!!! Kiểm tra lại id Bảng: Bộ giá trị ['id', 'pwt_id', 'brand_etl'] bị trùng'''))
            
    return TABLE

#write_stag_card_wallet_crm_line_value()


'''
first_tasks = [call_stag_employees, call_stag_products, call_stag_crm_information_consultant, call_temp_crm_id_update]
second_tasks = [call_stag_crm_consult_rel, call_stag_paymnent_transfer, call_stag_crm_booking_arrival_company, call_stag_res_partner_check_dupphone]

for task in first_tasks:
    task >> second_tasks  '''


# ---------------------------------------------------------------------------
# PHỤ THUỘC GIỮA CÁC TASK
#
# Khai báo theo đúng bảng mà từng task cần đọc, thay vì bắt mọi task nhóm sau chờ mọi task nhóm trước. Nhờ vậy một task hỏng chỉ chặn đúng nhánh của nó, các nhánh còn lại vẫn chạy bình thường.
# quentk: Note thế thôi chứ hầu hết tao đều set trigger_rule='all_done' rồi nên vẫn chạy tiếp :v (bảng trước chưa cập nhật chưa Update hầu như ít ảnh hưởng, đôi khi chỉ thiếu bản ghi mới cập nhật -> ít phát sinh)
#   task_1 employees ─┬─> task_7 consult_rel      (đọc stag_employees)
#   task_5 consultant ┘
#   task_1 employees ─┬─> task_9 booking_arrival  (đọc view_stag_employees_user + stag_products)
#   task_2 products  ─┘
#   task_1 employees ───> task_10 dupphone        (đọc view_stag_employees_user)
#   task_2 products  ───> task_6 card_wallet      (đọc view_stag_products)
#   task_4 temp_crm  ───> task_8 payment_transfer (đọc temp_crm_id_update)
# ---------------------------------------------------------------------------


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'task_1_stag_employees': write_stag_employees,
    'task_2_stag_products': write_stag_products,
    'task_4_temp_crm_id_update': write_temp_crm_id_update,
    'task_5_stag_crm_information_consultant': write_stag_crm_information_consultant,
    'task_7_stag_crm_consult_rel': write_stag_crm_consult_rel,
    'task_6_stag_card_wallet_crm_line_value': write_stag_card_wallet_crm_line_value,
    'task_8_stag_paymnent_transfer': write_stag_payment_transfer,
    'task_9_stag_crm_booking_arrival_company': write_stag_crm_booking_arrival_company,
    'task_10_stag_res_partner_check_dupphone': write_stag_res_partner_check_dupphone,
}


def main():
    parser = argparse.ArgumentParser(description='ETL stag_crm')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
