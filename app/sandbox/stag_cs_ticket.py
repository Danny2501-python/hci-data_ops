# -*- coding: utf-8 -*-
"""
STAG CS TICKET

Chuyen tu quentk_3_cs_ticket.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/stag_cs_ticket_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python stag_cs_ticket.py --task stag_cs_ticket
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

DATE_SYS = dt.date(2026,6,1)            # Ngày chặn Cập nhật dữ liệu toàn hệ thống
DATE_SYS_STR = DATE_SYS.strftime("%Y-%m-%d")

LS_COL_STR = ['id', 'team_service_id', 'order_product_id', 'form_id', 'lag_form_id', 'aggroleid']


conn = 'dev'   # Dev trên Jupyterlab -> kết nối với Sandbox, đồng bộ lên Airflow -> Tự động connect "new_product"
_CACHE = {}          # nhớ kết quả trong một tiến trình task, tránh truy vấn lặp

DATETRUNC = (dt.date.today() - relativedelta(months=1)).replace(day=1)
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

CRMID = ['crm_id', 'brand_etl']
PCLINEID = ['clineid', 'product_id', 'brand_etl']
PARTNERID = ['partner_id', 'brand_etl']
CLINEID = ['brand_etl', 'clineid']


# Tạo bảng tạm danh sách các ticket_id được Update lại

DTK = 10   # Số ngày giới hạn các Ticket tạo gần nhất
DTK_DATE = dt.date.today() - relativedelta(days=DTK)
DTK_DATE_STR= DTK_DATE.strftime("%Y-%m-%d")

TABLE_TEMP = 'temp_update_stag_cs_ticket'
qr_ud = f'''\
DROP TABLE IF EXISTS {TABLE_TEMP} ;
CREATE TABLE {TABLE_TEMP} AS
SELECT DISTINCT ON (cta.ticket_id) cta.ticket_id,  
	cta.requester_id, 
    SUBSTRING(cta.ad_id_value, '\d+') AS fb_ad_ad_id, 
    cta.updated_at, 
    cl.create_date AS crm_create_date
FROM cs_ticket_api cta
LEFT JOIN crm_lead cl ON cta.ticket_id = cl.ticket_id 
WHERE LEAST(cta.updated_at, cl.create_date) >= (CURRENT_DATE - {DTK}) ; 
ANALYZE {TABLE_TEMP};
'''

qr = f'''\
WITH 
filtered_cta AS (
    -- Bước 1: Driving table - Chỉ lấy thông tin gốc của các ticket mục tiêu.
    SELECT cta.*, --substring(cta.ad_id_value, '\d+') AS fb_ad_ad_id, 
    	(cta.requester::json)->> 'phone_no' AS phone_no, 
        DENSE_RANK() OVER (PARTITION BY cta.requester_id, cta.brand_id ORDER BY cta.ticket_id) AS drk_requester_ticket
    FROM cs_ticket_api cta
    INNER JOIN (SELECT DISTINCT requester_id FROM {TABLE_TEMP}) t ON cta.requester_id = t.requester_id
), 
caa AS 
(   --- Thông tin agent lọc trùng email, nhóm Sales CS => Chuyển comment do dùng sang JOIN LATERAL
    SELECT email, group_name, MAX(employee_code)::varchar AS employee_code 
    FROM cs_agents_api_mapping
    GROUP BY email, group_name
)/*, -- Đưa thành comment để chuyển sang dùng JOIN LATERAL
fix_requester AS (
SELECT cta.ticket_id, c.id, c.phone_no, c.created_at
FROM filtered_cta cta
LEFT JOIN (
	SELECT distinct on (phone_no) phone_no, id, created_at
	FROM cs_contact_api cca ORDER BY phone_no, cca.created_at 
	) c ON cta.phone_no = c.phone_no   
)*/
SELECT 
    cta.ticket_id, 
    cta.ticket_no,
    cl.id AS crm_id, 
    cl.type_crm_id, 
    DENSE_RANK() OVER (PARTITION BY cta.ticket_id, cl."type" ORDER BY cl.id) AS drk_ticket_crm,
    cta.ticket_status, 
    cta.ticket_source, 
    cta.ticket_subject, 
    COALESCE(cta.requester_id, chat.customer_id) AS org_requester_id,
    COALESCE(COALESCE(cta.requester_id, chat.customer_id), ccaf.id) AS requester_id,
    cta.drk_requester_ticket, 
    COALESCE(cta.service_id, chat.cca_service_id) AS service_id, 
    csa.service_name,
    csa.service_type, 
    cta.campaign AS js_campaign, 
    cta.tags, 
    --cta.requester, 
    src.ticket_source_detail_id, 
    ctsda.source_name AS ticket_source_detail, 
    ctsda.ad_id, 
    limit_tk.fb_ad_ad_id, 
    NULL AS ad_name, 
    ctsda.source_key, 
    ctsda.utm_source, 
    LOWER(ctsda.utm_medium) AS utm_medium, 
    ctsda.utm_campaign AS ctsda_utm_campaign, 
    ctsda.utm_content, 
    --cfac.fb_ad_campaign_id, 
    --cfac."name" AS fb_ad_campaign, 
    chat.cca_agent_name, 
    caa.employee_code AS agent_employee_code,
    COALESCE(INITCAP(se.employee), chat.cca_agent_name) AS agent_name, 
    se.job AS agent_job, 
    se.group_job AS agent_group_job, 
    se.department AS agent_department,  
    -- Chat info columns từ LATERAL JOIN
    chat.*,
    COALESCE(chat.cca_cus_phone, ccaf.phone_no) AS cus_phone, 
    COALESCE(cca.created_at, ccaf.created_at) AS contact_created_at, 
    -- Deal info columns từ LATERAL JOIN
    d.subject AS deal_subject, d.convert_by, d.convert_at, d.convert_type, d.closed_by, d.closed_at, d.custom_fields, d.comments, 
    se_cv.employee_code AS convert_agent_employee_code, 
    COALESCE(INITCAP(se_cv.employee), caa_cv.username) AS convert_agent,
    se_cv.job AS convert_agent_job, 
    caa_cv.group_name AS convert_group_name, 
    cta.created_at, 
    cta.updated_at, 
    COALESCE(cta.brand_id, chat.cca_brand_id) AS brand_id
FROM filtered_cta cta
INNER JOIN {TABLE_TEMP} limit_tk ON cta.ticket_id = limit_tk.ticket_id 
-- LATERAL JOIN: cho phép DB dừng quét ngay khi tìm thấy dòng đầu tiên (Top 1) nếu có index phù hợp.
LEFT JOIN LATERAL (
    SELECT 
        cca.ticket_id AS cca_ticket_id, 
        cca.customer_id, 
        cca.agent_name AS cca_agent_name, 
        cca.group_name, 
        cca.agent_email, 
        cca.cus_name, 
        cca.cus_phone AS cca_cus_phone, 
        cca.start_time, 
        cca.end_time, 
        cca.meet_time, 
        cca.waittime, 
        cca.answer_time, 
        LOWER(REPLACE(cca.chat_status, 'LBL_CHAT_STATUS_', '')) AS chat_status, 
        cca.facebook_page_id, 
        cca.service_id AS cca_service_id, 
        cca.brand_id AS cca_brand_id
    FROM cs_chat_api cca
    WHERE cca.ticket_id = cta.ticket_id
    ORDER BY cca.start_time ASC
    LIMIT 1
) chat ON TRUE
-- 2. LATERAL JOIN cho Source Detail (Top 1 theo created_at)
LEFT JOIN LATERAL (
    SELECT ctsa.ticket_source_detail_id, ctsa.feedback_status
    FROM cs_ticket_source_api ctsa
    WHERE ctsa.ticket_id = cta.ticket_id AND ctsa.ticket_source_detail_id IS NOT NULL
    ORDER BY ctsa.created_at ASC
    LIMIT 1
) src ON TRUE
-- 3. LATERAL JOIN cho contact đã merge (Không tồn tại requester_id trên cs_contact_api, check theo SĐT)
LEFT JOIN LATERAL ( 
    SELECT ccaf.id, ccaf.phone_no, ccaf.created_at
    FROM cs_contact_api ccaf 
    WHERE cta.phone_no = ccaf.phone_no 
    ORDER BY ccaf.created_at ASC 
    LIMIT 1 
) ccaf ON TRUE 
-- 5. Các Join cố định khác
LEFT JOIN cs_deal_detail_api_v2 d ON d.id = cta.ticket_id
LEFT JOIN cs_services_api csa ON cta.service_id = csa.service_id
LEFT JOIN crm_lead cl ON cta.ticket_id = cl.ticket_id
LEFT JOIN cs_ticket_source_detail_api ctsda ON src.ticket_source_detail_id::int = ctsda.id
LEFT JOIN cs_contact_api cca ON cta.requester_id = cca.id
LEFT JOIN caa ON chat.agent_email = caa.email AND chat.group_name = caa.group_name
LEFT JOIN stag_employees se ON caa.employee_code = se.employee_code
LEFT JOIN cs_agents_api_mapping caa_cv ON d.convert_by::int = caa_cv.id::int
LEFT JOIN stag_employees se_cv ON caa_cv.employee_code = se_cv.employee_code
/*LEFT JOIN fix_requester ccaf ON cta.ticket_id = ccaf.ticket_id
LEFT JOIN core_facebook_ad_ad cfaa on cfaa.fb_ad_ad_id = cta.fb_ad_ad_id
LEFT JOIN core_facebook_ad_set cfas on cfas.fb_ad_set_id = cfaa.fb_ad_set_id
LEFT JOIN core_facebook_ad_campaign cfac on cfac.fb_ad_campaign_id = cfas.fb_ad_campaign_id
WHERE cta.fb_ad_ad_id is not null  */
'''

#cur_dev.execute(qr_ud)
#cur_dev.connection.commit()
#dftk = query_df(conn, qr)
#dftk['rn_ticket'] = dftk.sort_values('crm_id').groupby('ticket_id')['ticket_id'].cumcount() + 1


# Danh mục Ad quảng cáo, tách dịch vụ trong utm_campaign 

QR_AD = '''\
WITH cp AS 
(
SELECT DISTINCT ON (i.ad_id) ad_id AS fb_ad_ad_id, 
	i.ad_name, i.adset_id AS ad_set_id, i.adset_name AS ad_set, 
	i.ad_campaign_id AS campaign_id, i.ad_campaign_name AS campaign_name
FROM cs_facebook_insight i 
)
SELECT * 
FROM cp 
UNION ALL
SELECT cfaa.fb_ad_ad_id, cfaa.name AS ad_name, 
	cfaa.fb_ad_set_id AS ad_set_id, cfas."name" AS ad_set,
	cfas.fb_ad_campaign_id AS campaign_id, cfac."name" AS campaign_name
FROM core_facebook_ad_ad cfaa 
LEFT JOIN core_facebook_ad_set cfas on cfas.fb_ad_set_id = cfaa.fb_ad_set_id
LEFT JOIN core_facebook_ad_campaign cfac on cfac.fb_ad_campaign_id = cfas.fb_ad_campaign_id 
LEFT JOIN cp ON cfaa.fb_ad_ad_id = cp.fb_ad_ad_id 
WHERE cp.fb_ad_ad_id IS NULL  \
'''

DICT_STR_ENC = {
    '(nang|ca|sua|ngay vang|chuyen khoa|quan tam) mui|^mui_|_mui_|mui.*cau truc': 'mui', 
    '(cat|bam|sup).*mi|ngay vang mat': 'mat',
    'cang (chi|da)': 'cchi', 
    'well.?ness': 'wellness', 
    'weight.*loss|mo noi tang': 'weightloss', 
    '(giam|hut).*mo|nanofat|giam beo': 'hutmoth', 
    'cay mo': 'caymo', 
    'hut tuyen|hoi nach': 'tuyen', 
    'seo.*lom': 'seolom', 
    'seo.*loi': 'seoloi', 
    'botox|fille': 'botox', 
     '(sac.*to|tri.*nam|tan.*nha)': 'sacto', 
    '(ham|got) mat|ham (mom|ho)|cam lem|pthm|ha go ma|don cam|_ham_': 'hammat', 
    'nguc|nang.*mong|sa tre|vong 1': 'ngucmong', 
    '(tm|tham my|tao hinh|tao|thu mong) moi|moi.*tim': 'moi', 
    'phun (xam|may|moi|mi)': 'pxam', 
    'tre hoa.*(tiem|radies|meso)|profhilo': 'thtiem', 
    'therma': 'trehoacnc', 
    'cham soc tre h': 'csth', 
    'tre hoa|mun thit': 'thda',  
    'tri mun': 'mun', 
    'rang su': 'su', 
    'nieng|nrk': 'nieng', 
    'impla.t': 'implant', 
    '(nho|tay trang| ) rang|^rang( |_)|nha khoa|rang khon': 'nkkhac', 
    'thuong\s*hieu|event|su kien|khai truong|sinh nhat|reel|htlx|mat (\+ )*mui|tgpn|sale tong|minigame|hoi thao|\
    liet mat|tai khai thac|cham soc|data|_(ta|ct|kn|inbox|view|nam|richar|like page)_': 'th'
}


def encode_str(inp, DICT_STR_ENC, pos=2):
    inp = remove_accent(inp, keep_space=True)                                    # Format sang chữ không in hoa + bỏ dấu tiếng Việt
    utmservice = inp.split('_')[pos] if len(inp.split('_')) > pos else False     # Tách chuỗi bởi "_" ở ký tự thứ pos
    if DICT_STR_ENC:
        if utmservice in DICT_STR_ENC.values(): 
            enc = utmservice       # Nếu giá trị sau tách chuỗi đã khớp với Mã hóa có sẵn => trả giá trị
            return enc
        for v, enc in DICT_STR_ENC.items(): 
            if re.search(v, inp):  # Trả Mã hóa bằng dictionary khớp từ khóa
                return enc
                break
    else: 
        print('Không tồn tại Dictionary "Từ khóa - Mã hóa"')

#inp = 'HAI NGOAI_Khuôn mặt (mat + mui)_Inbox _09/04'
#print(encode_str(inp, DICT_STR_ENC))

def get_df_ad(): 
    df_ad = query_df(conn, QR_AD)
    for i, name in {'campaign_id': 'campaign_name', 'ad_set_id': 'ad_set'}.items():
        df_ad[name] = df_ad.groupby(i)[name].transform('first')           # Điền null campaign_name nhómn theo id
    df_ad['utm_service'] = df_ad.campaign_name.apply(lambda cp: encode_str(cp, DICT_STR_ENC))
    return df_ad
#df_ad = get_df_ad()


# XỬ LÝ JSON, HÀM TÁCH id - Giá trị GIỚI HẠN TRONG CÁC ID CẦN TÁCH
def json_limit(js, lsid, dict_id_field):
    result = {dict_id_field[item['id']]: item['value'] for item in js if item.get('id') in lsid}
    return result


def get_json_value(js, key_in_value, key='name', remove_text=''): 
    if js: 
        lsvalue = [item[key] for item in js if key_in_value in item[key]]
        if lsvalue: 
            v = lsvalue[0].replace(key_in_value, '')
            v = re.sub(remove_text, '', v).strip()
            return v

# Hàm xử lý đồng bộ khớp lại đuôi email
dict_repl = {'scigroupvn.com': 'scigroup.com.vn'}
def replstr(str_ip, dict_repl):
    str_ip = str(str_ip.lower())
    for v, r in dict_repl.items():
        str_ip = str_ip.replace(v, r)
    str_ip = str_ip + '.vn' if str_ip[-10:] == 'hongha.com' else str_ip
    return str_ip

def get_stag_cs_ticket(): 
    # Bảng tạm các ticket_id cần Update
    # Danh mục Gán tên field theo các id json
    ccfl = query_df(conn, 'SELECT * FROM cs_custom_fields_label WHERE field IS NOT NULL')
    dict_id_field = dict(zip(ccfl.id, ccfl.field))
    
    # Danh mục giá trị (value_label) theo từng id custom_field
    df_custom_fields = query_df(conn, 'SELECT * FROM cs_custom_fields')
    df_custom_fields['value'] = df_custom_fields.value_lable.apply(lambda v: str(v).split(']')[-1].strip())
    df_custom_fields['code'] = df_custom_fields.value_lable.str.extract(r'\[([\w.]+)\]')
    
    # Danh mục Agent - Employee
    df_agent = query_df(conn, '''\
    SELECT se.work_email, INITCAP(se.employee) AS name, se.job, se.group_job, se.department, ag.* 
    FROM cs_agents_api_mapping ag 
    LEFT JOIN stag_employees se ON ag.employee_code = se.employee_code''')

    # Danh mục Ads quảng cáo
    df_ad = get_df_ad()

    print('1. Done! Tạo bảng tạm, lấy danh mục các field thông tin') 
          
    # DỮ LIỆU CS_TICKET
    dftk = query_df(conn, qr)
    dftk['rn_ticket'] = dftk.sort_values('crm_id').groupby('ticket_id')['ticket_id'].cumcount() + 1
    #dftk['drk_requester_ticket'] = dftk.sort_values('ticket_id').groupby('requester_id')['ticket_id'].transform('dense')  # Đã tính trong query SQL
    # Cut dữ liệu ticket tạo mới trong biến "dtk" ngày
    df = dftk[dftk.created_at.dt.date.ge(DTK_DATE)].copy()
    print('2. Done! Truy vấn bảng cs_ticket các bản ghi mới cập nhật trong N ngày') 

    # Lọc các bản ghi có giá trị custom_fields để xử lý    
    dfjs = df.query('custom_fields.notnull()', engine='python').copy()
    
    lsid = [9793,9794,9799]    # Các id cần tách trong json
    # Mã hóa lại tiếng Việt chuẩn và chuyển text thành list 
    dfjs['js'] = dfjs.custom_fields.apply(json.loads)
    # giới hạn các id cần tách trong json, tránh bung toàn bộ custom_fields làm chậm hiệu suất xử lý
    dfjs['js_cut'] = dfjs['js'].apply(lambda r: json_limit(r, lsid, dict_id_field))
    print('3.1. Done! Cut chỉ lấy các cặp id-value custom_fields cần tách') 

    # Tạo DataFrame tách các cặp Tên field: giá trị trong cột js_cut
    jsc = pd.DataFrame(dfjs.js_cut.to_list(), index=dfjs.index).reset_index()
    # Lấy giá trị trong bảng cs_custom_fields
    for i in lsid: 
        column_name = dict_id_field[i]
        ccf = df_custom_fields[df_custom_fields.custom_field_id.eq(str(i))][['value_id', 'value', 'code']].copy()
        ccf.rename(columns={'value_id': column_name, 
                            'value': column_name.replace('_id', ''), 
                            'code': column_name.replace('_id', '')+'_code'}
                   , inplace=True)
        #jsc['custom_field_id'] = jsc[dict_id_field[i]]
        jsc = jsc.merge(ccf, how = 'left', on = column_name)
        print(f''' - Tách field json (id = {i}: )''', str(ccfl[ccfl.id.eq(i)][['label']].iloc[0,0])) 
    jsc = jsc.set_index('index')  

    # Kết quả: Gán trả giá trị các id - value json vào DataFrame ban đầu
    col_add = jsc.columns
    df[col_add] = jsc[col_add]
    print('3.2. Done! Hoàn thành tách Json') 

    # XỬ LÝ CÁC AGENT NULL
    df_ag = df.query('agent_email.notnull() & agent_employee_code.isnull()', engine='python')\
            [['ticket_id', 'agent_employee_code', 'agent_name', 'group_name', 'agent_email', 'agent_job', 'agent_group_job', 'agent_department']].copy()
    df_ag['email'] = df_ag.agent_email.apply(lambda e: replstr(e, dict_repl))
    
    ca = df_agent.copy()  
    ca['email'] = ca.email.apply(lambda e: replstr(e, dict_repl))
    ca.drop_duplicates(['email'], inplace=True)
    
    df_ag = df_ag.reset_index().merge(ca, how='left', on='email', suffixes=('', '_ca'))  #.set_index('index')
    ls_col_repl = ['employee_code', 'name', 'job', 'group_job', 'department']
    for c in ls_col_repl: 
        df_ag['agent_' + c] = df_ag[c].fillna(df_ag['agent_' + c]) 
    df.update(df_ag[list('agent_' + c for c in ls_col_repl) + ['index']].set_index('index'), overwrite=True)
    print('4. Done! Update xử lý Agent null employee_code ERP') 

    # XỬ LÝ THÔNG TIN TÊN ADS, CAMPAIGN 
    # ad_name: Bóc json từ cs_ticket_api.tags => (nếu null) -> từ cs_ticket_api.ad_id_value lấy facebook_ad_name
    # campaign_name: Bóc json từ cs_ticket_api.campaign => (nếu null) -> cs_ticket_api.comments -> từ cs_ticket_api.ad_id_value lấy facebook_ad_campaign
    df['tags'] = df.tags.apply(lambda r: json.loads(r) if pd.notnull(r) else None)
    df['ad_name'] = df.tags.apply(lambda js: get_json_value(js, key_in_value='ad_name:'))
    df['tags_ad_id'] = df.tags.apply(lambda js: get_json_value(js, key_in_value='ad_id', remove_text='[\.:]'))
    df['ad_id'] = df.tags_ad_id.fillna(df.fb_ad_ad_id).fillna(df.ad_id)
    
    campaign_js = df.js_campaign.apply(lambda r: ast.literal_eval(r).get('campaign_name') if pd.notnull(r) else None)
    campaign_comments = df.comments.apply(lambda cp: get_fmatch(cp, r'campaign_name:(\w| )+')).str.replace('campaign_name:', '').str.strip()
    #df[df.campaign_comments.notnull() & df.campaign_name.ne(df.campaign_comments)]
    df['campaign_name']= campaign_js.fillna(campaign_comments)
        
    if df_ad.fb_ad_ad_id.is_unique:
        df_ad_temp = df[df.ad_id.notnull()][['ad_id']].reset_index().merge(df_ad, how='left', left_on='ad_id', right_on='fb_ad_ad_id').set_index('index')
        cols_ad_add = list(c for c in df_ad_temp.columns if c not in df.columns)
        df[cols_ad_add] = df_ad_temp[cols_ad_add]           # Chèn vào Dataframe df các cột mà bảng df chưa có (ad_set, utm_service...)
        df.update(df_ad_temp, overwrite=False)              # Update tên Campaign theo facebook_campaign cho các dòng null tên Campaign
        df['check_campaign'] = np.select([campaign_js.notnull(), campaign_comments.notnull(), df.campaign_name.notnull()], 
                                         ['cs_ticket_api.campaign', 'cs_deal_detail_api.comments', 'ad_id'], None)
        
        # Mã hóa tên dịch vụ campaign
        cc_name = df[['campaign_name','ad_name','ad_set']].fillna('').apply(lambda r: '-'.join(r.values.astype(str)), axis=1)
        df['utm_service'] = df.utm_service.fillna(cc_name.apply(lambda ad: encode_str(ad, DICT_STR_ENC)))
        print('5. Done! Xử lý tên Campaign, Set, Ad quảng cáo') 
    else: 
        print('Kiểm tra giá trị trùng "fb_ad_ad_id" bảng facebook_ad')

    df['id_etl'] = df[['ticket_id', 'crm_id']].fillna(0).astype(int).apply(lambda r: '-'.join(r.values.astype(str)), axis=1)
    if df.id_etl.is_unique: 
        print('DONE - TRẢ RA DATAFRAME THÔNG TIN TICKET SAU XỬ LÝ!')
        return df
    else: 
        print('Kiểm tra lại giá trị "id_etl" trong Data bị trùng')

#df = get_stag_cs_ticket()


def write_stag_cs_ticket(): 
    starttime = datetime.now()
    df = get_stag_cs_ticket()
    TABLE='stag_cs_ticket'

    WHERE_UD = f"created_at >= '{DTK_DATE_STR}'"
    replace_table_data(df, TABLE, where=WHERE_UD, conn=conn)
        
    endtime = datetime.now()
    print('Thời gian xử lý: ', endtime - starttime, 's')
    return TABLE

#write_stag_cs_ticket()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'stag_cs_ticket': write_stag_cs_ticket,
}


def main():
    parser = argparse.ArgumentParser(description='ETL stag_cs_ticket')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
