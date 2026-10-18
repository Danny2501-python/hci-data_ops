# -*- coding: utf-8 -*-
"""
REPORT ACCOUNT MOVE

Chuyen tu quentk_4_report_account_move.pyw (Airflow PythonOperator) sang mo hinh
KubernetesPodOperator cua data_ops. Logic SQL va xu ly giu NGUYEN VEN,
chi thay phan khung Airflow.

DAG dieu phoi: dags/sandbox/report_account_move_dag.py
Bien moi truong: POSTGRES_URI (DB dich)

Chay thu mot task:
    POSTGRES_URI=postgresql://... python report_account_move.py --task task_report_account_move
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


qr = '''
SELECT aml.id, aml.move_id, aml.name AS move_name, aml.date, 
	COALESCE(aml."ref", '') AS "ref", aml.name aml_label, 
	am.name account_move, am.type account_move_type, COALESCE(am.lydo, '') AS lydo, am.ma_phieu_in, am.is_netoff, 
	aml.partner_id, aml.partner_code, COALESCE(rp."name", '') AS partner, 
	aml.account_id, aa.name account, aa.code, LEFT(aa.code, 3) as code_lv1, LEFT(aa.code, 4) as code_lv2, aa.internal_type, aa.internal_group, 
	ias.account_lv1, iasr.account_lv1 as account_rec_lv1, 
	COALESCE(aml.product_id, smhcs.product_id) as product_id, 
	sp.product_category, sp.product, aml.quantity, sp.uom, 
	aml.price_unit, aml.debit, aml.credit, aml.balance, aml.amount_currency,  
	COALESCE(aml.currency_id, 23) AS currency_id, rcc."name" currency, rcc.symbol currency_symbol, rcc.currency_unit_label, 
    ---aml.balance/NULLIF(aml.amount_currency,0) as exchange_rate, 
	aml.price_subtotal, aml.price_total, 
	aml.date_maturity, 
	aml.journal_id, aj."name" account_journal, aj.type as journal_type, aj.is_gia_thanh,
	aml.payment_id, ap.name account_payment, ap.payment_date, 
	aml.tax_line_id, aml.tax_base_amount, aml.tax_audit, 
	aml.purchase_line_id, aml.account_rec_id, aar.name account_rec, 
	aar.code account_rec_code, LEFT(aar.code, 3) AS account_rec_code_lv1, LEFT(aar.code, 4) AS account_rec_code_lv2, 
	aml.cost_code cost_code_id, cc.group_code group_cost_code, cc."name" cost_code, 
	aml.cost_label_id, cl.name cost_label, cl.group_label group_cost_label, 
	aj.bank_account_id, rpb.bank_id, rpb.sanitized_acc_number as acc_bank_number, TRIM(bank."name") res_bank,  
	aml.company_id, mrc.name company, mrc.brand_id, 
	rb.name brand, UPPER(rb.name) brand_upper, 
	am.nguoi_lap nguoi_lap_uid, sel.employee_code nguoi_lap_employee_code, TRIM(INITCAP(sel.employee)) nguoi_lap, sel.job nguoi_lap_job, 
	aml.create_uid, aml.create_date, sec.employee_code creator_employee_code, TRIM(INITCAP(sec.employee)) creator, sec.job creator_job, 
	aml.write_uid, TRIM(INITCAP(se.employee)) writer, se.job writer_job, aml.write_date, 
	aml.company_id AS chinhanhphanquyen_fix, mrc.brand_id AS thuonghieuphanquyen_fix, 
	aml.brand_etl
FROM account_move_line aml
LEFT JOIN account_move am ON aml.move_id = am.id AND am.brand_etl = aml.brand_etl 
LEFT JOIN account_journal aj ON aml.journal_id = aj.id AND aj.brand_etl = aml.brand_etl 
LEFT JOIN account_account aa ON aml.account_id = aa.id AND aa.brand_etl = aml.brand_etl 
LEFT JOIN account_account aar ON aml.account_rec_id = aar.id AND aar.brand_etl = aml.brand_etl
LEFT JOIN cost_code cc ON aml.cost_code = cc.id AND cc.brand_etl = aml.brand_etl 
LEFT JOIN cost_label cl ON aml.cost_label_id = cl.id AND cl.brand_etl = aml.brand_etl 
LEFT JOIN res_partner rp ON aml.partner_id = rp.id AND rp.brand_etl = aml.brand_etl 
LEFT JOIN sh_medical_health_center_service smhcs ON aml.x_service_id = smhcs.id AND smhcs.brand_etl = aml.brand_etl 
LEFT JOIN view_stag_products sp ON COALESCE(aml.product_id, smhcs.product_id) = sp.product_id AND sp.brand_etl = aml.brand_etl 
LEFT JOIN account_payment ap ON aml.payment_id = ap.id AND ap.brand_etl = aml.brand_etl 
LEFT JOIN m_res_company mrc ON aml.company_id = mrc.id AND mrc.brand_etl = aml.brand_etl 
LEFT JOIN res_brand rb ON mrc.brand_id = rb.id AND rb.brand_etl = 7 
LEFT JOIN view_stag_employees_user se ON aml.write_uid = se.user_id AND se.brand_etl = aml.brand_etl 
LEFT JOIN view_stag_employees_user sec ON aml.create_uid = sec.user_id AND sec.brand_etl = aml.brand_etl 
LEFT JOIN view_stag_employees_user sel ON am.nguoi_lap = sel.user_id AND sel.brand_etl = aml.brand_etl 
LEFT JOIN import_accounting_system ias ON LEFT(aa.code, 3)::int = ias.code_lv1 and ias.is_get_lv1 
LEFT JOIN import_accounting_system iasr ON LEFT(aar.code, 3)::int = iasr.code_lv1 and iasr.is_get_lv1 
LEFT JOIN res_currency rcc ON COALESCE(aml.currency_id, 23) = rcc.id and rcc.brand_etl = aml.brand_etl 
LEFT JOIN res_partner_bank rpb ON rpb.id = aj.bank_account_id and rpb.brand_etl = aj.brand_etl 
LEFT JOIN res_bank bank ON rpb.bank_id = bank.id and rpb.brand_etl = bank.brand_etl 
WHERE aml.brand_etl IN (1,3) AND aml.parent_state = 'posted'
    AND (aml.debit + aml.credit) > 0
    AND aml.date < now()::date
    AND (LEFT(aa.code, 3) IN ('111','112') OR LEFT(aar.code, 3) IN ('111','112'))
'''

qrr = qr    #.replace('= aml.brand_etl', '= 1')
#print(qrr)
#dfam = query_df(conn, qrr)
#dfam.head(3)


# Một số hàm xử lý thông tin Account Ngân hàng

dict_re_bank = {'Vietcombank': ['VCB|vietcom', 'Ngân hàng TMCP Ngoại Thương Việt Nam'], 
                'Vietinbank': ['viet.*in', 'Ngân hàng TMCP Công thương Việt Nam'],  
                'BIDV': ['bidv', 'Ngân hàng TMCP Đầu tư và Phát triển Việt Nam'], 
                'Techcombank': ['tech', 'Ngân hàng TMCP Kỹ Thương'], 
                'VPBank': ['vp.*bank', 'Ngân hàng TMCP Việt Nam Thịnh Vượng'], 
                'Shinhan Bank': ['shinhan', 'Ngân hàng TNHH MTV Shinhan Việt Nam'], 
                'TPBank': ['tp.*bank|tienphong', 'Ngân hàng TMCP Tiên Phong'], 
                'MBBank': ['mb', 'Ngân hàng TMCP Quân Đội'], 
                'ABBank': ['ab.*bank|an bình', 'Ngân hàng TMCP An Bình'] }

def ret_sanitized_str(ls_inp, dict_re): 
    stz = None
    fname = None
    if not isinstance(dict_re, dict):
        'Cặp giá trị chuỗi chuẩn hóa nhãn phải là dạng Dictionary'
    else:
        #inp = str(inp) 
        for inp in ls_inp: 
            for k, v in dict_re.items(): 
                if stz == None: 
                    if re.search(v[0], str(inp), re.I): 
                        stz = k
                        fname = v[-1]
                        break
        stz = stz or str(inp).strip()
    return [stz, fname]


res = '(?!(KN|PR)) (Phạm|Nguyễn|Hán|Hồ|Phùng|Đàm|Lê |Vũ |Trịnh|Trần|Công ty)(\w| )+'
re_del='Tiền gửi ngân hàng VND tại điểm|không dùng|chủ tk:* |(\[|\().+'
sep = '-|–'
def get_substr_split(inp, res='None', sep='-', pos=0, pos_repl=None, re_del=None):
    inp = str(inp)
    pname = re.search(res, inp, flags=re.I)
    if pname != None: 
        getstr = pname[0]
    else: 
        ls = re.split(sep, inp)
        pos = pos if len(ls) >= pos+1 else pos_repl
        if pos != None: 
            getstr = re.sub(str(re_del), '', ls[pos], flags=re.I)
    getstr = re.sub(r'\s+', ' ', getstr).strip() 
    return getstr

#inp = 'Tiền VND gửi ngân hàng - dfgvgb- cdcd- Hán Thị Linh Nhung - 108000317533 - PR Miền Bắc (Pos)'
#get_substr_split(inp, res=res, sep='-', pos=2, pos_repl=-1, re_del=re_del)


def get_data_aml(): 
    starttime = datetime.now()
    dfam = query_df(conn, qrr)    # Truy vấn dữ liệu account_move_line

    # Cut dữ liệu đầu tài khoản 111, 112
    df = dfam[dfam.code_lv1.isin(['111','112'])].copy() 
    fix_type(df, [c for c in dfam.columns if check_substr(c, r'amount|debit|credit|balan|quantity|price')], 'float', np.nan)

    df.date = pd.to_datetime(df.date)
    df['account_type'] = np.where(df.internal_group.isin(['asset','expense']) | (df.code_lv1.eq('911') & df.debit.gt(0)), 'debit', 'credit')
    df['balance_type'] = np.where(df.debit.gt(0), 'debit', 'credit')
    df['stats_type'] = np.where(df.ref.str.contains('số dư đầu', case=False, na=False), 'cutoff', df.account_type)

    # Xử lý bóc thông tin Tài khoản Ngân hàng
    col_bk = ['account','res_bank','account_journal']
    joinstr_bank = df[col_bk].fillna('None').apply('-'.join,axis=1)
    joinstr_bank = joinstr_bank.mask(df.journal_type.ne('bank')&df.code_lv1.ne('112'), np.nan)
    df['bank_sanitized_number'] =  df.acc_bank_number.fillna(joinstr_bank.apply(lambda r: get_fmatch(r, r'\d{5,}')))
    #df['bank_name'] = joinstr_bank.mask(df.journal_type.ne('bank')&df.code_lv1.ne('112'), np.nan).apply(lambda r: str(get_fmatch(r, r'[\w]+(.bank)|bidv|vcb')).strip())
    stanitized_bank = df[col_bk].fillna('None').apply(lambda r: ret_sanitized_str(r.tolist(), dict_re_bank), axis=1)
    df['bank_brand_name'] = stanitized_bank.str[0].replace('None|nan',None)
    df['bank_full_name'] = stanitized_bank.str[-1].replace('None|nan',None)

    df['holder_name'] = df[['account']].apply(lambda r: get_substr_split(*r, res=res, sep=sep, pos=2, pos_repl=-1, re_del=re_del), axis=1)

    # Xử lý dữ liệu Cut-off theo tài khoản, đối tượng công nợ từng Ngày
    cols_gr = ['brand_etl','account_id','company_id','partner_id','currency_id','date']        # Các fields cut-off theo 'date', date luôn ở cuối list
    cols_max = ['partner','code_lv1','code_lv2','code','account_lv1','account','internal_type','internal_group',
                'journal_id','account_journal', 'journal_type',  
                'company','brand_id','brand','write_date','chinhanhphanquyen_fix','thuonghieuphanquyen_fix']
    cols_first = ['currency', 'currency_symbol', 'currency_unit_label', 'bank_account_id', 'bank_id', 'acc_bank_number', 'res_bank', 
                  'bank_sanitized_number', 'bank_brand_name', 'bank_full_name', 'holder_name'] 
    dict_agg = {c: 'max' for c in cols_max} #+ {'credit': 'sum', 'debit': 'sum'}

    value_cumsum = ['debit','credit','amount_currency']
    cut = df[df.date.dt.date.ge(DATE_SYS) | df.stats_type.eq('cutoff')].groupby(cols_gr, as_index=False, dropna=False)\
            .agg({**{c: 'max' for c in cols_max}, **{f: 'first' for f in cols_first}, **{s: 'sum' for s in value_cumsum}})\
            .sort_values('date')

    # Xử lý lại Dư Nợ/Có lũy kế đến Ngày cut-off
    cutoff = cut.copy()
    cutoff[[e+'_cumsum' for e in value_cumsum]] = cutoff.groupby(cols_gr[:-1], dropna=False)[value_cumsum].cumsum()
    co_du = cutoff.debit_cumsum - cutoff.credit_cumsum
    #cutoff['account_type'] = np.where(co_du.gt(0), 'debit', 'credit')
    cutoff['balance_type'] = np.where(cutoff.credit.gt(0), 'credit', 'dedit')
    cutoff['stats_type'] = 'cutoff'
    cutoff['debit'] = np.where(co_du.gt(0), co_du, 0)
    cutoff['credit'] = np.where(co_du.lt(0), -co_du,0)
    cutoff['balance'] = co_du
    cutoff['amount_currency'] = cutoff.amount_currency_cumsum
    cutoff['ref'] = '[QTDL] Số dư đến ngày ' + cutoff.date.dt.strftime('%d/%-m/%Y')
    cutoff['lydo'] = cutoff.ref
    cutoff['account_move'] = 'QTDL-SD/' + cutoff.date.dt.strftime('%Y/%m%d/') + \
                            cutoff[['account_id','company_id','partner_id']].fillna(0).astype(int).astype(str).apply(lambda i: '-'.join(i), axis=1)
    cutoff[cutoff.partner_id.eq(14432)&cutoff.company_id.eq(2)&cutoff.account_id.eq(10549)].head(2)

    # Nối dữ liệu Bút toán với Cut-off 
    dfc = pd.concat([df, cutoff[cutoff.date.dt.date.ge(DATE_SYS)]])
    dfc['exchange_rate'] = dfc.balance/(dfc.amount_currency.replace(0,np.nan))
    dfc['cutoff_id'] = dfc[cols_gr[:-1]].fillna(0).astype(int).astype(str).apply('-'.join,axis=1)
    dfc.sort_values(cols_gr + ['id'], inplace=True)
    ls_col_int = [c for c in dfc.columns if check_substr(c, r'id$|bank.*number') and 'cutoff' not in c]
    fix_type(dfc, ls_col_int, 'Int64', np.nan)

    endtime = datetime.now()
    print('Xử lý Bảng Dữ liệu Account_move_line + Cut-off: ', endtime - starttime)
    return dfc

#dfc = get_data_aml()


def write_report_account_move(): 
    dfc = get_data_aml()
    dfc[['balance_vas', 'group_code_label']] = pd.NA
    
    TABLE = 'report_account_move'
    replace_table_data(dfc, TABLE, conn=conn)
    print('HOÀN THÀNH! DỮ LIỆU BÁO CÁO DÒNG TIỀN (ACCOUNT MOVE LINE)')
    return TABLE

#write_report_account_move()


# ===========================================================================
# DIEM VAO - DAG goi tung task qua tham so --task
# ===========================================================================

TASKS = {
    'task_report_account_move': write_report_account_move,
}


def main():
    parser = argparse.ArgumentParser(description='ETL report_account_move')
    parser.add_argument('--task', required=True, choices=sorted(TASKS),
                        help='Task can chay')
    args = parser.parse_args()
    logger.info(f'=== BAT DAU task: {args.task} ===')
    t0 = time.time()
    TASKS[args.task]()
    logger.success(f'=== XONG task: {args.task} ({time.time() - t0:.0f}s) ===')


if __name__ == '__main__':
    main()
