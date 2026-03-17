import psycopg2
import pandas as pd
import numpy as np
import tempfile
import os
from loguru import logger
from typing import List

class EHCSyncManager:
    def __init__(self, conn_uris: dict = None):
        self.conn_uris = conn_uris or {}

    def get_conn(self, conn_name):
        uri = self.conn_uris.get(conn_name)
        if not uri:
            logger.error(f"No connection URI found for {conn_name}. Check environment variables.")
            return None
        try:
            return psycopg2.connect(uri)
        except Exception as e:
            logger.error(f"Failed to connect to {conn_name}: {e}")
            raise e

    def query_df(self, conn_name: str, query: str, ret='df'):
        connection = self.get_conn(conn_name)
        if not connection: return pd.DataFrame() if ret == 'df' else []
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                if ret == 'df':
                    if cursor.description is None:
                        return pd.DataFrame()
                    columns = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(cursor.fetchall(), columns=columns)
                else:
                    return [desc[0] for desc in cursor.description]
        finally:
            connection.close()

    def format_value(self, value):
        if isinstance(value, (float, complex, np.floating, np.complexfloating)):
            if value.is_integer():
                return int(value)
        return value

    def process_table_1(self, conn_name: str, table, table_dich):
        connection = self.get_conn(conn_name)
        if not connection: return False
        try:
            with connection.cursor() as cursor:
                query = f"DROP TABLE IF EXISTS {table}; SELECT * INTO {table} FROM {table_dich} LIMIT 0"
                cursor.execute(query)
                connection.commit()
                return True
        except Exception as e:
            logger.error(f"Error processing table {table}: {e}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def process_table_2(self, conn_name: str, table, table_dich):
        connection = self.get_conn(conn_name)
        if not connection: return False
        try:
            with connection.cursor() as cursor:
                if table_dich == 'stag_tongquan_khachhang_sm_huonghh1':
                    query_delete = f"DELETE FROM {table_dich} a WHERE EXISTS (SELECT 1 FROM {table} b WHERE a.fk = b.fk)"
                else:
                    query_delete = f"DELETE FROM {table_dich}"
                cursor.execute(query_delete)
                cursor.execute(f"INSERT INTO {table_dich} SELECT * FROM {table};")
                cursor.execute(f"DROP TABLE IF EXISTS {table};")
                connection.commit()
                logger.info(f"Table {table_dich} processing completed successfully.")
                return True
        except Exception as e:
            logger.error(f"Error processing table {table}: {e}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def sync_table(self, query_sp, table_dich, conn_source='ehc'):
        dfc = self.query_df(conn_source, query_sp)
        if dfc.empty:
            logger.info(f"No data to sync for {table_dich}")
            return

        if table_dich == 'HongHa_doanhso_EHC':
            dfc['book'] = np.nan

        temp_table = f"{table_dich}_1xjs2qcuj"
        self.process_table_1('staging_sci', temp_table, table_dich)
        
        existing_columns = self.query_df('staging_sci', f"SELECT * FROM {table_dich} LIMIT 0", ret='column')
        insert_query = f"COPY {temp_table} ({', '.join(existing_columns)}) FROM STDIN WITH CSV HEADER DELIMITER ','"

        df = dfc.applymap(self.format_value)
        for col in df.select_dtypes(include=['float']):
            has_decimal = df[col].dropna().apply(lambda x: isinstance(x, float) and not x.is_integer()).any()
            if not has_decimal:
                df[col] = df[col].astype(pd.Int64Dtype())

        temp_dir = os.getenv('TEMP_DIR', '/tmp')
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', dir=temp_dir) as temp_file:
            result = df[existing_columns]
            result.to_csv(temp_file.name, index=False, header=True)
            f_name = temp_file.name
        
        try:
            connection = self.get_conn('staging_sci')
            with connection.cursor() as cursor:
                with open(f_name, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(insert_query, f)
                connection.commit()
            connection.close()
            self.process_table_2('staging_sci', temp_table, table_dich)
            logger.info(f"COMPLETED! Updated {table_dich}")
        finally:
            if os.path.exists(f_name):
                os.remove(f_name)

    def write_HongHa_doanhso_EHC(self):
        query = """
            select-- Doanh số bc 001-cọc
                2 as "ut",
                ti.patientrecordid as patientrecordid,
                ti.invoiceid as invoiceid,
                date(ti.invoicedate) as ngay,
                tp2.patientname as ten_bn,
                tp2.patientcode as ma_bn,
                case
                    when ti.dm_invoice_typeid = 2 then -ti.sotienphieu
                    when ti3.invoicerequestid is not null
                    and ti.dm_invoice_typeid = 1 then 0
                    else ti.sotienphieu
                end as "thanh_toan",
                tn.nguoigioithieuname as nguon
            from
                tb_invoice ti
            left join tb_patientrecord tp on
                tp.patientrecordid = ti.patientrecordid
            left join tb_patient tp2 on
                tp2.patientid = tp.patientid
            left join tb_nguoigioithieu tn on
                tn.nguoigioithieuid = tp.nguoigioithieuid
            left join tb_dm_invoice_hinhthuc tdih on
                tdih.dm_invoice_hinhthucid = ti.dm_invoice_hinhthucid
            left join tb_invoicerequest ti3 on
                ti3.invoiceid = ti.invoiceid
            where
                ti.huyphieu_status = 0
                and ti.invoicegroupcode <> 'so_thu_tien_bm'
                and tn.nguoigioithieuname is not null
            union all
            select-- doanh số nhà thuốc 
                99 as "ut",
                ti.patientrecordid as patientrecordid,
                ti.invoicemedicineid as invoiceid,
                date(ti.invoicemedicinedate) as ngay,
                tp2.patientname as ten_bn,
                tp2.patientcode as ma_bn,
                case
                    when ti.dm_invoice_typeid = 1 then ti.sotienphieu
                    when ti.dm_invoice_typeid = 2 then -ti.sotienphieu
                end as "Thanh_toan",
                tn.nguoigioithieuname as nguon
            from
                tb_invoicemedicine ti
            left join tb_patientrecord tp on
                tp.patientrecordid = ti.patientrecordid
            left join tb_patient tp2 on
                tp2.patientid = tp.patientid
            left join tb_nguoigioithieu tn on
                tn.nguoigioithieuid = tp.nguoigioithieuid
            where
                ti.huyphieu_status = 0
            union all
            select -- thu khác
                78 as "ut",
                0 as patientrecordid,
                tpc.phieuthuchiid as invoiceid,
                date(tpc.phieuthuchidate) as ngay,
                'tbn' as ten_bn,
                'ma_bn' as ma_bn,
                tpc.sotienphieu as "thanh_toan",
                'nguon' as nguon
            from
                tb_phieuthuchi tpc
            where
                tpc.huyphieu_status = 0
                and tpc.dm_phieuthuchi_noidungid = 107
            union all 
            select-- doanh số ksk
                56 as "ut",
                0 as patientrecordid,
                tpc.phieuthuchiid as invoiceid,
                date(tpc.phieuthuchidate) as ngay,
                'tbn' as ten_bn,
                'ma_bn' as ma_bn,
                tpc.sotienphieu as "thanh_toan",
                'nguon' as nguon
            from
                tb_phieuthuchi tpc
            where
                tpc.huyphieu_status = 0
                and tpc.dm_phieuthuchi_noidungid = 34
            union all 
            select-- doanh số ksk
                56 as "ut",
                ti.patientrecordid as patientrecordid,
                ti.invoiceid as invoiceid,
                date(ti.invoicedate) as ngay,
                tp2.patientname as ten_bn,
                tp2.patientcode as ma_bn,
                case
                    when ti.dm_invoice_typeid = 2 then -ti.sotienphieu
                    when ti3.invoicerequestid is not null
                    and ti.dm_invoice_typeid = 1 then 0
                    else ti.sotienphieu
                end as "thanh_toan",
                tn.nguoigioithieuname as nguon
            from
                tb_invoice ti
            left join tb_patientrecord tp on
                tp.patientrecordid = ti.patientrecordid
            left join tb_patient tp2 on
                tp2.patientid = tp.patientid
            left join tb_nguoigioithieu tn on
                tn.nguoigioithieuid = tp.nguoigioithieuid
            left join tb_dm_invoice_hinhthuc tdih on
                tdih.dm_invoice_hinhthucid = ti.dm_invoice_hinhthucid
            left join tb_invoicerequest ti3 on
                ti3.invoiceid = ti.invoiceid
            where
                ti.huyphieu_status = 0
                and ti.invoicegroupcode <> 'so_thu_tien_bm'
                and tn.nguoigioithieuname is null
        """
        self.sync_table(query, 'HongHa_doanhso_EHC')

    def write_stag_thuchien_doanhso_hongha(self):
        query = """
         	with a as (select 
        'so_thu_thuc_hien' as so_thu,
        2 as bang,
            ts.servicedataid as id_phieu_thuc_hien,
            tp.patientrecordid, 
            ts.servicecode, 
            tds.dm_servicesubgroupname, 
            tr.roomname as phong,
            ts.servicename, 
            ts.tongbenhnhan as tien_thuc_hien,
            tp.patientcode,
            tp.patientname,
            ts.servicedatausedate as ngay,
            tp.id_booking as booking,
            tn.nguoigioithieuname nhom_nguon,
            case when ts.servicecode like '%XN%' then tr.roomname 
            when ts.servicecode like '%SA%' then tr.roomname 
            when ts.servicecode like '%GIUONG%' then tr.roomname 
            when ts.servicecode like '%VTBV%' then tr.roomname 
            when ts.servicecode like '%THBV%' then tr.roomname 
            else servicecode end as nhom_dv_he_thong
            from  tb_servicedata ts
            left join tb_patientrecord tp on tp.patientrecordid = ts.patientrecordid 
            left join tb_dm_servicesubgroup tds on tds.dm_servicesubgroupid = ts.dm_servicesubgroupid 
            left join tb_nguoigioithieu tn on tn.nguoigioithieuid = tp.nguoigioithieuid 
            left join tb_dm_hoahongnguoigioithieu_nguoigioithieu tdhn on tdhn.dm_hoahongnguoigioithieu_nguoigioithieuid = tn.dm_hoahongnguoigioithieu_nguoigioithieuid
            left join tb_room tr on tr.roomid = ts.roomid 
            where  tn.nguoigioithieuname  
            in(
            'CTV_Sale & MKT',
            'Data_Khai thác chéo thương hiệu',
            'Data_Mua ngoài',
            'MKT_Banner',
            'MKT_Coccoc',
            'MKT_Facebook',
            'MKT_Facebook_Lead',
            'MKT_GoogleDisplay',
            'MKT_GoogleSearch',
            'MKT_Hợp tác doanh nghiệp',
            'MKT_Hotline',
            'MKT_ IVF BẠCH MAI_Facebook',
            'MKT_KH giới thiệu KH',
            'MKT_KH thành công quay lại',
            'MKT_Phát triển cộng đồng (Social)',
            'MKT_Tái khai thác',
            'MKT_TikTok/Viber/livechat',
            'MKT_Voucher/Event/Trade',
            'MKT_Web/Seo',
            'MKT_Youtube',
            'MKT_Zalo',
            'Vãng lai_Nhận diện thương hiệu'
            ,'MO - Trade MKT'
        ,'ML - KH cũ quay lại (loyalty)'
        ,'ML - CTV Khách hàng'
        ,'MD - Zalo/Viber'
        ,'MD - Youtube'
        ,'MD - Tiktok'
        ,'MD - Search'
        ,'MD - Phát triển cộng đồng'
        ,'MD - Hotline'
        ,'MD - Facebook'
        ,'MD - Display'
        ,'CTV_Sale & MKT'
        ,'CTV_Sale & Marketing'
        ,'CTV_Chi nhánh'
        ,'CTV_Bên ngoài (Sale&MKT)'
        ) and ts.tongbenhnhan <> 0 )
        
            select *,
            case when a.phong = 'Phòng khám LGBT' then 'LGBT'
            when a.phong = 'Điều trị ngoại trú [Khoa Phụ sản]' then 'VSHM'
            when a.phong = 'Điều trị Sản' then 'VSHM'
            when a.phong = 'Phòng khám nam học' then 'VSHM'
            when a.phong = 'Phòng khám Phẫu thuật thẩm mỹ' then a.servicename 
            when a.phong = 'Phòng khám Sản [213]' then 'VSHM'
            when a.phong = 'Lễ tân' then a.servicename
            else a.phong
            end as dvhh
            from a 
            union all 
            select ti.invoicegroupcode as so_thu,
            1 as bang,
            ti.invoiceid as invoiceid, 
            ti.patientrecordid as patientrecordid,
            'DVHHcode' as servicecode,
            'Nhom_DV' as dm_subgroup,
            tr.roomname as phong,
            'DVHH' as servicename,
            case when ti.dm_invoice_typeid = 1 then ti.sotienphieu 
            when ti.dm_invoice_typeid = 2 then -ti.sotienphieu end as "Thanh_toan",
            tp.patientcode,
            tp.patientname,
            ti.invoicedate as ngay,
            tp.id_booking as booking,
            tn.nguoigioithieuname nhom_nguon,
            'Nhom_dv_HH' as nhom_dv_hh,
            'dvhh' as dvhh
            from tb_invoice ti 
            left join tb_patientrecord tp on tp.patientrecordid = ti.patientrecordid 
            left join tb_patient tp2 on tp2.patientid= tp.patientid 
            left join tb_nguoigioithieu tn on tn.nguoigioithieuid = tp.nguoigioithieuid
            left join tb_dm_hoahongnguoigioithieu_nguoigioithieu tdhn on tdhn.dm_hoahongnguoigioithieu_nguoigioithieuid = tn.dm_hoahongnguoigioithieu_nguoigioithieuid 
            left join tb_room tr on tr.roomid = ti.roomid 
            left join tb_invoicerequest ti2 on ti2.invoiceid = ti.invoiceid 
            where ti.huyphieu_status = 0  
            and  tn.nguoigioithieuname  
            in(
        'CTV_Sale & MKT',
            'Data_Khai thác chéo thương hiệu',
            'Data_Mua ngoài',
            'MKT_Banner',
            'MKT_Coccoc',
            'MKT_Facebook',
            'MKT_Facebook_Lead',
            'MKT_GoogleDisplay',
            'MKT_GoogleSearch',
            'MKT_Hợp tác doanh nghiệp',
            'MKT_Hotline',
            'MKT_ IVF BẠCH MAI_Facebook',
            'MKT_KH giới thiệu KH',
            'MKT_KH thành công quay lại',
            'MKT_Phát triển cộng đồng (Social)',
            'MKT_Tái khai thác',
            'MKT_TikTok/Viber/livechat',
            'MKT_Voucher/Event/Trade',
            'MKT_Web/Seo',
            'MKT_Youtube',
            'MKT_Zalo',
            'Vãng lai_Nhận diện thương hiệu'
            ,'MO - Trade MKT'
        ,'ML - KH cũ quay lại (loyalty)'
        ,'ML - CTV Khách hàng'
        ,'MD - Zalo/Viber'
        ,'MD - Youtube'
        ,'MD - Tiktok'
        ,'MD - Search'
        ,'MD - Phát triển cộng đồng'
        ,'MD - Hotline'
        ,'MD - Facebook'
        ,'MD - Display'
        ,'CTV_Sale & MKT'
        ,'CTV_Sale & Marketing'
        ,'CTV_Chi nhánh'
        ,'CTV_Bên ngoài (Sale&MKT)'
        ) and ti2.invoicerequestid is null 
            union all 
        select 
        'so_thu_thuoc' as so_thu,
        3 as bang,
        ti.invoicemedicineid as invoiceid,
        ti.patientrecordid as patientrecordid, 
        'DVHHcode' as servicecode,
        'Nhom_DV' as dm_subgroup,
        tr.roomname as phong,
        'DVHH' as servicename,
        case when ti.dm_invoice_typeid = 1 then ti.sotienphieu 
        when ti.dm_invoice_typeid = 2 then -ti.sotienphieu end as "Thanh_toan",
        tp.patientcode,
        tp.patientname,
        date(ti.invoicemedicinedate) as ngay,
        tp.id_booking as booking,
        tn.nguoigioithieuname nhom_nguon,
        'Nhom_dv_HH' as nhom_dv_hh,
        'dvhh' as dvhh
        from tb_invoicemedicine ti 
        left join tb_patientrecord tp on tp.patientrecordid = ti.patientrecordid 
        left join tb_patient tp2 on tp2.patientid= tp.patientid 
        left join tb_nguoigioithieu tn on tn.nguoigioithieuid = tp.nguoigioithieuid 
        left join tb_dm_hoahongnguoigioithieu_nguoigioithieu tdhn on tdhn.dm_hoahongnguoigioithieu_nguoigioithieuid = tn.dm_hoahongnguoigioithieu_nguoigioithieuid 
        left join tb_room tr on tr.roomid = ti.roomid 
        where tn.nguoigioithieuname  
            in(
            'CTV_Sale & MKT',
            'Data_Khai thác chéo thương hiệu',
            'Data_Mua ngoài',
            'MKT_Banner',
            'MKT_Coccoc',
            'MKT_Facebook',
            'MKT_Facebook_Lead',
            'MKT_GoogleDisplay',
            'MKT_GoogleSearch',
            'MKT_Hợp tác doanh nghiệp',
            'MKT_Hotline',
            'MKT_ IVF BẠCH MAI_Facebook',
            'MKT_KH giới thiệu KH',
            'MKT_KH thành công quay lại',
            'MKT_Phát triển cộng đồng (Social)',
            'MKT_Tái khai thác',
            'MKT_TikTok/Viber/livechat',
            'MKT_Voucher/Event/Trade',
            'MKT_Web/Seo',
            'MKT_Youtube',
            'MKT_Zalo',
            'Vãng lai_Nhận diện thương hiệu'
            ,'MO - Trade MKT'
        ,'ML - KH cũ quay lại (loyalty)'
        ,'ML - CTV Khách hàng'
        ,'MD - Zalo/Viber'
        ,'MD - Youtube'
        ,'MD - Tiktok'
        ,'MD - Search'
        ,'MD - Phát triển cộng đồng'
        ,'MD - Hotline'
        ,'MD - Facebook'
        ,'MD - Display'
        ,'CTV_Sale & MKT'
        ,'CTV_Sale & Marketing'
        ,'CTV_Chi nhánh'
        ,'CTV_Bên ngoài (Sale&MKT)'
        )
        """
        self.sync_table(query, 'stag_thuchien_doanhso_hongha')

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Sync EHC Data')
    parser.add_argument('--task', type=str, required=True, help='Task to run')
    args = parser.parse_args()

    conn_uris = {
        'staging_sci': os.getenv('STAGING_SCI_URI'),
        'ehc': os.getenv('EHC_URI')
    }

    manager = EHCSyncManager(conn_uris=conn_uris)
    
    if args.task == 'write_HongHa_doanhso_EHC':
        manager.write_HongHa_doanhso_EHC()
    elif args.task == 'write_stag_thuchien_doanhso_hongha':
        manager.write_stag_thuchien_doanhso_hongha()
    else:
        logger.error(f"Unknown task: {args.task}")
