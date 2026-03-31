import os
import tempfile
from typing import List

import numpy as np
import pandas as pd
import psycopg2
from loguru import logger


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
        if not connection:
            return pd.DataFrame() if ret == 'df' else []
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
        if not connection:
            return False
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
        if not connection:
            return False
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

    def sync_table(self, query_sp, table_dich, conn_source='ehc', conn_dest='staging_sci'):
        dfc = self.query_df(conn_source, query_sp)
        if dfc.empty:
            logger.info(f"No data to sync for {table_dich}")
            return

        if table_dich == 'HongHa_doanhso_EHC':
            dfc['book'] = np.nan

        temp_table = f"{table_dich}_1xjs2qcuj"
        self.process_table_1(conn_dest, temp_table, table_dich)

        existing_columns = self.query_df(conn_dest, f"SELECT * FROM {table_dich} LIMIT 0", ret='column')
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
            connection = self.get_conn(conn_dest)
            with connection.cursor() as cursor:
                with open(f_name, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(insert_query, f)
                connection.commit()
            connection.close()
            self.process_table_2(conn_dest, temp_table, table_dich)
            logger.info(f"COMPLETED! Updated {table_dich}")
        finally:
            if os.path.exists(f_name):
                os.remove(f_name)

    def write_HongHa_doanhso_EHC(self):
        query = """
            SELECT-- Doanh số bc 001-cọc
                2 AS "ut",
                ti.patientrecordid AS patientrecordid,
                ti.invoiceid AS invoiceid,
                DATE(ti.invoicedate) AS ngay,
                tp2.patientname AS ten_bn,
                tp2.patientcode AS ma_bn,
                CASE
                    WHEN ti.dm_invoice_typeid = 2 THEN -ti.sotienphieu
                    WHEN ti3.invoicerequestid IS NOT NULL AND ti.dm_invoice_typeid = 1 THEN 0
                    ELSE ti.sotienphieu
                END AS "thanh_toan",
                tn.nguoigioithieuname AS nguon
            FROM tb_invoice ti
            LEFT JOIN tb_patientrecord tp ON tp.patientrecordid = ti.patientrecordid
            LEFT JOIN tb_patient tp2 ON tp2.patientid = tp.patientid
            LEFT JOIN tb_nguoigioithieu tn ON tn.nguoigioithieuid = tp.nguoigioithieuid
            LEFT JOIN tb_dm_invoice_hinhthuc tdih ON tdih.dm_invoice_hinhthucid = ti.dm_invoice_hinhthucid
            LEFT JOIN tb_invoicerequest ti3 ON ti3.invoiceid = ti.invoiceid
            WHERE ti.huyphieu_status = 0
                AND ti.invoicegroupcode <> 'so_thu_tien_bm'
                AND tn.nguoigioithieuname IS NOT NULL
            
            UNION ALL
            
            SELECT-- doanh số nhà thuốc 
                99 AS "ut",
                ti.patientrecordid AS patientrecordid,
                ti.invoicemedicineid AS invoiceid,
                DATE(ti.invoicemedicinedate) AS ngay,
                tp2.patientname AS ten_bn,
                tp2.patientcode AS ma_bn,
                CASE
                    WHEN ti.dm_invoice_typeid = 1 THEN ti.sotienphieu
                    WHEN ti.dm_invoice_typeid = 2 THEN -ti.sotienphieu
                END AS "Thanh_toan",
                tn.nguoigioithieuname AS nguon
            FROM tb_invoicemedicine ti
            LEFT JOIN tb_patientrecord tp ON tp.patientrecordid = ti.patientrecordid
            LEFT JOIN tb_patient tp2 ON tp2.patientid = tp.patientid
            LEFT JOIN tb_nguoigioithieu tn ON tn.nguoigioithieuid = tp.nguoigioithieuid
            WHERE ti.huyphieu_status = 0
            
            UNION ALL
            
            SELECT -- thu khác
                78 AS "ut",
                0 AS patientrecordid,
                tpc.phieuthuchiid AS invoiceid,
                DATE(tpc.phieuthuchidate) AS ngay,
                'tbn' AS ten_bn,
                'ma_bn' AS ma_bn,
                tpc.sotienphieu AS "thanh_toan",
                'nguon' AS nguon
            FROM tb_phieuthuchi tpc
            WHERE tpc.huyphieu_status = 0
                AND tpc.dm_phieuthuchi_noidungid = 107
            
            UNION ALL 
            
            SELECT-- doanh số ksk
                56 AS "ut",
                0 AS patientrecordid,
                tpc.phieuthuchiid AS invoiceid,
                DATE(tpc.phieuthuchidate) AS ngay,
                'tbn' AS ten_bn,
                'ma_bn' AS ma_bn,
                tpc.sotienphieu AS "thanh_toan",
                'nguon' AS nguon
            FROM tb_phieuthuchi tpc
            WHERE tpc.huyphieu_status = 0
                AND tpc.dm_phieuthuchi_noidungid = 34
            
            UNION ALL 
            
            SELECT-- doanh số ksk
                56 AS "ut",
                ti.patientrecordid AS patientrecordid,
                ti.invoiceid AS invoiceid,
                DATE(ti.invoicedate) AS ngay,
                tp2.patientname AS ten_bn,
                tp2.patientcode AS ma_bn,
                CASE
                    WHEN ti.dm_invoice_typeid = 2 THEN -ti.sotienphieu
                    WHEN ti3.invoicerequestid IS NOT NULL AND ti.dm_invoice_typeid = 1 THEN 0
                    ELSE ti.sotienphieu
                END AS "thanh_toan",
                tn.nguoigioithieuname AS nguon
            FROM tb_invoice ti
            LEFT JOIN tb_patientrecord tp ON tp.patientrecordid = ti.patientrecordid
            LEFT JOIN tb_patient tp2 ON tp2.patientid = tp.patientid
            LEFT JOIN tb_nguoigioithieu tn ON tn.nguoigioithieuid = tp.nguoigioithieuid
            LEFT JOIN tb_dm_invoice_hinhthuc tdih ON tdih.dm_invoice_hinhthucid = ti.dm_invoice_hinhthucid
            LEFT JOIN tb_invoicerequest ti3 ON ti3.invoiceid = ti.invoiceid
            WHERE ti.huyphieu_status = 0
                AND ti.invoicegroupcode <> 'so_thu_tien_bm'
                AND tn.nguoigioithieuname IS NULL
        """
        self.sync_table(query, 'HongHa_doanhso_EHC')

    def write_stag_thuchien_doanhso_hongha(self):
        query = """
            WITH a AS (
                SELECT 
                    'so_thu_thuc_hien' AS so_thu, 2 AS bang,
                    ts.servicedataid AS id_phieu_thuc_hien,
                    tp.patientrecordid, ts.servicecode, tds.dm_servicesubgroupname, 
                    tr.roomname AS phong, ts.servicename, 
                    ts.tongbenhnhan AS tien_thuc_hien,
                    tp.patientcode, tp.patientname,
                    ts.servicedatausedate AS ngay,
                    tp.id_booking AS booking,
                    tn.nguoigioithieuname nhom_nguon,
                    CASE 
                        WHEN ts.servicecode LIKE '%XN%' THEN tr.roomname 
                        WHEN ts.servicecode LIKE '%SA%' THEN tr.roomname 
                        WHEN ts.servicecode LIKE '%GIUONG%' THEN tr.roomname 
                        WHEN ts.servicecode LIKE '%VTBV%' THEN tr.roomname 
                        WHEN ts.servicecode LIKE '%THBV%' THEN tr.roomname 
                        ELSE servicecode 
                    END AS nhom_dv_he_thong
                FROM tb_servicedata ts
                LEFT JOIN tb_patientrecord tp ON tp.patientrecordid = ts.patientrecordid 
                LEFT JOIN tb_dm_servicesubgroup tds ON tds.dm_servicesubgroupid = ts.dm_servicesubgroupid 
                LEFT JOIN tb_nguoigioithieu tn ON tn.nguoigioithieuid = tp.nguoigioithieuid 
                LEFT JOIN tb_dm_hoahongnguoigioithieu_nguoigioithieu tdhn ON tdhn.dm_hoahongnguoigioithieu_nguoigioithieuid = tn.dm_hoahongnguoigioithieu_nguoigioithieuid
                LEFT JOIN tb_room tr ON tr.roomid = ts.roomid 
                WHERE tn.nguoigioithieuname IN (
                    'CTV_Sale & MKT', 'Data_Khai thác chéo thương hiệu', 'Data_Mua ngoài',
                    'MKT_Banner', 'MKT_Coccoc', 'MKT_Facebook', 'MKT_Facebook_Lead',
                    'MKT_GoogleDisplay', 'MKT_GoogleSearch', 'MKT_Hợp tác doanh nghiệp',
                    'MKT_Hotline', 'MKT_ IVF BẠCH MAI_Facebook', 'MKT_KH giới thiệu KH',
                    'MKT_KH thành công quay lại', 'MKT_Phát triển cộng đồng (Social)',
                    'MKT_Tái khai thác', 'MKT_TikTok/Viber/livechat', 'MKT_Voucher/Event/Trade',
                    'MKT_Web/Seo', 'MKT_Youtube', 'MKT_Zalo', 'Vãng lai_Nhận diện thương hiệu',
                    'MO - Trade MKT', 'ML - KH cũ quay lại (loyalty)', 'ML - CTV Khách hàng',
                    'MD - Zalo/Viber', 'MD - Youtube', 'MD - Tiktok', 'MD - Search',
                    'MD - Phát triển cộng đồng', 'MD - Hotline', 'MD - Facebook', 'MD - Display',
                    'CTV_Sale & MKT', 'CTV_Sale & Marketing', 'CTV_Chi nhánh', 'CTV_Bên ngoài (Sale&MKT)'
                ) AND ts.tongbenhnhan <> 0 
            )
            SELECT *,
                CASE 
                    WHEN a.phong = 'Phòng khám LGBT' THEN 'LGBT'
                    WHEN a.phong = 'Điều trị ngoại trú [Khoa Phụ sản]' THEN 'VSHM'
                    WHEN a.phong = 'Điều trị Sản' THEN 'VSHM'
                    WHEN a.phong = 'Phòng khám nam học' THEN 'VSHM'
                    WHEN a.phong = 'Phòng khám Phẫu thuật thẩm mỹ' THEN a.servicename 
                    WHEN a.phong = 'Phòng khám Sản [213]' THEN 'VSHM'
                    WHEN a.phong = 'Lễ tân' THEN a.servicename
                    ELSE a.phong
                END AS dvhh
            FROM a 
            
            UNION ALL 
            
            SELECT 
                ti.invoicegroupcode AS so_thu, 1 AS bang,
                ti.invoiceid AS invoiceid, ti.patientrecordid AS patientrecordid,
                'DVHHcode' AS servicecode, 'Nhom_DV' AS dm_subgroup,
                tr.roomname AS phong, 'DVHH' AS servicename,
                CASE 
                    WHEN ti.dm_invoice_typeid = 1 THEN ti.sotienphieu 
                    WHEN ti.dm_invoice_typeid = 2 THEN -ti.sotienphieu 
                END AS "Thanh_toan",
                tp.patientcode, tp.patientname,
                ti.invoicedate AS ngay,
                tp.id_booking AS booking,
                tn.nguoigioithieuname nhom_nguon,
                'Nhom_dv_HH' AS nhom_dv_hh,
                'dvhh' AS dvhh
            FROM tb_invoice ti 
            LEFT JOIN tb_patientrecord tp ON tp.patientrecordid = ti.patientrecordid 
            LEFT JOIN tb_patient tp2 ON tp2.patientid = tp.patientid 
            LEFT JOIN tb_nguoigioithieu tn ON tn.nguoigioithieuid = tp.nguoigioithieuid
            LEFT JOIN tb_dm_hoahongnguoigioithieu_nguoigioithieu tdhn ON tdhn.dm_hoahongnguoigioithieu_nguoigioithieuid = tn.dm_hoahongnguoigioithieu_nguoigioithieuid 
            LEFT JOIN tb_room tr ON tr.roomid = ti.roomid 
            LEFT JOIN tb_invoicerequest ti2 ON ti2.invoiceid = ti.invoiceid 
            WHERE ti.huyphieu_status = 0  
                AND tn.nguoigioithieuname IN (
                    'CTV_Sale & MKT', 'Data_Khai thác chéo thương hiệu', 'Data_Mua ngoài',
                    'MKT_Banner', 'MKT_Coccoc', 'MKT_Facebook', 'MKT_Facebook_Lead',
                    'MKT_GoogleDisplay', 'MKT_GoogleSearch', 'MKT_Hợp tác doanh nghiệp',
                    'MKT_Hotline', 'MKT_ IVF BẠCH MAI_Facebook', 'MKT_KH giới thiệu KH',
                    'MKT_KH thành công quay lại', 'MKT_Phát triển cộng đồng (Social)',
                    'MKT_Tái khai thác', 'MKT_TikTok/Viber/livechat', 'MKT_Voucher/Event/Trade',
                    'MKT_Web/Seo', 'MKT_Youtube', 'MKT_Zalo', 'Vãng lai_Nhận diện thương hiệu',
                    'MO - Trade MKT', 'ML - KH cũ quay lại (loyalty)', 'ML - CTV Khách hàng',
                    'MD - Zalo/Viber', 'MD - Youtube', 'MD - Tiktok', 'MD - Search',
                    'MD - Phát triển cộng đồng', 'MD - Hotline', 'MD - Facebook', 'MD - Display',
                    'CTV_Sale & MKT', 'CTV_Sale & Marketing', 'CTV_Chi nhánh', 'CTV_Bên ngoài (Sale&MKT)'
                ) AND ti2.invoicerequestid IS NULL 
            
            UNION ALL 
            
            SELECT 
                'so_thu_thuoc' AS so_thu, 3 AS bang,
                ti.invoicemedicineid AS invoiceid, ti.patientrecordid AS patientrecordid, 
                'DVHHcode' AS servicecode, 'Nhom_DV' AS dm_subgroup,
                tr.roomname AS phong, 'DVHH' AS servicename,
                CASE 
                    WHEN ti.dm_invoice_typeid = 1 THEN ti.sotienphieu 
                    WHEN ti.dm_invoice_typeid = 2 THEN -ti.sotienphieu 
                END AS "Thanh_toan",
                tp.patientcode, tp.patientname,
                DATE(ti.invoicemedicinedate) AS ngay,
                tp.id_booking AS booking,
                tn.nguoigioithieuname nhom_nguon,
                'Nhom_dv_HH' AS nhom_dv_hh,
                'dvhh' AS dvhh
            FROM tb_invoicemedicine ti 
            LEFT JOIN tb_patientrecord tp ON tp.patientrecordid = ti.patientrecordid 
            LEFT JOIN tb_patient tp2 ON tp2.patientid = tp.patientid 
            LEFT JOIN tb_nguoigioithieu tn ON tn.nguoigioithieuid = tp.nguoigioithieuid 
            LEFT JOIN tb_dm_hoahongnguoigioithieu_nguoigioithieu tdhn ON tdhn.dm_hoahongnguoigioithieu_nguoigioithieuid = tn.dm_hoahongnguoigioithieu_nguoigioithieuid 
            LEFT JOIN tb_room tr ON tr.roomid = ti.roomid 
            WHERE tn.nguoigioithieuname IN (
                'CTV_Sale & MKT', 'Data_Khai thác chéo thương hiệu', 'Data_Mua ngoài',
                'MKT_Banner', 'MKT_Coccoc', 'MKT_Facebook', 'MKT_Facebook_Lead',
                'MKT_GoogleDisplay', 'MKT_GoogleSearch', 'MKT_Hợp tác doanh nghiệp',
                'MKT_Hotline', 'MKT_ IVF BẠCH MAI_Facebook', 'MKT_KH giới thiệu KH',
                'MKT_KH thành công quay lại', 'MKT_Phát triển cộng đồng (Social)',
                'MKT_Tái khai thác', 'MKT_TikTok/Viber/livechat', 'MKT_Voucher/Event/Trade',
                'MKT_Web/Seo', 'MKT_Youtube', 'MKT_Zalo', 'Vãng lai_Nhận diện thương hiệu',
                'MO - Trade MKT', 'ML - KH cũ quay lại (loyalty)', 'ML - CTV Khách hàng',
                'MD - Zalo/Viber', 'MD - Youtube', 'MD - Tiktok', 'MD - Search',
                'MD - Phát triển cộng đồng', 'MD - Hotline', 'MD - Facebook', 'MD - Display',
                'CTV_Sale & MKT', 'CTV_Sale & Marketing', 'CTV_Chi nhánh', 'CTV_Bên ngoài (Sale&MKT)'
            )
        """
        self.sync_table(query, 'stag_thuchien_doanhso_hongha')

    def write_tieuhoaviet_doanhso_ehc(self):
        query = """
            SELECT-- Doanh số bc 001-cọc
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
                tn.nguoigioithieuname as nguon,
                NULL as book
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
            
            union all
            
            select -- doanh số nhà thuốc 
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
                tn.nguoigioithieuname as nguon,
                NULL as book
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
        """
        self.sync_table(query, 'public.tieuhoaviet_doanhso_ehc', conn_source='tieuhoaviet', conn_dest='dwh_product')


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Sync EHC Data')
    parser.add_argument('--task', type=str, required=True, help='Task to run')
    args = parser.parse_args()

    conn_uris = {
        'staging_sci': os.getenv('STAGING_SCI_URI'),
        'ehc': os.getenv('EHC_URI'),
        'tieuhoaviet': os.getenv('TIEUHOAVIET_URI'),
        'dwh_product': os.getenv('DWH_PRODUCT_URI')
    }

    manager = EHCSyncManager(conn_uris=conn_uris)

    if args.task == 'write_HongHa_doanhso_EHC':
        manager.write_HongHa_doanhso_EHC()
    elif args.task == 'write_stag_thuchien_doanhso_hongha':
        manager.write_stag_thuchien_doanhso_hongha()
    elif args.task == 'write_tieuhoaviet_doanhso_ehc':
        manager.write_tieuhoaviet_doanhso_ehc()
    else:
        logger.error(f"Unknown task: {args.task}")
