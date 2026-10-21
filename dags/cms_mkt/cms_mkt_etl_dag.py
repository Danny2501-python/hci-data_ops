"""
Airflow DAG: CMS (database mkt) ETL – dữ liệu Loyalty cho báo cáo Loyalty & Campaign

    Postgres CMS mkt (10.10.10.234:54191, public.*, tài khoản chỉ đọc)
        ──► bronze.cms_mkt.<bảng> ──► silver.cms_mkt.<bảng>          (MinIO / Iceberg REST)
        ──► dwh_product_new.cms_mkt.<bảng>  +  dwh_sandbox.cms_mkt.<bảng>
        ──► dbt data_serving ──► Tableau

- DÙNG LẠI NGUYÊN code /app/hci_care/*.py (không sửa). Code đó lấy DB nguồn, namespace
  Iceberg, schema đích và danh sách bảng từ env / --tables, nên chỉ cần DAG này trỏ sang CMS.
  => Ai sửa code hci_care thì nhớ là DAG này cũng chạy code đó.
- Riêng bronze chạy /app/cms_mkt/cms_mkt_bronze_ingestion.py: gọi lại bronze hci_care nhưng
  bỏ thêm cột partner_erp_data.data_erp_kn (JSON ERP thô, làm OOM 21/10/2026) và các cột
  giấy tờ tuỳ thân (hộ chiếu / CCCD / CMND).
- Chỉ kéo các bảng trong DEFAULT_TABLES (không kéo cả DB: CMS có mail_message,
  caresoft_chat_message... rất lớn mà báo cáo không cần).
  QUAN TRỌNG: không truyền "None" cho --tables, vì "None" = kéo TẤT CẢ bảng.
- Bản chụp FULL mỗi đêm. Không kéo cột bí mật (mật khẩu, token, OTP, khoá) và cột bytea.
- Timestamp là giờ UTC như Odoo lưu -> làm báo cáo nhớ +7h.
- Mỗi tầng 1 pod; bảng lỗi -> task FAILED, tầng sau không chạy,
  Postgres giữ nguyên dữ liệu hôm trước.

Connections: cms_mkt_reader (nguồn CMS), dwh_product_new, dwh_sandbox, minio_logs_conn.
"""

import json
import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'
APP_DIR = '/app/hci_care'          # dùng lại code hci_care (silver, serving)
CMS_APP_DIR = '/app/cms_mkt'       # bronze: wrapper loại thêm cột cho CMS
ICEBERG_NAMESPACE = 'cms_mkt'      # tách riêng khỏi namespace hci_care
TARGET_SCHEMA = 'cms_mkt'          # new_product.cms_mkt.*
SOURCE_CONN_ID = 'cms_mkt_reader'

DEFAULT_TABLES = [
    # Loyalty – dữ liệu phát sinh (cập nhật hằng ngày)
    'loyalty_hci_card',               # thẻ thành viên, hạng hiện tại
    'loyalty_hci_card_rank_history',  # lịch sử nâng / giảm hạng
    'loyalty_hci_bonus_history',      # bonus cấp / dùng / hết hạn
    'loyalty_hci_discount',           # ưu đãi theo hạng
    'loyalty_hci_rank_history',       # lịch sử hạng cũ (dừng cập nhật 07/2026)
    'loyalty_hci_history',
    # Loyalty – danh mục / cấu hình
    'loyalty_hci_rank',
    'loyalty_hci_rank_benefit',
    'loyalty_hci_rank_reward',
    'loyalty_hci_reward',
    'loyalty_card_policy',
    'loyalty_card_bonus',
    'loyalty_card_condition',
    'loyalty_card_expiry',
    # Ghép nối CMS <-> ERP, danh mục chung
    'partner_erp_data',
    'erp_map_loyalty',
    'erp_map_partner',
    'erp_map_product',
    'res_partner',
    'res_brand',
    'res_company',
]

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}


def _pg_uri(conn_id: str) -> str:
    try:
        return PostgresHook(postgres_conn_id=conn_id).get_connection(conn_id).get_uri().split('?__extra__=')[0]
    except Exception as e:
        logger.warning(f"Could not load {conn_id} connection: {e}")
        return ''


def _minio_env() -> dict:
    try:
        c = BaseHook.get_connection('minio_logs_conn')
        extra = json.loads(c.extra or '{}')
        return {
            'S3_ENDPOINT': extra.get('endpoint_url') or 'http://10.152.183.65:9000',
            'AWS_ACCESS_KEY_ID': c.login or extra.get('aws_access_key_id', ''),
            'AWS_SECRET_ACCESS_KEY': c.password or extra.get('aws_secret_access_key', ''),
            'AWS_REGION': extra.get('region_name', 'us-east-1'),
        }
    except Exception as e:
        logger.warning(f"Could not load minio_logs_conn: {e}")
        return {}


source_uri = _pg_uri(SOURCE_CONN_ID)
dwh_product_new_uri = _pg_uri('dwh_product_new')
dwh_sandbox_uri = _pg_uri('dwh_sandbox')

lakehouse_env = {
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    'ICEBERG_REST_URI_SILVER': 'http://iceberg-rest-silver-zone:8181',
    'CATALOG_NAME_SILVER': 'silver',
    'HCI_CARE_NAMESPACE': ICEBERG_NAMESPACE,
    'HCI_CARE_KEEP_DAYS': '3',
    **_minio_env(),
}

# Giới hạn 6Gi (hci_care dùng 4Gi): res_partner CMS ~533 nghìn dòng / 282 MB, chừa dư.
resources = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'}, limits={'cpu': '2000m', 'memory': '6Gi'})

nfs_volume = k8s.V1Volume(name='nfs-scripts', nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH))
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)

SNAPSHOT = '{{ params.snapshot_date or ds }}'
# Bỏ trống param tables -> dùng DEFAULT_TABLES (KHÔNG BAO GIỜ là "None" = tất cả bảng)
TABLES = '{{ params.tables or "' + ','.join(DEFAULT_TABLES) + '" }}'
COMMON_ARGS = ['--snapshot-date', SNAPSHOT, '--tables', TABLES]


def pod(task_id: str, script_path: str, env: dict, timeout_h: int = 2):
    return KubernetesPodOperator(
        task_id=task_id,
        name=f'cms-mkt-{task_id.replace("_", "-")}',
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[script_path] + COMMON_ARGS,
        volumes=[nfs_volume],
        volume_mounts=[nfs_mount],
        env_vars=env,
        container_resources=resources,
        execution_timeout=timedelta(hours=timeout_h),
        on_finish_action='delete_pod',
        get_logs=True,
    )


# ============================================================================
# DAG
# ============================================================================

with DAG(
    'cms_mkt_etl',
    default_args=default_args,
    description='CMS mkt (Loyalty): Postgres -> Bronze -> Silver (Iceberg) -> Postgres cms_mkt.*',
    schedule_interval='0 2 * * *',  # 02:00 VN: sau hci_care (01:30), trước ERP (03:00) / dbt (04:00)
    start_date=datetime(2026, 10, 21, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'cms_mkt', 'loyalty', 'iceberg'],
    params={
        'snapshot_date': Param(default=None, type=['null', 'string'],
                               description='YYYY-MM-DD. Bỏ trống = ngày của data interval (hôm qua)'),
        'tables': Param(default=None, type=['null', 'string'],
                        description='Chỉ chạy các bảng này, cách nhau dấu phẩy (vd loyalty_hci_card,loyalty_hci_rank). '
                                    'Bỏ trống = danh sách mặc định DEFAULT_TABLES (không phải cả DB)'),
    },
) as dag:
    bronze = pod('bronze_ingestion', f'{CMS_APP_DIR}/cms_mkt_bronze_ingestion.py',
                 {**lakehouse_env, 'SOURCE_URI': source_uri})
    silver = pod('silver_transformation', f'{APP_DIR}/hci_care_silver_transformation.py', lakehouse_env)
    serving = pod('serving_layer', f'{APP_DIR}/hci_care_serving_layer.py',
                  {**lakehouse_env, 'POSTGRES_URI': dwh_product_new_uri, 'TARGET_SCHEMA': TARGET_SCHEMA})
    serving_sandbox = pod('serving_layer_sandbox', f'{APP_DIR}/hci_care_serving_layer.py',
                          {**lakehouse_env, 'POSTGRES_URI': dwh_sandbox_uri, 'TARGET_SCHEMA': TARGET_SCHEMA})

    bronze >> silver >> [serving, serving_sandbox]
