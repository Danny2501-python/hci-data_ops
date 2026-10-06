"""
Airflow DAG: HCI Care / App Kangnam Care ETL (TSK-1569)

    Postgres app hci_care (10.10.10.157, public.*)
        ──► bronze.hci_care.<bảng> ──► silver.hci_care.<bảng>        (MinIO / Iceberg REST)
        ──► dwh_product_new.hci_care.<bảng>  +  dwh_sandbox.hci_care.<bảng>
        ──► dbt data_serving ──► Tableau

- Bản chụp FULL toàn bộ bảng mỗi đêm (DB nhỏ, bảng lớn nhất ~550 nghìn dòng).
- Không kéo cột bí mật (mật khẩu, token, OTP, khoá) và cột file nhị phân (bytea).
- Mỗi tầng 1 pod, lặp qua mọi bảng; bảng lỗi -> task FAILED, tầng sau không chạy,
  Postgres giữ nguyên dữ liệu hôm trước.

Connections: hci_care_product (nguồn), dwh_product_new, dwh_sandbox, minio_logs_conn.
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
APP_DIR = '/app/hci_care'
TARGET_SCHEMA = 'hci_care'

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


source_uri = _pg_uri('hci_care_product')
dwh_product_new_uri = _pg_uri('dwh_product_new')
dwh_sandbox_uri = _pg_uri('dwh_sandbox')

lakehouse_env = {
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    'ICEBERG_REST_URI_SILVER': 'http://iceberg-rest-silver-zone:8181',
    'CATALOG_NAME_SILVER': 'silver',
    'HCI_CARE_NAMESPACE': 'hci_care',
    'HCI_CARE_KEEP_DAYS': '3',
    **_minio_env(),
}

resources = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'}, limits={'cpu': '2000m', 'memory': '4Gi'})

nfs_volume = k8s.V1Volume(name='nfs-scripts', nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH))
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)

SNAPSHOT = '{{ params.snapshot_date or ds }}'
TABLES = '{{ params.tables or "None" }}'
COMMON_ARGS = ['--snapshot-date', SNAPSHOT, '--tables', TABLES]


def pod(task_id: str, script: str, env: dict, timeout_h: int = 2):
    return KubernetesPodOperator(
        task_id=task_id,
        name=f'hci-care-{task_id.replace("_", "-")}',
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[f'{APP_DIR}/{script}'] + COMMON_ARGS,
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
    'hci_care_etl',
    default_args=default_args,
    description='App Kangnam Care (hci_care): Postgres -> Bronze -> Silver (Iceberg) -> Postgres hci_care.*',
    schedule_interval='30 1 * * *',  # 01:30 VN, trước CareSoft/Pancake/ERP/dbt
    start_date=datetime(2026, 10, 6, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'hci_care', 'kangnam_care', 'iceberg', 'TSK-1569'],
    params={
        'snapshot_date': Param(default=None, type=['null', 'string'],
                               description='YYYY-MM-DD. Bỏ trống = ngày của data interval (hôm qua)'),
        'tables': Param(default=None, type=['null', 'string'],
                        description='Chỉ chạy các bảng này, cách nhau dấu phẩy (vd app_booking,app_referral). '
                                    'Bỏ trống = tất cả'),
    },
) as dag:
    bronze = pod('bronze_ingestion', 'hci_care_bronze_ingestion.py',
                 {**lakehouse_env, 'SOURCE_URI': source_uri})
    silver = pod('silver_transformation', 'hci_care_silver_transformation.py', lakehouse_env)
    serving = pod('serving_layer', 'hci_care_serving_layer.py',
                  {**lakehouse_env, 'POSTGRES_URI': dwh_product_new_uri, 'TARGET_SCHEMA': TARGET_SCHEMA})
    serving_sandbox = pod('serving_layer_sandbox', 'hci_care_serving_layer.py',
                          {**lakehouse_env, 'POSTGRES_URI': dwh_sandbox_uri, 'TARGET_SCHEMA': TARGET_SCHEMA})

    bronze >> silver >> [serving, serving_sandbox]
