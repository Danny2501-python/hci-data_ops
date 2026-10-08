"""
Airflow DAG: Tỷ giá ACB (theo khuôn dags/hci_care/hci_care_etl_dag.py)

    API acb.com.vn (tỷ giá chốt 23:59 mỗi ngày)
        ──► bronze.acb_fx.ty_gia_acb ──► silver.acb_fx.ty_gia_acb      (MinIO / Iceberg REST)
        ──► dwh_product_new.public.ty_gia_acb  +  dwh_sandbox.public.ty_gia_acb
        ──► dbt data_serving (quy đổi doanh thu Philippines PHP -> VND) ──► Tableau

- Chạy 06:15 mỗi sáng (giờ VN): lấy lại 7 ngày gần nhất tới hôm qua, ghi đè theo ngày.
- Backfill: Trigger DAG w/ config {"start_date": "2026-09-10", "end_date": "2026-10-07"}.
- Bất kỳ ngày nào lỗi -> task FAILED, tầng sau không chạy, Postgres giữ nguyên.

Connections: dwh_product_new, dwh_sandbox, minio_logs_conn. Code: app/acb_fx (README.md).
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
APP_DIR = '/app/acb_fx'
TARGET_SCHEMA = 'public'
TARGET_TABLE = 'ty_gia_acb'

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


dwh_product_new_uri = _pg_uri('dwh_product_new')
dwh_sandbox_uri = _pg_uri('dwh_sandbox')

lakehouse_env = {
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    'ICEBERG_REST_URI_SILVER': 'http://iceberg-rest-silver-zone:8181',
    'CATALOG_NAME_SILVER': 'silver',
    'ACB_FX_NAMESPACE': 'acb_fx',
    'ACB_FX_KEEP_DAYS': '7',
    **_minio_env(),
}

serving_env = {'TARGET_SCHEMA': TARGET_SCHEMA, 'TARGET_TABLE': TARGET_TABLE}

resources = k8s.V1ResourceRequirements(
    requests={'cpu': '250m', 'memory': '512Mi'}, limits={'cpu': '1000m', 'memory': '1Gi'})

nfs_volume = k8s.V1Volume(name='nfs-scripts', nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH))
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)

# Khoá partition Iceberg của lượt chạy = ngày chạy theo giờ VN (cùng giá trị cho cả 3 tầng).
# KHÔNG dùng {{ ds }}: ds là ngày UTC của đầu data interval, lệch 2 ngày với lượt 06:15 VN -> lượt
# chạy tay (backfill) có thể bị lượt theo lịch vài ngày sau ghi đè partition.
SNAPSHOT_ARGS = ['--snapshot-date', '{{ data_interval_end.in_timezone("Asia/Ho_Chi_Minh").strftime("%Y-%m-%d") }}']
BRONZE_ARGS = SNAPSHOT_ARGS + [
    '--start-date', '{{ params.start_date or "None" }}',
    '--end-date', '{{ params.end_date or "None" }}',
    '--days-back', '{{ params.days_back }}',
]


def pod(task_id: str, script: str, env: dict, args: list, timeout_h: int = 1):
    return KubernetesPodOperator(
        task_id=task_id,
        name=f'acb-fx-{task_id.replace("_", "-")}',
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[f'{APP_DIR}/{script}'] + args,
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
    'acb_fx_etl',
    default_args=default_args,
    description='Tỷ giá ACB (VND) theo ngày: API -> Bronze -> Silver (Iceberg) -> Postgres public.ty_gia_acb',
    schedule_interval='15 6 * * *',  # 06:15 VN – tỷ giá hôm qua đã chốt; trước các DAG dbt dùng tỷ giá
    start_date=datetime(2026, 10, 8, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'ty-gia', 'acb', 'iceberg', 'philippines'],
    params={
        'start_date': Param(default=None, type=['null', 'string'],
                            description='YYYY-MM-DD. Bỏ trống = end_date - (days_back - 1). '
                                        'Backfill Philippines: 2026-09-10'),
        'end_date': Param(default=None, type=['null', 'string'],
                          description='YYYY-MM-DD, phải trước hôm nay. Bỏ trống = hôm qua (giờ VN)'),
        'days_back': Param(default=7, type='integer', minimum=1, maximum=60,
                           description='Số ngày lấy lại khi không nhập start_date'),
    },
) as dag:
    bronze = pod('bronze_ingestion', 'acb_fx_bronze_ingestion.py', lakehouse_env, BRONZE_ARGS)
    silver = pod('silver_transformation', 'acb_fx_silver_transformation.py', lakehouse_env, SNAPSHOT_ARGS)
    serving = pod('serving_layer', 'acb_fx_serving_layer.py',
                  {**lakehouse_env, **serving_env, 'POSTGRES_URI': dwh_product_new_uri}, SNAPSHOT_ARGS)
    serving_sandbox = pod('serving_layer_sandbox', 'acb_fx_serving_layer.py',
                          {**lakehouse_env, **serving_env, 'POSTGRES_URI': dwh_sandbox_uri}, SNAPSHOT_ARGS)

    bronze >> silver >> [serving, serving_sandbox]
