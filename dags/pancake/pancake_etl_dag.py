"""
Airflow DAG: Pancake ETL (lakehouse Bronze -> Silver -> Gold -> Postgres mart)

Mỗi entity (conversations, messages, customers, statistics) là 1 TaskGroup:
    bronze_ingestion -> silver_transformation -> gold_transformation -> serving (prod + sandbox)
messages.bronze phụ thuộc conversations.bronze (lấy danh sách hội thoại từ bronze).

    Pancake API ──► bronze.pancake.*_api ──► silver.pancake.* ──► gold.pancake.* ──► dwh_product_new.public.pancake_*
                         (MinIO / Iceberg REST catalog)                                    └─► dbt data_serving ─► Tableau

Cấu hình:
  - Page + token: Postgres public.core_pancake_page (xem app/pancake/README.md)
  - MinIO: lấy từ Airflow Connection `minio_logs_conn` (login/password/extra.endpoint_url),
    KHÔNG hardcode access key trong DAG.
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
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'
APP_DIR = '/app/pancake'
ENTITIES = ['conversations', 'messages', 'customers', 'statistics']

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
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    **_minio_env(),
}

resources_light = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '1Gi'}, limits={'cpu': '1000m', 'memory': '2Gi'})
resources_heavy = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'}, limits={'cpu': '2000m', 'memory': '4Gi'})

nfs_volume = k8s.V1Volume(name='nfs-scripts', nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH))
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)

SNAPSHOT = '{{ params.snapshot_date or ds }}'


def pod(task_id: str, script: str, args: list, env: dict, resources, timeout_h: int = 2,
        trigger_rule: str = 'all_success'):
    return KubernetesPodOperator(
        task_id=task_id,
        name=f'pancake-{task_id.replace("_", "-")}',
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
        trigger_rule=trigger_rule,
    )


# ============================================================================
# DAG
# ============================================================================

with DAG(
    'pancake_etl',
    default_args=default_args,
    description='Pancake: API -> Bronze -> Silver -> Gold (Iceberg) -> Postgres mart',
    schedule_interval='30 2 * * *',  # 02:30 VN; ds = ngày hôm qua (data interval) = snapshot_date
    start_date=datetime(2026, 10, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'pancake', 'iceberg', 'marketing'],
    params={
        'snapshot_date': Param(default=None, type=['null', 'string'],
                               description='YYYY-MM-DD (giờ VN). Bỏ trống = ngày của data interval (hôm qua)'),
        'days': Param(default=3, type='integer', minimum=1, maximum=31,
                      description='Số ngày lùi lại khi gọi API (bắt dữ liệu cập nhật trễ)'),
        'page_id': Param(default=None, type=['null', 'string'],
                         description='Chỉ chạy bronze cho 1 page (bỏ trống = mọi page active)'),
    },
) as dag:
    bronze_tasks = {}
    for entity in ENTITIES:
        heavy = entity == 'messages'
        with TaskGroup(group_id=entity):
            bronze = pod(
                'bronze_ingestion', 'pancake_bronze_ingestion.py',
                ['--entity', entity, '--snapshot-date', SNAPSHOT, '--days', '{{ params.days }}',
                 '--page-id', '{{ params.page_id or "None" }}'],
                {**lakehouse_env, 'POSTGRES_URI': dwh_product_new_uri, 'PANCAKE_MAX_PAGE_WORKERS': '4'},
                resources_heavy if heavy else resources_light, timeout_h=4 if heavy else 2,
                # messages đọc bronze conversations: vẫn chạy dù conversations có page lỗi
                trigger_rule='all_done' if entity == 'messages' else 'all_success',
            )
            # all_done: 1 page lỗi ở bronze (vd token hết hạn) vẫn đỏ để cảnh báo,
            # nhưng dữ liệu các page khác vẫn đi tiếp silver -> gold -> Postgres
            silver = pod('silver_transformation', 'pancake_silver_transformation.py',
                         ['--entity', entity, '--snapshot-date', SNAPSHOT], lakehouse_env,
                         resources_heavy if heavy else resources_light, trigger_rule='all_done')
            gold = pod('gold_transformation', 'pancake_gold_transformation.py',
                       ['--entity', entity, '--snapshot-date', SNAPSHOT], lakehouse_env,
                       resources_heavy if heavy else resources_light)
            serving = pod('serving_layer', 'pancake_serving_layer.py',
                          ['--entity', entity, '--snapshot-date', SNAPSHOT],
                          {**lakehouse_env, 'POSTGRES_URI': dwh_product_new_uri},
                          resources_heavy if heavy else resources_light)
            serving_sandbox = pod('serving_layer_sandbox', 'pancake_serving_layer.py',
                                  ['--entity', entity, '--snapshot-date', SNAPSHOT],
                                  {**lakehouse_env, 'POSTGRES_URI': dwh_sandbox_uri},
                                  resources_heavy if heavy else resources_light)
            bronze >> silver >> gold >> [serving, serving_sandbox]
        bronze_tasks[entity] = bronze

    # messages lấy danh sách hội thoại từ bronze conversations cùng snapshot
    bronze_tasks['conversations'] >> bronze_tasks['messages']
