
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
import logging
import json
import pendulum

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'call_functions_sync_delete_dag_sandbox',
    default_args=default_args,
    description='Sync Delete from sandbox auditlog_log to sandbox tables (backup + soft-delete flag is_delete cho bảng đã migrate cột, còn lại chỉ backup) — bản mirror của call_functions_sync_delete_dag, trỏ dwh_sandbox thay vì dwh_product_new',
    schedule_interval='20 3 * * *',  # Daily at 3:20 AM — chạy sau call_functions_erp_direct_dag_sandbox (3:00 AM) để auditlog_log đã kịp đồng bộ
    start_date=datetime(2026, 8, 14, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['call_functions', 'delete-sync', 'sandbox'],
)

# ============================================================================
# DWH CONNECTION
# ============================================================================

dwh_sandbox_uri = ''
try:
    hook = PostgresHook(postgres_conn_id='dwh_sandbox')
    conn = hook.get_connection('dwh_sandbox')
    dwh_sandbox_uri = conn.get_uri()
    if '?__extra__=' in dwh_sandbox_uri:
        dwh_sandbox_uri = dwh_sandbox_uri.split('?__extra__=')[0]
except Exception as e:
    logger.warning(f"Could not load connection details: {e}")

# ============================================================================
# K8S RESOURCES
# ============================================================================

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '1000m', 'memory': '1Gi'}
)
nfs_volume = k8s.V1Volume(name='nfs-scripts', nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH))
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)

# ============================================================================
# TASKS
# ============================================================================

# Define brands and their ETL IDs (giữ đúng danh sách như bản new_product — chưa gồm VDE)
brands = [
    ('KN', 1, 'Kangnam'),
    ('PR', 3, 'Paris'),
    ('HH', 4, 'HongHa'),
    ('SCI', 7, 'SCI')
]

# Biến để giữ task phía trước nhằm tạo chuỗi tuần tự
prev_task = None

for code, etl_id, full_name in brands:
    current_task = KubernetesPodOperator(
        task_id=f'sync_delete_sandbox_{code.lower()}',
        name=f'sync-delete-sandbox-{code.lower()}',
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[
            '/app/call_functions/trans_data_delete.py',
            '15',  # Look back 15 days
        ],
        volumes=[nfs_volume],
        volume_mounts=[nfs_mount],
        env_vars={
            'POSTGRES_URI': dwh_sandbox_uri,
            'BRAND_NAME': full_name,
            'BRAND_ETL': str(etl_id)
        },
        container_resources=resource_requirements,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag,
    )

    # Thiết lập chạy tuần tự: prev_task xong mới chạy current_task
    if prev_task:
        prev_task >> current_task

    prev_task = current_task
