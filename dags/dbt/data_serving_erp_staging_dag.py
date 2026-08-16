from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
import logging
import pendulum

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

PYTHON_ETL_IMAGE = '111050/python-etl-base:latest'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH_DBT = '/srv/nfs_share/dbt/data_serving' # Path on NFS Server

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'data_serving_erp_staging_etl',
    default_args=default_args,
    description='Run Data Serving ETL Pipeline - ERP Staging (Products, Employees, Payment Transfer) + Old View Store',
    schedule_interval="0 4 * * *",  # 04:00 VN — sau call_functions_erp_direct_dag (03:30), sớm hơn mọi DBT DAG khác
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['data-serving', 'erp_staging'],
)

# ============================================================================
# RESOURCES & ENV
# ============================================================================

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '2000m', 'memory': '2Gi'}
)

nfs_volume = k8s.V1Volume(
    name='nfs-dbt-data-serving',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH_DBT)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-dbt-data-serving',
    mount_path='/app/data_serving',
    read_only=True   # source code only — target/logs ghi vào /tmp trong pod
)

# ============================================================================
# TASKS
# ============================================================================

# Nạp seed backfill (stag_product_backfill_sandbox, xem seeds/seeds.yml) TRƯỚC khi build erp_staging,
# vì stag_products.sql ref() seed này qua dbt "run" thường (không phải "build") -- "dbt run" không tự
# load seed. Phải là task riêng, chạy trước, để đảm bảo bảng seed tồn tại/mới nhất trước khi model union
# vào nó. Idempotent, dữ liệu seed cố định (~9.209 dòng) nên chạy lại mỗi ngày không vấn đề gì.
dbt_seed_stag_product_backfill_sandbox = KubernetesPodOperator(
    task_id='dbt_seed_stag_product_backfill_sandbox',
    name='dbt-erp-staging-seed-backfill',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['dbt'],
    arguments=[
        'seed',
        '--select', 'stag_product_backfill_sandbox',
        '--project-dir', '/app/data_serving',
        '--profiles-dir', '/app/data_serving',
        '--target-path', '/tmp/dbt_target',  # ghi vào pod local, không ra NFS
        '--log-path',    '/tmp/dbt_logs',    # ghi vào pod local, không ra NFS
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

dbt_run_erp_staging = KubernetesPodOperator(
    task_id='dbt_run_erp_staging',
    name='dbt-erp-staging-run',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['dbt'],
    arguments=[
        'run',
        '--select', 'tag:erp_staging',
        '--project-dir', '/app/data_serving',
        '--profiles-dir', '/app/data_serving',
        '--target-path', '/tmp/dbt_target',  # ghi vào pod local, không ra NFS
        '--log-path',    '/tmp/dbt_logs',    # ghi vào pod local, không ra NFS
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# old_view_store (view, tag "old_view_store" - xem dbt_project.yml) chỉ chạy SAU khi các bảng
# erp_staging (products/employees/payment_transfer) đã build xong trong cùng lần chạy này.
dbt_run_old_view_store = KubernetesPodOperator(
    task_id='dbt_run_old_view_store',
    name='dbt-old-view-store-run',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['dbt'],
    arguments=[
        'run',
        '--select', 'tag:old_view_store',
        '--project-dir', '/app/data_serving',
        '--profiles-dir', '/app/data_serving',
        '--target-path', '/tmp/dbt_target',  # ghi vào pod local, không ra NFS
        '--log-path',    '/tmp/dbt_logs',    # ghi vào pod local, không ra NFS
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

dbt_seed_stag_product_backfill_sandbox >> dbt_run_erp_staging >> dbt_run_old_view_store
