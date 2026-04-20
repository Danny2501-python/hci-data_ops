
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

PYTHON_ETL_IMAGE = '111050/python-etl-base:latest' # Assuming user rebuilt image with dbt
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
    'data_serving_etl',
    default_args=default_args,
    description='Run Data Serving ETL Pipeline',
    schedule_interval="30 8,11 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['data-serving'],
)

# ============================================================================
# RESOURCES & ENV
# ============================================================================

# Modest resources for dbt
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '2000m', 'memory': '2Gi'}
)

# NFS Volume for DBT Project Code
nfs_volume = k8s.V1Volume(
    name='nfs-dbt-data-serving',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH_DBT)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-dbt-data-serving', 
    mount_path='/app/data_serving', 
    read_only=False # DBT needs to write to target/ and logs/
)

# ============================================================================
# TASKS
# ============================================================================

# Task for Kangnam (KN)
dbt_run_kn = KubernetesPodOperator(
    task_id='dbt_run_kn',
    name='dbt-kn-run',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['dbt'],
    arguments=[
        'run',
        '--select', 'tag:report_kn',
        '--project-dir', '/app/data_serving',
        '--profiles-dir', '/app/data_serving',
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Task for Paris (PR)
dbt_run_pr = KubernetesPodOperator(
    task_id='dbt_run_pr',
    name='dbt-pr-run',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['dbt'],
    arguments=[
        'run',
        '--select', 'tag:report_pr',
        '--project-dir', '/app/data_serving',
        '--profiles-dir', '/app/data_serving',
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Parallel execution
[dbt_run_kn, dbt_run_pr]
