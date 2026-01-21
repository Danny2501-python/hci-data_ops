from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.param import Param
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'

# Get Postgres Connection (for Serving Layer)
dwh_product_uri = ''
try:
    hook = PostgresHook(postgres_conn_id='dwh_product')
    conn = hook.get_connection('dwh_product')
    dwh_product_uri = conn.get_uri()
    if '?__extra__=' in dwh_product_uri:
        dwh_product_uri = dwh_product_uri.split('?__extra__=')[0]
except Exception as e:
    logger.warning(f"Could not load dwh_product connection: {e}")

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

dag = DAG(
    'caresoft_chat_agent_update_etl',
    default_args=default_args,
    description='CareSoft Flow 10: Hourly Agent Update for Chat',
    schedule_interval='45 5 * * *', # Hourly at 5:45 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'chat', 'agent_update', 'etl'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date (YYYY-MM-DD).',
        )
    },
)

# ============================================================================
# RESOURCES & ENV
# ============================================================================

common_env_vars = {
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
    'CARESOFT_PAGE_SIZE': '500',
}

# Resources
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '1000m', 'memory': '1Gi'}
)

# NFS
nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-scripts', mount_path='/app', read_only=True
)

# DuckDB Temp
duckdb_volume = k8s.V1Volume(
    name='duckdb-temp',
    empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='2Gi')
)
duckdb_mount = k8s.V1VolumeMount(
    name='duckdb-temp', mount_path='/tmp/duckdb', read_only=False
)

# ============================================================================
# TASKS
# ============================================================================

# Task 1: Check and Update Agent Emails in Iceberg (Bronze/Silver)
task_agent_update = KubernetesPodOperator(
    task_id='chat_agent_update',
    name='caresoft-chat-agent-update',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    # Use dag_run.conf to avoid params caching issues
    arguments=['/app/caresoft/chat/chat_agent_update.py', '{{ (dag_run.conf or {}).get("snapshot_date") or ds }}'],
    volumes=[nfs_volume, duckdb_volume],
    volume_mounts=[nfs_mount, duckdb_mount],
    env_vars={**common_env_vars, 'DUCKDB_TEMP_DIR': '/tmp/duckdb'},
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Task 2: Refresh Serving Layer (Postgres) to reflect updates
# This uses the standard serving layer script, processing the NEW records we just added
task_refresh_serving = KubernetesPodOperator(
    task_id='chat_serving_refresh',
    name='caresoft-chat-serving-refresh',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    # Use dag_run.conf to avoid params caching issues
    arguments=['/app/caresoft/chat/chat_serving_layer.py', '{{ (dag_run.conf or {}).get("snapshot_date") or ds }}'],
    volumes=[nfs_volume, duckdb_volume],
    volume_mounts=[nfs_mount, duckdb_mount],
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_product_uri,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb'
    },
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Dependencies
task_agent_update >> task_refresh_serving
