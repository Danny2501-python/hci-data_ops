from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.param import Param
import logging

import pendulum
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'

# Get Postgres Connections
dwh_product_new_uri = ''
try:
    hook_new = PostgresHook(postgres_conn_id='dwh_product_new')
    conn_new = hook_new.get_connection('dwh_product_new')
    dwh_product_new_uri = conn_new.get_uri()
    if '?__extra__=' in dwh_product_new_uri:
        dwh_product_new_uri = dwh_product_new_uri.split('?__extra__=')[0]
except Exception as e:
    logger.warning(f"Could not load dwh_product_new connection: {e}")

dwh_sandbox_uri = ''
try:
    hook_sandbox = PostgresHook(postgres_conn_id='dwh_sandbox')
    conn_sandbox = hook_sandbox.get_connection('dwh_sandbox')
    dwh_sandbox_uri = conn_sandbox.get_uri()
    if '?__extra__=' in dwh_sandbox_uri:
        dwh_sandbox_uri = dwh_sandbox_uri.split('?__extra__=')[0]
except Exception as e:
    logger.warning(f"Could not load dwh_sandbox connection: {e}")

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')


def get_yesterday_tz():
    from datetime import datetime, timedelta
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')


dag = DAG(
    'pancake_inbox_etl',
    default_args=default_args,
    description='Pancake Inbox Pipeline: Page sync -> Bronze (conversations) -> Serving (conversations + conversation<->ad bridge)',
    schedule_interval='30 2 * * *',  # after caresoft_chat_etl (02:00) to avoid NFS/pod contention
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['pancake', 'inbox', 'ads', 'etl', 'iceberg'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date for backfill (YYYY-MM-DD). Leave empty to use execution date.',
        )
    },
    user_defined_macros={'yesterday_tz': get_yesterday_tz},
)

# ============================================================================
# RESOURCES & ENV
# ============================================================================

# Ingestion needs both: Postgres (to load core_pancake_page credentials) and
# Iceberg Bronze (to write raw conversations). Serving needs Bronze (read) + Postgres (write).
common_env_vars = {
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',

    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
}

resource_requirements_ingestion = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

resource_requirements_serving = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-scripts', mount_path='/app', read_only=True
)

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

# Keeps core_pancake_page in step with the account: new pages get a page_access_token generated
# and are inserted with is_active = TRUE and the brand of their account. Accounts (+ their
# account-level "API Access Token", ~90 days) live in table core_pancake_account.
task_page_sync = KubernetesPodOperator(
    task_id='page_sync',
    name='pancake-page-sync',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=['/app/pancake/page_sync.py'],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars={
        'POSTGRES_URI': dwh_product_new_uri,
    },
    container_resources=k8s.V1ResourceRequirements(
        requests={'cpu': '250m', 'memory': '256Mi'},
        limits={'cpu': '500m', 'memory': '512Mi'}
    ),
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

task_conversation_ingestion = KubernetesPodOperator(
    task_id='conversation_ingestion_bronze',
    name='pancake-conversation-ingestion-bronze',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=['/app/pancake/inbox/conversation_ingestion.py', '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}'],
    volumes=[nfs_volume, duckdb_volume],
    volume_mounts=[nfs_mount, duckdb_mount],
    env_vars={
        **common_env_vars,
        # Reads core_pancake_page (page_id -> page_access_token) from the same DWH Postgres
        'POSTGRES_URI': dwh_product_new_uri,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
    },
    container_resources=resource_requirements_ingestion,
    is_delete_operator_pod=True,
    get_logs=True,
    # Still ingest the pages we already have if page_sync fails (e.g. expired account token);
    # the failure of page_sync itself is reported by its own email_on_failure.
    trigger_rule='all_done',
    dag=dag,
)

task_conversation_serving = KubernetesPodOperator(
    task_id='conversation_serving_layer',
    name='pancake-conversation-serving-layer',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=['/app/pancake/inbox/conversation_serving_layer.py', '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}'],
    volumes=[nfs_volume, duckdb_volume],
    volume_mounts=[nfs_mount, duckdb_mount],
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_product_new_uri,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
    },
    container_resources=resource_requirements_serving,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

task_conversation_serving_sandbox = KubernetesPodOperator(
    task_id='conversation_serving_layer_sandbox',
    name='pancake-conversation-serving-layer-sandbox',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=['/app/pancake/inbox/conversation_serving_layer.py', '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}'],
    volumes=[nfs_volume, duckdb_volume],
    volume_mounts=[nfs_mount, duckdb_mount],
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_sandbox_uri,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
    },
    container_resources=resource_requirements_serving,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# DEPS
task_page_sync >> task_conversation_ingestion >> [task_conversation_serving, task_conversation_serving_sandbox]
