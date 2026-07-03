from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator
import logging

import pendulum
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Docker image
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'

# Kubernetes
NAMESPACE = 'default'

# NFS
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'

# ============================================================================
# LOAD AIRFLOW CONNECTIONS
# ============================================================================

# DWH Product Connections (for serving layer)
dwh_product_new_uri = ''

try:
    hook_new = PostgresHook(postgres_conn_id='dwh_product_new')
    conn_new = hook_new.get_connection('dwh_product_new')
    dwh_product_new_uri = conn_new.get_uri()
    if '?__extra__=' in dwh_product_new_uri:
        dwh_product_new_uri = dwh_product_new_uri.split('?__extra__=')[0]
    logger.info("✅ Loaded connection URI for dwh_product_new")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_product_new connection: {e}")


dwh_sandbox_uri = ''
try:
    hook_sandbox = PostgresHook(postgres_conn_id='dwh_sandbox')
    conn_sandbox = hook_sandbox.get_connection('dwh_sandbox')
    dwh_sandbox_uri = conn_sandbox.get_uri()
    if '?__extra__=' in dwh_sandbox_uri:
        dwh_sandbox_uri = dwh_sandbox_uri.split('?__extra__=')[0]
    logger.info("✅ Loaded connection URI for dwh_sandbox")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_sandbox connection: {e}")

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

# Timezone for Vietnam
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

def get_yesterday_tz():
    """Get yesterday's date in Asia/Ho_Chi_Minh timezone"""
    from datetime import datetime, timedelta
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

dag = DAG(
    'caresoft_contact_thv_etl',
    default_args=default_args,
    description='CareSoft Contact THV Pipeline (erp_id=8): Bronze → Serving (PostgreSQL Bypass Silver)',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'contact', 'thv', 'etl', 'iceberg'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date (YYYY-MM-DD). Leave empty to use yesterday.',
        ),
        'contact_ids': Param(
            default=None,
            type=['null', 'string'],
            description='Comma-separated list of Contact IDs for manual backfill. If provided, ingestion is skipped.',
        ),
        'brand_id': Param(
            default=None,
            type=['null', 'integer', 'string'],
            description='Optional: Filter by Brand ID when backfilling.',
        ),
        'from_table': Param(
            default=None,
            type=['null', 'string'],
            description='Optional: Source table (in target DB) to pull IDs from (columns expected: requester_id).',
        )
    },
    user_defined_macros={'yesterday_tz': get_yesterday_tz},
)

# ============================================================================
# COMMON RESOURCES
# ============================================================================

common_env_vars = {
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    'ICEBERG_REST_URI_SILVER': 'http://iceberg-rest-silver-zone:8181',
    'CATALOG_NAME_SILVER': 'silver',
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
    'CARESOFT_PAGE_SIZE': '500',
}

resource_requirements_bronze = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

resource_requirements_silver = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)

nfs_volume_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)

duckdb_temp_volume = k8s.V1Volume(
    name='duckdb-temp',
    empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='5Gi')
)

duckdb_temp_volume_mount = k8s.V1VolumeMount(name='duckdb-temp', mount_path='/tmp/duckdb', read_only=False)

# ============================================================================
# BRANCHING LOGIC
# ============================================================================

def decide_etl_flow(**kwargs):
    conf = kwargs.get('dag_run').conf or {}
    contact_ids = conf.get('contact_ids')
    from_table = conf.get('from_table')
    
    if (contact_ids and contact_ids != "") or (from_table and from_table != ""):
        logger.info(f"🚀 Manual backfill detected (IDs or Table). Bypassing ingestion.")
        return ['contact_enrichment_silver_thv', 'contact_enrichment_silver_sandbox_thv']
    else:
        logger.info("📅 No manual IDs provided. Proceeding with standard ingestion.")
        return 'contact_ingestion_bronze_thv'

branch_task = BranchPythonOperator(
    task_id='decide_etl_flow',
    python_callable=decide_etl_flow,
    dag=dag,
)

# ============================================================================
# TASKS
# ============================================================================

task_contact_ingestion = KubernetesPodOperator(
    task_id='contact_ingestion_bronze_thv',
    name='caresoft-contact-ingestion-bronze-thv',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    cmds=['python'],
    arguments=[
        '/app/caresoft/contact/contact_ingestion_thv.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}'
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_volume_mount],
    env_vars=common_env_vars,
    container_resources=resource_requirements_bronze,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

task_contact_enrichment = KubernetesPodOperator(
    task_id='contact_enrichment_silver_thv',
    name='caresoft-contact-enrichment-silver-thv',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    cmds=['python'],
    arguments=[
        '/app/caresoft/contact/contact_detail_enrichment_thv.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        '{{ dag_run.conf.get("contact_ids") or "" }}',
        '{{ dag_run.conf.get("brand_id") or "" }}',
        '{{ dag_run.conf.get("from_table") or "" }}'
    ],
    volumes=[nfs_volume, duckdb_temp_volume],
    volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
    env_vars={
        **common_env_vars, 
        'DUCKDB_TEMP_DIR': '/tmp/duckdb', 
        'POSTGRES_URI': dwh_product_new_uri,
        'SOURCE_PG_URI': dwh_product_new_uri # Always look in New DWH for IDs
    },
    container_resources=resource_requirements_silver,
    is_delete_operator_pod=True,
    get_logs=True,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)


task_contact_enrichment_sandbox = KubernetesPodOperator(
    task_id='contact_enrichment_silver_sandbox_thv',
    name='caresoft-contact-enrichment-silver-sandbox-thv',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    cmds=['python'],
    arguments=[
        '/app/caresoft/contact/contact_detail_enrichment_thv.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        '{{ dag_run.conf.get("contact_ids") or "" }}',
        '{{ dag_run.conf.get("brand_id") or "" }}',
        '{{ dag_run.conf.get("from_table") or "" }}'
    ],
    volumes=[nfs_volume, duckdb_temp_volume],
    volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
    env_vars={
        **common_env_vars, 
        'DUCKDB_TEMP_DIR': '/tmp/duckdb', 
        'POSTGRES_URI': dwh_sandbox_uri,
        'SOURCE_PG_URI': dwh_product_new_uri # Always look in New DWH for IDs
    },
    container_resources=resource_requirements_silver,
    is_delete_operator_pod=True,
    get_logs=True,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# ============================================================================
# DEPENDENCIES
# ============================================================================

branch_task >> task_contact_ingestion >> [task_contact_enrichment, task_contact_enrichment_sandbox]
branch_task >> [task_contact_enrichment, task_contact_enrichment_sandbox]
