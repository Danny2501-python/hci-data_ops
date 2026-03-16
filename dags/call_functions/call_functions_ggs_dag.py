"""
Airflow DAG for Google Sheet ETL Pipeline
Bronze (Google Sheet -> Iceberg) -> Serving (Iceberg -> PostgreSQL)
Uses dedicated declare_api_googlesheet configuration table
"""

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
    """Get yesterday's date in Asia/Ho_Chi_Minh timezone"""
    from datetime import datetime, timedelta
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

dag = DAG(
    'call_functions_ggs_etl',
    default_args=default_args,
    description='Google Sheet Ingestion: Google Sheets -> Bronze -> Serving',
    schedule_interval='15 3 * * *',
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'googlesheet', 'iceberg'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date for backfill (YYYY-MM-DD). Leave empty to use T-1.',
        ),
        'sheet_id': Param(
            default=None,
            type=['null', 'integer'],
            description='Process specific sheet ID only. Leave empty to process all.',
        ),
        'drop_table': Param(
            default=False,
            type='boolean',
            description='Drop and recreate serving tables (full refresh)',
        ),
    },
    user_defined_macros={'yesterday_tz': get_yesterday_tz},
)

# ============================================================================
# COMMON RESOURCES
# ============================================================================

# Environment variables
common_env_vars = {
    # Bronze zone
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    
    # Gold zone (for metadata)
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    
    # MinIO config
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
    
    # Google Sheets credentials
    'GOOGLE_CREDENTIALS_PATH': '/app/credentials/probable-tape-399406-8bcc071004ba.json',
}

# DWH Product Connection (for serving layer - future use)
dwh_product_new_uri = ''
try:
    hook = PostgresHook(postgres_conn_id='dwh_product_new')
    conn = hook.get_connection('dwh_product_new')
    dwh_product_new_uri = conn.get_uri()
    
    if '?__extra__=' in dwh_product_new_uri:
        dwh_product_new_uri = dwh_product_new_uri.split('?__extra__=')[0]
    
    logger.info("✅ Loaded connection URI for dwh_product_new")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_product_new connection: {e}")

# K8s Resources
resource_requirements_bronze = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '1000m', 'memory': '1Gi'}
)

nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-scripts', mount_path='/app', read_only=True
)

# ============================================================================
# TASKS
# ============================================================================

# Task: Google Sheet Direct to Serving Ingestion
googlesheet_postgres_direct = KubernetesPodOperator(
    task_id='googlesheet_postgres_direct',
    name='googlesheet-postgres-direct',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/call_functions/googlesheet_postgres_direct.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        '{{ dag_run.conf.get("sheet_id") | default("", true) }}',
        '{{ "true" if dag_run.conf.get("drop_table") else "false" }}',
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_product_new_uri,
    },
    container_resources=resource_requirements_bronze,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Single step pipeline
googlesheet_postgres_direct
