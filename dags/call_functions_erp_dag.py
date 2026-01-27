from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.param import Param
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
    'call_functions_erp_etl',
    default_args=default_args,
    description='ETL Staging SCI: Source DBs → Bronze Iceberg',
    schedule_interval='0 1 * * *',  # Daily at 01:00 AM
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'call_functions', 'bronze', 'iceberg'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date for backfill (YYYY-MM-DD). Leave empty to use T-1.',
        ),
        'table_id': Param(
            default=None,
            type=['null', 'integer'],
            description='Process specific table ID only. Leave empty to process all tables.',
        ),
        'load_type': Param(
            default='incremental',
            type='string',
            enum=['incremental', 'full'],
            description='Load type: incremental (default) or full.'
        )
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
    
    # Source database connections (need to configure these)
    # Source database connections
    # Source database connections
    'DEFAULT_CONFIG': json.dumps({
        'user': 'admin',
        'password': 'QtdlxxrisA@!sfsgo#2024',
        'host': '10.10.10.214',
        'port': 5432,
        'dbname': 'etl_marketing'
    }),
    'CUSTOM_DB_CONFIG': json.dumps({
        'user': 'admin',
        'password': 'QtdlxxrisA@!sfsgo#2024',
        'host': '10.10.10.214',
        'port': 5432,
        'dbname': 'staging_sci'
    }),
    'CUSTOM_DB_DEV_CONFIG': json.dumps({
        'user': 'admin',
        'password': 'QtdlxxrisA@!sfsgo#2024',
        'host': '10.10.10.214',
        'port': 5432,
        'dbname': 'staging_sci'
    }),
    'DU_PHONG_CONFIG': json.dumps({
        'user': 'admin',
        'password': 'QtdlxxrisA@!sfsgo#2024',
        'host': '10.10.10.226',
        'port': 5432,
        'dbname': 'staging_sci'
    }),
    'ERP_SCI_CONFIG': json.dumps({
        'user': 'reporter',
        'password': 'xjsfpwzlqehd',
        'host': '10.10.10.154',
        'port': 5432,
        'dbname': 'sci_erp'
    }),
    'DEV_SCI_CONFIG': json.dumps({
        'user': 'reporter',
        'password': 'xjsfpwzlqehd',
        'host': '10.10.10.152',
        'port': 5432,
        'dbname': 'sci_erp'
    }),
    'TEST_SCI_CONFIG': json.dumps({
        'user': 'reporter',
        'password': 'xjsfpwzlqehd',
        'host': '10.10.10.249',
        'port': 5432,
        'dbname': 'sci_erp'
    }),
    'GIAOVIEC_CONFIG': json.dumps({
        'user': 'reader',
        'password': 'Sci@123',
        'host': '10.10.10.153',
        'port': 54222,
        'dbname': 'project'
    }),
    'SEEDING_CONFIG': json.dumps({
        'user': 'reader',
        'password': 'seeding@123',
        'host': '10.10.10.234',
        'port': 54300,
        'dbname': 'sale'
    }),
    'HONGHA_CONFIG': json.dumps({
        'user': 'reporter',
        'password': 'hovxmsgewkru',
        'host': 'db.benhvienhongha.com.vn',
        'port': 55000,
        'dbname': 'bvhongha'
    }),
    'PORTAL_GUIDE_CONFIG': json.dumps({
        'user': 'admin',
        'password': 'QtdlxxrisA@!sfsgo#2024',
        'host': '10.10.10.230',
        'port': 5432,
        'dbname': 'portal_guide'
    }),
}

# ============================================================================
# LOAD AIRFLOW CONNECTIONS
# ============================================================================

# DWH Product Connection (for serving layer)
dwh_product_uri = ''
try:
    hook = PostgresHook(postgres_conn_id='dwh_product')
    conn = hook.get_connection('dwh_product')
    dwh_product_uri = conn.get_uri()
    
    if '?__extra__=' in dwh_product_uri:
        dwh_product_uri = dwh_product_uri.split('?__extra__=')[0]
    
    logger.info("✅ Loaded connection URI for dwh_product")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_product connection: {e}")
    # Fallback to empty string or handle error appropriately
    # The serving task will fail if URI is missing, which is expected

# Resource requirements
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

# NFS Volume (for scripts)
nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-scripts', mount_path='/app', read_only=True
)

# DuckDB Temp Volume
duckdb_volume = k8s.V1Volume(
    name='duckdb-temp',
    empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='5Gi')
)
duckdb_mount = k8s.V1VolumeMount(
    name='duckdb-temp', mount_path='/tmp/duckdb', read_only=False
)

# ============================================================================
# TASKS
# ============================================================================

# Task 1: Bronze Ingestion
bronze_ingestion = KubernetesPodOperator(
    task_id='bronze_ingestion',
    name='etl-staging-sci-bronze-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/call_functions/bronze_ingestion.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        '{{ dag_run.conf.get("table_id") | default("", true) }}',
        '{{ params.load_type }}'
    ],
    volumes=[nfs_volume, duckdb_volume],
    volume_mounts=[nfs_mount, duckdb_mount],
    env_vars={
        **common_env_vars,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb'
    },
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Task 2: Serving Layer Ingestion (PostgreSQL)
serving_layer = KubernetesPodOperator(
    task_id='serving_layer',
    name='etl-staging-sci-serving-layer',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/call_functions/serving_ingestion.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        '{{ dag_run.conf.get("table_id") | default("", true) }}'
    ],
    volumes=[nfs_volume, duckdb_volume],
    volume_mounts=[nfs_mount, duckdb_mount],
    env_vars={
        **common_env_vars,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
        'POSTGRES_URI': dwh_product_uri,
    },
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

bronze_ingestion >> serving_layer
