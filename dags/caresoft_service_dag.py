from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable, Param
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from kubernetes.client import models as k8s
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Common configuration
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'

# Resource Limits
resource_requirements_ingestion = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1000m", "memory": "2Gi"}
)

resource_requirements_serving = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},
    limits={"cpu": "1000m", "memory": "1Gi"}
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
    empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='2Gi')
)
duckdb_mount = k8s.V1VolumeMount(
    name='duckdb-temp', mount_path='/tmp/duckdb', read_only=False
)

# Environment Variables
# Environment Variables
common_env_vars = {
    # Bronze zone
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    
    # Gold zone (for brand loading)
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    
    # MinIO config
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',

    # CareSoft API config
    'CARESOFT_PAGE_SIZE': '500',
}

# DWH Connection
dwh_uri = ''
try:
    hook = PostgresHook(postgres_conn_id='dwh_product')
    conn = hook.get_connection('dwh_product')
    dwh_uri = conn.get_uri()
    
    if '?__extra__=' in dwh_uri:
        dwh_uri = dwh_uri.split('?__extra__=')[0]
    
    logger.info("✅ Loaded connection URI for dwh_product")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_product connection: {e}")

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-ops',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'caresoft_service_etl',
    default_args=default_args,
    description='CareSoft Services Pipeline: Bronze → Serving',
    schedule_interval='15 01 * * *',  # 04:10 daily (matches old cron)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'service', 'etl', 'iceberg'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date for backfill (YYYY-MM-DD). Leave empty to use execution date.',
        )
    },
)

# ============================================================================
# TASKS
# ============================================================================

task_service_ingestion = KubernetesPodOperator(
    task_id='service_ingestion_bronze',
    name='caresoft-service-ingestion-bronze',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=['/app/caresoft/service/service_ingestion.py', '{{ (dag_run.conf or {}).get("snapshot_date") or ds }}'],
    volumes=[nfs_volume, duckdb_volume],
    volume_mounts=[nfs_mount, duckdb_mount],
    env_vars={**common_env_vars, 'DUCKDB_TEMP_DIR': '/tmp/duckdb'},
    container_resources=resource_requirements_ingestion,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

task_service_serving = KubernetesPodOperator(
    task_id='service_serving_layer',
    name='caresoft-service-serving-layer',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=['/app/caresoft/service/service_serving_layer.py', '{{ (dag_run.conf or {}).get("snapshot_date") or ds }}'],
    volumes=[nfs_volume, duckdb_volume],
    volume_mounts=[nfs_mount, duckdb_mount],
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_uri,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb'
    },
    container_resources=resource_requirements_serving,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

task_service_ingestion >> task_service_serving
