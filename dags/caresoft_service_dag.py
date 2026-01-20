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
NAMESPACE = 'airflow-jobs'
PYTHON_ETL_IMAGE = '10.1.20.120:5000/python-etl-base:1.1.0'

# Resource Limits
resource_requirements_ingestion = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1000m", "memory": "2Gi"}
)

resource_requirements_serving = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},
    limits={"cpu": "1000m", "memory": "1Gi"}
)

# Volume Configuration
nfs_volume = k8s.V1Volume(
    name='nfs-volume',
    nfs=k8s.V1NFSVolumeSource(
        server='10.1.20.20',
        path='/volume1/projects'
    )
)

nfs_mount = k8s.V1VolumeMount(
    name='nfs-volume',
    mount_path='/app',
    sub_path='data-ops/app'
)

duckdb_volume = k8s.V1Volume(
    name='duckdb-scratch',
    empty_dir=k8s.V1EmptyDirVolumeSource()
)

duckdb_mount = k8s.V1VolumeMount(
    name='duckdb-scratch',
    mount_path='/tmp/duckdb'
)

# Environment Variables
common_env_vars = {
    'ICEBERG_REST_URI_BRONZE': Variable.get("ICEBERG_REST_URI_BRONZE"),
    'CATALOG_NAME_BRONZE': Variable.get("CATALOG_NAME_BRONZE"),
    'S3_ENDPOINT': Variable.get("S3_ENDPOINT"),
    'AWS_ACCESS_KEY_ID': Variable.get("AWS_ACCESS_KEY_ID"),
    'AWS_SECRET_ACCESS_KEY': Variable.get("AWS_SECRET_ACCESS_KEY"),
    'AWS_REGION': Variable.get("AWS_REGION"),
    'PYTHONPATH': '/app'
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
    schedule_interval='10 04 * * *',  # 04:10 daily (matches old cron)
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
    arguments=['/app/caresoft/service/service_ingestion.py', '{{ params.snapshot_date if params.snapshot_date else ds }}'],
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
    arguments=['/app/caresoft/service/service_serving_layer.py', '{{ params.snapshot_date if params.snapshot_date else ds }}'],
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
