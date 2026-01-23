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

dag = DAG(
    'caresoft_contact_etl',
    default_args=default_args,
    description='CareSoft Contact Pipeline: Bronze → Silver → Serving',
    schedule_interval='0 1 * * *',  # 01:00 AM daily
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'contact', 'etl', 'iceberg'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date for backfill (YYYY-MM-DD). Leave empty to use execution date.',
        )
    },
)

# ============================================================================
# COMMON RESOURCES
# ============================================================================

# Environment variables (Iceberg catalogs + MinIO)
common_env_vars = {
    # Bronze zone
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    
    # Silver zone
    'ICEBERG_REST_URI_SILVER': 'http://iceberg-rest-silver-zone:8181',
    'CATALOG_NAME_SILVER': 'silver',
    
    # Gold zone (for loading core_brand table)
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

# Resource requirements
resource_requirements_bronze = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '1Gi'},
    limits={'cpu': '2000m', 'memory': '2Gi'}
)

resource_requirements_silver = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '1Gi'},
    limits={'cpu': '2000m', 'memory': '2Gi'}
)

# Serving layer needs less resources (only UPSERT to PostgreSQL)
resource_requirements_serving = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '1000m', 'memory': '1Gi'}
)

# NFS Volume (for scripts)
nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(
        server=NFS_SERVER,
        path=NFS_PATH
    )
)

nfs_volume_mount = k8s.V1VolumeMount(
    name='nfs-scripts',
    mount_path='/app',
    read_only=True
)

# DuckDB Temp Volume
duckdb_temp_volume = k8s.V1Volume(
    name='duckdb-temp',
    empty_dir=k8s.V1EmptyDirVolumeSource(
        medium='',
        size_limit='5Gi'
    )
)

duckdb_temp_volume_mount = k8s.V1VolumeMount(
    name='duckdb-temp',
    mount_path='/tmp/duckdb',
    read_only=False
)

# ============================================================================
# TASK 1: BRONZE - Contact Ingestion (All Brands)
# ============================================================================

task_contact_ingestion = KubernetesPodOperator(
    task_id='contact_ingestion_bronze',
    name='caresoft-contact-ingestion-bronze',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/contact/contact_ingestion.py',
        '{{ dag_run.conf.get("snapshot_date") | default("", true) }}'
    ],
    
    volumes=[nfs_volume],
    volume_mounts=[nfs_volume_mount],
    
    env_vars=common_env_vars,
    
    container_resources=resource_requirements_bronze,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK 2: SILVER - Contact Detail Enrichment (All Brands)
# ============================================================================

task_contact_enrichment = KubernetesPodOperator(
    task_id='contact_enrichment_silver',
    name='caresoft-contact-enrichment-silver',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/contact/contact_detail_enrichment.py',
        '{{ dag_run.conf.get("snapshot_date") | default("", true) }}'
    ],
    
    volumes=[nfs_volume, duckdb_temp_volume],
    volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
    
    env_vars={
        **common_env_vars,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
    },
    
    container_resources=resource_requirements_silver,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK 3: SERVING - Contact Detail to PostgreSQL DWH
# ============================================================================

task_contact_serving = KubernetesPodOperator(
    task_id='contact_detail_serving_layer',
    name='caresoft-contact-detail-serving',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/contact/contact_detail_serving_layer.py',
        '{{ dag_run.conf.get("snapshot_date") | default("", true) }}'
    ],
    
    volumes=[nfs_volume, duckdb_temp_volume],
    volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
    
    env_vars={
        **common_env_vars,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
        'POSTGRES_URI': dwh_product_uri,
    },
    
    container_resources=resource_requirements_serving,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Contact Pipeline Flow:
# 1. Bronze Ingestion → Silver Enrichment → Serving Layer
task_contact_ingestion >> task_contact_enrichment >> task_contact_serving
