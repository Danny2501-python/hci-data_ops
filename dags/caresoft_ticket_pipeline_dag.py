"""
CareSoft Ticket Pipeline DAG (Unified)
Flow 2: API → Ticket Bronze → Ticket Silver

Architecture:
1. Bronze Ingestion: CareSoft API → cs_ticket_api (Bronze)
2. Silver Enrichment: Scan IDs → API detail → cs_ticket_source_api (Silver)

Schedule: Daily at 2:00 AM (same as lakehouse DAG)
Tasks run sequentially: Bronze → Silver
"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
import logging

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

dag = DAG(
    'caresoft_ticket_pipeline',
    default_args=default_args,
    description='CareSoft Ticket Pipeline: Bronze → Silver',
    schedule_interval='0 5 * * *',  # Daily at 2:00 AM (Asia/Ho_Chi_Minh)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'ticket', 'pipeline', 'flow-2'],
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
# TASK 1: BRONZE - Ticket Ingestion (All Brands)
# ============================================================================

task_ticket_ingestion = KubernetesPodOperator(
    task_id='ticket_ingestion_bronze',
    name='caresoft-ticket-ingestion-bronze',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/ticket_ingestion.py',
        '{{ ds }}',  # snapshot_date
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
# TASK 2: SILVER - Ticket Detail Enrichment (All Brands)
# ============================================================================

task_ticket_enrichment = KubernetesPodOperator(
    task_id='ticket_enrichment_silver',
    name='caresoft-ticket-enrichment-silver',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/ticket_detail_enrichment.py',
        '{{ ds }}',  # snapshot_date
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
# TASK DEPENDENCIES
# ============================================================================

# Sequential flow: Bronze → Silver
task_ticket_ingestion >> task_ticket_enrichment
