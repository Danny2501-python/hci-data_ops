"""
CareSoft Ticket Pipeline DAG
Flow 2: API → Ticket Source

Architecture:
1. Bronze Ingestion: CareSoft API → cs_ticket_bronze
2. Silver Enrichment: Scan IDs → API detail → cs_ticket_source_api

Schedule:
- Bronze: Daily at 01:30 and 08:35 (2 runs/day)
- Silver: Every 12 minutes (120 runs/day)
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
# DAG 1: TICKET BRONZE INGESTION (Daily: 01:30, 08:35)
# ============================================================================

default_args_bronze = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag_ticket_bronze = DAG(
    'caresoft_ticket_bronze_ingestion',
    default_args=default_args_bronze,
    description='CareSoft Ticket Bronze Ingestion (API → Iceberg)',
    schedule_interval='30 1,8 * * *',  # 01:30 and 08:35 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'ticket', 'bronze', 'flow-2'],
)

# ============================================================================
# DAG 2: TICKET SILVER ENRICHMENT (Every 12 minutes)
# ============================================================================

default_args_silver = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,  # Less retries for high-frequency job
    'retry_delay': timedelta(minutes=2),
}

dag_ticket_silver = DAG(
    'caresoft_ticket_silver_enrichment',
    default_args=default_args_silver,
    description='CareSoft Ticket Silver Enrichment (Scan ID → API Detail)',
    schedule_interval='*/12 * * * *',  # Every 12 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'ticket', 'silver', 'flow-2'],
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
    requests={'cpu': '1000m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

resource_requirements_silver = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '1Gi'},
    limits={'cpu': '1000m', 'memory': '2Gi'}
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
# BRAND CONFIGURATION
# ============================================================================

# Brands are loaded from Gold layer core_brand table by the application scripts
# No need to specify brand IDs here - scripts will loop all active brands

# ============================================================================
# BRONZE TASK: Ticket Ingestion (All Brands)
# ============================================================================

task_ticket_ingestion = KubernetesPodOperator(
    task_id='ticket_ingestion',
    name='caresoft-ticket-ingestion',
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
    
    env_vars={
        **common_env_vars,
        # Gold zone (for loading core_brand table)
        'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
        'CATALOG_NAME_GOLD': 'gold',
    },
    
    container_resources=resource_requirements_bronze,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag_ticket_bronze,
)

# ============================================================================
# SILVER TASK: Ticket Detail Enrichment (All Brands)
# ============================================================================

task_ticket_detail_enrichment = KubernetesPodOperator(
    task_id='ticket_detail_enrichment',
    name='caresoft-ticket-detail-enrichment',
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
        # Gold zone (for loading core_brand table)
        'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
        'CATALOG_NAME_GOLD': 'gold',
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
    },
    
    container_resources=resource_requirements_silver,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag_ticket_silver,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# No dependencies - each DAG has single task
# Bronze and Silver are in separate DAGs with different schedules:
# - Bronze: Daily at 01:30 and 08:35
# - Silver: Every 12 minutes
