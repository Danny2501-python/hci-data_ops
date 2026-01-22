from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.postgres_hook import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.param import Param
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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'caresoft_deal_etl',
    default_args=default_args,
    description='CareSoft ETL: ERP → Bronze → Silver → Gold → Serving',
    schedule_interval='30 1 * * *',  # Daily at 1:30 AM UTC+7
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['caresoft', 'deal', 'etl', 'iceberg'],
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
    
    # Gold zone
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    
    # MinIO config
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
}

# Resource requirements (optimized for cluster capacity)
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'},   # Reduced to fit available memory
    limits={'cpu': '2000m', 'memory': '3Gi'}     # Sufficient for DuckDB transformations
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

# DuckDB Temp Volume (disk-backed for spilling)
duckdb_temp_volume = k8s.V1Volume(
    name='duckdb-temp',
    empty_dir=k8s.V1EmptyDirVolumeSource(
        medium='',  # Empty string = disk-backed (not RAM)
        size_limit='10Gi'
    )
)

duckdb_temp_volume_mount = k8s.V1VolumeMount(
    name='duckdb-temp',
    mount_path='/tmp/duckdb',
    read_only=False
)

# ============================================================================
# LOAD DATABASE CONNECTIONS (at DAG parse time)
# ============================================================================

# ERP Connections
erp_connection_env_vars = {}
erp_connections_config = {
    'SCI': 'sci_erp_sci',
    'KN': 'sci_erp_kn', 
    'PR': 'sci_erp_pr',
    'HH': 'sci_erp_hh',
}

for erp_name, conn_id in erp_connections_config.items():
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        uri = conn.get_uri()
        
        # Clean URI: Remove __extra__ parameter
        if '?__extra__=' in uri:
            uri = uri.split('?__extra__=')[0]
        
        erp_connection_env_vars[f'ERP_{erp_name}_URI'] = uri
        logger.info(f"✅ Loaded connection URI for ERP {erp_name}")
        
    except Exception as e:
        logger.warning(f"⚠️  Could not load {erp_name} connection: {e}")
        erp_connection_env_vars[f'ERP_{erp_name}_URI'] = ''

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
# TASK 1: ERP → BRONZE (Webhook Ingestion)
# ============================================================================

task_erp_webhook_ingestion = KubernetesPodOperator(
    task_id='erp_direct_ingestion',
    name='caresoft-erp-direct-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/deal/erp_webhook_ingestion.py',
        '{{ dag_run.conf.get("snapshot_date", "") }}'
    ],
    
    volumes=[nfs_volume],
    volume_mounts=[nfs_volume_mount],
    
    env_vars={
        **common_env_vars,
        **erp_connection_env_vars,
        'BRONZE_TABLE': 'caresoft.wehook_api_caresoft',
    },
    
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK 2: BRONZE → SILVER (Webhook Transformation)
# ============================================================================

task_webhook_transformation_silver = KubernetesPodOperator(
    task_id='webhook_transformation_silver',
    name='caresoft-webhook-transformation-silver',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/deal/webhook_transformation_silver.py',
        '{{ dag_run.conf.get("snapshot_date", "") }}'
    ],
    
    volumes=[nfs_volume, duckdb_temp_volume],
    volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
    
    env_vars={
        **common_env_vars,
        'BRONZE_TABLE': 'caresoft.wehook_api_caresoft',
        'SILVER_TABLE_DEAL': 'caresoft.cs_deal_wehook',
        'SILVER_TABLE_DEAL_ERROR': 'caresoft.cs_deal_wehook_error',
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
    },
    
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK 3: SILVER → GOLD (Deal Detail Enrichment via API)
# ============================================================================

task_deal_detail_enrichment = KubernetesPodOperator(
    task_id='deal_detail_enrichment',
    name='caresoft-deal-detail-enrichment',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/deal/deal_detail_enrichment.py',
        '{{ dag_run.conf.get("snapshot_date", "") }}'
    ],
    
    volumes=[nfs_volume, duckdb_temp_volume],
    volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
    
    env_vars={
        **common_env_vars,
        'WEBHOOK_TABLE': 'caresoft.cs_deal_wehook',
        'DEAL_DETAIL_TABLE': 'caresoft.cs_deal_detail_api',
        'DEAL_DETAIL_ERROR_TABLE': 'caresoft.cs_deal_detail_error_api',
        'BRAND_TABLE': 'caresoft.core_brand',
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
    },
    
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK 4: GOLD → SERVING (Refresh PostgreSQL Serving Layer)
# ============================================================================

task_refresh_serving_layer = KubernetesPodOperator(
    task_id='refresh_serving_layer',
    name='caresoft-refresh-serving-layer',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/deal/deal_detail_serving_layer.py',
        '{{ dag_run.conf.get("snapshot_date", "") }}'
    ],
    
    volumes=[nfs_volume, duckdb_temp_volume],
    volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
    
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_product_uri,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
        'SERVING_TABLE_NAME': 'cs_deal_detail_api_v1',
    },
    
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

task_refresh_serving_layer_v2 = KubernetesPodOperator(
    task_id='refresh_serving_layer_v2',
    name='caresoft-refresh-serving-layer-v2',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/deal/deal_detail_serving_layer.py',
        '{{ dag_run.conf.get("snapshot_date", "") }}'
    ],
    
    volumes=[nfs_volume, duckdb_temp_volume],
    volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
    
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_product_uri,
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
        'SERVING_TABLE_NAME': 'cs_deal_detail_api_v2',
    },
    
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)



# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# SIMPLIFIED FLOW: ERP → Bronze → Silver → Gold Enrichment → Serving (Production V1 & Production V2)
task_erp_webhook_ingestion >> task_webhook_transformation_silver >> task_deal_detail_enrichment
task_deal_detail_enrichment >> [task_refresh_serving_layer, task_refresh_serving_layer_v2]
