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
    description='ERP Multi-Source Ingestion: SCI, KN, PR, HH -> Bronze -> Serving',
    schedule_interval='0 4 * * *',  
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'erp', 'multisource', 'iceberg'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date for backfill (YYYY-MM-DD). Leave empty to use T-1.',
        ),
        'table_id': Param(
            default=None,
            type=['null', 'integer'],
            description='Process specific table ID only. Leave empty to process all.',
        ),
        'load_type': Param(
            default='incremental',
            type='string',
            enum=['incremental', 'full'],
            description='Load type: incremental (default) or full.'
        ),
        'drop_table': Param(
            default=False,
            type='boolean',
            description='Drop serving table before loading (Full Refresh). WARNING: Destructive.'
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
}

# ============================================================================
# LOAD ERP CONNECTIONS (Dynamic)
# ============================================================================

erp_configs = {}
erp_conn_mapping = {
    'ERP_SCI_URI': 'sci_erp_sci',
    'ERP_KN_URI': 'sci_erp_kn',  
    'ERP_PR_URI': 'sci_erp_pr',
    'ERP_HH_URI': 'sci_erp_hh',
}

for env_var_name, conn_id in erp_conn_mapping.items():
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        
        # Get URI directly for new logic
        uri = conn.get_uri()
        if '?__extra__=' in uri:
            uri = uri.split('?__extra__=')[0]
            
        erp_configs[env_var_name] = uri
        logger.info(f"✅ Loaded ERP URI for {env_var_name} from {conn_id}")
        
    except Exception as e:
        logger.warning(f"⚠️  Could not load {env_var_name} from {conn_id}: {e}")
        erp_configs[env_var_name] = ""

# DWH Product Connection (for serving layer)
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
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

# Serving Layer Resource requirements
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

# Temp Disk Volume (for large files, preventing OOM)
temp_disk_volume = k8s.V1Volume(
    name='temp-disk',
    empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='10Gi')
)
temp_disk_mount = k8s.V1VolumeMount(
    name='temp-disk', mount_path='/tmp', read_only=False
)

# ============================================================================
# TASKS
# ============================================================================

# Task 1: ERP Bronze Ingestion (Multi-Source Loop)
erp_bronze_ingestion = KubernetesPodOperator(
    task_id='erp_bronze_ingestion',
    name='erp-multisource-bronze-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/call_functions/bronze_ingestion.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        '{{ dag_run.conf.get("table_id") | default("", true) }}',
        '{{ params.load_type }}',
        'ERP'  # ARG4: Source Type
    ],
    volumes=[nfs_volume, temp_disk_volume],
    volume_mounts=[nfs_mount, temp_disk_mount],
    env_vars={
        **common_env_vars,
        **erp_configs,  # Inject all 4 ERP configs
        'TEMP_DIR': '/tmp',
    },
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Task 2: ERP Serving Layer (PostgreSQL)
erp_serving_layer = KubernetesPodOperator(
    task_id='erp_serving_layer',
    name='erp-multisource-serving-layer',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/call_functions/serving_ingestion.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        '{{ dag_run.conf.get("table_id") | default("", true) }}',
        'ERP', # ARG3: Source Type
        '{{ params.drop_table }}' # ARG4: drop_table flag
    ],
    volumes=[nfs_volume, temp_disk_volume],
    volume_mounts=[nfs_mount, temp_disk_mount],
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_product_new_uri,
        'TEMP_DIR': '/tmp',
    },
    container_resources=resource_requirements_serving,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

erp_bronze_ingestion >> erp_serving_layer
