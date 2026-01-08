from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Configuration
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'
NAMESPACE = 'default'

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'caresoft_lakehouse_etl',
    default_args=default_args,
    description='CareSoft ETL: ERP → Bronze → Silver → Gold',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM UTC+7
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['caresoft', 'lakehouse', 'etl', 'iceberg'],
)


# Common volume configuration for NFS
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

# Common environment variables
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

# Common# Resource requirements for pods
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '8Gi'}
)


# ============================================================================
# FLOW 1: ERP Direct Ingestion (ERP api_log → Iceberg Bronze webhook_raw)
# ============================================================================

# Load ERP connection URIs from Airflow at DAG parse time
# Using get_uri() to get complete connection string
from airflow.hooks.postgres_hook import PostgresHook

erp_connection_env_vars = {}
erp_connections_config = {
    'SCI': 'sci_erp_sci',
    'KN': 'sci_erp_kn', 
    'PR': 'sci_erp_pr',
    'HH': 'sci_erp_hh',
}

# Load connection URIs at DAG parse time
for erp_name, conn_id in erp_connections_config.items():
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        
        # Get complete connection URI (postgresql://user:password@host:port/database)
        uri = conn.get_uri()
        
        # Clean URI: Remove __extra__ parameter that causes psycopg2 errors
        if '?__extra__=' in uri:
            uri = uri.split('?__extra__=')[0]
            logger.info(f"Cleaned __extra__ parameter from {erp_name} URI")
        
        erp_connection_env_vars[f'ERP_{erp_name}_URI'] = uri
        
        logger.info(f"✅ Loaded connection URI for ERP {erp_name}")
        
    except Exception as e:
        logger.warning(f"⚠️  Could not load {erp_name} connection at parse time: {e}")
        # Set empty default if connection not found
        erp_connection_env_vars[f'ERP_{erp_name}_URI'] = ''


task_erp_webhook_ingestion = KubernetesPodOperator(
    task_id='erp_direct_ingestion',
    name='caresoft-erp-direct-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=['/app/caresoft/erp_webhook_ingestion.py', '{{ ds }}'],  # Always full load (drop partition + append)
    
    # Mount NFS for scripts
    volumes=[nfs_volume],
    volume_mounts=[nfs_volume_mount],
    
    # Environment variables - Bronze zone with ERP connections
    env_vars={
        **common_env_vars,
        **erp_connection_env_vars,  # Add ERP connections from PostgresHook
        # Task-specific table name (match PostgreSQL naming)
        'BRONZE_TABLE': 'caresoft.wehook_api_caresoft',
    },
    
    # Resource limits
    container_resources=resource_requirements,
    
    # Pod settings
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)