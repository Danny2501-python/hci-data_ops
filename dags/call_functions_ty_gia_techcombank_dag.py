"""
Airflow DAG for Ty Gia Techcombank ETL Pipeline
Bronze (Techcombank API -> Iceberg)
Fetches exchange rate data from Techcombank and stores in Bronze layer
"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
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

def get_30_days_ago_tz():
    """Get date 30 days ago in Asia/Ho_Chi_Minh timezone"""
    from datetime import datetime, timedelta
    now = datetime.now(local_tz)
    days_ago = now - timedelta(days=30)
    return days_ago.strftime('%Y-%m-%d')

dag = DAG(
    'call_functions_ty_gia_techcombank_etl',
    default_args=default_args,
    description='Ty Gia Techcombank Ingestion: Techcombank API -> Bronze Iceberg',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'ty-gia', 'techcombank', 'iceberg', 'bronze'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date for partitioning (YYYY-MM-DD). Leave empty to use T-1.',
        ),
        'start_date': Param(
            default=None,
            type=['null', 'string'],
            description='Start date for fetching exchange rates (YYYY-MM-DD). Leave empty to use T-30.',
        ),
        'end_date': Param(
            default=None,
            type=['null', 'string'],
            description='End date for fetching exchange rates (YYYY-MM-DD). Leave empty to use T-1.',
        ),
        'table_name': Param(
            default='ty_gia_techcombank',
            type='string',
            description='Target Iceberg table name',
        ),
    },
    user_defined_macros={
        'yesterday_tz': get_yesterday_tz,
        'days_30_ago_tz': get_30_days_ago_tz,
    },
)

# ============================================================================
# COMMON RESOURCES
# ============================================================================

# Environment variables
common_env_vars = {
    # Bronze zone
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    
    # Gold zone (for metadata if needed)
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    
    # MinIO config
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
}

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

# Task: Ty Gia Techcombank Bronze Ingestion
ty_gia_techcombank_bronze_ingestion = KubernetesPodOperator(
    task_id='ty_gia_techcombank_bronze_ingestion',
    name='ty-gia-techcombank-bronze-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/call_functions/ty_gia_techcombank_ingestion.py',
        # Argument 1: snapshot_date (for partitioning)
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        # Argument 2: start_date (for API fetching)
        '{{ dag_run.conf.get("start_date") or days_30_ago_tz() }}',
        # Argument 3: end_date (for API fetching)
        '{{ dag_run.conf.get("end_date") or yesterday_tz() }}',
        # Argument 4: table_name
        '{{ dag_run.conf.get("table_name") or "ty_gia_techcombank" }}',
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=common_env_vars,
    container_resources=resource_requirements_bronze,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Single task - no dependencies
ty_gia_techcombank_bronze_ingestion
