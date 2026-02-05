"""
Airflow DAG for Facebook Ads (Ad Ads) ETL
Fetches Ads with automatic dependency creation
Depends on: Base Entities DAG (Campaigns, Ad Sets, Creatives)
"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
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

dag = DAG(
    'facebook_ads_ads_etl',
    default_args=default_args,
    description='Facebook Ads (Ad Ads): Fetches Ads with dependency creation',
    schedule_interval='0 4 * * *',  # Run daily at 4 AM (after Base Entities DAG)
    start_date=datetime(2026, 2, 5, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'facebook_ads', 'ads', 'marketing'],
    params={
        'date_preset': Param(
            default='last_90d',
            type='string',
            description='Date preset for Facebook API (last_90d, last_30d, etc.)',
        ),
    },
)

# ============================================================================
# LOAD POSTGRES CONNECTION
# ============================================================================

dwh_dev_uri = ''
try:
    hook = PostgresHook(postgres_conn_id='dwh_dev')
    conn = hook.get_connection('dwh_dev')
    dwh_dev_uri = conn.get_uri()
    
    if '?__extra__=' in dwh_dev_uri:
        dwh_dev_uri = dwh_dev_uri.split('?__extra__=')[0]
    
    logger.info("✅ Loaded connection URI for dwh_dev")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_dev connection: {e}")

# ============================================================================
# COMMON RESOURCES
# ============================================================================

common_env_vars = {
    'POSTGRES_URI': dwh_dev_uri,
    'FB_API_VERSION': 'v18.0',
}

# K8s Resources - Higher for Ads (creates dependencies)
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '1Gi'},
    limits={'cpu': '2000m', 'memory': '2Gi'}
)

# NFS Volume
nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-scripts', mount_path='/app', read_only=True
)

# ============================================================================
# TASK - ADS (WITH DEPENDENCY CREATION)
# ============================================================================

facebook_ads_ads = KubernetesPodOperator(
    task_id='facebook_ads_ads',
    name='facebook-ads-ads',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/facebook/facebook_ads_ads_ingestion.py',
        '{{ params.date_preset }}',
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=common_env_vars,
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Wait for base entities, then run ads
facebook_ads_ads
