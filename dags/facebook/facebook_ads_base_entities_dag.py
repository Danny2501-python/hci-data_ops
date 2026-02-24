"""
Airflow DAG for Facebook Ads Base Entities ETL
Fetches Campaigns, Ad Sets, and Creatives SEQUENTIALLY
Tasks run in sequence to avoid hitting Facebook API rate limits (200 calls/hour per account)
"""

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
    'facebook_ads_base_entities_etl',
    default_args=default_args,
    description='Facebook Ads Base Entities: Campaigns → Ad Sets → Creatives (Sequential)',
    schedule_interval=None,  # Triggered by Master DAG or manually (was: '0 3 * * *')
    start_date=datetime(2026, 2, 5, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'facebook_ads', 'base_entities', 'marketing'],
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

# ============================================================================
# COMMON RESOURCES
# ============================================================================

common_env_vars = {
    'POSTGRES_URI': dwh_product_new_uri,
    'FB_API_VERSION': 'v18.0',
}

# K8s Resources
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '1000m', 'memory': '1Gi'}
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
# TASKS - BASE ENTITIES (NO DEPENDENCIES, RUN IN PARALLEL)
# ============================================================================

# Task 1: Campaigns
facebook_ads_campaigns = KubernetesPodOperator(
    task_id='facebook_ads_campaigns',
    name='facebook-ads-campaigns',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/facebook/facebook_ads_campaigns_ingestion.py',
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

# Task 2: Ad Sets
facebook_ads_adsets = KubernetesPodOperator(
    task_id='facebook_ads_adsets',
    name='facebook-ads-adsets',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/facebook/facebook_ads_adsets_ingestion.py',
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

# Task 3: Creatives
facebook_ads_creatives = KubernetesPodOperator(
    task_id='facebook_ads_creatives',
    name='facebook-ads-creatives',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/facebook/facebook_ads_creatives_ingestion.py',
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

# Run tasks SEQUENTIALLY to avoid hitting Facebook API rate limits
# All 3 tasks use the same Facebook Ad Account and share the same rate limit quota
# Running in parallel causes rate limit errors (200 calls/hour per account)
facebook_ads_campaigns >> facebook_ads_adsets >> facebook_ads_creatives
