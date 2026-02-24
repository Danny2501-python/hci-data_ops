"""
Airflow DAG for Facebook Ads Token Refresh Tool
Allows manual refreshing of Long-Lived Access Tokens via Airflow UI
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
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

DAG_ID = "facebook_ads_token_refresh_tool"
DAG_DESCRIPTION = "Tool để sinh và cập nhật Long-Lived Access Token cho Facebook Ads Account"

DAG_PARAMS = {
    "ad_account_id": Param(
        default="", 
        type="string", 
        description="Nhập Facebook Ad Account ID (Ví dụ: 6217684194909373)"
    ),
    "short_lived_token": Param(
        default="", 
        type="string", 
        description="Nhập Short-Lived Access Token (Lấy từ Graph API Explorer hoặc F12)",
        format="password" # Ẩn ký tự khi nhập
    )
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=DAG_DESCRIPTION,
    schedule_interval=None,
    start_date=datetime(2026, 2, 7, tzinfo=local_tz),
    catchup=False,
    tags=['tool', 'facebook_ads', 'token'],
    params=DAG_PARAMS
)

# ============================================================================
# LOAD POSTGRES CONNECTION
# ============================================================================

dwh_product_new_uri = ''
try:
    hook = PostgresHook(postgres_conn_id='dwh_product_new')
    conn = hook.get_connection('dwh_product_new')
    dwh_product_new_uri = conn.get_uri()
    
    # Remove extra params usually added by Airflow that might confuse some lib
    if '?__extra__=' in dwh_product_new_uri:
        dwh_product_new_uri = dwh_product_new_uri.split('?__extra__=')[0]
    
    logger.info("✅ Loaded connection URI for dwh_product_new")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_product_new connection: {e}")

# ============================================================================
# RESOURCES & VOLUMES
# ============================================================================

env_vars = {
    'POSTGRES_URI': dwh_product_new_uri,
    'FB_API_VERSION': 'v18.0',
}

# K8s Resources (Small for a simple script)
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '250m', 'memory': '256Mi'},
    limits={'cpu': '500m', 'memory': '512Mi'}
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
# TASKS
# ============================================================================

token_refresh_task = KubernetesPodOperator(
    task_id='refresh_facebook_token',
    name='fb-token-refresh',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/facebook/facebook_token_refresh.py',
        '{{ params.ad_account_id }}',
        '{{ params.short_lived_token }}'
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=env_vars,
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

token_refresh_task
