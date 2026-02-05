"""
Airflow DAG for Facebook Ads ETL Pipeline - SIMPLIFIED VERSION
Migrated from etl_marketing Django cron jobs
Fetches Facebook Ads insights and stores to PostgreSQL serving layer
Uses simplified script (no foreign keys, direct write)
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
    'call_functions_facebook_ads_simple_etl',
    default_args=default_args,
    description='Facebook Ads ETL (SIMPLIFIED): Facebook API → PostgreSQL Serving (No FK)',
    schedule_interval='0 4 * * *',  # Run daily at 4 AM
    start_date=datetime(2026, 2, 5, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'facebook_ads', 'serving', 'marketing', 'simplified'],
    params={
        'days_for_ago': Param(
            default=1,
            type='integer',
            description='Number of days ago to fetch data for (1 = yesterday)',
        ),
    },
)

# ============================================================================
# LOAD POSTGRES CONNECTION
# ============================================================================

# Custom DB Dev Connection (for Facebook Ads - existing Django tables)
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

# Environment variables
common_env_vars = {
    # PostgreSQL config from Airflow connection
    'POSTGRES_URI': dwh_dev_uri,
    
    # Facebook API config
    'FB_API_VERSION': 'v18.0',
}

# K8s Resources - Higher for Facebook API processing
resource_requirements_facebook = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '1Gi'},
    limits={'cpu': '2000m', 'memory': '2Gi'}
)

# NFS Volume (for scripts)
nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-scripts', mount_path='/app', read_only=True
)

# ============================================================================
# TASKS - SIMPLIFIED VERSION
# ============================================================================

# Task: Facebook Ads Insights Ingestion (T-1) - SIMPLIFIED
facebook_ads_insights_simple_t1 = KubernetesPodOperator(
    task_id='facebook_ads_insights_simple_t1',
    name='facebook-ads-insights-simple-t1',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/facebook/facebook_ads_insights_ingestion_simple.py',
        # Argument 1: days_for_ago
        '{{ dag_run.conf.get("days_for_ago") or 1 }}',
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=common_env_vars,
    container_resources=resource_requirements_facebook,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# # Task: Facebook Ads Insights Ingestion (T-2) - SIMPLIFIED
# facebook_ads_insights_simple_t2 = KubernetesPodOperator(
#     task_id='facebook_ads_insights_simple_t2',
#     name='facebook-ads-insights-simple-t2',
#     namespace=NAMESPACE,
#     image=PYTHON_ETL_IMAGE,
#     image_pull_policy='IfNotPresent',
#     cmds=['python'],
#     arguments=[
#         '/app/facebook/facebook_ads_insights_ingestion_simple.py',
#         # Argument 1: days_for_ago
#         '2',
#     ],
#     volumes=[nfs_volume],
#     volume_mounts=[nfs_mount],
#     env_vars=common_env_vars,
#     container_resources=resource_requirements_facebook,
#     is_delete_operator_pod=True,
#     get_logs=True,
#     dag=dag,
# )

# # Task: Facebook Ads Insights Ingestion (T-3) - SIMPLIFIED
# facebook_ads_insights_simple_t3 = KubernetesPodOperator(
#     task_id='facebook_ads_insights_simple_t3',
#     name='facebook-ads-insights-simple-t3',
#     namespace=NAMESPACE,
#     image=PYTHON_ETL_IMAGE,
#     image_pull_policy='IfNotPresent',
#     cmds=['python'],
#     arguments=[
#         '/app/facebook/facebook_ads_insights_ingestion_simple.py',
#         # Argument 1: days_for_ago
#         '3',
#     ],
#     volumes=[nfs_volume],
#     volume_mounts=[nfs_mount],
#     env_vars=common_env_vars,
#     container_resources=resource_requirements_facebook,
#     is_delete_operator_pod=True,
#     get_logs=True,
#     dag=dag,
# )

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Run T-1, T-2, T-3 in sequence (to avoid rate limiting)
facebook_ads_insights_simple_t1
