"""
Airflow DAG for Facebook Ads Insights ETL
Fetches Insights and Message Insights with automatic dependency creation
Depends on: Ads DAG (which depends on Base Entities DAG)
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


# ============================================================================
# LOAD POSTGRES CONNECTION
# ============================================================================

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
# COMMON RESOURCES
# ============================================================================

common_env_vars = {
    'POSTGRES_URI': dwh_product_uri,
    'FB_API_VERSION': 'v22.0',
}

# K8s Resources - Higher for Insights (creates dependencies + processes data)
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

def create_dag(dag_id, schedule, days_for_ago):
    dag = DAG(
        dag_id,
        default_args=default_args,
        description=f'Facebook Ads Insights (days_for_ago={days_for_ago})',
        schedule_interval=schedule,
        start_date=datetime(2026, 2, 5, tzinfo=local_tz),
        catchup=False,
        max_active_runs=1,
        tags=['etl', 'facebook_ads', 'insights', 'marketing'],
        params={
            'days_for_ago': Param(
                default=days_for_ago,
                type='integer',
                description='Number of days ago to fetch data for (1 = yesterday)',
            ),
        },
    )

    with dag:
        # Task 1: Regular Insights
        facebook_ads_insights = KubernetesPodOperator(
            task_id='facebook_ads_insights',
            name='facebook-ads-insights',
            namespace=NAMESPACE,
            image=PYTHON_ETL_IMAGE,
            image_pull_policy='IfNotPresent',
            cmds=['python'],
            arguments=[
                '/app/facebook/facebook_ads_insights_ingestion.py',
                '{{ params.days_for_ago }}',
            ],
            volumes=[nfs_volume],
            volume_mounts=[nfs_mount],
            env_vars=common_env_vars,
            container_resources=resource_requirements,
            is_delete_operator_pod=True,
            get_logs=True,
        )

        # # Task 2: Message Insights
        # facebook_ads_message_insights = KubernetesPodOperator(
        #     task_id='facebook_ads_message_insights',
        #     name='facebook-ads-message-insights',
        #     namespace=NAMESPACE,
        #     image=PYTHON_ETL_IMAGE,
        #     image_pull_policy='IfNotPresent',
        #     cmds=['python'],
        #     arguments=[
        #         '/app/facebook/facebook_ads_message_insights_ingestion.py',
        #         '{{ params.days_for_ago }}',
        #     ],
        #     volumes=[nfs_volume],
        #     volume_mounts=[nfs_mount],
        #     env_vars=common_env_vars,
        #     container_resources=resource_requirements,
        #     is_delete_operator_pod=True,
        #     get_logs=True,
        # )

    return dag

# Generate DAGs for 1 to 4 days ago with staggered schedules
dag_configs = [
    (1, '0 6 * * *'),   # days_for_ago=1 -> 06:00
    (2, '10 6 * * *'),  # days_for_ago=2 -> 06:10
    (3, '20 6 * * *'),  # days_for_ago=3 -> 06:20
    (4, '30 6 * * *'),  # days_for_ago=4 -> 06:30
]

for days, schedule in dag_configs:
    dag_id = f'facebook_ads_insights_etl_{days}d_ago'
    globals()[dag_id] = create_dag(dag_id, schedule, days)
