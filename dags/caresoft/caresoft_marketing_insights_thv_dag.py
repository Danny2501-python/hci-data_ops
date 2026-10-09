"""
CareSoft Marketing Insights - Tiêu Hóa Việt (erp_id = 8, domain drgiang)
API marketing/insights -> public.cs_facebook_insight (brand_etl = 8)

Bản sao của caresoft_marketing_insights_dag.py, chỉ khác:
  - chạy script marketing_insights_ingestion_thv.py (lấy brand theo erp_id = 8)
  - lịch 07:45 (sau job chung 07:30, trước mốc dbt 08:30 dùng cs_facebook_insight)
Mặc định lấy T-3 đến T-1 theo giờ VN; có thể trigger tay với params since / until để chạy lại.
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
# LOAD AIRFLOW CONNECTIONS
# ============================================================================

dwh_product_new_uri = ''
try:
    hook_new = PostgresHook(postgres_conn_id='dwh_product_new')
    conn_new = hook_new.get_connection('dwh_product_new')
    dwh_product_new_uri = conn_new.get_uri()
    if '?__extra__=' in dwh_product_new_uri:
        dwh_product_new_uri = dwh_product_new_uri.split('?__extra__=')[0]
    logger.info("✅ Loaded connection URI for dwh_product_new")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_product_new connection: {e}")

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')


def get_t_1_tz():
    now = datetime.now(local_tz)
    return (now - timedelta(days=1)).strftime('%Y-%m-%d')


def get_t_3_tz():
    now = datetime.now(local_tz)
    return (now - timedelta(days=3)).strftime('%Y-%m-%d')


dag = DAG(
    'caresoft_marketing_insights_thv_dag',
    default_args=default_args,
    description='CareSoft Marketing Insights Ingestion THV (drgiang, erp_id 8): API -> PostgreSQL',
    schedule_interval='45 7 * * *',
    start_date=datetime(2026, 10, 9, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'marketing', 'facebook', 'insight', 'thv'],
    params={
        'since': Param(
            default=None,
            type=['null', 'string'],
            description='Start date for backfill (YYYY-MM-DD). Default is T-3.',
        ),
        'until': Param(
            default=None,
            type=['null', 'string'],
            description='End date for backfill (YYYY-MM-DD). Default is T-1.',
        )
    },
    user_defined_macros={
        't_3_tz': get_t_3_tz,
        't_1_tz': get_t_1_tz
    },
)

# ============================================================================
# RESOURCES
# ============================================================================

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '1Gi'},
    limits={'cpu': '1000m', 'memory': '2Gi'}
)

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

common_env_vars = {
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
    'POSTGRES_URI': dwh_product_new_uri,
}

# ============================================================================
# TASKS
# ============================================================================

task_marketing_insights_thv = KubernetesPodOperator(
    task_id='marketing_insights_ingestion_thv',
    name='caresoft-marketing-insights-thv-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/marketing/marketing_insights_ingestion_thv.py',
        '{{ params.since or t_3_tz() }}',
        '{{ params.until or t_1_tz() }}'
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_volume_mount],
    env_vars=common_env_vars,
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)
