from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    from datetime import datetime, timedelta
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

dag = DAG(
    'call_functions_report_daily_dag',
    default_args=default_args,
    description='ETL & Report Daily: Ingestion IDs (593,614) + Call 4 Functions',
    schedule_interval='30 4 * * *', # 4:30 AM daily
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['report', 'daily', 'ctv', 'sales'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date (YYYY-MM-DD). Leave empty to use T-1.',
        ),
        'table_id': Param(
            default='593,614',
            type=['null', 'string', 'array'],
            description='Table IDs to process.',
        ),
        'load_type': Param(
            default='incremental',
            type='string',
            enum=['incremental', 'full'],
            description='Load type: incremental or full.'
        ),
        'drop_table': Param(
            default=False,
            type='boolean',
            description='Drop serving table before loading.'
        ),
        'source': Param(
            default='sandbox',
            type='string',
            description='Source type filter'
        )
    },
    user_defined_macros={'yesterday_tz': get_yesterday_tz},
)

# ============================================================================
# COMMON RESOURCES
# ============================================================================

common_env_vars = {
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
    'DEFAULT_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.214', 'port': 5432, 'dbname': 'etl_marketing'}),
    'CUSTOM_DB_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.214', 'port': 5432, 'dbname': 'staging_sci'}),
}

# Load Target Connection
dwh_product_new_uri = ''
try:
    hook = PostgresHook(postgres_conn_id='dwh_product_new')
    conn = hook.get_connection('dwh_product_new')
    dwh_product_new_uri = conn.get_uri()
    if '?__extra__=' in dwh_product_new_uri:
        dwh_product_new_uri = dwh_product_new_uri.split('?__extra__=')[0]
except Exception as e:
    logger.warning(f"Could not load dwh_product_new connection: {e}")

resource_requirements = k8s.V1ResourceRequirements(requests={'cpu': '2000m', 'memory': '4Gi'}, limits={'cpu': '3000m', 'memory': '6Gi'})
nfs_volume = k8s.V1Volume(name='nfs-scripts', nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH))
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)
temp_disk_volume = k8s.V1Volume(name='temp-disk', empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='10Gi'))
temp_disk_mount = k8s.V1VolumeMount(name='temp-disk', mount_path='/tmp', read_only=False)

# ============================================================================
# TASKS
# ============================================================================

# 1. Direct Ingestion (Bypassing Iceberg)
direct_ingestion = KubernetesPodOperator(
    task_id='direct_ingestion',
    name='etl-report-daily-direct-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/call_functions/direct_postgres_ingestion.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        '{{ params.table_id }}',
        '{{ params.source }}',
        '{{ dag_run.conf.get("load_type") or "incremental" }}',
        '{{ params.drop_table }}'
    ],
    volumes=[nfs_volume, temp_disk_volume],
    volume_mounts=[nfs_mount, temp_disk_mount],
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_product_new_uri,
        'TEMP_DIR': '/tmp',
    },
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# 2. Call Postgres Functions
call_functions = PostgresOperator(
    task_id='call_report_functions',
    postgres_conn_id='dwh_product_new',
    sql="""
        SELECT public.report_ctv_kn1();
        SELECT public.report_ctv_pr1();
        SELECT public.bc_sales_paris();
        SELECT public.op_convert_kangnam();
    """,
    dag=dag,
)

direct_ingestion >> call_functions
