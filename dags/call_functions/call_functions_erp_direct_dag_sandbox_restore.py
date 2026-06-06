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
    from datetime import datetime, timedelta
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

dag = DAG(
    'call_functions_erp_direct_dag_sandbox_restore',
    default_args=default_args,
    description='ETL ERP SCI: Source DBs → Direct Postgres Ingestion',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'call_functions', 'erp', 'direct_ingestion'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date for backfill (YYYY-MM-DD). Leave empty to use T-1.',
        ),
        'table_id': Param(
            default=None,
            type=['null', 'string', 'array'],
            description='Process specific table ID(s). Can be a single ID, a comma-separated string, or a list [1, 2]. Leave empty to process all.',
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
        ),
        'source': Param(
            default='erp',
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
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
    'DEFAULT_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.214', 'port': 5432, 'dbname': 'etl_marketing'}),
    'CUSTOM_DB_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.214', 'port': 5432, 'dbname': 'staging_sci'}),
    'DU_PHONG_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.226', 'port': 5432, 'dbname': 'staging_sci'}),
    'ERP_SCI_CONFIG': json.dumps({'user': 'report_user', 'password': '3EC4BUOtK4QokfJ8wJQA0uLcJaB7j+Ue', 'host': '10.10.10.221', 'port': 54321, 'dbname': 'sci_erp'}),
    'GIAOVIEC_CONFIG': json.dumps({'user': 'reader', 'password': 'Sci@123', 'host': '10.10.10.153', 'port': 54222, 'dbname': 'project'}),
    'SEEDING_CONFIG': json.dumps({'user': 'reader', 'password': 'seeding@123', 'host': '10.10.10.234', 'port': 54300, 'dbname': 'sale'}),
    'HONGHA_CONFIG': json.dumps({'user': 'reporter', 'password': 'hovxmsgewkru', 'host': 'db.benhvienhongha.com.vn', 'port': 55000, 'dbname': 'bvhongha'}),
    'PORTAL_GUIDE_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.230', 'port': 5432, 'dbname': 'portal_guide'}),
}

sandbox_old_uri = ''
try:
    hook = PostgresHook(postgres_conn_id='sandbox_old')
    conn = hook.get_connection('sandbox_old')
    sandbox_old_uri = conn.get_uri()
    if '?__extra__=' in sandbox_old_uri:
        sandbox_old_uri = sandbox_old_uri.split('?__extra__=')[0]
except Exception as e:
    logger.warning(f"Could not load sandbox_old connection: {e}")

erp_configs = {}
erp_conn_mapping = {'ERP_SCI_URI': 'sci_erp_sci', 'ERP_KN_URI': 'sci_erp_kn_old', 'ERP_PR_URI': 'sci_erp_pr_old', 'ERP_HH_URI': 'sci_erp_hh'}
for env_var_name, conn_id in erp_conn_mapping.items():
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        uri = conn.get_uri()
        if '?__extra__=' in uri: uri = uri.split('?__extra__=')[0]
        erp_configs[env_var_name] = uri
    except Exception as e:
        logger.warning(f"Could not load {conn_id}: {e}")

resource_requirements = k8s.V1ResourceRequirements(requests={'cpu': '2000m', 'memory': '4Gi'}, limits={'cpu': '3000m', 'memory': '6Gi'})
nfs_volume = k8s.V1Volume(name='nfs-scripts', nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH))
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)
temp_disk_volume = k8s.V1Volume(name='temp-disk', empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='10Gi'))
temp_disk_mount = k8s.V1VolumeMount(name='temp-disk', mount_path='/tmp', read_only=False)

# ============================================================================
# TASKS
# ============================================================================

direct_ingestion = KubernetesPodOperator(
    task_id='direct_ingestion',
    name='etl-erp-sci-direct-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/call_functions/direct_postgres_ingestion_not_truncate_erp.py',
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
        '{{ dag_run.conf.get("table_id") | default("", true) }}',
        '{{ params.source }}',
        '{{ dag_run.conf.get("load_type") or "incremental" }}',
        '{{ params.drop_table }}'
    ],
    volumes=[nfs_volume, temp_disk_volume],
    volume_mounts=[nfs_mount, temp_disk_mount],
    env_vars={
        **common_env_vars,
        **erp_configs,
        'POSTGRES_URI': sandbox_old_uri,
        'TEMP_DIR': '/tmp',
    },
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

direct_ingestion
