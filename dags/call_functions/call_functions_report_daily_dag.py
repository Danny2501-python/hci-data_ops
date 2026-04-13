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
    schedule_interval='0 5 * * *', # 5:00 AM daily
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
            default=[593, 613, 614, 580],
            type=['null', 'string', 'array'],
            description='Table IDs to process (e.g. [593, 614] or "593,614").',
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

# Fetch Connections
dwh_product_new_uri = ''
erp_configs = {}
try:
    hook = PostgresHook(postgres_conn_id='dwh_product_new')
    conn = hook.get_connection('dwh_product_new')
    dwh_product_new_uri = conn.get_uri()
    if '?__extra__=' in dwh_product_new_uri:
        dwh_product_new_uri = dwh_product_new_uri.split('?__extra__=')[0]

    erp_conn_mapping = {'ERP_SCI_URI': 'sci_erp_sci', 'ERP_KN_URI': 'sci_erp_kn', 'ERP_PR_URI': 'sci_erp_pr', 'ERP_HH_URI': 'sci_erp_hh'}
    for env_var_name, conn_id in erp_conn_mapping.items():
        try:
            e_hook = PostgresHook(postgres_conn_id=conn_id)
            e_conn = e_hook.get_connection(conn_id)
            uri = e_conn.get_uri()
            if '?__extra__=' in uri: uri = uri.split('?__extra__=')[0]
            erp_configs[env_var_name] = uri
        except:
            pass
except Exception as e:
    logger.warning(f"Could not load connection details: {e}")

resource_requirements = k8s.V1ResourceRequirements(requests={'cpu': '2000m', 'memory': '4Gi'}, limits={'cpu': '3000m', 'memory': '6Gi'})
nfs_volume = k8s.V1Volume(name='nfs-scripts', nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH))
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)
temp_disk_volume = k8s.V1Volume(name='temp-disk', empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='10Gi'))
temp_disk_mount = k8s.V1VolumeMount(name='temp-disk', mount_path='/tmp', read_only=False)

from airflow.operators.python import PythonOperator

# ============================================================================
# TASKS
# ============================================================================

# 1. Direct Ingestion (Bypassing Iceberg)
# direct_ingestion = KubernetesPodOperator(
#     task_id='direct_ingestion',
#     name='etl-report-daily-direct-ingestion',
#     namespace=NAMESPACE,
#     image=PYTHON_ETL_IMAGE,
#     image_pull_policy='IfNotPresent',
#     cmds=['python'],
#     arguments=[
#         '/app/call_functions/direct_postgres_ingestion.py',
#         '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
#         '{{ params.table_id }}',
#         '{{ params.source }}',
#         '{{ dag_run.conf.get("load_type") or "incremental" }}',
#         '{{ params.drop_table }}'
#     ],
#     volumes=[nfs_volume, temp_disk_volume],
#     volume_mounts=[nfs_mount, temp_disk_mount],
#     env_vars={
#         **common_env_vars,
#         **erp_configs,
#         'POSTGRES_URI': dwh_product_new_uri,
#         'TEMP_DIR': '/tmp',
#     },
#     container_resources=resource_requirements,
#     is_delete_operator_pod=True,
#     get_logs=True,
#     dag=dag,
# )

# 2. Call Postgres Functions with Results Logging
def execute_report_functions(**kwargs):
    hook = PostgresHook(postgres_conn_id='dwh_product_new')
    statements = [
        "SELECT public.report_ctv_kn1();",
        "SELECT public.report_ctv_pr1();",
        "SELECT public.bc_sales_paris();",
        "SELECT public.op_convert_kangnam();",
        "SELECT public.bc_sales_kangnam();"
    ]
    
    errors = []
    for sql in statements:
        try:
            logger.info(f"🚀 Running: {sql}")
            # Each get_first call handles its own connection open/close from the pool
            result = hook.get_first(sql)
            logger.info(f"✅ Result from DB: {result}")
        except Exception as e:
            error_msg = f"❌ Error executing {sql}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            
    if errors:
        # If any errors occurred, we might want to flag the task as failed 
        # at the end while still having attempted everything.
        raise Exception(f"Task completed with some errors: {'; '.join(errors)}")

call_functions = PythonOperator(
    task_id='call_report_functions',
    python_callable=execute_report_functions,
    dag=dag,
)

call_functions
