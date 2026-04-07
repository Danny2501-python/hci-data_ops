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
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

dag = DAG(
    'call_functions_facebook_daily_dag',
    default_args=default_args,
    description='Daily Facebook ETL: Facebook Source → DWH Product New & Sandbox',
    schedule_interval='30 7 * * *', 
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'facebook', 'daily', 'product_new', 'sandbox'],
    params={
        'snapshot_date': Param(default=None, type=['null', 'string']),
        'table_id': Param(default=None, type=['null', 'string', 'array']),
        'load_type': Param(default='incremental', type='string', enum=['incremental', 'full']),
        'drop_table': Param(default=False, type='boolean')
    },
    user_defined_macros={
        'yesterday_tz': get_yesterday_tz,
        'get_conn_uri': lambda conn_id: PostgresHook(postgres_conn_id=conn_id).get_connection(conn_id).get_uri().split('?__extra__=')[0] if conn_id else ""
    },
)

# ============================================================================
# ENVIRONMENT & RESOURCES
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
    
    # DB Configs
    'DEFAULT_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.214', 'port': 5432, 'dbname': 'etl_marketing'}),
    'CUSTOM_DB_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.214', 'port': 5432, 'dbname': 'new_product'}),
    'CUSTOM_DB_DEV_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.214', 'port': 5432, 'dbname': 'staging_sci'}),
    'DU_PHONG_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.226', 'port': 5432, 'dbname': 'staging_sci'}),
    'ERP_SCI_CONFIG': json.dumps({'user': 'report_user', 'password': '3EC4BUOtK4QokfJ8wJQA0uLcJaB7j+Ue', 'host': '10.10.10.221', 'port': 54321, 'dbname': 'sci_erp'}),
    'DEV_SCI_CONFIG': json.dumps({'user': 'reporter', 'password': 'xjsfpwzlqehd', 'host': '10.10.10.152', 'port': 5432, 'dbname': 'sci_erp'}),
    'TEST_SCI_CONFIG': json.dumps({'user': 'reporter', 'password': 'xjsfpwzlqehd', 'host': '10.10.10.249', 'port': 5432, 'dbname': 'sci_erp'}),
    'GIAOVIEC_CONFIG': json.dumps({'user': 'reader', 'password': 'Sci@123', 'host': '10.10.10.153', 'port': 54222, 'dbname': 'project'}),
    'SEEDING_CONFIG': json.dumps({'user': 'reader', 'password': 'seeding@123', 'host': '10.10.10.234', 'port': 54300, 'dbname': 'sale'}),
    'HONGHA_CONFIG': json.dumps({'user': 'reporter', 'password': 'hovxmsgewkru', 'host': 'db.benhvienhongha.com.vn', 'port': 55000, 'dbname': 'bvhongha'}),
    'PORTAL_GUIDE_CONFIG': json.dumps({'user': 'admin', 'password': 'QtdlxxrisA@!sfsgo#2024', 'host': '10.10.10.230', 'port': 5432, 'dbname': 'portal_guide'}),
}

# ERP Logic
erp_configs = {}
erp_conn_mapping = {'ERP_SCI_URI': 'sci_erp_sci', 'ERP_KN_URI': 'sci_erp_kn', 'ERP_PR_URI': 'sci_erp_pr', 'ERP_HH_URI': 'sci_erp_hh'}
for env_var_name, conn_id in erp_conn_mapping.items():
    try:
        uri = PostgresHook(postgres_conn_id=conn_id).get_connection(conn_id).get_uri().split('?__extra__=')[0]
        erp_configs[env_var_name] = uri
    except:
        erp_configs[env_var_name] = ""

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '2000m', 'memory': '4Gi'},
    limits={'cpu': '3000m', 'memory': '6Gi'}
)

nfs_volume = k8s.V1Volume(name='nfs-scripts', nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH))
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)
temp_disk_volume = k8s.V1Volume(name='temp-disk', empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='10Gi'))
temp_disk_mount = k8s.V1VolumeMount(name='temp-disk', mount_path='/tmp', read_only=False)

# ============================================================================
# TASK GENERATOR
# ============================================================================

def create_ingestion_task(target_conn_id):
    task_id = f'fb_to_{target_conn_id}'.replace('-', '_')
    return KubernetesPodOperator(
        task_id=task_id,
        name=f'etl-fb-{target_conn_id.replace("_", "-")}',
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[
            '/app/call_functions/direct_postgres_ingestion.py',
            '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}',
            '{{ dag_run.conf.get("table_id") | default("", true) }}',
            'facebook',
            '{{ dag_run.conf.get("load_type") or "incremental" }}',
            '{{ params.drop_table }}'
        ],
        volumes=[nfs_volume, temp_disk_volume],
        volume_mounts=[nfs_mount, temp_disk_mount],
        env_vars={
            **common_env_vars,
            **erp_configs,
            'POSTGRES_URI': f'{{{{ get_conn_uri("{target_conn_id}") }}}}',
            'TEMP_DIR': '/tmp',
        },
        container_resources=resource_requirements,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag,
    )

task_product_new = create_ingestion_task('dwh_product_new')
task_sandbox = create_ingestion_task('dwh_sandbox')
