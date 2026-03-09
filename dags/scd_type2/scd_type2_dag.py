from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
import pendulum
import logging

logger = logging.getLogger(__name__)

# Timezone for Vietnam
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

# Configuration
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'

# Load Connection URIs from Airflow
def get_postgres_uri(conn_id):
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        uri = conn.get_uri()
        if '?__extra__=' in uri:
            uri = uri.split('?__extra__=')[0]
        return uri
    except Exception as e:
        logger.warning(f"⚠️  Could not load {conn_id} connection: {e}")
        return ""

# Connection IDs used in logic
# staging_sci_uri: Target/Backend for settings/logs and SCD tables
# erp_uri: Source ERP database
STAGING_SCI_URI = get_postgres_uri('dwh_product_new') 
STAGING_ERP_URI = get_postgres_uri('dwh_product_new')

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================================
# SCD TYPE 2 DAG (Replicated from erp_connector_backend)
# ============================================================================

# Common Pod Config
def get_pod_operator(task_id, name, script_path, args=None, env=None):
    # Envs for all pods
    common_env = {
        'STAGING_SCI_URI': STAGING_SCI_URI,
        'ERP_URI': STAGING_ERP_URI,
        'PYTHONUNBUFFERED': '1'
    }
    if env:
        common_env.update(env)
        
    return KubernetesPodOperator(
        task_id=task_id,
        name=name,
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[script_path] + (args or []),
        volumes=[
            k8s.V1Volume(
                name='nfs-scripts',
                nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)
        ],
        env_vars=common_env,
        is_delete_operator_pod=True,
        get_logs=True,
    )

with DAG(
    'scd_type2_daily_sync',
    default_args=default_args,
    description='ERP SCD Type 2 Sync (Replicated from erp_connector_backend)',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['erp', 'scd2'],
    params={
        'table_id': Param(
            default=None,
            type=['null', 'string', 'array'],
            description='Process specific table ID(s). Can be a single ID, a comma-separated string, or a list [1, 2]. Leave empty to process all.',
        ),
    },
) as dag:
    
    scd_processing = get_pod_operator(
        task_id='scd_type2_processing',
        name='scd-type2-processor',
        script_path='/app/scd_type2/scd_processor.py',
        args=['{{ dag_run.conf.get("table_id") | default("", true) }}']
    )

    scd_processing
