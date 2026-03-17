from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
import pendulum
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'

# Resolve Connection URIs in the DAG (Scheduler side)
def get_conn_uri(conn_id):
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        uri = conn.get_uri()
        if '?__extra__=' in uri:
            uri = uri.split('?__extra__=')[0]
        return uri
    except Exception as e:
        logger.warning(f"Could not resolve connection {conn_id}: {e}")
        return ""

STAGING_SCI_URI = get_conn_uri('dwh_product_new')
EHC_URI = get_conn_uri('sci_EHC')

# Local timezone for Vietnam
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

default_args = {
    'owner': 'huonghh',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1, tzinfo=local_tz),
    'email_on_failure': True,
    'email': ['huonghh@scigroup.com.vn'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='Product_huonghh_run_func_proce_ehc_k8s',
    default_args=default_args,
    schedule_interval='10 4 * * *',
    tags=['Product', 'EHC', 'Jupyter'],
    catchup=False
)

# ============================================================================
# KUBERNETES RESOURCES
# ============================================================================

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-scripts', mount_path='/app', read_only=True
)

temp_disk_volume = k8s.V1Volume(
    name='temp-disk',
    empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='5Gi') 
)
temp_disk_mount = k8s.V1VolumeMount(
    name='temp-disk', mount_path='/tmp', read_only=False
)

# ============================================================================
# TASK HELPER
# ============================================================================

def create_k8s_task(task_id, sync_task_name):
    return KubernetesPodOperator(
        task_id=task_id,
        name=f'ehc-sync-{task_id.replace("_", "-")}',
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[
            '/app/jupyter_sync/Product_huonghh_run_func_proce_ehc.py',
            '--task', sync_task_name
        ],
        volumes=[nfs_volume, temp_disk_volume],
        volume_mounts=[nfs_mount, temp_disk_mount],
        env_vars={
            'STAGING_SCI_URI': STAGING_SCI_URI,
            'EHC_URI': EHC_URI,
            'TEMP_DIR': '/tmp'
        },
        container_resources=resource_requirements,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag,
    )

# --- Define Tasks ---
task_doanhso = create_k8s_task('task_HongHa_doanhso_EHC', 'write_HongHa_doanhso_EHC')
task_thuchien = create_k8s_task('task_stag_thuchien_doanhso_hongha', 'write_stag_thuchien_doanhso_hongha')

# --- Dependencies ---
task_doanhso >> task_thuchien
