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
SCI_URI = get_conn_uri('sci_erp_sci')
KN_URI = get_conn_uri('sci_erp_kn')
PR_URI = get_conn_uri('sci_erp_pr')
HH_URI = get_conn_uri('sci_erp_hh')

# Local timezone for Vietnam
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

default_args = {
    'owner': 'dungtv',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1, tzinfo=local_tz),
    'email_on_failure': True,
    'email': ['dungtv@scigroup.com.vn', 'quantridulieu@scigroup.com.vn'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='Product_dungtv_job_stag_mail_message_erp_k8s',
    default_args=default_args,
    schedule_interval='20 6 * * *',
    tags=['Product', 'ERP', 'Mail', 'Jupyter'],
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

def create_transfer_task(task_id, sync_task_name):
    return KubernetesPodOperator(
        task_id=task_id,
        name=f'mail-message-erp-{task_id.replace("_", "-")}',
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[
            '/app/jupyter_sync/stag_mail_message_erp.py',
            '--task', sync_task_name
        ],
        volumes=[nfs_volume, temp_disk_volume],
        volume_mounts=[nfs_mount, temp_disk_mount],
        env_vars={
            'STAGING_SCI_URI': STAGING_SCI_URI,
            'SCI_URI': SCI_URI,
            'KN_URI': KN_URI,
            'PR_URI': PR_URI,
            'HH_URI': HH_URI,
            'TEMP_DIR': '/tmp'
        },
        container_resources=resource_requirements,
        is_delete_operator_pod=True,
        get_logs=True,
        trigger_rule='all_done',
        dag=dag,
    )

# --- Define Tasks ---
task_sci = create_transfer_task('sync_sci', 'sync_sci')
task_kn = create_transfer_task('sync_kn', 'sync_kn')
task_pr = create_transfer_task('sync_pr', 'sync_pr')
task_hh = create_transfer_task('sync_hh', 'sync_hh')

# --- Dependencies ---
task_sci >> task_kn >> task_pr >> task_hh
