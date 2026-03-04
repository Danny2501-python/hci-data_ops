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
ERP_URI = get_conn_uri('sci_erp')
DEV_URI = get_conn_uri('sci_dev')
SANDBOX_URI = get_conn_uri('sci')

# Local timezone for Vietnam
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1, tzinfo=local_tz),
    'email_on_failure': True,
    'email': ['huonghh@scigroup.com.vn'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='Product_huonghh_Trans_field_k8s',
    default_args=default_args,
    schedule_interval=None,
    tags=['Product', 'Jupyter'],
    catchup=False
)

# ============================================================================
# KUBERNETES RESOURCES
# ============================================================================

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '1Gi'},
    limits={'cpu': '2000m', 'memory': '2Gi'}
)

nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-scripts', mount_path='/app', read_only=True
)

# Temp Disk Volume (for large CSV files for COPY)
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
        name=f'product-sync-{task_id.replace("_", "-")}',
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[
            '/app/jupyter_sync/product_trans_field.py',
            '--task', sync_task_name
        ],
        volumes=[nfs_volume, temp_disk_volume],
        volume_mounts=[nfs_mount, temp_disk_mount],
        env_vars={
            'STAGING_SCI_URI': STAGING_SCI_URI,
            'ERP_URI': ERP_URI,
            'DEV_URI': DEV_URI,
            'SANDBOX_URI': SANDBOX_URI,
            'TEMP_DIR': '/tmp'
        },
        container_resources=resource_requirements,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag,
    )

# Stage 1: Medical Sync
t_med_specialty_supply = create_k8s_task('sync_sh_medical_specialty_supply', 'sync_sh_medical_specialty_supply')
t_med_specialty_team = create_k8s_task('sync_sh_medical_specialty_team', 'sync_sh_medical_specialty_team')
t_med_surgery_supply = create_k8s_task('sync_sh_medical_surgery_supply', 'sync_sh_medical_surgery_supply')
t_med_surgery_team = create_k8s_task('sync_sh_medical_surgery_team', 'sync_sh_medical_surgery_team')

medical_tasks = [t_med_specialty_supply, t_med_specialty_team, t_med_surgery_supply, t_med_surgery_team]

# Stage 2: Sales & CRM Sync
t_sale_order_line = create_k8s_task('sync_sale_order_line', 'sync_sale_order_line')
t_crm_line = create_k8s_task('sync_crm_line', 'sync_crm_line')
t_crm_line_product = create_k8s_task('sync_crm_line_product', 'sync_crm_line_product')
t_crm_lead = create_k8s_task('sync_crm_lead', 'sync_crm_lead')

crm_tasks = [t_sale_order_line, t_crm_line, t_crm_line_product, t_crm_lead]

# Stage 3: KS Sync (Medical)
t_ks_med_spec_supply = create_k8s_task('sync_ks_sh_medical_specialty_supply', 'sync_ks_sh_medical_specialty_supply')
t_ks_med_spec_team = create_k8s_task('sync_ks_sh_medical_specialty_team', 'sync_ks_sh_medical_specialty_team')
t_ks_med_surg_supply = create_k8s_task('sync_ks_sh_medical_surgery_supply', 'sync_ks_sh_medical_surgery_supply')
t_ks_med_surg_team = create_k8s_task('sync_ks_sh_medical_surgery_team', 'sync_ks_sh_medical_surgery_team')

ks_medical_tasks = [t_ks_med_spec_supply, t_ks_med_spec_team, t_ks_med_surg_supply, t_ks_med_surg_team]

# Stage 4: KS Sync (Sales & CRM)
t_ks_sale_order = create_k8s_task('sync_ks_sale_order_line', 'sync_ks_sale_order_line')
t_ks_crm_line = create_k8s_task('sync_ks_crm_line', 'sync_ks_crm_line')
t_ks_crm_line_prod = create_k8s_task('sync_ks_crm_line_product', 'sync_ks_crm_line_product')
t_ks_crm_lead = create_k8s_task('sync_ks_crm_lead', 'sync_ks_crm_lead')

ks_crm_tasks = [t_ks_sale_order, t_ks_crm_line, t_ks_crm_line_prod, t_ks_crm_lead]

# --- Dependencies ---
# Airflow doesn't support list >> list, so we connect them sequentially or use cross_downstream
for med_task in medical_tasks:
    med_task >> crm_tasks

for crm_task in crm_tasks:
    crm_task >> ks_medical_tasks

for ks_med_task in ks_medical_tasks:
    ks_med_task >> ks_crm_tasks
