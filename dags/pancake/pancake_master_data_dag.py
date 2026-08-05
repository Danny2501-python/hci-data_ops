"""
Airflow DAG for Pancake Master Data Ingestion (Shops & Master Entities)
Daily sync of Shops metadata from Pancake POS API.
"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
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

# Volume mounting
nfs_volume = k8s.V1Volume(
    name='nfs-app',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(name='nfs-app', mount_path='/app')

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'pancake_master_data_etl',
    default_args=default_args,
    description='Pancake Master Data Ingestion: Daily Sync of Shops and Entities',
    schedule_interval='30 2 * * *',  # Daily at 02:30 AM
    start_date=datetime(2026, 8, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'pancake', 'pos', 'shops', 'master_data'],
)

ingest_shops_task = KubernetesPodOperator(
    task_id='ingest_pancake_shops',
    name='pancake-shops-ingestion-pod',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    cmds=['python', '/app/pancake/pancake_shops_ingestion.py'],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=[
        k8s.V1EnvVar(name='POSTGRES_URI', value='postgresql://admin:QtdlxxrisA%40%21sfsgo%232024@10.10.10.214:5432/new_product'),
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)
