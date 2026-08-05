"""
Airflow DAG for Pancake POS Ingestion & Chat Retention Policy
Orchestrates hourly ingestion of POS Orders, Customers, and 30-day Chat Log Retention Cleanup.
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
    'pancake_pos_orders_etl',
    default_args=default_args,
    description='Pancake POS Ingestion & Retention: Hourly Sync Orders, Customers & 30-day Chat Cleanup',
    schedule_interval='0 * * * *',  # Hourly
    start_date=datetime(2026, 8, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'pancake', 'pos', 'orders', 'customers', 'retention'],
)

ingest_orders_task = KubernetesPodOperator(
    task_id='ingest_pancake_orders',
    name='pancake-orders-ingestion-pod',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    cmds=['python', '/app/pancake/pancake_orders_ingestion.py'],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=[
        k8s.V1EnvVar(name='POSTGRES_URI', value='postgresql://admin:QtdlxxrisA%40%21sfsgo%232024@10.10.10.214:5432/new_product'),
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

ingest_customers_task = KubernetesPodOperator(
    task_id='ingest_pancake_customers',
    name='pancake-customers-ingestion-pod',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    cmds=['python', '/app/pancake/pancake_customers_ingestion.py'],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=[
        k8s.V1EnvVar(name='POSTGRES_URI', value='postgresql://admin:QtdlxxrisA%40%21sfsgo%232024@10.10.10.214:5432/new_product'),
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

cleanup_chat_logs_task = KubernetesPodOperator(
    task_id='cleanup_pancake_chat_logs',
    name='pancake-chat-cleanup-pod',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    cmds=['python', '/app/pancake/pancake_chat_cleanup.py'],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=[
        k8s.V1EnvVar(name='POSTGRES_URI', value='postgresql://admin:QtdlxxrisA%40%21sfsgo%232024@10.10.10.214:5432/new_product'),
        k8s.V1EnvVar(name='PANCAKE_CHAT_RETENTION_DAYS', value='30'),
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

[ingest_orders_task, ingest_customers_task] >> cleanup_chat_logs_task
