
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.param import Param
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

# Target Zones for Cleanup
CLEANUP_ZONES = [
    {
        'id': 'bronze', 
        'uri': 'http://iceberg-rest-bronze-zone:8181', 
        'catalog': 'bronze', 
        'ns': 'caresoft'
    },
    {
        'id': 'silver', 
        'uri': 'http://iceberg-rest-silver-zone:8181', 
        'catalog': 'silver', 
        'ns': 'caresoft'
    },
    {
        'id': 'gold', 
        'uri': 'http://iceberg-rest-gold-zone:8181', 
        'catalog': 'gold', 
        'ns': 'caresoft'
    },
]

# Namespace pancake (pipeline Pancake API) – cùng 3 zone, cùng chính sách retention
CLEANUP_ZONES += [
    {**z, 'id': f"{z['id']}_pancake", 'ns': 'pancake'}
    for z in CLEANUP_ZONES if z['ns'] == 'caresoft'
]

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'maintenance_iceberg_retention',
    default_args=default_args,
    description='Cleanup Iceberg data in Bronze, Silver, and Gold zones',
    schedule_interval='0 19 * * *',
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'iceberg', 'cleanup'],
    params={
        'retention_days': Param(
            default=3,
            type='integer',
            description='Retention period in days.',
        ),
    },
)

# ============================================================================
# RESOURCES & ENV
# ============================================================================

env_vars = {
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
    'TRINO_HOST': 'trino.lakehouse.com',
    'TRINO_PORT': '80',
    'TRINO_USER': 'admin',
}

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

# ============================================================================
# TASKS
# ============================================================================

for zone in CLEANUP_ZONES:
    KubernetesPodOperator(
        task_id=f'iceberg_retention_{zone["id"]}',
        name=f'iceberg-retention-{zone["id"]}',
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[
            '/app/maintenance/iceberg_retention.py',
            '{{ params.retention_days }}',
            zone['uri'],
            zone['catalog'],
            zone['ns'],
        ],
        volumes=[nfs_volume],
        volume_mounts=[nfs_mount],
        env_vars=env_vars,
        container_resources=resource_requirements,
        host_aliases=[
            k8s.V1HostAlias(ip='10.10.10.200', hostnames=['trino.lakehouse.com'])
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag,
    )
