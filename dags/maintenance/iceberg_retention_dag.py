
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

dag = DAG(
    'maintenance_iceberg_retention',
    default_args=default_args,
    description='Delete Iceberg data older than X days and clean up snapshots/files',
    schedule_interval='0 19 * * *',  # Daily at 7:00 PM (after ETLs)
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'iceberg', 'cleanup'],
    params={
        'retention_days': Param(
            default=3,
            type='integer',
            description='Retention period in days (delete data strictly older than this)',
        ),
        'target_uri': Param(
            default='http://iceberg-rest-bronze-zone:8181',
            type='string',
            description='Iceberg REST URI.',
        ),
        'catalog_name': Param(
            default='bronze',
            type='string',
            description='Catalog Name.',
        ),
        'namespace': Param(
            default='caresoft',
            type='string',
            description='Target Namespace to clean up.',
        ),
    },
)

# ============================================================================
# RESOURCES & ENV
# ============================================================================

# Environment variables (Iceberg + S3)
env_vars = {
    # MinIO config (Required for physical file access via Iceberg)
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
    # Trino Connection
    'TRINO_HOST': 'trino.lakehouse.com',
    'TRINO_PORT': '80',
    'TRINO_USER': 'admin',
}

# Modest resources for metadata operations
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '1000m', 'memory': '1Gi'}
)

# NFS Volume
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

iceberg_retention_task = KubernetesPodOperator(
    task_id='iceberg_retention_cleanup',
    name='maintenance-iceberg-retention',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/maintenance/iceberg_retention.py',
        '{{ params.retention_days }}',
        '{{ params.target_uri or "" }}',
        '{{ params.catalog_name or "" }}',
        '{{ params.namespace }}',
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=env_vars,
    container_resources=resource_requirements,
    # Add Host Alias for internal Trino resolution
    host_aliases=[
        k8s.V1HostAlias(
            ip='10.10.10.200',
            hostnames=['trino.lakehouse.com']
        )
    ],
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)
