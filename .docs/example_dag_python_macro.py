from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.param import Param
import logging
import pendulum

logger = logging.getLogger(__name__)

# Timezone for Vietnam
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

# ============================================================================
# CUSTOM MACRO: Calculate snapshot_date in Python
# ============================================================================

def get_yesterday_tz():
    """
    Get yesterday's date in Asia/Ho_Chi_Minh timezone
    This is called every time DAG is parsed (every 30s)
    """
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'caresoft_agent_etl',
    default_args=default_args,
    description='CareSoft Agent ETL',
    schedule_interval='30 1 * * *',
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['caresoft', 'agent', 'etl'],
    params={
        'snapshot_date': Param(
            default=None,
            type=['null', 'string'],
            description='Override snapshot date (YYYY-MM-DD). Leave empty for T-1.',
        )
    },
    # Register custom macro
    user_defined_macros={
        'yesterday_tz': get_yesterday_tz
    }
)

# ============================================================================
# RESOURCES
# ============================================================================

PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'

common_env_vars = {
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
}

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '1000m', 'memory': '1Gi'}
)

nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)

nfs_mount = k8s.V1VolumeMount(
    name='nfs-scripts',
    mount_path='/app',
    read_only=True
)

# ============================================================================
# TASKS
# ============================================================================

task_agent_ingestion = KubernetesPodOperator(
    task_id='agent_ingestion_bronze',
    name='caresoft-agent-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/agent/agent_ingestion.py',
        # Use custom Python macro instead of Airflow variables
        '{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}'
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars=common_env_vars,
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# More tasks...
