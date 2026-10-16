from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
# LOAD AIRFLOW CONNECTIONS
# ============================================================================

dwh_product_new_uri = ''
try:
    hook_new = PostgresHook(postgres_conn_id='dwh_product_new')
    conn_new = hook_new.get_connection('dwh_product_new')
    dwh_product_new_uri = conn_new.get_uri()
    if '?__extra__=' in dwh_product_new_uri:
        dwh_product_new_uri = dwh_product_new_uri.split('?__extra__=')[0]
    logger.info("✅ Loaded connection URI for dwh_product_new")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_product_new connection: {e}")

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

def get_t_1_tz():
    now = datetime.now(local_tz)
    t_1 = now - timedelta(days=1)
    return t_1.strftime('%Y-%m-%d')

def get_t_3_tz():
    now = datetime.now(local_tz)
    t_3 = now - timedelta(days=3)
    return t_3.strftime('%Y-%m-%d')

def get_t_0_tz():
    now = datetime.now(local_tz)
    return now.strftime('%Y-%m-%d')

dag = DAG(
    'caresoft_marketing_insights_dag',
    default_args=default_args,
    description='CareSoft Marketing Insights Ingestion: API -> PostgreSQL',
    # 4 tiếng/lần: 02:30, 06:30, 10:30, 14:30, 18:30, 22:30 VN (2026-10-16; trước đó 1 lần/ngày 06:30, trước nữa 07:30).
    # ⚠ PHẢI giữ mốc 06:30: data_serving_rtds_etl 07:00 có ExternalTaskSensor chờ task này với execution_delta=30 phút
    #   (xem data_serving_rtds_dag.py). Cron nào làm mất lượt 06:30 sẽ khiến sensor đó treo rồi fail sau 3 giờ.
    # caresoft_marketing_insights_thv_dag lệch 1 giờ (45 3,7,11,15,19,23) để không gọi API CareSoft cùng lúc;
    # mọi DAG dbt đọc cs_facebook_insight đều >= 07:00 nên các lượt trong ngày chỉ làm số mới hơn, không phá thứ tự.
    # Lấy T-3 → T-0: từ 2026-10-16 có cả chi phí trong ngày hôm nay (số T-0 chưa chốt, FB còn hồi tố — mỗi lượt
    # delete-insert ghi đè lại đúng cửa sổ này nên chạy lặp an toàn).
    schedule_interval='30 2,6,10,14,18,22 * * *',
    start_date=datetime(2026, 3, 20, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'marketing', 'facebook', 'insight'],
    params={
        'since': Param(
            default=None,
            type=['null', 'string'],
            description='Start date for backfill (YYYY-MM-DD). Default is T-3.',
        ),
        'until': Param(
            default=None,
            type=['null', 'string'],
            description='End date for backfill (YYYY-MM-DD). Default is T-0 (hôm nay).',
        )
    },
    user_defined_macros={
        't_3_tz': get_t_3_tz,
        't_1_tz': get_t_1_tz,
        't_0_tz': get_t_0_tz
    },
)

# ============================================================================
# RESOURCES
# ============================================================================

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '2Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(
        server=NFS_SERVER,
        path=NFS_PATH
    )
)

nfs_volume_mount = k8s.V1VolumeMount(
    name='nfs-scripts',
    mount_path='/app',
    read_only=True
)

common_env_vars = {
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
    'POSTGRES_URI': dwh_product_new_uri,
}

# ============================================================================
# TASKS
# ============================================================================

task_marketing_insights = KubernetesPodOperator(
    task_id='marketing_insights_ingestion',
    name='caresoft-marketing-insights-ingestion',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/marketing/marketing_insights_ingestion.py',
        '{{ params.since or t_3_tz() }}',
        '{{ params.until or t_0_tz() }}'
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_volume_mount],
    env_vars=common_env_vars,
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)
