from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.postgres_hook import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from airflow.models.param import Param
import logging
import pendulum

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Docker image
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'

# Kubernetes
NAMESPACE = 'default'

# NFS
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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Timezone for Vietnam
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')



def get_yesterday_tz():
    """Get yesterday's date in Asia/Ho_Chi_Minh timezone"""
    from datetime import datetime, timedelta
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

# Kéo bù deal CareSoft bị thiếu (22/10/2026) — chỉ chạy tay, không có lịch.
# Xem app/caresoft/deal/deal_backfill_list_api.py và doc cs_deal_vs_ticket_check_2026-10-22.md
# Nên chạy dry_run = true trước để xem số deal thiếu.
dag = DAG(
    'caresoft_deal_backfill_etl',
    default_args={**default_args, 'retries': 1},
    description='Kéo bù deal CareSoft thiếu: /deals (created range) − Postgres → Gold → Serving',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['caresoft', 'deal', 'backfill', 'iceberg'],
    params={
        'snapshot_date': Param(
            default='2026-10-15', type='string',
            description='Nhãn partition Gold (YYYY-MM-DD). Dùng ngày cũ hơn 3 ngày để không đụng job hằng ngày.',
        ),
        'since': Param(default='2026-05-20', type='string', description='Deal tạo từ ngày (YYYY-MM-DD, tính cả ngày này)'),
        'until': Param(default='2026-10-22', type='string', description='Đến ngày (YYYY-MM-DD, KHÔNG tính ngày này)'),
        'brands': Param(default='3', type='string', description='erp_id cách nhau dấu phẩy, vd "3" hoặc "1,3"'),
        'dry_run': Param(default='true', type='string', description='"true" = chỉ đếm số deal thiếu, không ghi gì'),
    },
)

# ============================================================================
# COMMON RESOURCES
# ============================================================================

# Environment variables (Iceberg catalogs + MinIO)
common_env_vars = {
    # Bronze zone
    'ICEBERG_REST_URI_BRONZE': 'http://iceberg-rest-bronze-zone:8181',
    'CATALOG_NAME_BRONZE': 'bronze',
    
    # Silver zone
    'ICEBERG_REST_URI_SILVER': 'http://iceberg-rest-silver-zone:8181',
    'CATALOG_NAME_SILVER': 'silver',
    
    # Gold zone
    'ICEBERG_REST_URI_GOLD': 'http://iceberg-rest-gold-zone:8181',
    'CATALOG_NAME_GOLD': 'gold',
    
    # MinIO config
    'S3_ENDPOINT': 'http://10.152.183.65:9000',
    'AWS_ACCESS_KEY_ID': 'admin',
    'AWS_SECRET_ACCESS_KEY': 'password',
    'AWS_REGION': 'us-east-1',
}

# Resource requirements (optimized for cluster capacity)
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '3Gi'},   # Reduced to fit available memory
    limits={'cpu': '2000m', 'memory': '6Gi'}     # Sufficient for DuckDB transformations
)

# NFS Volume (for scripts)
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

# DuckDB Temp Volume (disk-backed for spilling)
duckdb_temp_volume = k8s.V1Volume(
    name='duckdb-temp',
    empty_dir=k8s.V1EmptyDirVolumeSource(
        medium='',  # Empty string = disk-backed (not RAM)
        size_limit='10Gi'
    )
)

duckdb_temp_volume_mount = k8s.V1VolumeMount(
    name='duckdb-temp',
    mount_path='/tmp/duckdb',
    read_only=False
)

# ============================================================================
# LOAD DATABASE CONNECTIONS (at DAG parse time)
# ============================================================================

# ERP Connections
erp_connection_env_vars = {}
erp_connections_config = {
    'SCI': 'sci_erp_sci',
    'KN': 'sci_erp_kn', 
    'PR': 'sci_erp_pr',
    'HH': 'sci_erp_hh',
}

for erp_name, conn_id in erp_connections_config.items():
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        uri = conn.get_uri()
        
        # Clean URI: Remove __extra__ parameter
        if '?__extra__=' in uri:
            uri = uri.split('?__extra__=')[0]
        
        erp_connection_env_vars[f'ERP_{erp_name}_URI'] = uri
        logger.info(f"✅ Loaded connection URI for ERP {erp_name}")
        
    except Exception as e:
        logger.warning(f"⚠️  Could not load {erp_name} connection: {e}")
        erp_connection_env_vars[f'ERP_{erp_name}_URI'] = ''

# DWH Product Connections (for serving layer)
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


dwh_sandbox_uri = ''
try:
    hook_sandbox = PostgresHook(postgres_conn_id='dwh_sandbox')
    conn_sandbox = hook_sandbox.get_connection('dwh_sandbox')
    dwh_sandbox_uri = conn_sandbox.get_uri()
    if '?__extra__=' in dwh_sandbox_uri:
        dwh_sandbox_uri = dwh_sandbox_uri.split('?__extra__=')[0]
    logger.info("✅ Loaded connection URI for dwh_sandbox")
except Exception as e:
    logger.warning(f"⚠️  Could not load dwh_sandbox connection: {e}")



# ============================================================================
# TASK 1: /deals (created range) − Postgres → GET /deal/{id} → GOLD
# ============================================================================

SNAP = '{{ params.snapshot_date }}'

task_deal_backfill = KubernetesPodOperator(
    task_id='deal_backfill_enrichment',
    name='caresoft-deal-backfill-enrichment',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=[
        '/app/caresoft/deal/deal_backfill_list_api.py',
        SNAP,
        '{{ params.since }}',
        '{{ params.until }}',
        '{{ params.brands }}',
        '{{ params.dry_run }}',
    ],
    volumes=[nfs_volume, duckdb_temp_volume],
    volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
    env_vars={
        **common_env_vars,
        'POSTGRES_URI': dwh_product_new_uri,
        'SERVING_TABLE_NAME': 'cs_deal_detail_api_v2',
        'DEAL_DETAIL_TABLE': 'caresoft.cs_deal_detail_api',
        'DEAL_DETAIL_ERROR_TABLE': 'caresoft.cs_deal_detail_error_api',
        'BRAND_TABLE': 'caresoft.core_brand',
        'DUCKDB_TEMP_DIR': '/tmp/duckdb',
    },
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    execution_timeout=timedelta(hours=12),
    dag=dag,
)


def _pod(task_id, name, script, env):
    return KubernetesPodOperator(
        task_id=task_id,
        name=name,
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=[script, SNAP],
        volumes=[nfs_volume, duckdb_temp_volume],
        volume_mounts=[nfs_volume_mount, duckdb_temp_volume_mount],
        env_vars={**common_env_vars, **env},
        container_resources=resource_requirements,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag,
    )


# ============================================================================
# TASK 2..: GOLD → SERVING + COMMENTS (giống caresoft_deal_etl, cùng snapshot nhãn)
# ============================================================================

task_serving = _pod('refresh_serving_layer_v2', 'caresoft-backfill-serving-v2',
                    '/app/caresoft/deal/deal_detail_serving_layer.py',
                    {'POSTGRES_URI': dwh_product_new_uri, 'DUCKDB_TEMP_DIR': '/tmp/duckdb',
                     'SERVING_TABLE_NAME': 'cs_deal_detail_api_v2'})

task_serving_sandbox = _pod('refresh_serving_layer_v2_sandbox', 'caresoft-backfill-serving-v2-sandbox',
                            '/app/caresoft/deal/deal_detail_serving_layer.py',
                            {'POSTGRES_URI': dwh_sandbox_uri, 'DUCKDB_TEMP_DIR': '/tmp/duckdb',
                             'SERVING_TABLE_NAME': 'cs_deal_detail_api_v2'})

task_comments = _pod('deal_comments_transformation', 'caresoft-backfill-comments-transformation',
                     '/app/caresoft/deal/deal_comments_transformation.py',
                     {'DUCKDB_TEMP_DIR': '/tmp/duckdb'})

task_comments_serving = _pod('deal_comments_serving_layer', 'caresoft-backfill-comments-serving',
                             '/app/caresoft/deal/deal_comments_serving_layer.py',
                             {'POSTGRES_URI': dwh_product_new_uri, 'SERVING_TABLE_NAME': 'cs_deal_api_trans_comments'})

task_comments_serving_sandbox = _pod('deal_comments_serving_layer_sandbox', 'caresoft-backfill-comments-serving-sandbox',
                                     '/app/caresoft/deal/deal_comments_serving_layer.py',
                                     {'POSTGRES_URI': dwh_sandbox_uri, 'SERVING_TABLE_NAME': 'cs_deal_api_trans_comments'})

task_deal_backfill >> [task_serving, task_serving_sandbox] >> task_comments >> [task_comments_serving, task_comments_serving_sandbox]
