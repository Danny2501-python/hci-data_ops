from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sensors.external_task import ExternalTaskSensor
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
import logging
import pendulum

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

PYTHON_ETL_IMAGE = '111050/python-etl-base:latest'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH_DBT = '/srv/nfs_share/dbt/data_serving' # Path on NFS Server

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-eng@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'data_serving_rtds_etl',
    default_args=default_args,
    description='Run Data Serving ETL Pipeline - Báo cáo doanh số tập đoàn HCI (tag:rtds)',
    # 08:45 VN — sau khi cs_facebook_insight được nạp (caresoft_marketing_insights_dag 07:30, delete-insert
    # T-3 → T-1) và sau data_serving_pr_etl (08:30, build mart_report_marketing_ad_cost_pr — nguồn chi phí
    # ads Paris của int_rtds_chi_phi_ads). 2 sensor bên dưới chặn cứng thứ tự này: nếu DAG nguồn chạy chậm
    # thì chờ, nếu DAG nguồn fail thì DAG này fail (không build báo cáo trên dữ liệu ads thiếu).
    # Các nguồn khác đều xong trước theo lịch: data_serving_erp_staging_etl 04:00 (stag_payment_transfer,
    # view_stag_products), report_crm_lead_customer_out_sold 03:30.
    schedule_interval="45 8 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['data-serving', 'rtds', 'bao_cao_doanh_so_hci'],
)

# ============================================================================
# RESOURCES & ENV
# ============================================================================

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '1Gi'},
    limits={'cpu': '2000m', 'memory': '4Gi'}
)

nfs_volume = k8s.V1Volume(
    name='nfs-dbt-data-serving',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH_DBT)
)
nfs_mount = k8s.V1VolumeMount(
    name='nfs-dbt-data-serving',
    mount_path='/app/data_serving',
    read_only=True   # source code only — target/logs ghi vào /tmp trong pod
)

# ============================================================================
# TASKS
# ============================================================================

# Chờ task nạp cs_facebook_insight (caresoft_marketing_insights_dag, 07:30) của CÙNG ngày chạy.
# execution_delta = lệch lịch 08:45 - 07:30 để khớp logical date của DAG nguồn.
wait_cs_facebook_insight = ExternalTaskSensor(
    task_id='wait_cs_facebook_insight',
    external_dag_id='caresoft_marketing_insights_dag',
    external_task_id='marketing_insights_ingestion',
    execution_delta=timedelta(hours=1, minutes=15),
    allowed_states=['success'],
    failed_states=['failed', 'skipped', 'upstream_failed'],
    check_existence=True,
    mode='reschedule',          # nhả worker slot trong lúc chờ
    poke_interval=300,
    timeout=3 * 60 * 60,        # chờ tối đa 3 giờ
    dag=dag,
)

# Chờ data_serving_pr_etl (08:30) build xong mart_report_marketing_ad_cost_pr (chi phí ads Paris).
wait_report_pr = ExternalTaskSensor(
    task_id='wait_data_serving_pr',
    external_dag_id='data_serving_pr_etl',
    external_task_id='dbt_run_pr',
    execution_delta=timedelta(minutes=15),
    allowed_states=['success'],
    failed_states=['failed', 'skipped', 'upstream_failed'],
    check_existence=True,
    mode='reschedule',
    poke_interval=300,
    timeout=3 * 60 * 60,
    dag=dag,
)

# dbt build = run + test: test assert_rtds_out_sold_du_du_lieu_kangnam chặn build khi bảng
# report_crm_lead_customer_out_sold bị hỏng (báo cáo giữ số lần build tốt gần nhất).
dbt_build_rtds = KubernetesPodOperator(
    task_id='dbt_build_rtds',
    name='dbt-rtds-build',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['dbt'],
    arguments=[
        'build',
        '--select', 'tag:rtds',
        '--project-dir', '/app/data_serving',
        '--profiles-dir', '/app/data_serving',
        '--target-path', '/tmp/dbt_target',  # ghi vào pod local, không ra NFS
        '--log-path',    '/tmp/dbt_logs',    # ghi vào pod local, không ra NFS
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

[wait_cs_facebook_insight, wait_report_pr] >> dbt_build_rtds
