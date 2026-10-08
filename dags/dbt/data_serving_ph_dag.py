"""
Airflow DAG: dbt data_serving – báo cáo PHILIPPINES (tag `phi`) -> sandbox_ph.*

    ph.* (ERP SUN, brand_etl = 9)  +  public.ty_gia_acb (DAG acb_fx_etl 06:15)  +  FB campaign sun_
        ──► dbt build --select tag:phi ──► sandbox_ph.report_marketing_ad_cost_ph(_thu),
                                           sandbox_ph.report_ph_sale_online,
                                           sandbox_ph.report_ph_doanh_thu, sandbox_ph.report_ph_doanh_thu_chi_phi

- 09:00 giờ VN, sau khi đủ nguồn:
    ERP sync ph.* (03:00–03:45), tỷ giá ACB (acb_fx_etl 06:15),
    sheet inbox m_inbox_sunwoo (cùng kiểu m_inbox_kangnam – call_functions_ggs_etl 03:15 / 08:15),
    cs_facebook_insight (mốc 08:30 đã kiểm chứng đủ dữ liệu, xem data_serving_report_pr_daily_chinhanh_dag);
  không trùng giờ các DAG dbt 08:30–08:50.
- CHẠY KÈM TEST (khác các DAG dbt khác dùng --exclude resource_type:test): test lỗi
  (thiếu tỷ giá, lệch nguồn, trùng bảng nhập tay sandbox_ph.tk_test / danh_dau_thu_cong…)
  -> các model phụ thuộc phía sau không build (giữ số cũ), task FAILED để biết mà xử lý.
- Chỉ ghi schema sandbox_ph (macro generate_schema_name), chỉ đọc ph.* / public.*.
Chi tiết logic: data_serving models/report_ph_doanh_thu/README.md.
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

PYTHON_ETL_IMAGE = '111050/python-etl-base:latest'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH_DBT = '/srv/nfs_share/dbt/data_serving'  # Path on NFS Server

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'data_serving_ph_etl',
    default_args=default_args,
    description='Run Data Serving dbt - Philippines (tag phi) -> sandbox_ph, có chạy test',
    schedule_interval='0 9 * * *',   # 09:00 VN – xem docstring
    start_date=datetime(2026, 10, 8, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['data-serving', 'ph', 'philippines'],
)

# ============================================================================
# RESOURCES & ENV
# ============================================================================

resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '500m', 'memory': '512Mi'},
    limits={'cpu': '2000m', 'memory': '2Gi'}
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

dbt_build_ph = KubernetesPodOperator(
    task_id='dbt_build_ph',
    name='dbt-ph-build',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['dbt'],
    arguments=[
        'build',
        '--select', 'tag:phi',
        '--project-dir', '/app/data_serving',
        '--profiles-dir', '/app/data_serving',
        '--target-path', '/tmp/dbt_target',  # ghi vào pod local, không ra NFS
        '--log-path',    '/tmp/dbt_logs',    # ghi vào pod local, không ra NFS
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    container_resources=resource_requirements,
    execution_timeout=timedelta(hours=1),
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

dbt_build_ph
