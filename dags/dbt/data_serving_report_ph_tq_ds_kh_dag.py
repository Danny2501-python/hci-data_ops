"""
Airflow DAG: dbt data_serving – TỔNG QUAN DOANH SỐ & KHÁCH HÀNG MKT SUNWOO (tag `report_ph_tq_ds_kh`)
            -> sandbox_ph.report_ph_tqdskh_khach / _ds / _chi_tieu

    ph.* (ERP SUN, brand_etl = 9, sync 03:00–03:45)  +  public.ty_gia_acb (DAG acb_fx_etl 06:15)
        ──► dbt build --select tag:report_ph_tq_ds_kh ──► 3 bảng sandbox_ph cho workbook Tableau Sunwoo

- 07:05 giờ VN: đủ nguồn (chỉ đọc ph.* và tỷ giá ACB; KHÔNG dùng stag_* nên không phải chờ
  data_serving_erp_staging_etl 04:00, cũng không dùng cs_facebook_insight nên không phải chờ 08:30).
  Chọn 07:05 thay vì 07:00 để không giẫm pod với data_serving_report_convert_kangnam_etl (07:00),
  và xong trước cụm DAG dbt 07:30 (stag_discount_review / report_hh_revenue).
- CHẠY KÈM TEST (giống data_serving_ph_etl, khác các DAG dbt khác dùng --exclude resource_type:test).
- 3 bảng này cũng nằm trong tag `phi`, nên được build lại lần nữa trong data_serving_ph_etl (09:00)
  cùng toàn bộ báo cáo PH — DAG này là lượt ra số sớm cho Tableau. Giữ nguyên lịch 09:00 của DAG đó.
- LƯU Ý seed: `target_sunwoo_chitieu` (chỉ tiêu Sunwoo) chỉ gắn tag `phi`, KHÔNG gắn
  `report_ph_tq_ds_kh` → DAG này không load lại seed. Sửa seeds/ph/target_sunwoo_chitieu.csv thì
  số chỉ tiêu mới chỉ vào bảng ở lượt 09:00 (hoặc chạy tay `dbt seed --select target_sunwoo_chitieu`).
- Chỉ ghi schema sandbox_ph (macro generate_schema_name), chỉ đọc ph.* / public.*.
Chi tiết logic: data_serving models/report_ph_tq_ds_kh/README.md.
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
    'data_serving_report_ph_tq_ds_kh_etl',
    default_args=default_args,
    description='Run Data Serving dbt - Tong quan DS & KH MKT Sunwoo (tag report_ph_tq_ds_kh) -> sandbox_ph, co chay test',
    schedule_interval='5 7 * * *',   # 07:05 VN – xem docstring
    start_date=datetime(2026, 10, 12, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['data-serving', 'ph', 'philippines', 'report_ph_tq_ds_kh'],
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

dbt_build_report_ph_tq_ds_kh = KubernetesPodOperator(
    task_id='dbt_build_report_ph_tq_ds_kh',
    name='dbt-report-ph-tq-ds-kh-build',
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['dbt'],
    arguments=[
        'build',
        '--select', 'tag:report_ph_tq_ds_kh',
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

dbt_build_report_ph_tq_ds_kh
