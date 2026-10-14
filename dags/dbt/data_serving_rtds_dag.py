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
# LỊCH CHẠY (2026-10-15, user chốt chạy 2 lần/ngày — trước đó 1 lần 08:45)
# ============================================================================
#  1. data_serving_rtds_etl      07:15 — build toàn bộ tag:rtds + chi phí ads Paris (mart_report_marketing_ad_cost_pr
#                                         và 2 bảng FB của nó) để có doanh số / khách sớm.
#     Nguồn: EHC HH/THV (~03:50), crm_lead / out_sold (~03:45), data_serving_erp_staging_etl 04:00 — xong trước theo lịch;
#            3 sensor chờ: caresoft_marketing_insights_dag 06:30 (cs_facebook_insight), data_serving_hh_revenue_etl 06:45,
#            data_serving_thv_revenue_etl 06:50.
#     ⚠ Google Sheet ads (google_ad_paris, tiktok_raw_ad, cp_google_kangnam, ggs_target_ads_thv_hh…) chỉ đủ T-1 sau lượt
#       call_functions_ggs 08:15 → lúc 07:15 chi phí Google/TikTok còn thiếu ngày hôm qua.
#  2. data_serving_rtds_ads_etl  08:45 — build lại riêng chi phí ads: int_rtds_chi_phi_ads+ (→ report_rtds_doanh_so_target_
#                                         thang_tableau). Chờ data_serving_pr_etl 08:30 (đã build mart_report_marketing_ad_cost_pr
#                                         trên sheet 08:15 — KHÔNG build lại bảng Paris ở đây để khỏi giẫm DAG Paris) và chờ
#                                         lượt 07:15 kết thúc (thành công hay lỗi) để 2 lượt không cùng ghi một bảng.
# execution_delta = lệch giờ lịch giữa DAG này và DAG nguồn (cùng là lịch ngày) để khớp logical date.

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
    description='Run Data Serving ETL Pipeline - Báo cáo doanh số tập đoàn HCI (tag:rtds) - lượt 07:15',
    schedule_interval="15 7 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['data-serving', 'rtds', 'bao_cao_doanh_so_hci'],
)

dag_ads = DAG(
    'data_serving_rtds_ads_etl',
    default_args=default_args,
    description='Run Data Serving ETL Pipeline - Báo cáo doanh số tập đoàn HCI - build lại chi phí ads lượt 08:45',
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


def wait_for(task_id, external_dag_id, external_task_id, execution_delta, target_dag,
             allowed_states=('success',), failed_states=('failed', 'skipped', 'upstream_failed')):
    """Sensor chờ task của DAG nguồn cùng ngày; mặc định nguồn fail → DAG này fail (không build trên dữ liệu thiếu)."""
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        execution_delta=execution_delta,
        allowed_states=list(allowed_states),
        failed_states=list(failed_states),
        check_existence=True,
        mode='reschedule',          # nhả worker slot trong lúc chờ
        poke_interval=300,
        timeout=3 * 60 * 60,        # chờ tối đa 3 giờ
        dag=target_dag,
    )


def dbt_build(task_id, name, select, target_dag, exclude=()):
    return KubernetesPodOperator(
        task_id=task_id,
        name=name,
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['dbt'],
        arguments=[
            'build',
            '--select', *select,
            *(['--exclude', *exclude] if exclude else []),
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
        dag=target_dag,
    )

# ============================================================================
# TASKS — lượt 07:15
# ============================================================================

# cs_facebook_insight (caresoft_marketing_insights_dag 06:30): 07:15 - 06:30
wait_cs_facebook_insight = wait_for('wait_cs_facebook_insight', 'caresoft_marketing_insights_dag',
                                    'marketing_insights_ingestion', timedelta(minutes=45), dag)
# rhh_ehc_payment_service / rhh_doanh_so_nha_thuoc (int_rtds_hongha): 07:15 - 06:45
wait_hh_revenue = wait_for('wait_hh_revenue', 'data_serving_hh_revenue_etl',
                           'dbt_run_hh_revenue', timedelta(minutes=30), dag)
# report_thv_revenue (int_rtds_thv): 07:15 - 06:50
wait_thv_revenue = wait_for('wait_thv_revenue', 'data_serving_thv_revenue_etl',
                            'dbt_run_thv_revenue', timedelta(minutes=25), dag)

# dbt build = run + test: test assert_rtds_out_sold_du_du_lieu_kangnam chặn build khi bảng
# report_crm_lead_customer_out_sold bị hỏng (báo cáo giữ số lần build tốt gần nhất).
# Kèm chi phí ads Paris (user chốt build ngay trong rtds, không chờ data_serving_pr_etl 08:30). Không build
# lại dim_campaign / dim_camp_adset_ad / m_utm_source_old_new_map (của DAG master_data/kn) — dùng bản sẵn có.
# --exclude tag:erp_staging: từ 2026-10-14 thư mục erp_staging cũng mang tag rtds (để build tay `-s tag:rtds` kéo theo
# stag_payment_transfer) — DAG data_serving_erp_staging_etl 04:00 đã build, không build lại lúc 07:15 (nặng, và tạo lại
# view_stag_products sẽ cascade-drop các view phụ thuộc giữa giờ các DAG khác đang đọc).
dbt_build_rtds = dbt_build('dbt_build_rtds', 'dbt-rtds-build',
                           ['stg_frmacpr_fb_insight_v2', 'stg_frmacpr_fb_insight_v3',
                            'mart_report_marketing_ad_cost_pr', 'tag:rtds'], dag,
                           exclude=['tag:erp_staging'])

[wait_cs_facebook_insight, wait_hh_revenue, wait_thv_revenue] >> dbt_build_rtds

# ============================================================================
# TASKS — lượt 08:45 (chi phí ads đủ Google Sheet T-1)
# ============================================================================

# mart_report_marketing_ad_cost_pr build trên sheet lượt 08:15: 08:45 - 08:30
wait_report_pr = wait_for('wait_data_serving_pr', 'data_serving_pr_etl',
                          'dbt_run_pr', timedelta(minutes=15), dag_ads)
# Lượt 07:15 phải KẾT THÚC (thành công hay lỗi đều được) để 2 lượt không cùng ghi report_rtds_doanh_so_target_thang_tableau.
wait_rtds_0715 = wait_for('wait_rtds_0715', 'data_serving_rtds_etl', 'dbt_build_rtds',
                          timedelta(hours=1, minutes=30), dag_ads,
                          allowed_states=('success', 'failed', 'upstream_failed', 'skipped'), failed_states=())

dbt_build_rtds_ads = dbt_build('dbt_build_rtds_ads', 'dbt-rtds-ads-build',
                               ['int_rtds_chi_phi_ads+'], dag_ads)

[wait_report_pr, wait_rtds_0715] >> dbt_build_rtds_ads
