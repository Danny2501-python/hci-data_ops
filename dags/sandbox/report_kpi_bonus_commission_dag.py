# -*- coding: utf-8 -*-
"""
DAG dieu phoi ETL report_kpi_bonus_commission.

Code thuc thi: app/sandbox/report_kpi_bonus_commission.py (mount NFS tai /app/sandbox)
Nguon goc    : Adhoc_python_code/sandbox_transfer/quentk_3_report_kpi_bonus_commission_New_sys.pyw
Connection   : dwh_product_new
"""

from datetime import datetime, timedelta
import logging

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from kubernetes.client import models as k8s

logger = logging.getLogger(__name__)

# ============================================================================
# CAU HINH
# ============================================================================
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NAMESPACE = 'default'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'
TARGET_CONN_ID = 'dwh_product_new'

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')


def get_conn_uri(conn_id):
    """Resolve Airflow Connection -> URI ngay tai scheduler."""
    try:
        conn = PostgresHook(postgres_conn_id=conn_id).get_connection(conn_id)
        uri = conn.get_uri()
        if '?__extra__=' in uri:
            uri = uri.split('?__extra__=')[0]
        return uri
    except Exception as e:
        logger.warning(f'Khong resolve duoc connection {conn_id}: {e}')
        return ''


POSTGRES_URI = get_conn_uri(TARGET_CONN_ID)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026,6,1, tzinfo=local_tz),
    'email_on_failure': True,
    'email': ['quentk@scigroup.com.vn','quantridulieu@scigroup.com.vn'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='sandbox_report_kpi_bonus_commission',
    default_args=default_args,
    description='ETL report_kpi_bonus_commission (port tu quentk_3_report_kpi_bonus_commission_New_sys.pyw)',
    schedule='20 4 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['quentk'],
)

# ============================================================================
# TAI NGUYEN KUBERNETES
# ============================================================================
resource_requirements = k8s.V1ResourceRequirements(
    requests={'cpu': '1000m', 'memory': '4Gi'},
    limits={'cpu': '2000m', 'memory': '8Gi'},
)

nfs_volume = k8s.V1Volume(
    name='nfs-scripts',
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH),
)
nfs_mount = k8s.V1VolumeMount(name='nfs-scripts', mount_path='/app', read_only=True)

temp_disk_volume = k8s.V1Volume(
    name='temp-disk',
    empty_dir=k8s.V1EmptyDirVolumeSource(medium='', size_limit='5Gi'),
)
temp_disk_mount = k8s.V1VolumeMount(name='temp-disk', mount_path='/tmp', read_only=False)


def create_task(task_id, trigger_rule='all_success'):
    return KubernetesPodOperator(
        task_id=task_id,
        name='report-kpi-bonus-commission-' + task_id.replace('_', '-')[:40],
        namespace=NAMESPACE,
        image=PYTHON_ETL_IMAGE,
        image_pull_policy='IfNotPresent',
        cmds=['python'],
        arguments=['/app/sandbox/report_kpi_bonus_commission.py', '--task', task_id],
        volumes=[nfs_volume, temp_disk_volume],
        volume_mounts=[nfs_mount, temp_disk_mount],
        env_vars={
            'POSTGRES_URI': POSTGRES_URI,
            'TEMP_DIR': '/tmp',
        },
        container_resources=resource_requirements,
        is_delete_operator_pod=True,
        get_logs=True,
        trigger_rule=trigger_rule,
        dag=dag,
    )


def wait_for(dag_id, delta_minutes):
    """Cho DAG thuong nguon chay xong truoc khi bat dau.

    execution_delta = khoang cach gio chay giua hai DAG (cung chay hang ngay).
    mode='reschedule' de nha worker slot trong luc cho.
    """
    return ExternalTaskSensor(
        task_id=f'wait_{dag_id}',
        external_dag_id=f'sandbox_{dag_id}',
        external_task_id=None,                  # cho ca DAG chay xong
        execution_delta=timedelta(minutes=delta_minutes),
        # external_task_id=None -> sensor cho trang thai DAG RUN, nen chi duoc
        # dung gia tri cua DagRunState: queued / running / success / failed.
        # Dua 'skipped' hay 'upstream_failed' (trang thai TASK) vao se loi
        # ValueError ngay luc Airflow parse file.
        allowed_states=['success'],
        failed_states=['failed'],
        check_existence=True,
        mode='reschedule',
        poke_interval=300,
        timeout=3 * 60 * 60,
        dag=dag,
    )


# ============================================================================
# SENSOR PHU THUOC
# ============================================================================
s1 = wait_for('stag_crm', 20)

# ============================================================================
# TASK
# ============================================================================
t1 = create_task('report_kpi_bonus_commission')
t2 = create_task('report_kpi_bonus_commission_transpose')
t3 = create_task('update_info')

# ============================================================================
# PHU THUOC
# ============================================================================
# giu nguyen do thi phu thuoc cua ban goc
t1 >> t2 >> t3

# sensor chan cac task goc cua DAG
[s1] >> t1
