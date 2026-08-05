"""Build and test the Pancake dbt staging models."""

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


PYTHON_ETL_IMAGE = "111050/python-etl-base:latest"
NAMESPACE = "default"
NFS_SERVER = "10.10.10.245"
NFS_PATH_DBT = "/srv/nfs_share/dbt/data_serving"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-eng@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
dag = DAG(
    "data_serving_pancake_etl",
    default_args=default_args,
    description="Build and test Pancake staging views",
    schedule_interval="30 * * * *",
    start_date=datetime(2026, 8, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["data-serving", "pancake"],
)

resource_requirements = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},
    limits={"cpu": "1000m", "memory": "1Gi"},
)
nfs_volume = k8s.V1Volume(
    name="nfs-dbt-data-serving",
    nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH_DBT),
)
nfs_mount = k8s.V1VolumeMount(
    name="nfs-dbt-data-serving",
    mount_path="/app/data_serving",
    read_only=True,
)

dbt_build_pancake = KubernetesPodOperator(
    task_id="dbt_build_pancake",
    name="dbt-pancake-build",
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy="IfNotPresent",
    cmds=["dbt"],
    arguments=[
        "build",
        "--select",
        "tag:pancake",
        "--project-dir",
        "/app/data_serving",
        "--profiles-dir",
        "/app/data_serving",
        "--target-path",
        "/tmp/dbt_target",
        "--log-path",
        "/tmp/dbt_logs",
    ],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    container_resources=resource_requirements,
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

dbt_build_pancake
