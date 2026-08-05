"""Hourly Pancake POS order ingestion and chat retention cleanup."""

import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s


logger = logging.getLogger(__name__)
PYTHON_ETL_IMAGE = "111050/python-etl-base:1.0.0"
NAMESPACE = "default"
NFS_SERVER = "10.10.10.245"
NFS_PATH = "/srv/nfs_share/app"


def _postgres_uri() -> str:
    try:
        connection = PostgresHook(postgres_conn_id="dwh_product_new").get_connection(
            "dwh_product_new"
        )
        uri = connection.get_uri().split("?__extra__=", 1)[0]
        logger.info("Loaded dwh_product_new Airflow connection")
        return uri
    except Exception as exc:
        logger.warning(f"Could not load dwh_product_new connection: {exc}")
        return ""


dwh_product_new_uri = _postgres_uri()
pancake_user_access_token = Variable.get(
    "pancake_user_access_token", default_var=""
)

resource_requirements = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},
    limits={"cpu": "1000m", "memory": "1Gi"},
)
nfs_volume = k8s.V1Volume(
    name="nfs-app", nfs=k8s.V1NFSVolumeSource(server=NFS_SERVER, path=NFS_PATH)
)
nfs_mount = k8s.V1VolumeMount(name="nfs-app", mount_path="/app", read_only=True)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-eng@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
dag = DAG(
    "pancake_pos_orders_etl",
    default_args=default_args,
    description="Pancake POS hourly orders and 30-day chat retention",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 8, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "pancake", "pos", "orders", "retention"],
)

ingest_orders_task = KubernetesPodOperator(
    task_id="ingest_pancake_orders",
    name="pancake-orders-ingestion-pod",
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy="IfNotPresent",
    cmds=["python"],
    arguments=["/app/pancake/pancake_orders_ingestion.py"],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars={
        "POSTGRES_URI": dwh_product_new_uri,
        "PANCAKE_USER_ACCESS_TOKEN": pancake_user_access_token,
    },
    container_resources=resource_requirements,
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

cleanup_chat_logs_task = KubernetesPodOperator(
    task_id="cleanup_pancake_chat_logs",
    name="pancake-chat-cleanup-pod",
    namespace=NAMESPACE,
    image=PYTHON_ETL_IMAGE,
    image_pull_policy="IfNotPresent",
    cmds=["python"],
    arguments=["/app/pancake/pancake_chat_cleanup.py"],
    volumes=[nfs_volume],
    volume_mounts=[nfs_mount],
    env_vars={
        "POSTGRES_URI": dwh_product_new_uri,
        "PANCAKE_CHAT_RETENTION_DAYS": "30",
    },
    container_resources=resource_requirements,
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

ingest_orders_task >> cleanup_chat_logs_task
