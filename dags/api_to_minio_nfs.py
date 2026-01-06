from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

# Configuration
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
}

dag = DAG(
    'api_to_minio_nfs',
    default_args=default_args,
    description='Fetch API data and save to MinIO raw-zone',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['etl', 'api', 'minio', 'nfs'],
)

fetch_api_to_minio = KubernetesPodOperator(
    task_id='fetch_api_to_minio',
    name='api-to-minio',
    namespace='default',
    image=PYTHON_ETL_IMAGE,
    cmds=['python'],
    arguments=['/app/api_to_minio.py', '{{ ds }}'],
    volumes=[
        k8s.V1Volume(
            name='nfs-scripts',
            nfs=k8s.V1NFSVolumeSource(
                server=NFS_SERVER,
                path=NFS_PATH
            )
        )
    ],
    volume_mounts=[
        k8s.V1VolumeMount(
            name='nfs-scripts',
            mount_path='/app',
            read_only=True
        )
    ],
    env_vars={
        'PYTHONPATH': '/app',
        'API_URL': 'https://jsonplaceholder.typicode.com/posts',
        'S3_ENDPOINT': '10.152.183.65:9000',
        'AWS_ACCESS_KEY_ID': 'admin',
        'AWS_SECRET_ACCESS_KEY': 'password',
    },
    container_resources=k8s.V1ResourceRequirements(
        requests={'cpu': '250m', 'memory': '512Mi'},
        limits={'cpu': '1000m', 'memory': '2Gi'}
    ),
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

fetch_api_to_minio
