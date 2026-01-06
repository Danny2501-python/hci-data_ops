from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

# Configuration
PYTHON_ETL_IMAGE = '111050/python-etl-base:1.0.0'
NFS_SERVER = '10.10.10.245'
NFS_PATH = '/srv/nfs_share/app'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
}

dag = DAG(
    'api_to_iceberg_test',
    default_args=default_args,
    description='Test API to Iceberg using pyiceberg',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['etl', 'iceberg', 'test'],
)

api_to_iceberg = KubernetesPodOperator(
    task_id='api_to_iceberg',
    name='api-to-iceberg',
    namespace='default',
    image=PYTHON_ETL_IMAGE,
    image_pull_policy='IfNotPresent',
    cmds=['python'],
    arguments=['/app/api_to_iceberg.py', '{{ ds }}'],
    
    # Mount NFS for scripts
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
            mount_path='/app',  # ← Mount directly to /app (container /app is empty)
            read_only=True
        )
    ],
    
    # Environment variables
    env_vars={
        'PYTHONPATH': '/app',  # ← PYTHONPATH points to /app
        
        # API config
        'API_URL': 'https://jsonplaceholder.typicode.com/posts',
        
        # Iceberg REST catalog config
        'ICEBERG_REST_URI': 'http://iceberg-rest-bronze-zone:8181',  # ← Fixed service name
        'CATALOG_NAME': 'bronze',
        'TABLE_NAME': 'api_data.posts',
        
        # MinIO config
        'S3_ENDPOINT': 'http://10.152.183.65:9000',
        'AWS_ACCESS_KEY_ID': 'admin',
        'AWS_SECRET_ACCESS_KEY': 'password',
    },
    
    # Resource limits
    container_resources=k8s.V1ResourceRequirements(
        requests={'cpu': '500m', 'memory': '1Gi'},
        limits={'cpu': '2000m', 'memory': '4Gi'}
    ),
    
    # Pod settings
    is_delete_operator_pod=True,  # ← Auto cleanup after completion
    get_logs=True,
    dag=dag,
)

api_to_iceberg
