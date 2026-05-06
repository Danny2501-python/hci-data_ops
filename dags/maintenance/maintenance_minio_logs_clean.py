from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Cấu hình
BUCKET_NAME = 'airflow-logs'
RETENTION_DAYS = 30
CONN_ID = 'minio_logs_conn'

def delete_old_logs():
    s3_hook = S3Hook(aws_conn_id=CONN_ID)
    
    # Lấy danh sách tất cả các keys (files) trong bucket
    keys = s3_hook.list_keys(bucket_name=BUCKET_NAME)
    
    if not keys:
        logging.info(f"Không có file nào trong bucket {BUCKET_NAME}")
        return

    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
    deleted_count = 0

    for key in keys:
        # Lấy metadata của file để xem ngày chỉnh sửa cuối cùng
        obj = s3_hook.get_key(key, BUCKET_NAME)
        # obj.last_modified trả về datetime ở dạng UTC
        if obj.last_modified.replace(tzinfo=None) < cutoff_date:
            s3_hook.delete_objects(bucket=BUCKET_NAME, keys=[key])
            deleted_count += 1
            logging.info(f"Đã xóa: {key} (Sửa đổi lần cuối: {obj.last_modified})")

    logging.info(f"Hoàn thành dọn dẹp MinIO. Tổng số file đã xóa: {deleted_count}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'maintenance_minio_logs_clean',
    default_args=default_args,
    description='Dọn dẹp log cũ trên MinIO (S3)',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['maintenance', 'minio', 'logs'],
) as dag:

    clean_minio_task = PythonOperator(
        task_id='clean_minio_logs',
        python_callable=delete_old_logs,
    )

    clean_minio_task
