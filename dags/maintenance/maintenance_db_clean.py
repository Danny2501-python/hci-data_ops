from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình: Giữ lại dữ liệu trong vòng 30 ngày
RETENTION_DAYS = 1000

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'maintenance_db_clean',
    default_args=default_args,
    description='Dọn dẹp dữ liệu cũ trong Metadata Database (Postgres)',
    schedule_interval='@weekly', # Chạy định kỳ hàng tuần
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['maintenance', 'system'],
) as dag:

    # Task thực hiện dọn dẹp
    # Lệnh: airflow db clean --clean-before-timestamp YYYY-MM-DD --yes
    clean_db = BashOperator(
        task_id='clean_airflow_db',
        bash_command=f"""
        # Bước 1: Dọn dữ liệu cũ vào bảng archived
        airflow db clean --clean-before-timestamp $(date -d '{RETENTION_DAYS} days ago' +%Y-%m-%d) --yes
        
        # Bước 2: Xóa hẳn các bảng archived để giải phóng dung lượng đĩa
        airflow db drop-archived --yes
        """,
    )

    clean_db
