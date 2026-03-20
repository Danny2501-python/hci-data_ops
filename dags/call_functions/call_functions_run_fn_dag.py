from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pendulum

logger = logging.getLogger(__name__)

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

dag = DAG(
    'call_functions_run_fn_dag',
    default_args=default_args,
    description='Run function in dwh',
    schedule_interval='15 4 * * *',  # 4:15 AM daily
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['run_fn'],
)

# ============================================================================
# TASKS
# ============================================================================

def execute_agent_sync_functions(**kwargs):
    # Connections to sync to
    target_conns = ['dwh_product_new', 'dwh_sandbox']
    
    errors = []
    
    for conn_id in target_conns:
        try:
            logger.info(f"🚀 Connecting to: {conn_id}")
            hook = PostgresHook(postgres_conn_id=conn_id)
            
            sql = "SELECT sync_cs_agents_api_to_mapping();"
            logger.info(f"👉 Running: {sql}")
            
            result = hook.get_first(sql)
            logger.info(f"✅ Success on {conn_id}. Result: {result}")
        except Exception as e:
            error_msg = f"❌ Error executing on {conn_id}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            
    if errors:
        raise Exception(f"Task completed with some errors: {'; '.join(errors)}")

call_sync_functions = PythonOperator(
    task_id='call_agent_sync_functions',
    python_callable=execute_agent_sync_functions,
    dag=dag,
)

call_sync_functions
