from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
import pendulum

# ============================================================================
# BACKFILL CONFIGURATION (Edit dates to change the date range)
# ============================================================================
START_DATE_STR = "2026-01-01"            # Start date (YYYY-MM-DD)
END_DATE_STR = "2026-05-01"              # End date (YYYY-MM-DD)
# ============================================================================

# Timezone for Vietnam
local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def generate_date_list(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    delta = end - start
    
    dates = []
    for i in range(delta.days + 1):
        day = start + timedelta(days=i)
        dates.append(day.strftime("%Y-%m-%d"))
    return dates

# Generate date list at parsing time
dates_to_backfill = generate_date_list(START_DATE_STR, END_DATE_STR)

with DAG(
    'caresoft_thv_backfill_dag',
    default_args=default_args,
    description='Backfill runner DAG: Triggers target DAG sequentially over a date range',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['caresoft', 'backfill', 'thv'],
    params={
        'target_dag_id': Param(
            default="caresoft_agent_thv_etl",
            type="string",
            description="The DAG ID you want to backfill (e.g. caresoft_agent_thv_etl, caresoft_ticket_thv_etl, caresoft_deal_thv_etl)",
        )
    },
) as dag:

    previous_task = None
    
    for date_str in dates_to_backfill:
        task_id = f"trigger_{date_str.replace('-', '_')}"
        
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="{{ params.target_dag_id }}",
            conf={"snapshot_date": date_str},
            wait_for_completion=True,  # Wait for the triggered DAG to complete before starting next day
            poke_interval=15,          # Poll the status of target DAG run every 15 seconds
        )
        
        if previous_task:
            previous_task >> trigger_task
        previous_task = trigger_task
