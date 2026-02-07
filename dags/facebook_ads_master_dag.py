"""
Airflow Master DAG for Facebook Ads ETL Pipeline
Triggers 3 DAGs sequentially:
1. Base Entities (Campaigns, AdSets, Creatives)
2. Ads
3. Insights (Insights + Message Insights)

Default Parameters:
- date_preset: last_3d (last 3 days)
- days_for_ago: 1 (yesterday)
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.models.param import Param
import logging
import pendulum

logger = logging.getLogger(__name__)

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
    'facebook_ads_master_etl',
    default_args=default_args,
    description='Master DAG: Triggers Base Entities → Ads → Insights (Sequential)',
    schedule_interval=None,
    start_date=datetime(2026, 2, 6, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'facebook_ads', 'master', 'marketing'],
    params={
        'date_preset': Param(
            default='last_3d',
            type='string',
            description='Date preset for Base Entities & Ads (last_3d, last_7d, last_30d, etc.)',
        ),
        'days_for_ago': Param(
            default=1,
            type='integer',
            description='Days ago for Insights (1 = yesterday, 3 = 3 days ago)',
        ),
    },
)

# ============================================================================
# TRIGGER TASKS
# ============================================================================

# Task 1: Trigger Base Entities DAG (Campaigns, AdSets, Creatives)
trigger_base_entities = TriggerDagRunOperator(
    task_id='trigger_base_entities',
    trigger_dag_id='facebook_ads_base_entities_etl',
    conf={
        'date_preset': '{{ params.date_preset }}',
    },
    wait_for_completion=True,  # Wait for DAG to complete before continuing
    poke_interval=30,  # Check every 30 seconds
    dag=dag,
)

# Task 2: Trigger Ads DAG
trigger_ads = TriggerDagRunOperator(
    task_id='trigger_ads',
    trigger_dag_id='facebook_ads_ads_etl',
    conf={
        'date_preset': '{{ params.date_preset }}',
    },
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# Task 3: Trigger Insights DAG (Insights + Message Insights)
trigger_insights = TriggerDagRunOperator(
    task_id='trigger_insights',
    trigger_dag_id='facebook_ads_insights_etl',
    conf={
        'days_for_ago': '{{ params.days_for_ago }}',
    },
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES - SEQUENTIAL EXECUTION
# ============================================================================

# Run DAGs in sequence to avoid rate limit issues
# Base Entities → Ads → Insights
trigger_base_entities >> trigger_ads >> trigger_insights
