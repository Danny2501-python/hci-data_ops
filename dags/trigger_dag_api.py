# Python script to trigger Airflow DAG with custom config via REST API
# Usage: python trigger_dag_api.py --dag_id caresoft_deal_etl --snapshot_date 2026-01-15

import requests
import argparse
from datetime import datetime, timedelta

# Airflow configuration
AIRFLOW_URL = "http://localhost:8080"  # Change to your Airflow URL
USERNAME = "admin"  # Change to your username
PASSWORD = "admin"  # Change to your password

def trigger_dag(dag_id: str, snapshot_date: str):
    """Trigger Airflow DAG with custom config"""
    
    url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
    
    payload = {
        "conf": {
            "snapshot_date": snapshot_date
        }
    }
    
    response = requests.post(
        url,
        json=payload,
        auth=(USERNAME, PASSWORD),
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code == 200:
        print(f"✅ Successfully triggered DAG: {dag_id}")
        print(f"   Snapshot Date: {snapshot_date}")
        print(f"   DAG Run ID: {response.json().get('dag_run_id')}")
    else:
        print(f"❌ Failed to trigger DAG: {response.status_code}")
        print(f"   Error: {response.text}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trigger Airflow DAG with config")
    parser.add_argument("--dag_id", default="caresoft_deal_etl", help="DAG ID to trigger")
    parser.add_argument(
        "--snapshot_date", 
        default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Snapshot date (YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    trigger_dag(args.dag_id, args.snapshot_date)
