#!/bin/bash
# Script to trigger Airflow DAG with custom config
# Usage: ./trigger_dag_with_config.sh <dag_id> <snapshot_date>

DAG_ID=${1:-caresoft_deal_etl}
SNAPSHOT_DATE=${2:-$(date -d "yesterday" +%Y-%m-%d)}

echo "Triggering DAG: $DAG_ID"
echo "Snapshot Date: $SNAPSHOT_DATE"

# Trigger DAG with config
airflow dags trigger "$DAG_ID" \
  --conf "{\"snapshot_date\": \"$SNAPSHOT_DATE\"}"

echo "✅ DAG triggered successfully!"
