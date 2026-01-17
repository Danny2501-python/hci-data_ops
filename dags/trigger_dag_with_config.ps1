# PowerShell script to trigger Airflow DAG with custom config
# Usage: .\trigger_dag_with_config.ps1 -DagId "caresoft_deal_etl" -SnapshotDate "2026-01-15"

param(
    [string]$DagId = "caresoft_deal_etl",
    [string]$SnapshotDate = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
)

Write-Host "Triggering DAG: $DagId" -ForegroundColor Cyan
Write-Host "Snapshot Date: $SnapshotDate" -ForegroundColor Cyan

# Build config JSON
$config = @{
    snapshot_date = $SnapshotDate
} | ConvertTo-Json -Compress

# Trigger DAG with config
kubectl exec -n default deployment/airflow-scheduler -- `
    airflow dags trigger "$DagId" --conf "$config"

Write-Host "✅ DAG triggered successfully!" -ForegroundColor Green
