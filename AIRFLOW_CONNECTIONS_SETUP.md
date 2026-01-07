# Airflow Connections Setup Guide

## Overview
The CareSoft Lakehouse ETL pipeline requires Airflow Connections to be configured for accessing the 4 ERP systems.

## Required Connections

### 1. SCI ERP Connection
**Connection ID**: `sci_erp_sci`  
**Connection Type**: `Postgres`  
**Configuration**:
- Host: `<SCI_ERP_HOST>`
- Schema: `<SCI_DATABASE_NAME>`
- Login: `<SCI_USERNAME>`
- Password: `<SCI_PASSWORD>`
- Port: `5432`

**Brand ETL ID**: 7

### 2. KN ERP Connection
**Connection ID**: `sci_erp_kn`  
**Connection Type**: `Postgres`  
**Configuration**:
- Host: `<KN_ERP_HOST>`
- Schema: `<KN_DATABASE_NAME>`
- Login: `<KN_USERNAME>`
- Password: `<KN_PASSWORD>`
- Port: `5432`

**Brand ETL ID**: 8

### 3. PR ERP Connection
**Connection ID**: `sci_erp_pr`  
**Connection Type**: `Postgres`  
**Configuration**:
- Host: `<PR_ERP_HOST>`
- Schema: `<PR_DATABASE_NAME>`
- Login: `<PR_USERNAME>`
- Password: `<PR_PASSWORD>`
- Port: `5432`

**Brand ETL ID**: 9

### 4. HH ERP Connection
**Connection ID**: `sci_erp_hh`  
**Connection Type**: `Postgres`  
**Configuration**:
- Host: `<HH_ERP_HOST>`
- Schema: `<HH_DATABASE_NAME>`
- Login: `<HH_USERNAME>`
- Password: `<HH_PASSWORD>`
- Port: `5432`

**Brand ETL ID**: 10

## Setup via Airflow UI

### Step 1: Navigate to Connections
1. Open Airflow UI: http://airflow.lakehouse.com
2. Go to **Admin** → **Connections**

### Step 2: Add Each Connection
For each ERP system (SCI, KN, PR, HH):

1. Click **"+"** (Add a new record)
2. Fill in the form:
   - **Connection Id**: `sci_erp_<erp_name>` (e.g., `sci_erp_sci`)
   - **Connection Type**: `Postgres`
   - **Host**: ERP database host
   - **Schema**: Database name
   - **Login**: Database username
   - **Password**: Database password
   - **Port**: `5432` (default PostgreSQL port)
3. Click **Save**

### Step 3: Test Connections
After adding all connections, test them:

```bash
# Test SCI connection
airflow connections test sci_erp_sci

# Test KN connection
airflow connections test sci_erp_kn

# Test PR connection
airflow connections test sci_erp_pr

# Test HH connection
airflow connections test sci_erp_hh
```

## Setup via Airflow CLI

Alternatively, you can add connections via CLI:

```bash
# Add SCI ERP connection
airflow connections add 'sci_erp_sci' \
    --conn-type 'postgres' \
    --conn-host '<SCI_HOST>' \
    --conn-schema '<SCI_DB>' \
    --conn-login '<SCI_USER>' \
    --conn-password '<SCI_PASSWORD>' \
    --conn-port 5432

# Add KN ERP connection
airflow connections add 'sci_erp_kn' \
    --conn-type 'postgres' \
    --conn-host '<KN_HOST>' \
    --conn-schema '<KN_DB>' \
    --conn-login '<KN_USER>' \
    --conn-password '<KN_PASSWORD>' \
    --conn-port 5432

# Add PR ERP connection
airflow connections add 'sci_erp_pr' \
    --conn-type 'postgres' \
    --conn-host '<PR_HOST>' \
    --conn-schema '<PR_DB>' \
    --conn-login '<PR_USER>' \
    --conn-password '<PR_PASSWORD>' \
    --conn-port 5432

# Add HH ERP connection
airflow connections add 'sci_erp_hh' \
    --conn-type 'postgres' \
    --conn-host '<HH_HOST>' \
    --conn-schema '<HH_DB>' \
    --conn-login '<HH_USER>' \
    --conn-password '<HH_PASSWORD>' \
    --conn-port 5432
```

## Setup via Environment Variables (Kubernetes Secrets)

For production, it's recommended to use Kubernetes Secrets:

### Step 1: Create Kubernetes Secrets
```bash
# Create secret for SCI ERP
kubectl create secret generic airflow-conn-sci-erp-sci \
  --from-literal=conn-uri="postgresql://<USER>:<PASSWORD>@<HOST>:5432/<DB>" \
  -n airflow

# Create secret for KN ERP
kubectl create secret generic airflow-conn-sci-erp-kn \
  --from-literal=conn-uri="postgresql://<USER>:<PASSWORD>@<HOST>:5432/<DB>" \
  -n airflow

# Create secret for PR ERP
kubectl create secret generic airflow-conn-sci-erp-pr \
  --from-literal=conn-uri="postgresql://<USER>:<PASSWORD>@<HOST>:5432/<DB>" \
  -n airflow

# Create secret for HH ERP
kubectl create secret generic airflow-conn-sci-erp-hh \
  --from-literal=conn-uri="postgresql://<USER>:<PASSWORD>@<HOST>:5432/<DB>" \
  -n airflow
```

### Step 2: Mount Secrets in Airflow Deployment
Add to your Airflow deployment YAML:

```yaml
env:
  - name: AIRFLOW_CONN_SCI_ERP_SCI
    valueFrom:
      secretKeyRef:
        name: airflow-conn-sci-erp-sci
        key: conn-uri
  - name: AIRFLOW_CONN_SCI_ERP_KN
    valueFrom:
      secretKeyRef:
        name: airflow-conn-sci-erp-kn
        key: conn-uri
  - name: AIRFLOW_CONN_SCI_ERP_PR
    valueFrom:
      secretKeyRef:
        name: airflow-conn-sci-erp-pr
        key: conn-uri
  - name: AIRFLOW_CONN_SCI_ERP_HH
    valueFrom:
      secretKeyRef:
        name: airflow-conn-sci-erp-hh
        key: conn-uri
```

## How Connections are Used in the DAG

The DAG uses **PostgresHook** to load connection details at DAG parse time, following the same pattern as `Dev_huonghh_ETL_ERP_Thuonghieu.py`:

```python
from airflow.hooks.postgres_hook import PostgresHook

erp_connection_env_vars = {}
erp_connections_config = {
    'SCI': 'sci_erp_sci',
    'KN': 'sci_erp_kn', 
    'PR': 'sci_erp_pr',
    'HH': 'sci_erp_hh',
}

# Load connections at DAG parse time
for erp_name, conn_id in erp_connections_config.items():
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        
        # Extract connection details
        erp_connection_env_vars[f'ERP_{erp_name}_HOST'] = conn.host or ''
        erp_connection_env_vars[f'ERP_{erp_name}_PORT'] = str(conn.port or 5432)
        erp_connection_env_vars[f'ERP_{erp_name}_DB'] = conn.schema or ''
        erp_connection_env_vars[f'ERP_{erp_name}_USER'] = conn.login or ''
        erp_connection_env_vars[f'ERP_{erp_name}_PASSWORD'] = conn.password or ''
    except Exception as e:
        logger.warning(f"Could not load {erp_name} connection: {e}")
        # Set empty defaults if connection not found
```

These environment variables are then passed to the KubernetesPodOperator:

```python
task_erp_webhook_ingestion = KubernetesPodOperator(
    ...
    env_vars={
        **common_env_vars,
        **erp_connection_env_vars,  # ERP connections from PostgresHook
        'ICEBERG_REST_URI': 'http://iceberg-rest-bronze-zone:8181',
        'CATALOG_NAME': 'bronze-zone',
    },
    ...
)
```

**Important Notes**:
- Connections are loaded at **DAG parse time** (when Airflow reads the DAG file)
- If a connection is not found, empty values are used (the task will skip that ERP)
- The Python script running in the pod receives these as environment variables
- This approach is consistent with existing DAGs like `Dev_huonghh_ETL_ERP_Thuonghieu.py`

## Verification

After setting up connections, verify they work:

### 1. Check Airflow UI
Go to **Admin** → **Connections** and verify all 4 connections are listed.

### 2. Test DAG
Trigger the DAG manually:

```bash
airflow dags trigger caresoft_lakehouse_etl
```

### 3. Check Logs
View the logs for the `erp_direct_ingestion` task:

```bash
airflow tasks logs caresoft_lakehouse_etl erp_direct_ingestion <execution_date>
```

You should see logs like:
```
Processing ERP: SCI
Created connection to ERP SCI
Successfully read X records from ERP SCI
...
```

## Troubleshooting

### Issue: "Connection not found"
**Solution**: Ensure connection IDs match exactly:
- `sci_erp_sci`
- `sci_erp_kn`
- `sci_erp_pr`
- `sci_erp_hh`

### Issue: "Authentication failed"
**Solution**: Verify username and password are correct in the connection configuration.

### Issue: "Host not reachable"
**Solution**: 
- Check network connectivity from Airflow to ERP systems
- Verify firewall rules allow connections
- Ensure ERP databases are accessible from Kubernetes cluster

### Issue: "Table api_log not found"
**Solution**: Verify the table name is correct in each ERP database. If the table has a different name, update `erp_webhook_ingestion.py`.

## Security Best Practices

1. **Use Kubernetes Secrets** for production environments
2. **Rotate passwords** regularly
3. **Use read-only database users** for ETL jobs
4. **Enable SSL/TLS** for database connections if possible
5. **Audit connection access** regularly

## Next Steps

After setting up connections:
1. Deploy scripts to NFS
2. Create Iceberg tables
3. Test the pipeline
4. Monitor execution

---
**Last Updated**: 2026-01-07  
**Maintained By**: Data Engineering Team
