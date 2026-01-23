# Database Connection Environment Variables for data-ops

## Source Database Connections

Based on `etl_marketing/app/app/settings.py`, these are the environment variables needed:

### 1. ERP Database (10.10.10.154)
```bash
ERP_SCI_URI=postgresql://${PG_USER_ERP}:${PG_PASSWORD_ERP}@10.10.10.154:5432/${PG_DATABASE_ERP}
```

**Environment variables:**
- `PG_USER_ERP` - ERP database user
- `PG_PASSWORD_ERP` - ERP database password  
- `PG_DATABASE_ERP` - ERP database name

---

### 2. GiaoViec Database (10.10.10.153)
```bash
GIAOVIEC_URI=postgresql://${PG_USER_GIAOVIEC}:${PG_PASSWORD_GIAOVIEC}@10.10.10.153:${PG_PORT_GIAOVIEC}/${PG_GIAOVIEC}
```

**Environment variables:**
- `PG_USER_GIAOVIEC` - GiaoViec database user
- `PG_PASSWORD_GIAOVIEC` - GiaoViec database password
- `PG_GIAOVIEC` - GiaoViec database name
- `PG_HOST_GIAOVIEC` - GiaoViec host (10.10.10.153)
- `PG_PORT_GIAOVIEC` - GiaoViec port

---

### 3. Seeding Database (10.10.10.234)
```bash
SEEDING_URI=postgresql://${PG_USER_SEEDING}:${PG_PASSWORD_SEEDING}@10.10.10.234:${PG_PORT_SEEDING}/${PG_SEEDING}
```

**Environment variables:**
- `PG_USER_SEEDING` - Seeding database user
- `PG_PASSWORD_SEEDING` - Seeding database password
- `PG_SEEDING` - Seeding database name
- `PG_HOST_SEEDING` - Seeding host (10.10.10.234)
- `PG_PORT_SEEDING` - Seeding port

---

### 4. Custom DB / Staging SCI (10.10.10.214)
```bash
CUSTOM_DB_URI=postgresql://${PG_USER}:${PG_PASSWORD}@10.10.10.214:${PG_PORT}/${PG_DATABASE_CARESOFT}
```

**Environment variables:**
- `PG_USER` - Default PostgreSQL user
- `PG_PASSWORD` - Default PostgreSQL password
- `PG_DATABASE_CARESOFT` - CareSoft database name
- `PG_HOST` - Default host
- `PG_PORT` - Default port

---

### 5. HongHa Database (118.70.128.33)
```bash
HONGHA_URI=postgresql://${PG_USER_HONGHA}:${PG_PASSWORD_HONGHA}@118.70.128.33:${PG_PORT_HONGHA}/${PG_HONGHA}
```

**Environment variables:**
- `PG_USER_HONGHA` - HongHa database user
- `PG_PASSWORD_HONGHA` - HongHa database password
- `PG_HONGHA` - HongHa database name
- `PG_HOST_HONGHA` - HongHa host (118.70.128.33)
- `PG_PORT_HONGHA` - HongHa port

---

### 6. Du Phong Database (10.10.10.226)
```bash
DU_PHONG_URI=postgresql://${PG_USER}:${PG_PASSWORD}@10.10.10.226:${PG_PORT}/${PG_DATABASE_CARESOFT}
```

**Environment variables:**
- `PG_USER` - Default PostgreSQL user
- `PG_PASSWORD` - Default PostgreSQL password
- `PG_DATABASE_CARESOFT` - CareSoft database name
- `PG_HOST_DU_PHONG` - Du Phong host (10.10.10.226)
- `PG_PORT` - Default port

---

### 7. Dev SCI Database (10.10.10.152)
```bash
DEV_SCI_URI=postgresql://${PG_USER}:${PG_PASSWORD}@10.10.10.152:${PG_PORT}/${PG_DATABASE}
```

**Environment variables:**
- `PG_USER` - Default PostgreSQL user
- `PG_PASSWORD` - Default PostgreSQL password
- `PG_DATABASE` - Default database name
- `PG_PORT` - Default port

---

### 8. Test SCI Database (10.10.10.249)
```bash
TEST_SCI_URI=postgresql://${PG_USER}:${PG_PASSWORD}@10.10.10.249:${PG_PORT}/${PG_DATABASE}
```

**Environment variables:**
- `PG_USER` - Default PostgreSQL user
- `PG_PASSWORD` - Default PostgreSQL password
- `PG_DATABASE` - Default database name
- `PG_PORT` - Default port

---

### 9. Portal Guide Database (10.10.10.230)
```bash
PORTAL_GUIDE_URI=postgresql://${PG_PORTAL_GUIDE_USER}:${PG_PORTAL_GUIDE_PASSWORD}@10.10.10.230:${PG_PORTAL_GUIDE_PORT}/${PG_PORTAL_GUIDE_DB}
```

**Environment variables:**
- `PG_PORTAL_GUIDE_USER` - Portal Guide database user
- `PG_PORTAL_GUIDE_PASSWORD` - Portal Guide database password
- `PG_PORTAL_GUIDE_DB` - Portal Guide database name
- `PG_PORTAL_GUIDE_HOST` - Portal Guide host (10.10.10.230)
- `PG_PORTAL_GUIDE_PORT` - Portal Guide port

---

## How to Set in Airflow

### Option 1: Airflow Connections (Recommended)

Create connections in Airflow UI or via CLI:

```bash
# ERP
airflow connections add 'erp_sci' \
  --conn-type 'postgres' \
  --conn-host '10.10.10.154' \
  --conn-schema '${PG_DATABASE_ERP}' \
  --conn-login '${PG_USER_ERP}' \
  --conn-password '${PG_PASSWORD_ERP}' \
  --conn-port '5432'

# GiaoViec
airflow connections add 'giaoviec' \
  --conn-type 'postgres' \
  --conn-host '10.10.10.153' \
  --conn-schema '${PG_GIAOVIEC}' \
  --conn-login '${PG_USER_GIAOVIEC}' \
  --conn-password '${PG_PASSWORD_GIAOVIEC}' \
  --conn-port '${PG_PORT_GIAOVIEC}'

# ... (repeat for other databases)
```

### Option 2: Environment Variables in DAG

Add to `call_functions_erp_dag.py`:

```python
common_env_vars = {
    # ... existing vars ...
    
    # Source database connections
    'ERP_SCI_URI': os.getenv('ERP_SCI_URI'),
    'GIAOVIEC_URI': os.getenv('GIAOVIEC_URI'),
    'SEEDING_URI': os.getenv('SEEDING_URI'),
    'CUSTOM_DB_URI': os.getenv('CUSTOM_DB_URI'),
    'HONGHA_URI': os.getenv('HONGHA_URI'),
    'DU_PHONG_URI': os.getenv('DU_PHONG_URI'),
    'DEV_SCI_URI': os.getenv('DEV_SCI_URI'),
    'TEST_SCI_URI': os.getenv('TEST_SCI_URI'),
    'PORTAL_GUIDE_URI': os.getenv('PORTAL_GUIDE_URI'),
}
```

### Option 3: Kubernetes Secrets

Create a secret with all connection strings:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: source-db-connections
type: Opaque
stringData:
  ERP_SCI_URI: "postgresql://user:pass@10.10.10.154:5432/erp"
  GIAOVIEC_URI: "postgresql://user:pass@10.10.10.153:5432/giaoviec"
  # ... etc
```

Then reference in DAG using `env_from`.

---

## Current Status

✅ `SOURCE_DB_CONNECTIONS` already defined in `bronze_ingestion.py`  
⏳ Need to add environment variables to Airflow/Kubernetes  
⏳ Need to update DAG to pass these env vars to tasks

---

## Next Steps

1. Get actual credentials from `etl_marketing` `.env` file
2. Create Kubernetes secret or Airflow variables
3. Update `call_functions_erp_dag.py` to include these env vars
4. Test connection from Bronze ingestion script
