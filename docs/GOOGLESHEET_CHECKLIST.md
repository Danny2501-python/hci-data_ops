# Google Sheet ETL Implementation Checklist

## ✅ Completed Tasks

### 1. Core Infrastructure
- [x] Create `googlesheet_config.py` module
- [x] Create `googlesheet_ingestion.py` script
- [x] Add `ingestion_timestamp` metadata column
- [x] Add `snapshot_date` partition column
- [x] Create Iceberg schema for `declare_api_googlesheet`

### 2. Configuration
- [x] Define schema matching old PostgreSQL table
- [x] Support all 20 fields from old system
- [x] Filter by `loai='IN'` for import direction
- [x] Add `gspread` and `oauth2client` to requirements.txt

### 3. Data Transformation
- [x] Column removal (`remove_column` CSV format)
- [x] Column renaming (`rename_column` JSON format)
- [x] Lowercase column normalization
- [x] Numeric type conversion
- [x] Empty column removal

### 4. Ingestion Logic
- [x] Google Sheets API connection
- [x] Read data from worksheets
- [x] Truncate mode (`method_data=True`)
- [x] Incremental mode (delete by `snapshot_date`)
- [x] Schema inference from DataFrame
- [x] Partition by `snapshot_date`

### 5. Airflow Integration
- [x] Create `googlesheet_lakehouse_dag.py`
- [x] Configure Google credentials volume mount
- [x] Set resource requirements
- [x] Add retry logic (2 retries, 5 min delay)

### 6. Migration
- [x] Create `migrate_googlesheet_config.py` script
- [x] Map all fields from old to new schema
- [x] Support full data migration

### 7. Documentation
- [x] Create `GOOGLESHEET_ETL.md`
- [x] Create `GOOGLESHEET_MIGRATION.md`
- [x] Create `ARCHITECTURE_REVIEW.md`
- [x] Create `source_types.py` constants

## ⬜ Pending Tasks

### 1. Testing
- [ ] Unit tests for `googlesheet_ingestion.py`
- [ ] Integration test with real Google Sheet
- [ ] Test migration script with sample data
- [ ] Test Airflow DAG execution
- [ ] Verify data quality in Bronze layer

### 2. Serving Layer
- [ ] Update `serving_ingestion.py` to read from Google Sheet Bronze tables
- [ ] Test UPSERT logic with Google Sheet data
- [ ] Verify primary key handling (`key` field)
- [ ] Test truncate mode in serving layer

### 3. Deployment
- [ ] Build Docker image with new dependencies
- [ ] Create Kubernetes secret for Google credentials
- [ ] Deploy to staging environment
- [ ] Run migration script in staging
- [ ] Validate data in staging

### 4. Production Rollout
- [ ] Backup old `declare_api_googsheet` table
- [ ] Run migration script in production
- [ ] Test 1-2 pilot Google Sheets
- [ ] Monitor logs and metrics
- [ ] Gradually migrate all sheets
- [ ] Decommission old Django-based system

### 5. Monitoring
- [ ] Add data quality checks
- [ ] Create Airflow alerts for failures
- [ ] Set up metrics dashboard
- [ ] Document troubleshooting procedures

### 6. Future Enhancements
- [ ] Implement OUT direction (DB → Google Sheet)
- [ ] Add scheduling based on `active_schedule` field
- [ ] Support custom queries (`query` field)
- [ ] Add incremental Google Sheet updates
- [ ] Implement data validation rules

## 🔧 Configuration Requirements

### Environment Variables
```bash
# Iceberg
ICEBERG_REST_URI_BRONZE=http://iceberg-rest:8181
CATALOG_NAME_BRONZE=bronze
ICEBERG_REST_URI_GOLD=http://iceberg-rest:8181
CATALOG_NAME_GOLD=gold

# S3/MinIO
S3_ENDPOINT=http://minio:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1

# Google Sheets
GOOGLE_CREDENTIALS_PATH=/app/credentials/google-credentials.json

# Old PostgreSQL (for migration)
OLD_POSTGRES_URI=postgresql://user:password@old-db:5432/etl_marketing
```

### Kubernetes Resources

#### NFS Volume
Credentials file is stored in NFS volume:
```bash
# File path in NFS
/mnt/nfs/data-ops/app/credentials/google-credentials.json

# No Kubernetes secret needed - file is mounted via NFS
```

#### ConfigMap (if needed)
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: googlesheet-config
  namespace: airflow
data:
  ICEBERG_REST_URI_BRONZE: "http://iceberg-rest:8181"
  CATALOG_NAME_BRONZE: "bronze"
  # ... other configs
```

## 📊 Success Criteria

### Phase 1: Development (Week 1)
- [x] All code files created
- [x] Documentation complete
- [ ] Unit tests passing
- [ ] Local testing successful

### Phase 2: Staging (Week 2)
- [ ] Staging deployment complete
- [ ] Migration script tested
- [ ] 2-3 pilot sheets migrated
- [ ] Data validation passed

### Phase 3: Production (Week 3)
- [ ] Production deployment
- [ ] All sheets migrated
- [ ] Monitoring in place
- [ ] Old system decommissioned

### Phase 4: Optimization (Week 4)
- [ ] Performance tuning
- [ ] Cost optimization
- [ ] User feedback incorporated
- [ ] Documentation updated

## 🐛 Known Issues & Limitations

### Current Limitations
1. **No OUT direction**: Export from DB to Google Sheet not implemented
2. **No scheduling**: `active_schedule` and `schedule` fields not used yet
3. **No custom queries**: `query` field for OUT direction not supported
4. **Manual serving sync**: Serving layer must be triggered separately

### Workarounds
1. **OUT direction**: Use old system temporarily
2. **Scheduling**: Use Airflow DAG schedule (daily 1 AM)
3. **Custom queries**: Not applicable for IN direction
4. **Serving sync**: Add serving task to DAG (future enhancement)

## 📝 Notes

### Design Decisions
1. **Separate config table**: Keep `declare_api_googlesheet` separate from `declare_table_etl` to maintain original logic
2. **Two-step process**: Bronze → Serving (vs direct load) for data versioning
3. **Metadata columns**: Add `ingestion_timestamp` for audit trail
4. **Partition strategy**: Use `snapshot_date` for efficient queries

### Migration Strategy
1. **Parallel run**: Keep old system running during migration
2. **Incremental migration**: Migrate sheets one by one
3. **Validation**: Compare row counts and sample data
4. **Rollback plan**: Keep old system available for 2 weeks

## 🔗 Related Documents

- [GOOGLESHEET_ETL.md](./GOOGLESHEET_ETL.md) - User guide
- [GOOGLESHEET_MIGRATION.md](./GOOGLESHEET_MIGRATION.md) - Migration details
- [ARCHITECTURE_REVIEW.md](./ARCHITECTURE_REVIEW.md) - Overall architecture
- [source_types.py](../app/common/source_types.py) - Source type constants

## 👥 Team Responsibilities

### Data Engineering
- [ ] Code review and approval
- [ ] Deployment to staging/production
- [ ] Monitoring and alerting setup

### QA
- [ ] Test plan creation
- [ ] Integration testing
- [ ] Data validation

### DevOps
- [ ] Kubernetes secret creation
- [ ] Docker image build and push
- [ ] Infrastructure provisioning

### Business Users
- [ ] UAT testing
- [ ] Feedback and bug reports
- [ ] Sign-off for production

---

**Last Updated:** 2026-02-03  
**Status:** Development Complete, Testing Pending  
**Next Milestone:** Staging Deployment
