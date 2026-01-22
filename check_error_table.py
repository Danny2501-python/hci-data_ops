"""
Quick script to check error table data
"""
import os
from pyiceberg.catalog import load_catalog
from datetime import date

# Config
ICEBERG_REST_URI_GOLD = os.getenv('ICEBERG_REST_URI_GOLD', 'http://iceberg-rest-gold-zone:8181')
CATALOG_NAME_GOLD = os.getenv('CATALOG_NAME_GOLD', 'gold')
S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://10.152.183.65:9000')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

# Connect
catalog = load_catalog(
    CATALOG_NAME_GOLD,
    uri=ICEBERG_REST_URI_GOLD,
    **{
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": AWS_ACCESS_KEY,
        "s3.secret-access-key": AWS_SECRET_KEY,
        "s3.region": AWS_REGION,
    }
)

# Load table
table = catalog.load_table('caresoft.cs_deal_detail_error_api')

print(f"Table: {table.identifier}")
print(f"Schema: {table.schema()}")
print(f"\nCurrent snapshot: {table.current_snapshot()}")

# Scan with filter
from pyiceberg.expressions import EqualTo
from datetime import date, timedelta

snapshot_date = date.today() - timedelta(days=1)  # Yesterday
print(f"\nQuerying snapshot_date = {snapshot_date}")

scan = table.scan(
    row_filter=EqualTo("snapshot_date", snapshot_date),
    limit=10
)

print("\nFirst 10 error records:")
for batch in scan.to_arrow_batch_reader():
    print(batch.to_pandas())
    break

# Count total
print(f"\nTotal errors for {snapshot_date}:")
count_scan = table.scan(row_filter=EqualTo("snapshot_date", snapshot_date))
total = sum(len(batch) for batch in count_scan.to_arrow_batch_reader())
print(f"Total: {total} records")
