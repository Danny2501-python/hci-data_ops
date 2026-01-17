-- Check actual row count in PostgreSQL
SELECT COUNT(*) as total_rows 
FROM cs_ticket_source_detail_api_v1;

-- Check by brand_id
SELECT brand_id, COUNT(*) as count
FROM cs_ticket_source_detail_api_v1
GROUP BY brand_id
ORDER BY brand_id;

-- Check sample data
SELECT id, brand_id, source_name, updated_at, snapshot_date
FROM cs_ticket_source_detail_api_v1
ORDER BY id
LIMIT 20;
