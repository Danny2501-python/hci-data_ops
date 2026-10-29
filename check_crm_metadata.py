from app.common.etl_table_config import ensure_table_config_table_exists

tables = {
    "crm_case",
    "crm_content_complain",
    "crm_complain",
    "crm_complain_group",
    "crm_content_complain_service"
}

table = ensure_table_config_table_exists()

print("\n========== CRM ETL CONFIG ==========")

found = 0

for batch in table.scan().to_arrow_batch_reader():
    for row in batch.to_pylist():
        target = (row.get("table_name") or "").split(".")[-1].lower()
        source = (row.get("table_name_source") or "").split(".")[-1].lower()

        if target in tables or source in tables:
            found += 1
            print({
                k: row.get(k)
                for k in [
                    "id",
                    "table_name",
                    "table_name_source",
                    "source",
                    "active",
                    "is_transfrom",
                    "truncate"
                ]
            })

print(f"\nTotal matching configurations: {found}")
