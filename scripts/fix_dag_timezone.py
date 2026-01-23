"""
Fix timezone for all Airflow DAGs
Add pendulum import and timezone-aware start_date
"""

import os
import re

# DAG files to fix
dag_files = [
    'd:/sci-projects/data-ops/dags/caresoft_ticket_dag.py',
    'd:/sci-projects/data-ops/dags/caresoft_contact_dag.py',
    'd:/sci-projects/data-ops/dags/caresoft_service_dag.py',
    'd:/sci-projects/data-ops/dags/caresoft_group_dag.py',
    'd:/sci-projects/data-ops/dags/caresoft_custom_field_dag.py',
    'd:/sci-projects/data-ops/dags/caresoft_chat_dag.py',
    'd:/sci-projects/data-ops/dags/caresoft_chat_agent_update_dag.py',
    'd:/sci-projects/data-ops/dags/caresoft_call_dag.py',
    'd:/sci-projects/data-ops/dags/caresoft_agent_dag.py',
    'd:/sci-projects/data-ops/dags/call_functions_erp_dag.py',
]

def fix_dag_timezone(filepath):
    """Fix timezone for a single DAG file"""
    print(f"Processing: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if already has pendulum
    if 'import pendulum' in content:
        print(f"  ✅ Already has pendulum import")
        return False
    
    # 1. Add pendulum import after other imports
    import_pattern = r'(import logging\s*\n)'
    if re.search(import_pattern, content):
        content = re.sub(
            import_pattern,
            r'\1import pendulum\n',
            content,
            count=1
        )
        print(f"  ✅ Added pendulum import")
    else:
        print(f"  ⚠️  Could not find import logging")
        return False
    
    # 2. Add timezone definition before DAG definition
    dag_pattern = r'(}\s*\n\s*\ndag = DAG\()'
    if re.search(dag_pattern, content):
        content = re.sub(
            dag_pattern,
            r'}\n\n# Timezone for Vietnam\nlocal_tz = pendulum.timezone(\'Asia/Ho_Chi_Minh\')\n\ndag = DAG(',
            content,
            count=1
        )
        print(f"  ✅ Added timezone definition")
    else:
        print(f"  ⚠️  Could not find DAG definition")
        return False
    
    # 3. Fix start_date
    start_date_pattern = r'start_date=datetime\(2026, 1, 1\),'
    if re.search(start_date_pattern, content):
        content = re.sub(
            start_date_pattern,
            r'start_date=datetime(2026, 1, 1, tzinfo=local_tz),',
            content
        )
        print(f"  ✅ Fixed start_date")
    else:
        print(f"  ⚠️  Could not find start_date")
        return False
    
    # Write back
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"  ✅ Successfully fixed {filepath}\n")
    return True

def main():
    print("=" * 80)
    print("Fixing timezone for all Airflow DAGs")
    print("=" * 80)
    print()
    
    fixed_count = 0
    skipped_count = 0
    error_count = 0
    
    for dag_file in dag_files:
        if not os.path.exists(dag_file):
            print(f"❌ File not found: {dag_file}\n")
            error_count += 1
            continue
        
        try:
            if fix_dag_timezone(dag_file):
                fixed_count += 1
            else:
                skipped_count += 1
        except Exception as e:
            print(f"❌ Error processing {dag_file}: {e}\n")
            error_count += 1
    
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✅ Fixed: {fixed_count}")
    print(f"⏭️  Skipped: {skipped_count}")
    print(f"❌ Errors: {error_count}")
    print(f"📊 Total: {len(dag_files)}")
    print()
    
    if fixed_count > 0:
        print("Next steps:")
        print("1. Review changes: git diff dags/")
        print("2. Commit: git add dags/ && git commit -m 'Fix timezone for all DAGs'")
        print("3. Push: git push")

if __name__ == '__main__':
    main()
