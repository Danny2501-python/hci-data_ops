"""
Fix snapshot_date template in all DAGs
Replace default("", true) with Airflow macro for T-1
Allows manual override while defaulting to data_interval_start
"""

import glob
import re

files = glob.glob('dags/*_dag.py')

# Pattern to match and replace
old_pattern = r'\{\{ dag_run\.conf\.get\("snapshot_date"\) \| default\("", true\) \}\}'
new_pattern = r'{{ dag_run.conf.get("snapshot_date") or (data_interval_start | ds) }}'

print("=" * 80)
print("Fixing snapshot_date template in all DAGs")
print("=" * 80)
print()
print("OLD: {{ dag_run.conf.get(\"snapshot_date\") | default(\"\", true) }}")
print("NEW: {{ dag_run.conf.get(\"snapshot_date\") or (data_interval_start | ds) }}")
print()
print("Benefits:")
print("✅ Auto runs: Uses T-1 from Airflow (data_interval_start)")
print("✅ Manual runs: Can override with custom date")
print("✅ No UTC timezone issues")
print()
print("=" * 80)
print()

fixed_count = 0
total_replacements = 0

for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Count occurrences
    count = len(re.findall(old_pattern, content))
    
    if count > 0:
        # Replace
        new_content = re.sub(old_pattern, new_pattern, content)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"✅ {filepath}: {count} replacements")
        fixed_count += 1
        total_replacements += count
    else:
        print(f"⏭️  {filepath}: no changes needed")

print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Files fixed: {fixed_count}/{len(files)}")
print(f"📝 Total replacements: {total_replacements}")
print()

if fixed_count > 0:
    print("✅ Done! Changes:")
    print("   - Auto runs: snapshot_date = data_interval_start (T-1)")
    print("   - Manual runs: snapshot_date = dag_run.conf.snapshot_date")
    print()
    print("Next steps:")
    print("1. Review: git diff dags/")
    print("2. Test: Trigger a manual run to verify")
    print("3. Commit: git add dags/ && git commit -m 'Use Airflow macro for snapshot_date'")
else:
    print("ℹ️  All files already use correct template")
