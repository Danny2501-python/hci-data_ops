"""
Apply Python macro solution to all DAGs
Replace Airflow template variables with custom Python function
"""

import glob
import re

files = glob.glob('dags/*_dag.py')

print("=" * 80)
print("Applying Python Macro Solution to All DAGs")
print("=" * 80)
print()

# Step 1: Add custom macro function after timezone definition
macro_function = """
def get_yesterday_tz():
    \"\"\"Get yesterday's date in Asia/Ho_Chi_Minh timezone\"\"\"
    from datetime import datetime, timedelta
    now = datetime.now(local_tz)
    yesterday = now - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')
"""

# Step 2: Add user_defined_macros to DAG
# Step 3: Update arguments template

fixed_count = 0

for filepath in files:
    print(f"Processing: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    modified = False
    
    # Check if already has the macro
    if 'def get_yesterday_tz' in content:
        print(f"  ⏭️  Already has macro function")
    else:
        # Add macro function after local_tz definition
        pattern = r"(local_tz = pendulum\.timezone\('Asia/Ho_Chi_Minh'\)\s*\n)"
        if re.search(pattern, content):
            content = re.sub(
                pattern,
                r"\1\n" + macro_function + "\n",
                content,
                count=1
            )
            print(f"  ✅ Added macro function")
            modified = True
        else:
            print(f"  ⚠️  Could not find local_tz definition")
    
    # Add user_defined_macros to DAG if not exists
    if 'user_defined_macros' not in content:
        # Find DAG definition and add user_defined_macros before closing )
        pattern = r"(    params=\{[^}]+\}\s*\n    \}\s*\n)(\))"
        if re.search(pattern, content, re.DOTALL):
            content = re.sub(
                pattern,
                r"\1    user_defined_macros={'yesterday_tz': get_yesterday_tz},\n\2",
                content,
                count=1
            )
            print(f"  ✅ Added user_defined_macros to DAG")
            modified = True
        else:
            print(f"  ⚠️  Could not add user_defined_macros")
    else:
        print(f"  ⏭️  Already has user_defined_macros")
    
    # Update template from data_interval_start to yesterday_tz()
    old_template = r'\{\{ dag_run\.conf\.get\("snapshot_date"\) or \(data_interval_start \| ds\) \}\}'
    new_template = r'{{ dag_run.conf.get("snapshot_date") or yesterday_tz() }}'
    
    count = len(re.findall(old_template, content))
    if count > 0:
        content = re.sub(old_template, new_template, content)
        print(f"  ✅ Updated {count} template(s) to use yesterday_tz()")
        modified = True
    else:
        print(f"  ⏭️  Templates already correct or not found")
    
    if modified:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✅ Saved changes")
        fixed_count += 1
    
    print()

print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Files modified: {fixed_count}/{len(files)}")
print()
print("Changes applied:")
print("1. Added get_yesterday_tz() function")
print("2. Added user_defined_macros to DAG")
print("3. Updated templates to use yesterday_tz()")
print()
print("Benefits:")
print("✅ No dependency on Airflow template variables")
print("✅ Always calculates fresh T-1 in Asia/Ho_Chi_Minh timezone")
print("✅ Still allows manual override via dag_run.conf")
print("✅ No cache issues")
print()
print("Next steps:")
print("1. Review: git diff dags/")
print("2. Test: Trigger a DAG and verify snapshot_date")
print("3. Commit: git add dags/ && git commit -m 'Use Python macro for snapshot_date'")
