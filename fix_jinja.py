import os
import re

# Pattern to find and replace
old_pattern = r'\{\{ dag_run\.conf\.get\("snapshot_date", ""\) \}\}'
new_pattern = r'{{ dag_run.conf.get("snapshot_date") | default("", true) }}'

# Directory to search
base_dir = r"d:\sci-projects\data-ops\dags"

# Counter
count = 0
files_updated = []

# Walk through all Python files
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.startswith('caresoft_') and file.endswith('.py'):
            filepath = os.path.join(root, file)
            
            # Read file
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Replace
            new_content = re.sub(old_pattern, new_pattern, content)
            
            # Write back if changed
            if new_content != content:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                count += 1
                files_updated.append(file)
                print(f"Updated: {file}")

print(f"\nTotal files updated: {count}")
print(f"Files: {', '.join(files_updated)}")
