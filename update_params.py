import os
import re

# Pattern to find and replace
old_pattern = r"snapshot_date_str = sys\.argv\[1\] if len\(sys\.argv\) > 1 else None"
new_pattern = r"snapshot_date_str = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None"

# Directory to search
base_dir = r"d:\sci-projects\data-ops\app\caresoft"

# Counter
count = 0

# Walk through all Python files
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith('.py'):
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
                print(f"Updated: {filepath}")

print(f"\nTotal files updated: {count}")
