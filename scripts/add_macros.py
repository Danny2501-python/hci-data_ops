"""
Add user_defined_macros to all DAGs that have get_yesterday_tz function
"""

import glob
import re

files = glob.glob('dags/*_dag.py')

for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Skip if already has user_defined_macros
    if 'user_defined_macros' in content:
        print(f"⏭️  {filepath}: already has user_defined_macros")
        continue
    
    # Skip if doesn't have get_yesterday_tz
    if 'def get_yesterday_tz' not in content:
        print(f"⏭️  {filepath}: no get_yesterday_tz function")
        continue
    
    # Add user_defined_macros before closing )
    pattern = r"(    \},\s*\n)(\))"
    if re.search(pattern, content):
        new_content = re.sub(
            pattern,
            r"\1    user_defined_macros={'yesterday_tz': get_yesterday_tz},\n\2",
            content,
            count=1
        )
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"✅ {filepath}: added user_defined_macros")
    else:
        print(f"⚠️  {filepath}: pattern not found")

print("\n✅ Done!")
