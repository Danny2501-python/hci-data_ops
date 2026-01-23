"""Fix escaped quotes in DAG files"""
import glob

files = glob.glob('dags/*_dag.py')

for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Fix escaped quotes
    if "\\'" in content:
        content = content.replace("\\'", "'")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ Fixed: {filepath}")
    else:
        print(f"⏭️  Skipped: {filepath}")

print("\n✅ Done!")
