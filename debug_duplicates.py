"""
Debug script to check duplicate keys in ticket source metadata
"""

from collections import Counter

# Simulate the deduplication logic
def check_duplicates(rows, columns):
    id_idx = columns.index('id')
    brand_id_idx = columns.index('brand_id')
    
    # Count unique keys
    keys = [(row[id_idx], row[brand_id_idx]) for row in rows]
    key_counts = Counter(keys)
    
    print(f"Total rows: {len(rows)}")
    print(f"Unique keys: {len(key_counts)}")
    print(f"Duplicates: {len(rows) - len(key_counts)}")
    
    # Show top duplicates
    duplicates = [(k, v) for k, v in key_counts.items() if v > 1]
    if duplicates:
        print(f"\nTop 10 duplicate keys:")
        for key, count in sorted(duplicates, key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {key}: {count} times")
    
    return len(key_counts)

# This would be called in the serving layer
# unique_count = check_duplicates(rows, columns)
