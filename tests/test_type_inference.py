"""
Test script for Google Sheet type inference
Verify that pandas dtype -> Iceberg type mapping works correctly
"""

import pandas as pd
import numpy as np
from datetime import datetime, date

# Create test DataFrame with various types
test_data = {
    'bool_col': [True, False, True],
    'int_col': [1, 2, 3],
    'float_col': [1.5, 2.5, 3.5],
    'string_col': ['a', 'b', 'c'],
    'datetime_col': [datetime.now(), datetime.now(), datetime.now()],
    'mixed_col': ['1', '2', '3'],  # Will be string
}

df = pd.DataFrame(test_data)

print("DataFrame dtypes:")
print(df.dtypes)
print("\n" + "="*60 + "\n")

# Test type inference logic
def get_iceberg_type(dtype):
    """Map pandas dtype to Iceberg type with proper precedence"""
    # Check bool BEFORE int (bool is subtype of int in pandas)
    if pd.api.types.is_bool_dtype(dtype):
        return "BooleanType"
    # Check datetime before numeric (some timestamps can be numeric)
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TimestampType"
    # Check integer
    elif pd.api.types.is_integer_dtype(dtype):
        return "IntegerType"
    # Check float
    elif pd.api.types.is_float_dtype(dtype):
        return "DoubleType"
    # Default to string
    else:
        return "StringType"

print("Type inference results:")
for col_name, dtype in df.dtypes.items():
    iceberg_type = get_iceberg_type(dtype)
    print(f"{col_name:20} | {str(dtype):20} | {iceberg_type}")

print("\n" + "="*60 + "\n")

# Test edge cases
print("Edge case tests:")

# Boolean check
bool_series = pd.Series([True, False, True])
print(f"Boolean series: is_bool={pd.api.types.is_bool_dtype(bool_series.dtype)}, "
      f"is_int={pd.api.types.is_integer_dtype(bool_series.dtype)}")

# Integer check
int_series = pd.Series([1, 2, 3])
print(f"Integer series: is_int={pd.api.types.is_integer_dtype(int_series.dtype)}, "
      f"is_bool={pd.api.types.is_bool_dtype(int_series.dtype)}")

# Float check
float_series = pd.Series([1.5, 2.5, 3.5])
print(f"Float series: is_float={pd.api.types.is_float_dtype(float_series.dtype)}, "
      f"is_int={pd.api.types.is_integer_dtype(float_series.dtype)}")

# String check
string_series = pd.Series(['a', 'b', 'c'])
print(f"String series: is_string={pd.api.types.is_string_dtype(string_series.dtype)}, "
      f"is_object={pd.api.types.is_object_dtype(string_series.dtype)}")

print("\n✅ Type inference test completed!")
