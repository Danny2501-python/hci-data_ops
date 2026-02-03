# Type Inference Logic - Google Sheet Ingestion

## Problem

Khi infer schema từ pandas DataFrame sang Iceberg, cần chú ý **thứ tự check type** vì pandas có type hierarchy:

```python
# ❌ WRONG ORDER - Bool sẽ bị nhận nhầm thành Int
type_checks = [
    (pd.api.types.is_integer_dtype, IntegerType()),  # Bool matches this!
    (pd.api.types.is_bool_dtype, BooleanType()),     # Never reached
]

# ✅ CORRECT ORDER - Check specific types first
if pd.api.types.is_bool_dtype(dtype):      # Check bool FIRST
    return BooleanType()
elif pd.api.types.is_integer_dtype(dtype):  # Then check int
    return IntegerType()
```

## Why Order Matters

### Pandas Type Hierarchy

```
object (most general)
  ├── string
  ├── datetime64
  └── numeric
      ├── bool (subtype of int!)
      ├── int64
      └── float64
```

**Key insight:** `is_integer_dtype(bool_series)` returns `True` vì bool là subtype của int trong pandas!

## Solution

### Implementation in `googlesheet_ingestion.py`

```python
def get_iceberg_type(dtype):
    """Map pandas dtype to Iceberg type with proper precedence"""
    
    # 1. Check bool BEFORE int (bool is subtype of int)
    if pd.api.types.is_bool_dtype(dtype):
        return BooleanType()
    
    # 2. Check datetime before numeric
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return TimestampType()
    
    # 3. Check integer
    elif pd.api.types.is_integer_dtype(dtype):
        return IntegerType()
    
    # 4. Check float
    elif pd.api.types.is_float_dtype(dtype):
        return DoubleType()
    
    # 5. Default to string
    else:
        return StringType()
```

## Type Mapping Table

| Pandas dtype | Check Function | Iceberg Type | Priority |
|--------------|----------------|--------------|----------|
| `bool` | `is_bool_dtype()` | `BooleanType()` | 1 (highest) |
| `datetime64[ns]` | `is_datetime64_any_dtype()` | `TimestampType()` | 2 |
| `int64`, `int32` | `is_integer_dtype()` | `IntegerType()` | 3 |
| `float64`, `float32` | `is_float_dtype()` | `DoubleType()` | 4 |
| `object`, `string` | (default) | `StringType()` | 5 (lowest) |

## Test Cases

### Test 1: Boolean Column
```python
df = pd.DataFrame({'col': [True, False, True]})
# dtype: bool
# Expected: BooleanType()
# ✅ Correct with new logic
# ❌ Would be IntegerType() with wrong order
```

### Test 2: Integer Column
```python
df = pd.DataFrame({'col': [1, 2, 3]})
# dtype: int64
# Expected: IntegerType()
# ✅ Correct
```

### Test 3: Float Column
```python
df = pd.DataFrame({'col': [1.5, 2.5, 3.5]})
# dtype: float64
# Expected: DoubleType()
# ✅ Correct
```

### Test 4: String Column
```python
df = pd.DataFrame({'col': ['a', 'b', 'c']})
# dtype: object
# Expected: StringType()
# ✅ Correct
```

### Test 5: Datetime Column
```python
df = pd.DataFrame({'col': [datetime.now(), datetime.now()]})
# dtype: datetime64[ns]
# Expected: TimestampType()
# ✅ Correct
```

## Google Sheets Specific Behavior

Google Sheets API returns all data as **strings** initially. The code then tries to convert to numeric:

```python
# From read_google_sheet()
for col in df.columns:
    try:
        df[col] = df[col].astype(float)  # Try convert to float
    except ValueError:
        continue  # Keep as string if conversion fails
```

**Result:**
- Numeric cells → `float64` → `DoubleType()`
- Text cells → `object` → `StringType()`
- Empty cells → `object` → `StringType()`

**Note:** Google Sheets doesn't have native boolean or datetime types in the API response, so:
- Booleans are stored as strings: `"TRUE"`, `"FALSE"` → `StringType()`
- Dates are stored as strings: `"2026-02-03"` → `StringType()`

## Comparison with Bronze Ingestion

`bronze_ingestion.py` doesn't use pandas type inference. It reads schema directly from PostgreSQL:

```python
# bronze_ingestion.py approach
target_schema = schema_to_pyarrow(table.schema())  # From Iceberg table
# Then maps data to match this schema
```

**Advantage:** More accurate, uses actual database types  
**Disadvantage:** Requires table to exist first

`googlesheet_ingestion.py` must infer because:
- No pre-existing schema
- Data comes from Google Sheets (no type info)
- Need to create Iceberg table on first run

## Best Practices

1. **Always check specific types before general types**
   - Bool before Int
   - Datetime before Numeric
   
2. **Use elif chain, not list comprehension**
   - Ensures only one type matches
   - Clear precedence order
   
3. **Test with edge cases**
   - Empty DataFrames
   - Mixed type columns
   - All-null columns
   
4. **Document type hierarchy**
   - Help future maintainers understand why order matters

## Running Tests

```bash
# Run type inference test
cd d:\sci-projects\data-ops
python tests\test_type_inference.py

# Expected output:
# DataFrame dtypes:
# bool_col         bool
# int_col          int64
# float_col        float64
# string_col       object
# datetime_col     datetime64[ns]
# mixed_col        object
#
# Type inference results:
# bool_col             | bool                 | BooleanType
# int_col              | int64                | IntegerType
# float_col            | float64              | DoubleType
# string_col           | object               | StringType
# datetime_col         | datetime64[ns]       | TimestampType
# mixed_col            | object               | StringType
```

## References

- [Pandas dtype hierarchy](https://pandas.pydata.org/docs/reference/api/pandas.api.types.html)
- [PyIceberg types](https://py.iceberg.apache.org/api/#types)
- [Google Sheets API data format](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values)
