import pandas as pd
import os

file_path = 'data/products.parquet'

print("--- TASK 1.4: PARQUET PROFILING ---")

# 1. Report file size
print(f"File Size: {os.path.getsize(file_path)} bytes")

# 2. Read file with pandas (requires pyarrow)
df = pd.read_parquet(file_path)

# 3. Row and column counts & dtypes
print(f"Shape (Rows, Columns): {df.shape}\n")
print("Data Types & Schema Behavior:")
print(df.dtypes)
print("\n")

# 4. Missing values and duplicates
print("Missing Values per Column:")
print(df.isna().sum())
print(f"\nExact Duplicate Rows: {df.duplicated().sum()}")