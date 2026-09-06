import pandas as pd
import json

file_path = 'data/orders.json'

print("--- TASK 1.3: JSON PROFILING ---")

# 1. Confirm that the root structure is a list of records
with open(file_path, 'r') as file:
    raw_json = json.load(file)
print(f"1. Root structure type: {type(raw_json)}")

# Load into pandas for tabular profiling
df = pd.read_json(file_path)

# 2. List top-level keys
print(f"\n2. Top-level keys (columns):")
print(df.columns.tolist())

# 3. Identify timestamp and numeric fields (using dtypes)
print("\n3. Inferred Data Types:")
print(df.dtypes)

# 4. Count records and inspect nulls/missing keys
print(f"\n4. Total Records: {len(df)}")
print("Missing values per key:")
print(df.isna().sum())