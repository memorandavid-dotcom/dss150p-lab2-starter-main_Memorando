import pandas as pd
import os

file_path = 'data/customers.csv'
df = pd.read_csv(file_path)

print("--- TASK 1.2: CSV PROFILING ---")

# 1. Report file size and row/column counts
print(f"File Size: {os.path.getsize(file_path)} bytes")
print(f"Shape (Rows, Columns): {df.shape}\n")

# 2. List columns and infer an appropriate logical type for each
print("Columns and Data Types:")
print(df.dtypes)
print("\n")

# 3. Count missing values by column
print("Missing Values per Column:")
print(df.isna().sum())
print("\n")

# 4. Detect exact duplicate rows
duplicates = df.duplicated().sum()
print(f"Exact Duplicate Rows: {duplicates}\n")

# 5. Test whether customer_id is unique
# If total rows == unique customer_ids, it is unique.
is_unique = df['customer_id'].is_unique
print(f"Is customer_id strictly unique? {is_unique}")