"""Task 1.3: Comprehensive JSON Profiling (data/orders.json).

Profiles orders.json including:
- Root structure confirmation (list vs object)
- Total record count and file size
- Top-level schema & nested structure inspection
- Null / missing key detection across all records
- Inferred logical types for numeric and datetime fields
- Primary key uniqueness test (order_id)
- Foreign key candidate relationship (customer_id)
- Order date temporal range and order status distribution
- Acquisition & ingestion recommendations
"""
from pathlib import Path
import os
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FILE_PATH = ROOT / 'data' / 'orders.json'

def profile_orders_json():
    print("=" * 70)
    print("TASK 1.3: JSON PROFILING - orders.json")
    print("=" * 70)

    if not FILE_PATH.exists():
        print(f"ERROR: File not found at {FILE_PATH}")
        return

    file_size = os.path.getsize(FILE_PATH)
    print(f"File Path    : {FILE_PATH.relative_to(ROOT)}")
    print(f"File Size    : {file_size:,} bytes")

    with FILE_PATH.open('r', encoding='utf-8') as f:
        raw_data = json.load(f)

    # 1. Root structure verification
    root_type = type(raw_data).__name__
    print(f"1. Root Structure: {root_type}")
    if isinstance(raw_data, list):
        print("  - Structure is a JSON Array containing tabular/record objects.")
        total_records = len(raw_data)
        print(f"  - Total Record Count: {total_records}")
    else:
        print("  - Structure is a JSON Object (non-standard collection).")
        return

    # Convert to DataFrame for analytical profiling
    df = pd.json_normalize(raw_data)

    # 2. Top-level Schema and Data Types
    print("\n2. Top-Level Fields and Inferred Types:")
    print(f"  {'Field Name':<20} {'Pandas Dtype':<16} {'Inferred Logical Type':<20}")
    print("  " + "-" * 58)
    logical_map = {
        'order_id': 'String (Identifier)',
        'customer_id': 'String (Foreign Key)',
        'order_date': 'Timestamp (ISO-8601)',
        'total_amount': 'Float (Currency USD)',
        'status': 'String (Categorical Status)',
        'items_count': 'Integer (Item Quantity)'
    }
    for col in df.columns:
        print(f"  {col:<20} {str(df[col].dtype):<16} {logical_map.get(col, 'Derived'):<20}")

    # 3. Missing Value / Null Analysis
    print("\n3. Missing Value Analysis (per key across all records):")
    print(f"  {'Field Name':<20} {'Missing Count':>15} {'Missing %':>12}")
    print("  " + "-" * 49)
    missing = df.isna().sum()
    for col in df.columns:
        cnt = missing[col]
        pct = (cnt / total_records) * 100
        print(f"  {col:<20} {cnt:>15} {pct:>11.2f}%")

    # 4. Primary Key & Foreign Key Integrity
    print("\n4. Key Integrity Checks:")
    if 'order_id' in df.columns:
        is_pk_unique = df['order_id'].is_unique
        unique_orders = df['order_id'].nunique()
        print(f"  order_id unique?     : {is_pk_unique} ({unique_orders} / {total_records})")
    if 'customer_id' in df.columns:
        unique_customers = df['customer_id'].nunique()
        print(f"  customer_id distinct : {unique_customers} distinct customer references")

    # 5. Temporal Range Analysis (order_date)
    print("\n5. Temporal Analysis (order_date):")
    if 'order_date' in df.columns:
        try:
            dates = pd.to_datetime(df['order_date'])
            print(f"  Earliest Order       : {dates.min().isoformat()}")
            print(f"  Latest Order         : {dates.max().isoformat()}")
        except Exception as e:
            print(f"  Error parsing order_date: {e}")

    # 6. Numeric Summary
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    if num_cols:
        print("\n6. Numeric Summary Statistics:")
        print(df[num_cols].describe().round(2).to_string())

    # 7. Categorical Breakdown (status)
    if 'status' in df.columns:
        print("\n7. Order Status Breakdown:")
        for status, cnt in df['status'].value_counts().items():
            print(f"  - {status:<15}: {cnt:>5} ({cnt/total_records*100:.1f}%)")

    # 8. Recommendation
    print("\n8. Acquisition Recommendation:")
    print("  - Ingestion Method : Batch file copy to raw/files/orders.json.")
    print("  - Idempotency      : SHA-256 fingerprinting via raw/manifest.json.")
    print("  - Partitioning     : Partition downstream tables by order_date (daily or monthly).")
    print("=" * 70 + "\n")

if __name__ == '__main__':
    profile_orders_json()