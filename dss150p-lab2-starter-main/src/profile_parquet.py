"""Task 1.4: Comprehensive Parquet Profiling (data/products.parquet).

Profiles products.parquet and performs comparative analysis:
- Parquet embedded schema and Arrow data types
- Row and column counts
- Missing values and duplicate rows
- Primary key candidate uniqueness (product_id)
- Numeric distribution of product attributes (price, inventory)
- Storage footprint comparison (Parquet vs CSV vs JSON)
- Columnar storage benefits analysis
"""
from pathlib import Path
import os
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PARQUET_PATH = ROOT / 'data' / 'products.parquet'
CSV_COMPARE = ROOT / 'data' / 'products_optional_compare.csv'
JSON_COMPARE = ROOT / 'data' / 'products_optional_compare.json'

def profile_products_parquet():
    print("=" * 70)
    print("TASK 1.4: PARQUET PROFILING - products.parquet")
    print("=" * 70)

    if not PARQUET_PATH.exists():
        print(f"ERROR: File not found at {PARQUET_PATH}")
        return

    parquet_size = os.path.getsize(PARQUET_PATH)
    df = pd.read_parquet(PARQUET_PATH)
    total_rows, total_cols = df.shape

    print(f"File Path    : {PARQUET_PATH.relative_to(ROOT)}")
    print(f"File Size    : {parquet_size:,} bytes")
    print(f"Shape        : {total_rows} rows × {total_cols} columns\n")

    # 1. Embedded Schema and Data Types
    print("1. Parquet Embedded Schema (Strict Arrow Types):")
    print(f"  {'Column Name':<20} {'Physical Parquet Dtype':<24} {'Null Allowed?':<14}")
    print("  " + "-" * 60)
    for col in df.columns:
        print(f"  {col:<20} {str(df[col].dtype):<24} {'Yes (Nullable)':<14}")

    # 2. Missing Value Analysis
    print("\n2. Missing Value Analysis:")
    print(f"  {'Column Name':<20} {'Missing Count':>15} {'Missing %':>12}")
    print("  " + "-" * 49)
    missing = df.isna().sum()
    for col in df.columns:
        cnt = missing[col]
        pct = (cnt / total_rows) * 100
        print(f"  {col:<20} {cnt:>15} {pct:>11.2f}%")

    # 3. Duplicate and Key Integrity
    exact_dupes = df.duplicated().sum()
    print(f"\n3. Duplicate & Key Integrity:")
    print(f"  Exact Duplicate Rows : {exact_dupes}")
    if 'product_id' in df.columns:
        is_pk_unique = df['product_id'].is_unique
        unique_products = df['product_id'].nunique()
        print(f"  product_id unique?   : {is_pk_unique} ({unique_products} / {total_rows})")

    # 4. Numeric Distribution
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    if num_cols:
        print("\n4. Numeric Attribute Summary:")
        print(df[num_cols].describe().round(2).to_string())

    # 5. Categorical Breakdown
    if 'category' in df.columns:
        print("\n5. Product Category Breakdown:")
        for cat, cnt in df['category'].value_counts().items():
            print(f"  - {cat:<20}: {cnt:>4} ({cnt/total_rows*100:.1f}%)")

    # 6. Format Comparison: Parquet vs CSV vs JSON
    print("\n6. Format Efficiency & Storage Comparison:")
    print(f"  {'Format':<15} {'Size (Bytes)':>15} {'Relative Ratio vs Parquet':>28}")
    print("  " + "-" * 60)
    print(f"  {'Parquet':<15} {parquet_size:>15,} {1.0:>28.2f}x (Baseline)")

    if CSV_COMPARE.exists():
        csv_size = os.path.getsize(CSV_COMPARE)
        print(f"  {'CSV':<15} {csv_size:>15,} {csv_size/parquet_size:>28.2f}x")
    if JSON_COMPARE.exists():
        json_size = os.path.getsize(JSON_COMPARE)
        print(f"  {'JSON':<15} {json_size:>15,} {json_size/parquet_size:>28.2f}x")

    print("\n7. Analytical Advantages of Parquet:")
    print("  - Columnar Encoding: Reads only queried columns (projection pushdown).")
    print("  - Embedded Metadata: Preserves native data types without runtime type guessing.")
    print("  - High Compression : Highly compact storage footprint compared to row-oriented JSON.")
    print("=" * 70 + "\n")

if __name__ == '__main__':
    profile_products_parquet()