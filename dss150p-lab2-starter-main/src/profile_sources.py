"""Week 2 Master Source Profiler.

Orchestrates complete profiling across all five lab data sources:
1. CSV (customers.csv)
2. JSON (orders.json)
3. Parquet (products.parquet) + format comparison vs CSV/JSON
4. REST API (/api/events)
5. PostgreSQL (support_tickets table / seed)

Run: python src/profile_sources.py
"""
from pathlib import Path
import sys

# Ensure src/ is on path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from profile_csv import profile_customers_csv
from profile_json import profile_orders_json
from profile_parquet import profile_products_parquet
from profile_api import profile_api_events
from profile_postgres import profile_support_tickets

def main():
    print("#" * 70)
    print(" DSS150P LAB 2: MASTER SOURCE PROFILING SUITE")
    print("#" * 70)

    # 1. Profile CSV
    profile_customers_csv()

    # 2. Profile JSON
    profile_orders_json()

    # 3. Profile Parquet
    profile_products_parquet()

    # 4. Profile PostgreSQL Support Tickets
    profile_support_tickets()

    # 5. Profile REST API (handles offline gracefully)
    print("Checking REST API availability...")
    api_success = profile_api_events()
    if not api_success:
        print("[INFO] REST API server is currently offline.")
        print("       Start it using: python src/local_api_server.py to profile live endpoints.")

    print("\n" + "#" * 70)
    print(" MASTER PROFILING COMPLETED")
    print("#" * 70)

if __name__ == '__main__':
    main()
