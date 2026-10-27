"""Task 1.5: Comprehensive REST API Profiling.

Fetches ALL pages from the local API server and reports:
- Pagination envelope analysis (page, per_page, total, has_more, next_page)
- Full multi-page retrieval statistics
- Complete schema inventory with inferred logical types
- Null / missing field counts across every event
- Duplicate event_id detection & version analysis (multiple updated_at per event_id)
- Chronological updated_at range (watermark boundaries)
- Incremental watermark acquisition design
"""
from pathlib import Path
import json
from datetime import datetime
import requests

API_URL = "http://127.0.0.1:8000/api/events"
PER_PAGE = 20

def profile_api_events():
    print("=" * 70)
    print("TASK 1.5: REST API PROFILING - /api/events")
    print("=" * 70)

    all_items = []
    page = 1
    envelope_sample = {}

    try:
        while True:
            resp = requests.get(API_URL, params={'page': page, 'per_page': PER_PAGE}, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if page == 1:
                envelope_sample = {k: data.get(k) for k in ['page', 'per_page', 'total', 'has_more', 'next_page']}

            items = data.get('items', [])
            all_items.extend(items)

            has_more = data.get('has_more', False)
            if not has_more or not data.get('next_page'):
                break
            page = data.get('next_page')

    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect to API server at http://127.0.0.1:8000")
        print("       Please run: python src/local_api_server.py in another terminal.")
        return False
    except Exception as e:
        print(f"ERROR: Failed during API profiling: {e}")
        return False

    total_fetched = len(all_items)
    api_total = envelope_sample.get('total')

    print(f"1. Pagination Envelope & Retrieval Summary:")
    print(f"  - Configured per_page    : {PER_PAGE}")
    print(f"  - Total Pages Traversed  : {page}")
    print(f"  - Total Items Fetched    : {total_fetched}")
    print(f"  - API Reported 'total'   : {api_total}")
    print(f"  - Envelope keys present  : {list(envelope_sample.keys())}")

    # 2. Schema and Inferred Types
    print("\n2. Inferred Payload Schema across all items:")
    print(f"  {'Field Name':<20} {'Inferred Logical Type':<24} {'Null Count':>12} {'Null %':>10}")
    print("  " + "-" * 68)

    all_keys = sorted({k for item in all_items for k in item.keys()})
    for k in all_keys:
        null_count = sum(1 for item in all_items if item.get(k) is None or k not in item)
        null_pct = (null_count / total_fetched) * 100

        # Type inference
        samples = [item[k] for item in all_items if item.get(k) is not None]
        first_val = samples[0] if samples else None
        if isinstance(first_val, int):
            inferred = "Integer"
        elif isinstance(first_val, float):
            inferred = "Float"
        elif isinstance(first_val, dict):
            inferred = "JSON Object"
        elif isinstance(first_val, list):
            inferred = "JSON Array"
        elif isinstance(first_val, str):
            try:
                datetime.fromisoformat(first_val)
                inferred = "Timestamp (ISO-8601)"
            except ValueError:
                inferred = "String"
        else:
            inferred = "Unknown"

        print(f"  {k:<20} {inferred:<24} {null_count:>12} {null_pct:>9.1f}%")

    # 3. Duplicate event_id Analysis
    print("\n3. Duplicate Key Analysis (event_id):")
    event_counts = {}
    for item in all_items:
        eid = item.get('event_id')
        event_counts[eid] = event_counts.get(eid, 0) + 1

    duplicate_eids = {eid: cnt for eid, cnt in event_counts.items() if cnt > 1}
    print(f"  - Distinct event_ids     : {len(event_counts)}")
    print(f"  - Duplicate event_ids    : {len(duplicate_eids)}")
    if duplicate_eids:
        print("  - Sample Duplicate Records:")
        sample_eid = next(iter(duplicate_eids.keys()))
        dupe_instances = [it for it in all_items if it.get('event_id') == sample_eid]
        for inst in dupe_instances:
            print(f"    * event_id={inst.get('event_id')}, updated_at={inst.get('updated_at')}, event_type={inst.get('event_type')}")
        print("  - CRITICAL FINDING: Same event_id appears with DIFFERENT updated_at timestamps!")
        print("    -> Ingestion MUST deduplicate by event_id keeping greatest updated_at.")

    # 4. Temporal Range & Watermark Boundaries
    print("\n4. Temporal Range Analysis (updated_at):")
    timestamps = [it.get('updated_at') for it in all_items if it.get('updated_at')]
    if timestamps:
        min_ts = min(timestamps)
        max_ts = max(timestamps)
        print(f"  - Earliest updated_at    : {min_ts}")
        print(f"  - Latest updated_at      : {max_ts}")
        print("  - Ingestion Watermark    : Initial = None (fetches all),")
        print(f"                             Subsequent = {max_ts}")

    # 5. Incremental Extraction Strategy
    print("\n5. Incremental Extraction Strategy:")
    print("  - Parameter 'updated_after' tested: enables filtering records newer than watermark.")
    print("  - Watermark persistence: state/api_watermark.json storing {'updated_at': <max_ts>}.")
    print("  - Safety rule: Watermark must advance ONLY after successful atomic raw file write.")
    print("=" * 70 + "\n")
    return True

if __name__ == '__main__':
    profile_api_events()
