"""Starter validation checks for raw outputs."""
from pathlib import Path
from datetime import datetime
import json

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'raw'
STATE = ROOT / 'state'

def main():
    print("Starting raw data validation checks...")

    # 1. Assert expected raw files exist
    manifest_path = RAW / 'manifest.json'
    api_events_path = RAW / 'api' / 'events.jsonl'
    watermark_path = STATE / 'api_watermark.json'

    assert manifest_path.exists(), f"Missing manifest file at {manifest_path}"
    assert api_events_path.exists(), f"Missing API events file at {api_events_path}"
    assert watermark_path.exists(), f"Missing watermark file at {watermark_path}"
    print(" [PASS] Expected raw files exist.")

    # Load API events from JSONL
    api_events = []
    with api_events_path.open('r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                api_events.append(json.loads(line))

    # 2. Assert API event_ids are unique
    event_ids = [e['event_id'] for e in api_events]
    assert len(event_ids) == len(set(event_ids)), "Duplicate event_ids found in raw API events!"
    print(" [PASS] API event_ids are unique.")

    # 3. Assert required metadata fields present
    for e in api_events:
        assert '_ingested_at' in e, f"Missing _ingested_at in event {e.get('event_id')}"
        assert '_source' in e, f"Missing _source in event {e.get('event_id')}"
        assert e['_source'] == 'local_api', f"Unexpected source value: {e['_source']}"
    print(" [PASS] Required ingestion metadata fields present.")

    # 4. Assert updated_at is parseable as a timestamp
    for e in api_events:
        try:
            datetime.fromisoformat(e['updated_at'])
        except ValueError as err:
            raise AssertionError(f"Invalid timestamp format for updated_at in event {e.get('event_id')}: {err}")
    print(" [PASS] All event timestamps are parseable.")

    # 5. Assert watermark equals max updated_at after ingestion
    watermark_data = json.loads(watermark_path.read_text())
    saved_watermark = watermark_data.get('updated_at')
    max_updated_at = max(e['updated_at'] for e in api_events)
    
    assert saved_watermark == max_updated_at, f"Watermark mismatch! Saved: {saved_watermark}, Max updated_at: {max_updated_at}"
    print(f" [PASS] Watermark matches max updated_at ({saved_watermark}).")

    print("\nAll raw validation checks passed successfully!")

if __name__ == '__main__':
    main()